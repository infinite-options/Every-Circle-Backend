import os
import pymysql
import json
import re
import uuid
from flask import Flask, request, jsonify
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance
from openai import OpenAI

# -------------------------
# Load environment
# -------------------------
load_dotenv()
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ["HF_HOME"] = "/home/ec2-user/.cache/huggingface"

# -------------------------
# MySQL config (RDS)
# -------------------------
MYSQL = dict(
    host=os.environ["RDS_HOST"],
    port=int(os.environ["RDS_PORT"]),
    user=os.environ["RDS_USER"],
    password=os.environ["RDS_PW"],
    database=os.environ["RDS_DB"]
)

# -------------------------
# Config
# -------------------------
MODEL_NAME = os.getenv("MODEL_NAME", "sentence-transformers/paraphrase-MiniLM-L6-v2")
EMBED_DIM = int(os.getenv("EMBED_DIM", "1024"))
TOP_K = int(os.getenv("TOP_K", "5"))
QDRANT_HOST = os.getenv("QDRANT_HOST", "127.0.0.1")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o")
client = OpenAI(api_key=OPENAI_API_KEY)

# -------------------------
# Setup
# -------------------------
app = Flask(__name__)
embedder = SentenceTransformer(MODEL_NAME)
qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

# -------------------------
# Helpers
# -------------------------
def embed_text(text: str):
    return embedder.encode(text).tolist()

def make_uuid(value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(value)))

def mysql_connect():
    return pymysql.connect(**MYSQL)

# -------------------------
# Ensure collections exist
# -------------------------
def ensure_collections():
    if not qdrant.collection_exists("businesses"):
        qdrant.create_collection(
            collection_name="businesses",
            vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE)
        )
    print("✅ Collection 'businesses' ready.")

# -------------------------
# Sync Logic (Incremental per request)
# -------------------------
def sync_businesses(biz_map):
    """Re-query MySQL on each call, compare to in-memory map."""
    conn = mysql_connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT business_uid, updated_at FROM business")
    rows = cur.fetchall()
    conn.close()

    current_state = {r["business_uid"]: str(r["updated_at"]) for r in rows}
    all_uids = set(biz_map.keys()) | set(current_state.keys())

    inserted, updated, deleted = [], [], []

    for uid in all_uids:
        if uid not in current_state:
            qdrant.delete(collection_name="businesses", points_selector=[make_uuid(uid)])
            biz_map.pop(uid, None)
            deleted.append(uid)
            print(f"🗑️ Deleted business {uid}")
        elif uid not in biz_map:
            print(f"🆕 New business found: {uid}")
            upsert_business(uid)
            biz_map[uid] = current_state[uid]
            inserted.append(uid)
        elif biz_map[uid] != current_state[uid]:
            print(f"🔁 Business updated: {uid}")
            upsert_business(uid)
            biz_map[uid] = current_state[uid]
            updated.append(uid)

    return biz_map, {"inserted": inserted, "updated": updated, "deleted": deleted}

# -------------------------
# Upsert Business to Qdrant
# -------------------------
def upsert_business(uid):
    conn = mysql_connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("""
        SELECT business_uid, business_name, business_short_bio, business_tag_line,
               business_city, business_state, business_country, business_google_rating, updated_at
        FROM business WHERE business_uid=%s
    """, (uid,))
    row = cur.fetchone()
    conn.close()
    if not row:
        print(f"⚠️ Business {uid} not found in DB.")
        return

    text = f"{row['business_name']} - {row.get('business_short_bio') or ''} - {row.get('business_tag_line') or ''}"
    tags = [t.strip().lower() for t in (row.get('business_tag_line') or '').split(',') if t.strip()]
    payload = row | {"tags": tags}

    try:
        payload["business_google_rating"] = float(payload.get("business_google_rating", 0.0))
    except (TypeError, ValueError):
        payload["business_google_rating"] = 0.0

    qdrant.upsert(
        collection_name="businesses",
        points=[PointStruct(id=make_uuid(uid), vector=embed_text(text), payload=payload)]
    )
    print(f"✅ Upserted business: {row['business_name']}")

# -------------------------
# Search Endpoint (Verbose)
# -------------------------
@app.route("/search_business", methods=["GET"])
def search_business():
    global biz_map
    biz_map, changes = sync_businesses(biz_map)

    query = request.args.get("q", "")
    tag = request.args.get("tag", "")
    location = request.args.get("location", "")
    min_rating = request.args.get("min_rating", "")
    limit = int(request.args.get("limit", TOP_K))

    vector = embed_text(query) if query else None
    must_filters = []

    if tag:
        must_filters.append({"key": "tags", "match": {"value": tag.lower()}})
    if location:
        must_filters.append({"key": "business_city", "match": {"value": location}})
    if min_rating:
        try:
            must_filters.append({"key": "business_google_rating", "range": {"gte": float(min_rating)}})
        except ValueError:
            pass

    query_filter = {"must": must_filters} if must_filters else None

    results_payload = []
    if vector:
        results = qdrant.search("businesses", query_vector=vector, query_filter=query_filter, limit=limit)
        results_payload = [{"score": r.score, **(r.payload or {})} for r in results]
    elif must_filters:
        results, _ = qdrant.scroll("businesses", scroll_filter=query_filter, limit=limit)
        results_payload = [r.payload for r in results]
    else:
        return jsonify({"error": "Must provide q, tag, or filter"}), 400

    return jsonify({
        "sync_summary": changes,
        "results": results_payload
    })

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    ensure_collections()
    biz_map = {}
    print("🔄 Building initial state map...")
    conn = mysql_connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT business_uid, updated_at FROM business")
    rows = cur.fetchall()
    conn.close()
    biz_map = {r["business_uid"]: str(r["updated_at"]) for r in rows}
    print(f"✅ Initial state map contains {len(biz_map)} businesses. Ready for incremental updates.")
    app.run(host="0.0.0.0", port=5001)
