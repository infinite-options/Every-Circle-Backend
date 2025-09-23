# QdrntTest.py — Infinite Options RDS + Qdrant + OpenAI Tags (UUID-safe IDs, with business tags)

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
TOP_K = int(os.getenv("TOP_K", "5")) # TODO check top_k

QDRANT_HOST = os.getenv("QDRANT_HOST", "127.0.0.1")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

# OpenAI
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
    """Deterministically convert a string (like business_uid) into a valid UUID."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(value)))

# -------------------------
# Recreate collections
# -------------------------
def recreate_collections():
    for col in ["wishes", "expertise", "businesses"]:
        qdrant.recreate_collection(
            collection_name=col,
            vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE)
        )
    print("✅ Qdrant collections recreated.")

# -------------------------
# Sync tables → Qdrant
# -------------------------
def sync_wishes():
    conn = pymysql.connect(**MYSQL)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT profile_wish_uid, profile_wish_title, profile_wish_description FROM profile_wish")
    rows = cur.fetchall()
    points = []
    for row in rows:
        text = f"{row['profile_wish_title']} - {row['profile_wish_description'] or ''}"
        points.append(PointStruct(
            id=make_uuid(row['profile_wish_uid']),
            vector=embed_text(text),
            payload=row
        ))
        print(f"Inserted wish: {row['profile_wish_title']}")
    qdrant.upsert(collection_name="wishes", points=points)
    conn.close()
    print(f"✅ Synced {len(points)} wishes.")

def sync_expertise():
    conn = pymysql.connect(**MYSQL)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT profile_expertise_uid, profile_expertise_title, profile_expertise_description FROM profile_expertise")
    rows = cur.fetchall()
    points = []
    for row in rows:
        text = f"{row['profile_expertise_title']} - {row['profile_expertise_description'] or ''}"
        points.append(PointStruct(
            id=make_uuid(row['profile_expertise_uid']),
            vector=embed_text(text),
            payload=row
        ))
        print(f"Inserted expertise: {row['profile_expertise_title']}")
    qdrant.upsert(collection_name="expertise", points=points)
    conn.close()
    print(f"✅ Synced {len(points)} expertise entries.")

def sync_businesses():
    conn = pymysql.connect(**MYSQL)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("SELECT business_uid, business_name, business_short_bio, business_tag_line FROM business")
    rows = cur.fetchall()
    points = []
    for row in rows:
        # Embed name + bio + tag line (onion layering)
        text = f"{row['business_name']} - {row['business_short_bio'] or ''} - {row['business_tag_line'] or ''}"
        
        # Still keep tags in payload for filtering if needed later
        tags = [t.strip().lower() for t in (row.get("business_tag_line") or "").split(",") if t.strip()]
        payload = row | {"tags": tags}
        
        points.append(PointStruct(
            id=make_uuid(row['business_uid']),
            vector=embed_text(text),
            payload=payload
        ))
        print(f"Inserted business: {row['business_name']} with tags {tags}")
    qdrant.upsert(collection_name="businesses", points=points)
    conn.close()
    print(f"✅ Synced {len(points)} businesses.")

# -------------------------
# Search Endpoints
# -------------------------
@app.route("/search_business", methods=["GET"])
def search_business():
    query = request.args.get("q", "")
    tag = request.args.get("tag", "")
    limit = int(request.args.get("limit", TOP_K))

    vector = embed_text(query) if query else None

    scroll_filter = None
    if tag:
        scroll_filter = {
            "must": [
                {"key": "tags", "match": {"value": tag.lower()}}
            ]
        }

    if vector:  # semantic + optional tag filter
        results = qdrant.search(
            collection_name="businesses",
            query_vector=vector,
            query_filter=scroll_filter,
            limit=limit
        )
        return jsonify([{"score": r.score, **(r.payload or {})} for r in results])

    elif tag:  # pure tag search
        results, _ = qdrant.scroll(
            collection_name="businesses",
            scroll_filter=scroll_filter,
            limit=limit
        )
        return jsonify([r.payload for r in results])

    else:
        return jsonify({"error": "Must provide either q or tag"}), 400

@app.route("/search_wishes", methods=["GET"])
def search_wishes_api():
    query = request.args.get("q", "")
    if not query:
        return jsonify({"error": "Missing query parameter"}), 400

    vector = embed_text(query)
    results = qdrant.search(collection_name="wishes", query_vector=vector, limit=TOP_K)

    return jsonify([{"score": r.score, **(r.payload or {})} for r in results])

@app.route("/search_expertise", methods=["GET"])
def search_expertise_api():
    query = request.args.get("q", "")
    if not query:
        return jsonify({"error": "Missing query parameter"}), 400

    vector = embed_text(query)
    results = qdrant.search(collection_name="expertise", query_vector=vector, limit=TOP_K)

    return jsonify([{"score": r.score, **(r.payload or {})} for r in results])

# -------------------------
# AI Tag Generation
# -------------------------
def generate_tags(name, description, max_tags=10):
    prompt = f"""
You are a tagging assistant. Generate concise, single-word search tags.

Rules:
- ONE WORD PER TAG
- lowercase only
- Prefer common search terms
- Avoid repeating the name
- Return ONLY a JSON array of 5–10 strings

Name: {name}
Description: {description}
"""
    resp = client.chat.completions.create(
        model=OPENAI_MODEL,
        messages=[
            {"role": "system", "content": "You generate practical, single-word search tags."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.3
    )
    content = (resp.choices[0].message.content or "").strip()

    try:
        tags = json.loads(content)
        if not isinstance(tags, list):
            tags = [str(tags)]
    except Exception:
        tags = [t.strip() for t in re.split(r"[,;\n]+", content) if t.strip()]

    return tags[:max_tags]

@app.route("/generate_tags", methods=["POST"])
def api_generate_tags():
    data = request.json or {}
    name = data.get("name", "")
    description = data.get("description", "")
    max_tags = int(data.get("max_tags", 10))

    if not description and not name:
        return jsonify({"error": "Name or description required"}), 400

    tags = generate_tags(name, description, max_tags=max_tags)
    return jsonify({"tags": tags})

# -------------------------
# Main
# -------------------------
if __name__ == "__main__":
    recreate_collections()
    sync_wishes()
    sync_expertise()
    sync_businesses()
    app.run(host="0.0.0.0", port=5001, debug=True)
