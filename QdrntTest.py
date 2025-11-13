import os
import pymysql
import uuid
from flask import Flask, request, jsonify
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
from qdrant_client import QdrantClient
from qdrant_client.models import PointStruct, VectorParams, Distance

# ---------------------------------------------------------
# Environment Setup
# ---------------------------------------------------------
load_dotenv()
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
os.environ["HF_HOME"] = "/home/ec2-user/.cache/huggingface"

MYSQL = dict(
    host=os.environ["RDS_HOST"],
    port=int(os.environ["RDS_PORT"]),
    user=os.environ["RDS_USER"],
    password=os.environ["RDS_PW"],
    database=os.environ["RDS_DB"]
)

MODEL_NAME = "sentence-transformers/paraphrase-MiniLM-L6-v2"
EMBED_DIM = 384
TOP_K = 99999
QDRANT_HOST = os.getenv("QDRANT_HOST", "127.0.0.1")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

app = Flask(__name__)
embedder = SentenceTransformer(MODEL_NAME)
qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------
def embed_text(text: str):
    return embedder.encode(text).tolist()

def make_uuid(value: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, str(value)))

def mysql_connect():
    return pymysql.connect(**MYSQL)

# ---------------------------------------------------------
# Qdrant Insert Verification
# ---------------------------------------------------------
def verify_qdrant_insert(collection, uid_key, uid_value):
    try:
        points, _ = qdrant.scroll(
            collection_name=collection,
            scroll_filter={"must": [{"key": uid_key, "match": {"value": uid_value}}]},
            limit=1
        )
        return len(points) > 0
    except:
        return False

# ---------------------------------------------------------
# Collection Creation
# ---------------------------------------------------------
def ensure_collections():
    for col in ["businesses", "wishes", "expertise"]:
        if not qdrant.collection_exists(col):
            print(f"🆕 Creating Qdrant collection '{col}'...")
            qdrant.create_collection(
                collection_name=col,
                vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE),
            )
        print(f"✅ Collection '{col}' ready.")

# ---------------------------------------------------------
# BUSINESS SYNC (Insert / Update / Delete)
# ---------------------------------------------------------
def sync_businesses(biz_map):
    print("\n==============================")
    print("📦 SYNCING BUSINESSES")
    print("==============================")

    conn = mysql_connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("""
        SELECT business_uid, business_name, business_short_bio, business_tag_line,
               business_city, business_state, business_country,
               business_google_rating, updated_at
        FROM business
    """)
    rows = cur.fetchall()
    conn.close()

    current_state = {r["business_uid"]: str(r["updated_at"]) for r in rows}

    # INSERT + UPDATE
    for row in rows:
        uid = row["business_uid"]

        is_new = uid not in biz_map
        is_updated = not is_new and biz_map[uid] != current_state[uid]

        if is_new:
            print(f"🆕 New business detected: {uid}")
        elif is_updated:
            print(f"🔄 Updated business detected: {uid}")

        if is_new or is_updated:
            upsert_business(row)
            success = verify_qdrant_insert("businesses", "business_uid", uid)
            print(("✔" if success else "❌") + f" {uid} — {row['business_name']}")

        biz_map[uid] = current_state[uid]

    # DELETE
    for old_uid in list(biz_map.keys()):
        if old_uid not in current_state:
            print(f"🗑 Removing business: {old_uid}")
            qdrant.delete("businesses", points_selector=[make_uuid(old_uid)])
            biz_map.pop(old_uid, None)

    return biz_map


def upsert_business(row):
    uid = row["business_uid"]
    text = f"{row['business_name']} - {row.get('business_short_bio') or ''} - {row.get('business_tag_line') or ''}"
    tags = [t.strip().lower() for t in (row.get("business_tag_line") or "").split(",") if t.strip()]

    payload = row | {"tags": tags}

    qdrant.upsert(
        collection_name="businesses",
        points=[PointStruct(id=make_uuid(uid), vector=embed_text(text), payload=payload)]
    )

# ---------------------------------------------------------
# WISHES SYNC (Insert / Update / Delete)
# ---------------------------------------------------------
def sync_wishes(wish_map):
    print("\n==============================")
    print("💫 SYNCING WISHES")
    print("==============================")

    conn = mysql_connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("""
        SELECT profile_wish_uid, profile_wish_title, profile_wish_description, updated_at
        FROM profile_wish
    """)
    rows = cur.fetchall()
    conn.close()

    current_state = {r["profile_wish_uid"]: str(r["updated_at"]) for r in rows}

    # INSERT + UPDATE
    for row in rows:
        uid = row["profile_wish_uid"]

        is_new = uid not in wish_map
        is_updated = not is_new and wish_map[uid] != current_state[uid]

        if is_new:
            print(f"🆕 New wish detected: {uid}")
        elif is_updated:
            print(f"🔄 Updated wish detected: {uid}")

        if is_new or is_updated:
            upsert_wish(row)
            success = verify_qdrant_insert("wishes", "profile_wish_uid", uid)
            print(("✔" if success else "❌") + f" {uid} — {row['profile_wish_title']}")

        wish_map[uid] = current_state[uid]

    # DELETE
    for old_uid in list(wish_map.keys()):
        if old_uid not in current_state:
            print(f"🗑 Removing wish: {old_uid}")
            qdrant.delete("wishes", points_selector=[make_uuid(old_uid)])
            wish_map.pop(old_uid, None)

    return wish_map


def upsert_wish(row):
    uid = row["profile_wish_uid"]
    text = f"{row['profile_wish_title']} - {row.get('profile_wish_description') or ''}"
    qdrant.upsert(
        collection_name="wishes",
        points=[PointStruct(id=make_uuid(uid), vector=embed_text(text), payload=row)]
    )

# ---------------------------------------------------------
# EXPERTISE SYNC (Insert / Update / Delete)
# ---------------------------------------------------------
def sync_expertise(exp_map):
    print("\n==============================")
    print("🎓 SYNCING EXPERTISE")
    print("==============================")

    conn = mysql_connect()
    cur = conn.cursor(pymysql.cursors.DictCursor)
    cur.execute("""
        SELECT profile_expertise_uid, profile_expertise_title,
               profile_expertise_description, updated_at
        FROM profile_expertise
    """)
    rows = cur.fetchall()
    conn.close()

    current_state = {r["profile_expertise_uid"]: str(r["updated_at"]) for r in rows}

    # INSERT + UPDATE
    for row in rows:
        uid = row["profile_expertise_uid"]
        is_new = uid not in exp_map
        is_updated = not is_new and exp_map[uid] != current_state[uid]

        if is_new:
            print(f"🆕 New expertise detected: {uid}")
        elif is_updated:
            print(f"🔄 Updated expertise detected: {uid}")

        if is_new or is_updated:
            upsert_expertise(row)
            success = verify_qdrant_insert("expertise", "profile_expertise_uid", uid)
            print(("✔" if success else "❌") + f" {uid} — {row['profile_expertise_title']}")

        exp_map[uid] = current_state[uid]

    # DELETE
    for old_uid in list(exp_map.keys()):
        if old_uid not in current_state:
            print(f"🗑 Removing expertise: {old_uid}")
            qdrant.delete("expertise", points_selector=[make_uuid(old_uid)])
            exp_map.pop(old_uid, None)

    return exp_map


def upsert_expertise(row):
    uid = row["profile_expertise_uid"]
    text = f"{row['profile_expertise_title']} - {row.get('profile_expertise_description') or ''}"
    qdrant.upsert(
        collection_name="expertise",
        points=[PointStruct(id=make_uuid(uid), vector=embed_text(text), payload=row)]
    )

# ---------------------------------------------------------
# SEARCH ENDPOINTS
# ---------------------------------------------------------
@app.route("/search_business", methods=["GET"])
def search_business():
    global biz_map
    biz_map = sync_businesses(biz_map)
    query = request.args.get("q", "")
    vector = embed_text(query)
    results = qdrant.search("businesses", query_vector=vector, limit=99999)
    return jsonify([{"score": r.score, **r.payload} for r in results])

@app.route("/search_wishes", methods=["GET"])
def search_wishes():
    global wish_map
    wish_map = sync_wishes(wish_map)
    query = request.args.get("q", "")
    vector = embed_text(query)
    results = qdrant.search("wishes", query_vector=vector, limit=99999)
    return jsonify([{"score": r.score, **r.payload} for r in results])

@app.route("/search_expertise", methods=["GET"])
def search_expertise():
    global exp_map
    exp_map = sync_expertise(exp_map)
    query = request.args.get("q", "")
    vector = embed_text(query)
    results = qdrant.search("expertise", query_vector=vector, limit=99999)
    return jsonify([{"score": r.score, **r.payload} for r in results])

# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    ensure_collections()

    global biz_map, wish_map, exp_map
    biz_map = {}
    wish_map = {}
    exp_map = {}

    print("\n🚀 INITIAL SYNC STARTING NOW...")
    biz_map = sync_businesses(biz_map)
    wish_map = sync_wishes(wish_map)
    exp_map = sync_expertise(exp_map)

    print("\n🚀 Live sync enabled for: businesses, wishes, expertise\n")
    app.run(host="0.0.0.0", port=5001)
