# App.py — Qdrant + E5 embeddings (instruction-tuned, L2-normalized)
import os
import uuid
import pymysql
import numpy as np
from math import radians, cos, sin, asin, sqrt
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.http.models import (
    VectorParams, Distance, Filter, FieldCondition, Range, MatchValue
)

# =========================
# Load environment
# =========================
print("[BOOT] Loading environment variables...")
load_dotenv()
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
print("[OK] Environment variables loaded.")

# =========================
# MySQL
# =========================
print("[BOOT] Setting up MySQL connection parameters...")
MYSQL = dict(
    host=os.getenv("MYSQL_HOST", "127.0.0.1"),
    port=int(os.getenv("MYSQL_PORT", "3306")),
    user=os.getenv("MYSQL_USER", "root"),
    password=os.getenv("MYSQL_PASSWORD", ""),
    database=os.getenv("MYSQL_DATABASE", "every_circle"),
    cursorclass=pymysql.cursors.Cursor,  # tuple rows (we zip to dict)
)
print("[OK] MySQL config ready.")

# =========================
# Embedding Model (UPGRADED)
# =========================
# Use an instruction-tuned retrieval model with query/passages prefixes.
# Strong choices: "intfloat/e5-large-v2" (1024-d) or "intfloat/e5-small-v2" (384-d)
MODEL_NAME = os.getenv("MODEL_NAME", "intfloat/e5-large-v2")
EMBED_DIM = int(os.getenv("EMBED_DIM", "1024"))  # 1024 for e5-large-v2, 384 for e5-small-v2
TOP_K = int(os.getenv("TOP_K", "5"))

# Qdrant config
QDRANT_HOST = os.getenv("QDRANT_HOST", "localhost")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))
QDRANT_COLLECTION = os.getenv("QDRANT_COLLECTION", "businesses")

print(f"[BOOT] Config: MODEL_NAME={MODEL_NAME}, EMBED_DIM={EMBED_DIM}, TOP_K={TOP_K}")
print("[BOOT] Loading embedding model...")
model = SentenceTransformer(MODEL_NAME)
print("[OK] Model loaded successfully.")

def l2norm(v: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(v)
    return v / (n + 1e-12)

def encode_query(text: str) -> list:
    # E5 expects instruction prefix "query: "
    v = model.encode(f"query: {text}", normalize_embeddings=False)
    return l2norm(np.asarray(v)).tolist()

def encode_doc(name: str, tagline: str, bio: str) -> list:
    # E5 expects instruction prefix "passage: "
    text = " ".join([str(name or ""), str(tagline or ""), str(bio or "")]).strip()
    v = model.encode(f"passage: {text}", normalize_embeddings=False)
    return l2norm(np.asarray(v)).tolist()

# =========================
# Business attributes used
# =========================
ATTRIBUTES = [
    "business_uid",
    "business_name",
    "business_tag_line",
    "business_short_bio",
    "business_address_line_1",
    "business_city",
    "business_state",
    "business_country",
    "business_google_rating",
    "business_price_level",
    "business_latitude",
    "business_longitude",
]

TEXT_FIELDS_FOR_EMBEDDING = [
    "business_name",
    "business_tag_line",
    "business_short_bio",
]

# =========================
# Flask
# =========================
print("[BOOT] Initializing Flask...")
app = Flask(__name__)
print("[OK] Flask app initialized.")

# =========================
# DB helper
# =========================
def get_db_connection():
    print("[STEP] Connecting to MySQL...")
    return pymysql.connect(**MYSQL)

# =========================
# Distance helper
# =========================
def haversine_miles(lat1, lon1, lat2, lon2):
    """Great-circle distance between two lat/lon points (miles)."""
    lat1, lon1, lat2, lon2 = map(radians, [float(lat1), float(lon1), float(lat2), float(lon2)])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat/2.0)**2 + cos(lat1) * cos(lat2) * sin(dlon/2.0)**2
    c = 2.0 * asin(sqrt(a))
    return 3959.0 * c  # Earth radius in miles

# =========================
# Qdrant
# =========================
print("[BOOT] Connecting to Qdrant...")
qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

def recreate_collection():
    print("[STEP] (Re)creating Qdrant collection...")
    if qdrant.collection_exists(QDRANT_COLLECTION):
        qdrant.delete_collection(QDRANT_COLLECTION)
    qdrant.create_collection(
        collection_name=QDRANT_COLLECTION,
        vectors_config=VectorParams(size=EMBED_DIM, distance=Distance.COSINE),
    )
    print("[OK] Collection ready.")

def initial_load():
    print("[STEP] Loading businesses from MySQL into Qdrant...")
    conn = get_db_connection()
    with conn.cursor() as cursor:
        cursor.execute(f"SELECT {', '.join(ATTRIBUTES)} FROM business")
        rows = cursor.fetchall()
    conn.close()
    print(f"[OK] MySQL returned {len(rows)} rows for indexing.")

    # Build points
    points = []
    for row in rows:
        doc = dict(zip(ATTRIBUTES, row))

        name = doc.get("business_name")
        tagline = doc.get("business_tag_line")
        bio = doc.get("business_short_bio")

        vec = encode_doc(name, tagline, bio)

        # Deterministic UUID from business_uid (Qdrant IDs must be uint or UUID)
        point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, str(doc.get("business_uid"))))

        points.append({"id": point_id, "vector": vec, "payload": doc})

    # Upsert (batching optional; small dataset can go as one)
    if points:
        qdrant.upsert(collection_name=QDRANT_COLLECTION, points=points)
    print(f"[OK] Inserted/updated {len(points)} docs in Qdrant.")

# =========================
# Routes
# =========================
@app.route("/search", methods=["GET"])
def search():
    print("[ROUTE] /search called.")

    query = request.args.get("q", "") or ""
    city_filter = request.args.get("city")
    min_rating = request.args.get("min_rating", type=float)
    user_lat = request.args.get("lat", type=float)
    user_lon = request.args.get("lon", type=float)
    radius_miles = request.args.get("radius_miles", default=5.0, type=float)

    print(f"[DEBUG] Params: q='{query}', city='{city_filter}', min_rating={min_rating}, "
          f"lat={user_lat}, lon={user_lon}, radius={radius_miles}")

    # Encode query using upgraded model (E5) with instruction prefix + L2 norm
    print("[STEP] Encoding query...")
    query_vector = encode_query(query)
    print("[OK] Query vector ready.")

    # Build Qdrant filter from city/rating (others remain in Python, identical behavior)
    conditions = []
    if city_filter:
        conditions.append(FieldCondition(key="business_city", match=MatchValue(value=city_filter)))
        print(f"[DEBUG] City filter applied: {city_filter}")
    if min_rating is not None:
        conditions.append(FieldCondition(key="business_google_rating", range=Range(gte=min_rating)))
        print(f"[DEBUG] Rating filter applied: >= {min_rating}")

    q_filter = Filter(must=conditions) if conditions else None

    # Search in Qdrant (fetch extra to allow geo pruning)
    fetch_limit = TOP_K * 5
    print(f"[STEP] Qdrant search: limit={fetch_limit}")
    results = qdrant.search(
        collection_name=QDRANT_COLLECTION,
        query_vector=query_vector,
        query_filter=q_filter,
        limit=fetch_limit
    )
    print(f"[OK] Qdrant returned {len(results)} candidates.")

    # Build docs + optional geo filter
    docs = []
    for res in results:
        doc = res.payload
        doc["score"] = float(res.score)

        if user_lat is not None and user_lon is not None:
            try:
                b_lat = float(doc.get("business_latitude"))
                b_lon = float(doc.get("business_longitude"))
                distance = haversine_miles(user_lat, user_lon, b_lat, b_lon)
                doc["distance_miles"] = round(distance, 2)
                if distance > radius_miles:
                    # Outside radius: skip
                    continue
            except Exception as e:
                print(f"[WARN] Skipping doc due to invalid coords: {e}")
                continue
        else:
            doc["distance_miles"] = None

        docs.append(doc)

    print(f"[OK] After geo filtering, {len(docs)} docs remain.")

    # Sorting (unchanged): by distance first (if geo given), then by score desc
    print("[STEP] Sorting results...")
    if user_lat is not None and user_lon is not None:
        docs.sort(key=lambda x: (x["distance_miles"], -x["score"]))
    else:
        docs.sort(key=lambda x: -x["score"])

    print("[OK] Returning top-k results.")
    return jsonify(docs[:TOP_K])

# =========================
# Run
# =========================
if __name__ == "__main__":
    print("[BOOT] Preparing Qdrant collection and indexing...")
    recreate_collection()
    initial_load()
    print("[BOOT] Starting Flask app on 0.0.0.0:5001 ...")
    app.run(host="0.0.0.0", port=5001, debug=True, use_reloader=False)
