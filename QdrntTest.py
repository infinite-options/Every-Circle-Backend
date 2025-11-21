import os
import pymysql
import uuid
import math
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

# Chose this model because it fits on ec2, might consider upgrading for better search
MODEL_NAME = "sentence-transformers/paraphrase-MiniLM-L6-v2" 
EMBED_DIM = 384
QDRANT_HOST = os.getenv("QDRANT_HOST", "127.0.0.1")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

app = Flask(__name__)
embedder = SentenceTransformer(MODEL_NAME)
qdrant = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)

# ---------------------------------------------------------
# LIMIT LOGIC
# ---------------------------------------------------------
def get_limit(param, max_results):
    if param is None or param == "":
        return 5

    value = str(param).strip().upper()

    if value == "ALL":
        return max_results

    if value.isdigit():
        return int(value)

    return 5

# ---------------------------------------------------------
# Haversine Distance (Miles)
# ---------------------------------------------------------
def haversine_miles(lat1, lon1, lat2, lon2):
    """
    Returns distance in miles.
    Returns None if any coordinate is missing.
    """
    if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
        return None

    lat1 = float(lat1)
    lon1 = float(lon1)
    lat2 = float(lat2)
    lon2 = float(lon2)

    R = 3958.8  # Radius of Earth in miles

    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c

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
# Qdrant Insert Verification - (May not need this)
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
        SELECT 
            business_uid,
            business_name,
            business_short_bio,
            business_tag_line,
            business_city,
            business_state,
            business_country,
            business_latitude,
            business_longitude,
            business_phone_number,
            business_phone_number_is_public,
            business_email_id,
            business_email_id_is_public,
            business_images_url,
            business_images_is_public,
            business_owner_fn,
            business_owner_ln,
            business_price_level,
            business_google_rating,
            business_reward_type,
            business_reward_amount,
            updated_at
        FROM business
    """)
    rows = cur.fetchall()

    cur.execute("SELECT bs_business_id, bs_tags FROM business_services")
    service_rows = cur.fetchall()
    conn.close()

    # Map business → service tags
    service_map = {}
    for s in service_rows:
        bid = s["bs_business_id"]
        if bid not in service_map:
            service_map[bid] = []
        if s["bs_tags"]:
            service_map[bid].extend(
                [t.strip().lower() for t in s["bs_tags"].split(",") if t.strip()]
            )

    current_state = {r["business_uid"]: str(r["updated_at"]) for r in rows}

    # INSERT or UPDATE
    for row in rows:
        uid = row["business_uid"]
        row["bs_tags"] = service_map.get(uid, [])

        is_new = uid not in biz_map
        is_updated = (not is_new and biz_map[uid] != current_state[uid])

        if is_new:
            print(f"🆕 New business detected: {uid}")
        elif is_updated:
            print(f"🔄 Updated business detected: {uid}")

        if is_new or is_updated:
            upsert_business(row)
            success = verify_qdrant_insert("businesses", "business_uid", uid)
            print(("✔" if success else "❌") + f" {uid} — {row['business_name']}")

        biz_map[uid] = current_state[uid]

    # DELETE removed businesses
    for old_uid in list(biz_map.keys()):
        if old_uid not in current_state:
            print(f"🗑 Removing business: {old_uid}")
            qdrant.delete("businesses", points_selector=[make_uuid(old_uid)])
            biz_map.pop(old_uid, None)

    return biz_map

# ---------------------------------------------------------
# BUSINESS UPSERT - (Insert or Update)
# ---------------------------------------------------------
def upsert_business(row):
    uid = row["business_uid"]

    text = (
        f"{row['business_name']} "
        f"{row.get('business_short_bio') or ''} "
        f"{row.get('business_tag_line') or ''}"
    )

    tags = []
    if row.get("business_tag_line"):
        tags = [t.strip().lower() for t in row["business_tag_line"].split(",") if t.strip()]

    payload = {
        **row,
        "tags": tags,
        "bs_tags": row.get("bs_tags", [])
    }

    qdrant.upsert(
        collection_name="businesses",
        points=[PointStruct(id=make_uuid(uid), vector=embed_text(text), payload=payload)]
    )

# ---------------------------------------------------------
# SEARCH BUSINESS (UPDATED WITH DISTANCE AND RATING FILTERS)
# ---------------------------------------------------------
@app.route("/search_business", methods=["GET"])
def search_business():
    global biz_map
    biz_map = sync_businesses(biz_map)

    query = request.args.get("q", "")
    limit_param = request.args.get("limit")

    # NEW FILTER PARAMS
    user_lat = request.args.get("user_lat", type=float)
    user_lon = request.args.get("user_lon", type=float)
    max_distance = request.args.get("max_distance", type=float)
    min_rating = request.args.get("min_rating", type=float)
    max_rating = request.args.get("max_rating", type=float)

    max_results = 99999
    final_limit = get_limit(limit_param, max_results)

    vector = embed_text(query)

    # Search ALL results first
    results = qdrant.search("businesses", query_vector=vector, limit=max_results)

    # Build list of UIDs for SQL fetch
    business_uids = [r.payload.get("business_uid") for r in results]

    additional_info = {}

    if business_uids:
        conn = mysql_connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)

        placeholders = ",".join(["%s"] * len(business_uids))

        cur.execute(f"""
            SELECT *
            FROM business
            WHERE business_uid IN ({placeholders})
        """, business_uids)

        rows = cur.fetchall()
        conn.close()

        for row in rows:
            additional_info[row["business_uid"]] = row

    # -----------------------------------------------------
    # APPLY FILTERS + ADD DISTANCE
    # -----------------------------------------------------
    filtered = []
    for r in results:
        uid = r.payload.get("business_uid")

        merged = {"score": r.score, **r.payload}

        if uid in additional_info:
            merged.update(additional_info[uid])

        # Calculate distance if user lat/lon provided
        if user_lat is not None and user_lon is not None:
            dist = haversine_miles(
                user_lat,
                user_lon,
                merged.get("business_latitude"),
                merged.get("business_longitude")
            )
            merged["distance_miles"] = dist

            # max_distance filter
            if max_distance is not None and dist is not None:
                if dist > max_distance:
                    continue

        # rating filters
        rating = merged.get("business_google_rating")
        if rating is not None:
            rating = float(rating)

            if min_rating is not None and rating < min_rating:
                continue
            if max_rating is not None and rating > max_rating:
                continue

        filtered.append(merged)

        # stop when limit reached
        if len(filtered) >= final_limit:
            break

    return jsonify(filtered)

# ---------------------------------------------------------
# WISHES SYNC
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

    for row in rows:
        uid = row["profile_wish_uid"]
        is_new = uid not in wish_map
        is_updated = (not is_new and wish_map[uid] != current_state[uid])

        if is_new:
            print(f"🆕 New wish detected: {uid}")
        elif is_updated:
            print(f"🔄 Updated wish detected: {uid}")

        if is_new or is_updated:
            upsert_wish(row)
            success = verify_qdrant_insert("wishes", "profile_wish_uid", uid)
            print(("✔" if success else "❌") + f" {uid} — {row['profile_wish_title']}")

        wish_map[uid] = current_state[uid]

    for old_uid in list(wish_map.keys()):
        if old_uid not in current_state:
            print(f"🗑 Removing wish: {old_uid}")
            qdrant.delete("wishes", points_selector=[make_uuid(old_uid)])
            wish_map.pop(old_uid, None)

    return wish_map

# ---------------------------------------------------------
# UPSERT WISH
# ---------------------------------------------------------
def upsert_wish(row):
    uid = row["profile_wish_uid"]
    text = f"{row['profile_wish_title']} {row.get('profile_wish_description') or ''}"

    qdrant.upsert(
        collection_name="wishes",
        points=[PointStruct(id=make_uuid(uid), vector=embed_text(text), payload=row)]
    )

# ---------------------------------------------------------
# SEARCH WISHES (unchanged)
# ---------------------------------------------------------
@app.route("/search_wishes", methods=["GET"])
def search_wishes():
    global wish_map
    wish_map = sync_wishes(wish_map)

    query = request.args.get("q", "")
    limit_param = request.args.get("limit")

    max_results = 99999
    final_limit = get_limit(limit_param, max_results)

    vector = embed_text(query)

    results = qdrant.search("wishes", query_vector=vector, limit=max_results)
    results = results[:final_limit]

    wish_uids = [r.payload.get("profile_wish_uid") for r in results]

    additional_info = {}

    if wish_uids:
        conn = mysql_connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        placeholders = ",".join(["%s"] * len(wish_uids))

        cur.execute(f"""
            SELECT profile_wish.*,
                   user_email_id,
                   profile_personal_first_name, profile_personal_last_name,
                   profile_personal_email_is_public, profile_personal_phone_number,
                   profile_personal_phone_number_is_public,
                   profile_personal_city, profile_personal_state, profile_personal_country,
                   profile_personal_location_is_public, profile_personal_latitude, profile_personal_longitude,
                   profile_personal_image, profile_personal_image_is_public,
                   profile_personal_tag_line, profile_personal_tag_line_is_public
            FROM profile_wish
            LEFT JOIN every_circle.profile_personal
                ON profile_personal_uid = profile_wish_profile_personal_id
            LEFT JOIN every_circle.users
                ON user_uid = profile_personal_user_id
            WHERE profile_wish_uid IN ({placeholders})
        """, wish_uids)

        rows = cur.fetchall()
        conn.close()

        for row in rows:
            additional_info[row["profile_wish_uid"]] = row

    response = []
    for r in results:
        uid = r.payload.get("profile_wish_uid")
        item = {"score": r.score, **r.payload}
        if uid in additional_info:
            item.update(additional_info[uid])
        response.append(item)

    return jsonify(response)


# ---------------------------------------------------------
# EXPERTISE SYNC
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

    for row in rows:
        uid = row["profile_expertise_uid"]
        is_new = uid not in exp_map
        is_updated = (not is_new and exp_map[uid] != current_state[uid])

        if is_new:
            print(f"🆕 New expertise detected: {uid}")
        elif is_updated:
            print(f"🔄 Updated expertise detected: {uid}")

        if is_new or is_updated:
            upsert_expertise(row)
            success = verify_qdrant_insert("expertise", "profile_expertise_uid", uid)
            print(("✔" if success else "❌") + f" {uid} — {row['profile_expertise_title']}")

        exp_map[uid] = current_state[uid]

    for old_uid in list(exp_map.keys()):
        if old_uid not in current_state:
            print(f"🗑 Removing expertise: {old_uid}")
            qdrant.delete("expertise", points_selector=[make_uuid(old_uid)])
            exp_map.pop(old_uid, None)

    return exp_map

# ---------------------------------------------------------
# UPSERT EXPERTISE
# ---------------------------------------------------------
def upsert_expertise(row):
    uid = row["profile_expertise_uid"]
    text = f"{row['profile_expertise_title']} {row.get('profile_expertise_description') or ''}"

    qdrant.upsert(
        collection_name="expertise",
        points=[PointStruct(id=make_uuid(uid), vector=embed_text(text), payload=row)]
    )

# ---------------------------------------------------------
# SEARCH EXPERTISE (UNCHANGED)
# ---------------------------------------------------------
@app.route("/search_expertise", methods=["GET"])
def search_expertise():
    global exp_map
    exp_map = sync_expertise(exp_map)

    query = request.args.get("q", "")
    limit_param = request.args.get("limit")

    max_results = 99999
    final_limit = get_limit(limit_param, max_results)

    vector = embed_text(query)

    results = qdrant.search("expertise", query_vector=vector, limit=max_results)
    results = results[:final_limit]

    exp_uids = [r.payload.get("profile_expertise_uid") for r in results]

    additional_info = {}

    if exp_uids:
        conn = mysql_connect()
        cur = conn.cursor(pymysql.cursors.DictCursor)
        placeholders = ",".join(["%s"] * len(exp_uids))

        cur.execute(f"""
            SELECT profile_expertise.*,
                   user_email_id,
                   profile_personal_first_name, profile_personal_last_name,
                   profile_personal_email_is_public, profile_personal_phone_number,
                   profile_personal_phone_number_is_public,
                   profile_personal_city, profile_personal_state, profile_personal_country,
                   profile_personal_location_is_public, profile_personal_latitude, profile_personal_longitude,
                   profile_personal_image, profile_personal_image_is_public,
                   profile_personal_tag_line, profile_personal_tag_line_is_public
            FROM profile_expertise
            LEFT JOIN every_circle.profile_personal
                ON profile_personal_uid = profile_expertise_profile_personal_id
            LEFT JOIN every_circle.users
                ON user_uid = profile_personal_user_id
            WHERE profile_expertise_uid IN ({placeholders})
        """, exp_uids)

        rows = cur.fetchall()
        conn.close()

        for row in rows:
            additional_info[row["profile_expertise_uid"]] = row

    response = []
    for r in results:
        uid = r.payload.get("profile_expertise_uid")
        item = {"score": r.score, **r.payload}
        if uid in additional_info:
            item.update(additional_info[uid])
        response.append(item)

    return jsonify(response)


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------
if __name__ == "__main__":
    ensure_collections()

    global biz_map, wish_map, exp_map
    biz_map = {}
    wish_map = {}
    exp_map = {}

    app.run(host="0.0.0.0", port=5001)
