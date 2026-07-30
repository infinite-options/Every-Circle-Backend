"""Lightweight profile avatar lookups for connection-path UI and similar.

Avoids the heavy userprofileinfo payload when only icon + display name are needed.
Full profile details are loaded on navigation to ProfileScreen.
"""

from flask import request
from flask_restful import Resource

from data_ec import connect


def _escape_uid(uid):
    return str(uid).replace("'", "''").strip()


def _row_to_avatar(row):
    image_is_public = row.get("profile_personal_image_is_public")
    try:
        image_public = int(image_is_public) == 1
    except (TypeError, ValueError):
        image_public = False

    image_url = ""
    if image_public:
        raw = row.get("profile_personal_image")
        if raw is not None and str(raw).strip():
            image_url = str(raw).strip()

    return {
        "profile_uid": row.get("profile_personal_uid"),
        "first_name": row.get("profile_personal_first_name") or "",
        "last_name": row.get("profile_personal_last_name") or "",
        "image_url": image_url,
        "image_is_public": image_public,
    }


def _fetch_avatars(db, uids):
    cleaned = []
    seen = set()
    for uid in uids:
        u = _escape_uid(uid)
        if not u or u in seen:
            continue
        seen.add(u)
        cleaned.append(u)

    if not cleaned:
        return []

    placeholders = ",".join(f"'{u}'" for u in cleaned)
    response = db.execute(
        f"""
        SELECT
            profile_personal_uid,
            profile_personal_first_name,
            profile_personal_last_name,
            profile_personal_image,
            profile_personal_image_is_public
        FROM every_circle.profile_personal
        WHERE profile_personal_uid IN ({placeholders})
        """
    )
    rows = (response or {}).get("result") or []
    by_uid = {r.get("profile_personal_uid"): _row_to_avatar(r) for r in rows}
    # Preserve request order; skip missing
    return [by_uid[u] for u in cleaned if u in by_uid]


class ProfileAvatar(Resource):
    """GET /api/v1/profile_avatar/<profile_uid> — single lightweight avatar."""

    def get(self, profile_uid):
        uid = _escape_uid(profile_uid)
        if not uid:
            return {"message": "profile_uid is required", "code": 400}, 400

        try:
            with connect() as db:
                avatars = _fetch_avatars(db, [uid])
            if not avatars:
                return {"message": f"No profile found for {uid}", "code": 404}, 404
            return avatars[0], 200
        except Exception as e:
            print(f"ProfileAvatar GET error: {e}")
            return {"message": f"Internal Server Error: {str(e)}", "code": 500}, 500


class ProfileAvatars(Resource):
    """POST /api/v1/profile_avatars — batch avatar lookup.

    Body: { "profile_uids": ["110-...", ...] }
    Returns: { "avatars": [ { profile_uid, first_name, last_name, image_url, image_is_public }, ... ] }
    """

    def post(self):
        body = request.get_json(silent=True) or {}
        uids = body.get("profile_uids") or body.get("uids") or []
        if isinstance(uids, str):
            uids = [u.strip() for u in uids.split(",") if u.strip()]
        if not isinstance(uids, list):
            return {"message": "profile_uids must be an array", "code": 400}, 400
        if len(uids) > 50:
            return {"message": "Too many profile_uids (max 50)", "code": 400}, 400

        try:
            with connect() as db:
                avatars = _fetch_avatars(db, uids)
            return {"avatars": avatars}, 200
        except Exception as e:
            print(f"ProfileAvatars POST error: {e}")
            return {"message": f"Internal Server Error: {str(e)}", "code": 500}, 500
