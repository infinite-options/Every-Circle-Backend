"""Shared helpers for deleted-profile tombstones (account deletion)."""

_PROFILE_PERSONAL_TABLE = "every_circle.profile_personal"


def _deleted_flag_from_row(row):
    if not row:
        return False
    return bool(int(row.get("profile_personal_is_deleted") or 0))


def is_profile_deleted(row_or_uid, db=None):
    """
    Return True when a profile is an anonymized deletion tombstone.

    Accepts a profile_personal row dict, or a profile uid string (db required).
    """
    if isinstance(row_or_uid, dict):
        return _deleted_flag_from_row(row_or_uid)

    uid = str(row_or_uid or "").strip()
    if not uid:
        return False
    if db is None:
        raise ValueError("db is required when checking deletion by profile uid")
    return bool(batch_deleted_status(db, [uid]).get(uid))


def deleted_profile_sql_clause(alias="pp"):
    """SQL fragment that excludes deleted profile tombstones."""
    prefix = f"{alias}." if alias else ""
    return f"AND COALESCE({prefix}profile_personal_is_deleted, 0) = 0"


def stub_deleted_profile_row(uid):
    """Minimal API payload for a deleted profile (no PII)."""
    return {
        "profile_personal_uid": uid,
        "is_deleted": True,
    }


def batch_deleted_status(db, uids):
    """
    Map profile_personal_uid -> is_deleted for the given uids.

    Missing profiles map to False.
    """
    uids = [str(uid).strip() for uid in (uids or []) if uid]
    if not uids:
        return {}

    placeholders = ", ".join(["%s"] * len(uids))
    res = db.execute(
        f"""
        SELECT profile_personal_uid, profile_personal_is_deleted
        FROM {_PROFILE_PERSONAL_TABLE}
        WHERE profile_personal_uid IN ({placeholders})
        """,
        tuple(uids),
    )
    by_uid = {
        row.get("profile_personal_uid"): _deleted_flag_from_row(row)
        for row in (res.get("result") or [])
    }
    return {uid: by_uid.get(uid, False) for uid in uids}


def build_connection_path_nodes(db, combined_path):
    """
    Enrich a comma-separated path with per-node tombstone metadata for UI.
    """
    if not combined_path or combined_path == "No common ancestor found.":
        return []

    uids = [uid.strip() for uid in str(combined_path).split(",") if uid.strip()]
    if not uids:
        return []

    deleted_map = batch_deleted_status(db, uids)
    active_uids = [uid for uid in uids if not deleted_map.get(uid)]
    active_rows = {}
    if active_uids:
        placeholders = ", ".join(["%s"] * len(active_uids))
        res = db.execute(
            f"""
            SELECT
                profile_personal_uid,
                profile_personal_first_name,
                profile_personal_last_name,
                profile_personal_image,
                profile_personal_image_is_public
            FROM {_PROFILE_PERSONAL_TABLE}
            WHERE profile_personal_uid IN ({placeholders})
            """,
            tuple(active_uids),
        )
        for row in (res.get("result") or []):
            uid = row.get("profile_personal_uid")
            if uid:
                active_rows[uid] = row

    nodes = []
    for uid in uids:
        if deleted_map.get(uid):
            nodes.append(stub_deleted_profile_row(uid))
            continue
        row = active_rows.get(uid) or {}
        image_public = int(row.get("profile_personal_image_is_public") or 0) == 1
        image_url = ""
        if image_public and row.get("profile_personal_image"):
            image_url = str(row.get("profile_personal_image")).strip()
        nodes.append(
            {
                "profile_personal_uid": uid,
                "is_deleted": False,
                "profile_personal_first_name": row.get("profile_personal_first_name"),
                "profile_personal_last_name": row.get("profile_personal_last_name"),
                "profile_personal_image": image_url or None,
            }
        )
    return nodes


def tombstone_network_fields(uid, is_deleted=False):
    """Strip display fields for deleted tombstone nodes in network graph."""
    if not is_deleted:
        return {}
    return {
        "is_deleted": True,
        "profile_personal_first_name": None,
        "profile_personal_last_name": None,
        "profile_personal_tag_line": None,
        "profile_personal_phone_number": None,
        "profile_personal_image": None,
        "profile_personal_email_is_public": 0,
        "profile_personal_phone_number_is_public": 0,
        "profile_personal_tag_line_is_public": 0,
        "profile_personal_image_is_public": 0,
    }
