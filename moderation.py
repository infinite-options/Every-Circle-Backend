import json
import os
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

MODERATED_ACTIVE = 0
MODERATED_TAKEN_DOWN = 1
MODERATED_PENDING_REVIEW = 2
MODERATED_ACKNOWLEDGED = 3
TARGET_TYPE_OFFERING = "offering"
TARGET_TYPE_SEEKING = "seeking"

_OFFERING_UID_PREFIX = "150"
_WISH_UID_PREFIX = "160"
_PROFILE_EXPERTISE_TABLE = "every_circle.profile_expertise"
_PROFILE_WISH_TABLE = "every_circle.profile_wish"
_CONTENT_REPORTS_TABLE = "every_circle.content_reports"
_CONTENT_RESUBMISSIONS_TABLE = "every_circle.content_resubmissions"


def _takedown_threshold():
    try:
        return int(os.getenv("CONTENT_FLAG_TAKEDOWN_THRESHOLD", "3"))
    except (TypeError, ValueError):
        return 3


def _moderated_value(row):
    if not row:
        return MODERATED_ACTIVE
    return int(row.get("profile_expertise_moderated") or 0)


def _db_write_succeeded(res):
    return bool(res) and res.get("code") == 200


def _serialize_value(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def count_pending_flags(db, target_uid):
    query = """
        SELECT COUNT(*) AS flag_count
        FROM every_circle.content_reports
        WHERE report_target_uid = %s
          AND report_status = 'pending'
    """
    res = db.execute(query, (target_uid,))
    rows = res.get("result") or []
    if not rows:
        return 0
    return int(rows[0].get("flag_count") or 0)


def get_offering(db, expertise_uid):
    expertise_uid = str(expertise_uid or "").strip()
    if not expertise_uid.startswith(_OFFERING_UID_PREFIX):
        return None
    res = db.select(
        _PROFILE_EXPERTISE_TABLE,
        where={"profile_expertise_uid": expertise_uid},
    )
    rows = res.get("result") or []
    return rows[0] if rows else None


def get_offering_owner_profile_uid(db, expertise_uid):
    row = get_offering(db, expertise_uid)
    if not row:
        return None
    return row.get("profile_expertise_profile_personal_id")


def is_offering_publicly_visible(row, viewer_profile_uid=None, viewer_is_admin=False):
    # Acknowledged (user dismissed) offerings are never returned in profile lists.
    if _moderated_value(row) == MODERATED_ACKNOWLEDGED:
        return False
    if viewer_is_admin:
        return True
    owner_uid = row.get("profile_expertise_profile_personal_id")
    if viewer_profile_uid and str(viewer_profile_uid) == str(owner_uid):
        return True
    return _moderated_value(row) == MODERATED_ACTIVE


def build_expertise_snapshot(row):
    """JSON-serializable offering fields for content_resubmissions snapshots."""
    if not row:
        return {}

    snapshot = {
        "uid": row.get("profile_expertise_uid"),
        "title": row.get("profile_expertise_title"),
        "description": row.get("profile_expertise_description"),
        "details": row.get("profile_expertise_details"),
        "cost": _serialize_value(row.get("profile_expertise_cost")),
        "costCurrency": row.get("profile_expertise_cost_currency"),
        "bounty": _serialize_value(row.get("profile_expertise_bounty")),
        "quantity": row.get("profile_expertise_quantity"),
        "isPublic": row.get("profile_expertise_is_public"),
        "isTaxable": row.get("profile_expertise_is_taxable"),
        "taxRate": _serialize_value(row.get("profile_expertise_tax_rate")),
        "refundPolicy": row.get("profile_expertise_refund_policy"),
        "returnWindowDays": row.get("profile_expertise_return_window_days"),
        "startDateTime": _serialize_value(row.get("profile_expertise_start")),
        "endDateTime": _serialize_value(row.get("profile_expertise_end")),
        "location": row.get("profile_expertise_location"),
        "latitude": _serialize_value(row.get("profile_expertise_latitude")),
        "longitude": _serialize_value(row.get("profile_expertise_longitude")),
        "city": row.get("profile_expertise_city"),
        "state": row.get("profile_expertise_state"),
        "mode": row.get("profile_expertise_mode"),
        "image": row.get("profile_expertise_image"),
        "imageIsPublic": row.get("profile_expertise_image_is_public"),
    }
    return {k: v for k, v in snapshot.items() if v is not None}


def _get_latest_resubmission(db, target_uid, status=None):
    if status:
        query = """
            SELECT *
            FROM every_circle.content_resubmissions
            WHERE resubmission_target_uid = %s
              AND resubmission_status = %s
            ORDER BY resubmission_created_at DESC
            LIMIT 1
        """
        res = db.execute(query, (target_uid, status))
    else:
        query = """
            SELECT *
            FROM every_circle.content_resubmissions
            WHERE resubmission_target_uid = %s
            ORDER BY resubmission_created_at DESC
            LIMIT 1
        """
        res = db.execute(query, (target_uid,))
    rows = res.get("result") or []
    return rows[0] if rows else None


def can_offering_be_edited(db, expertise_uid, offering=None):
    """Only active offerings can be edited by the owner."""
    offering = offering or get_offering(db, expertise_uid)
    if not offering:
        return False
    return _moderated_value(offering) == MODERATED_ACTIVE


def _moderation_status_label(moderated, latest_resubmission):
    if moderated == MODERATED_ACKNOWLEDGED:
        return "acknowledged"
    if moderated == MODERATED_PENDING_REVIEW:
        return "pending_review"
    if moderated == MODERATED_TAKEN_DOWN:
        if latest_resubmission and latest_resubmission.get("resubmission_status") == "rejected":
            return "rejected"
        return "taken_down"
    return "active"


def build_offering_moderation_metadata(db, expertise_uid):
    offering = get_offering(db, expertise_uid)
    moderated = _moderated_value(offering)
    latest = _get_latest_resubmission(db, expertise_uid)
    metadata = {
        "flagCount": count_pending_flags(db, expertise_uid),
        "moderated": moderated,
        "status": _moderation_status_label(moderated, latest),
        "canEdit": can_offering_be_edited(db, expertise_uid, offering),
        "resubmissionStatus": None,
        "resubmissionAdminNote": None,
        "resubmissionCreatedAt": None,
    }
    if latest:
        metadata["resubmissionStatus"] = latest.get("resubmission_status")
        metadata["resubmissionAdminNote"] = latest.get("resubmission_admin_note")
        created_at = latest.get("resubmission_created_at")
        metadata["resubmissionCreatedAt"] = _serialize_value(created_at)
        if latest.get("resubmission_status") == "rejected":
            metadata["rejectionNote"] = latest.get("resubmission_admin_note")
    return metadata


def _parse_snapshot(raw):
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw
    return raw


def _create_pending_resubmission(db, target_uid, snapshot):
    """Insert a pending resubmission row for admin review."""
    pending = _get_latest_resubmission(db, target_uid, status="pending")
    if pending:
        return pending["resubmission_uid"]

    resubmission_uid = _new_resubmission_uid(db)
    if not resubmission_uid:
        return None

    ins_res = db.insert(
        _CONTENT_RESUBMISSIONS_TABLE,
        {
            "resubmission_uid": resubmission_uid,
            "resubmission_target_uid": target_uid,
            "resubmission_snapshot": json.dumps(snapshot, default=str),
            "resubmission_status": "pending",
            "resubmission_created_at": _now_str(),
        },
    )
    if not _db_write_succeeded(ins_res):
        return None
    return resubmission_uid


def apply_takedown_if_threshold(db, target_uid):
    """
    If pending flag count reaches the configured threshold while the offering is
    active, hide it from the public and queue it for admin review.

    Returns True when the offering is queued for review on this call.
    """
    flag_count = count_pending_flags(db, target_uid)
    if flag_count < _takedown_threshold():
        return False

    offering = get_offering(db, target_uid)
    if not offering:
        return False
    if _moderated_value(offering) != MODERATED_ACTIVE:
        return False

    snapshot = build_expertise_snapshot(offering)
    resubmission_uid = _create_pending_resubmission(db, target_uid, snapshot)
    if not resubmission_uid:
        return False

    upd_res = db.update(
        _PROFILE_EXPERTISE_TABLE,
        {"profile_expertise_uid": target_uid},
        {
            "profile_expertise_moderated": MODERATED_PENDING_REVIEW,
            "profile_expertise_is_public": 0,
        },
    )
    return _db_write_succeeded(upd_res)


def _new_resubmission_uid(db):
    """UID from every_circle.new_content_resubmissions_uid stored procedure."""
    uid_res = db.call(procedure="every_circle.new_content_resubmissions_uid")
    rows = uid_res.get("result") or []
    if not rows:
        return None
    return rows[0].get("new_id")


def queue_offering_for_review(db, expertise_uid, editor_profile_uid):
    """
    When an owner edits a moderated offering, snapshot the current content,
    queue it for admin review, and set moderated = 2 on the same row.
    """
    offering = get_offering(db, expertise_uid)
    if not offering:
        return {"ok": False, "message": "Offering not found"}

    moderated = _moderated_value(offering)
    if moderated not in (MODERATED_TAKEN_DOWN, MODERATED_PENDING_REVIEW):
        return {"ok": False, "message": "Offering is not under moderation"}

    if not can_offering_be_edited(db, expertise_uid, offering):
        return {
            "ok": False,
            "message": "This offering cannot be edited while it is taken down",
        }

    owner_uid = offering.get("profile_expertise_profile_personal_id")
    if editor_profile_uid and str(editor_profile_uid) != str(owner_uid):
        return {"ok": False, "message": "Only the offering owner can resubmit"}

    snapshot = build_expertise_snapshot(offering)
    now = _now_str()
    pending = _get_latest_resubmission(db, expertise_uid, status="pending")

    if pending:
        upd_res = db.update(
            _CONTENT_RESUBMISSIONS_TABLE,
            {"resubmission_uid": pending["resubmission_uid"]},
            {
                "resubmission_snapshot": json.dumps(snapshot, default=str),
                "resubmission_created_at": now,
                "resubmission_reviewed_at": None,
                "resubmission_reviewed_by": None,
                "resubmission_admin_note": None,
            },
        )
        if not _db_write_succeeded(upd_res):
            return {
                "ok": False,
                "message": upd_res.get("message", "Failed to update resubmission"),
            }
        resubmission_uid = pending["resubmission_uid"]
    else:
        resubmission_uid = _new_resubmission_uid(db)
        if not resubmission_uid:
            return {"ok": False, "message": "Failed to generate resubmission UID"}
        ins_res = db.insert(
            _CONTENT_RESUBMISSIONS_TABLE,
            {
                "resubmission_uid": resubmission_uid,
                "resubmission_target_uid": expertise_uid,
                "resubmission_snapshot": json.dumps(snapshot, default=str),
                "resubmission_status": "pending",
                "resubmission_created_at": now,
            },
        )
        if not _db_write_succeeded(ins_res):
            return {
                "ok": False,
                "message": ins_res.get("message", "Failed to create resubmission"),
            }

    mod_res = db.update(
        _PROFILE_EXPERTISE_TABLE,
        {"profile_expertise_uid": expertise_uid},
        {"profile_expertise_moderated": MODERATED_PENDING_REVIEW},
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to queue offering for review"}

    return {
        "ok": True,
        "resubmission_uid": resubmission_uid,
        "snapshot": snapshot,
    }


def _dismiss_pending_reports(db, target_uid):
    query = """
        UPDATE every_circle.content_reports
        SET report_status = 'dismissed'
        WHERE report_target_uid = %s
          AND report_status = 'pending'
    """
    return db.execute(query, (target_uid,), cmd="post")


def _finalize_pending_resubmission(db, target_uid, admin_uid, note, status):
    pending = _get_latest_resubmission(db, target_uid, status="pending")
    if not pending:
        return True, pending
    upd_res = db.update(
        _CONTENT_RESUBMISSIONS_TABLE,
        {"resubmission_uid": pending["resubmission_uid"]},
        {
            "resubmission_status": status,
            "resubmission_reviewed_at": _now_str(),
            "resubmission_reviewed_by": admin_uid,
            "resubmission_admin_note": note,
        },
    )
    return _db_write_succeeded(upd_res), pending


def approve_offering_review(db, target_uid, admin_uid, note=None):
    offering = get_offering(db, target_uid)
    if not offering:
        return {"ok": False, "message": "Offering not found"}
    if _moderated_value(offering) != MODERATED_PENDING_REVIEW:
        return {"ok": False, "message": "Offering is not pending admin review"}

    finalized, resubmission_row = _finalize_pending_resubmission(
        db, target_uid, admin_uid, note, "approved"
    )
    if not finalized:
        return {"ok": False, "message": "Failed to approve resubmission"}

    snapshot = {}
    if resubmission_row:
        snapshot = _parse_snapshot(resubmission_row.get("resubmission_snapshot")) or {}

    is_public = int(snapshot.get("isPublic", offering.get("profile_expertise_is_public") or 1))
    mod_res = db.update(
        _PROFILE_EXPERTISE_TABLE,
        {"profile_expertise_uid": target_uid},
        {
            "profile_expertise_moderated": MODERATED_ACTIVE,
            "profile_expertise_is_public": is_public,
        },
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to restore offering"}

    _dismiss_pending_reports(db, target_uid)
    return {"ok": True, "profile_expertise_uid": target_uid}


def reject_offering_review(db, target_uid, admin_uid, note=None):
    offering = get_offering(db, target_uid)
    if not offering:
        return {"ok": False, "message": "Offering not found"}
    if _moderated_value(offering) != MODERATED_PENDING_REVIEW:
        return {"ok": False, "message": "Offering is not pending admin review"}

    note_text = str(note or "").strip()
    if not note_text:
        return {"ok": False, "message": "Admin note is required when rejecting an offering"}

    finalized, _ = _finalize_pending_resubmission(
        db, target_uid, admin_uid, note_text, "rejected"
    )
    if not finalized:
        return {"ok": False, "message": "Failed to reject resubmission"}

    mod_res = db.update(
        _PROFILE_EXPERTISE_TABLE,
        {"profile_expertise_uid": target_uid},
        {
            "profile_expertise_moderated": MODERATED_TAKEN_DOWN,
            "profile_expertise_is_public": 0,
        },
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to reject offering"}

    return {
        "ok": True,
        "profile_expertise_uid": target_uid,
        "rejection_note": note_text,
    }


def acknowledge_offering_takedown(db, target_uid, requester_profile_uid):
    """
    Owner acknowledges a rejected / taken-down offering.
    Sets profile_expertise_moderated = 3 so it is no longer returned to the user.
    """
    offering = get_offering(db, target_uid)
    if not offering:
        return {"ok": False, "message": "Offering not found", "code": 404}

    owner_uid = offering.get("profile_expertise_profile_personal_id")
    if not requester_profile_uid or str(requester_profile_uid) != str(owner_uid):
        return {
            "ok": False,
            "message": "Only the offering owner can acknowledge a takedown",
            "code": 403,
        }

    moderated = _moderated_value(offering)
    if moderated == MODERATED_ACKNOWLEDGED:
        return {
            "ok": True,
            "profile_expertise_uid": target_uid,
            "already_acknowledged": True,
        }

    if moderated != MODERATED_TAKEN_DOWN:
        return {
            "ok": False,
            "message": "Only rejected / taken-down offerings can be acknowledged",
            "code": 400,
        }

    latest = _get_latest_resubmission(db, target_uid)
    if not latest or latest.get("resubmission_status") != "rejected":
        return {
            "ok": False,
            "message": "Offering must be rejected by an admin before acknowledgment",
            "code": 400,
        }

    mod_res = db.update(
        _PROFILE_EXPERTISE_TABLE,
        {"profile_expertise_uid": target_uid},
        {
            "profile_expertise_moderated": MODERATED_ACKNOWLEDGED,
            "profile_expertise_is_public": 0,
        },
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to acknowledge offering", "code": 500}

    return {
        "ok": True,
        "profile_expertise_uid": target_uid,
        "already_acknowledged": False,
    }


def resolve_content_target(db, target_uid):
    """Return (target_type, row) for an offering or seeking UID, else (None, None)."""
    target_uid = str(target_uid or "").strip()
    if target_uid.startswith(_OFFERING_UID_PREFIX):
        row = get_offering(db, target_uid)
        return (TARGET_TYPE_OFFERING, row) if row else (None, None)
    if target_uid.startswith(_WISH_UID_PREFIX):
        row = get_wish(db, target_uid)
        return (TARGET_TYPE_SEEKING, row) if row else (None, None)
    return None, None


def apply_content_takedown_if_threshold(db, target_uid):
    """Apply takedown when flag threshold is reached for offering or seeking content."""
    target_type, _ = resolve_content_target(db, target_uid)
    if target_type == TARGET_TYPE_OFFERING:
        return apply_takedown_if_threshold(db, target_uid)
    if target_type == TARGET_TYPE_SEEKING:
        return apply_wish_takedown_if_threshold(db, target_uid)
    return False


def _wish_moderated_value(row):
    if not row:
        return MODERATED_ACTIVE
    return int(row.get("profile_wish_moderated") or 0)


def get_wish(db, wish_uid):
    wish_uid = str(wish_uid or "").strip()
    if not wish_uid.startswith(_WISH_UID_PREFIX):
        return None
    res = db.select(
        _PROFILE_WISH_TABLE,
        where={"profile_wish_uid": wish_uid},
    )
    rows = res.get("result") or []
    return rows[0] if rows else None


def get_wish_owner_profile_uid(db, wish_uid):
    row = get_wish(db, wish_uid)
    if not row:
        return None
    return row.get("profile_wish_profile_personal_id")


def is_wish_publicly_visible(row, viewer_profile_uid=None, viewer_is_admin=False):
    if _wish_moderated_value(row) == MODERATED_ACKNOWLEDGED:
        return False
    if viewer_is_admin:
        return True
    owner_uid = row.get("profile_wish_profile_personal_id")
    if viewer_profile_uid and str(viewer_profile_uid) == str(owner_uid):
        return True
    return _wish_moderated_value(row) == MODERATED_ACTIVE


def build_wish_snapshot(row):
    """JSON-serializable seeking fields for content_resubmissions snapshots."""
    if not row:
        return {}

    snapshot = {
        "uid": row.get("profile_wish_uid"),
        "title": row.get("profile_wish_title"),
        "description": row.get("profile_wish_description"),
        "cost": _serialize_value(row.get("profile_wish_cost")),
        "costCurrency": row.get("profile_wish_cost_currency"),
        "bounty": _serialize_value(row.get("profile_wish_bounty")),
        "quantity": row.get("profile_wish_quantity"),
        "isPublic": row.get("profile_wish_is_public"),
        "isTaxable": row.get("profile_wish_is_taxable"),
        "taxRate": _serialize_value(row.get("profile_wish_tax_rate")),
        "refundPolicy": row.get("profile_wish_refund_policy"),
        "returnWindowDays": row.get("profile_wish_return_window_days"),
        "startDateTime": _serialize_value(row.get("profile_wish_start")),
        "endDateTime": _serialize_value(row.get("profile_wish_end")),
        "location": row.get("profile_wish_location"),
        "latitude": _serialize_value(row.get("profile_wish_latitude")),
        "longitude": _serialize_value(row.get("profile_wish_longitude")),
        "city": row.get("profile_wish_city"),
        "state": row.get("profile_wish_state"),
        "mode": row.get("profile_wish_mode"),
        "image": row.get("profile_wish_image"),
        "imageIsPublic": row.get("profile_wish_image_is_public"),
    }
    return {k: v for k, v in snapshot.items() if v is not None}


def can_wish_be_edited(db, wish_uid, wish=None):
    """Only active seeking posts can be edited by the owner."""
    wish = wish or get_wish(db, wish_uid)
    if not wish:
        return False
    return _wish_moderated_value(wish) == MODERATED_ACTIVE


def build_wish_moderation_metadata(db, wish_uid):
    wish = get_wish(db, wish_uid)
    moderated = _wish_moderated_value(wish)
    latest = _get_latest_resubmission(db, wish_uid)
    metadata = {
        "flagCount": count_pending_flags(db, wish_uid),
        "moderated": moderated,
        "status": _moderation_status_label(moderated, latest),
        "canEdit": can_wish_be_edited(db, wish_uid, wish),
        "resubmissionStatus": None,
        "resubmissionAdminNote": None,
        "resubmissionCreatedAt": None,
    }
    if latest:
        metadata["resubmissionStatus"] = latest.get("resubmission_status")
        metadata["resubmissionAdminNote"] = latest.get("resubmission_admin_note")
        created_at = latest.get("resubmission_created_at")
        metadata["resubmissionCreatedAt"] = _serialize_value(created_at)
        if latest.get("resubmission_status") == "rejected":
            metadata["rejectionNote"] = latest.get("resubmission_admin_note")
    return metadata


def apply_wish_takedown_if_threshold(db, target_uid):
    """
    If pending flag count reaches the configured threshold while the seeking post is
    active, hide it from the public and queue it for admin review.
    """
    flag_count = count_pending_flags(db, target_uid)
    if flag_count < _takedown_threshold():
        return False

    wish = get_wish(db, target_uid)
    if not wish:
        return False
    if _wish_moderated_value(wish) != MODERATED_ACTIVE:
        return False

    snapshot = build_wish_snapshot(wish)
    resubmission_uid = _create_pending_resubmission(db, target_uid, snapshot)
    if not resubmission_uid:
        return False

    upd_res = db.update(
        _PROFILE_WISH_TABLE,
        {"profile_wish_uid": target_uid},
        {
            "profile_wish_moderated": MODERATED_PENDING_REVIEW,
            "profile_wish_is_public": 0,
        },
    )
    return _db_write_succeeded(upd_res)


def queue_wish_for_review(db, wish_uid, editor_profile_uid):
    """Queue an edited moderated seeking post for admin review."""
    wish = get_wish(db, wish_uid)
    if not wish:
        return {"ok": False, "message": "Seeking post not found"}

    moderated = _wish_moderated_value(wish)
    if moderated not in (MODERATED_TAKEN_DOWN, MODERATED_PENDING_REVIEW):
        return {"ok": False, "message": "Seeking post is not under moderation"}

    if not can_wish_be_edited(db, wish_uid, wish):
        return {
            "ok": False,
            "message": "This seeking post cannot be edited while it is taken down",
        }

    owner_uid = wish.get("profile_wish_profile_personal_id")
    if editor_profile_uid and str(editor_profile_uid) != str(owner_uid):
        return {"ok": False, "message": "Only the seeking post owner can resubmit"}

    snapshot = build_wish_snapshot(wish)
    now = _now_str()
    pending = _get_latest_resubmission(db, wish_uid, status="pending")

    if pending:
        upd_res = db.update(
            _CONTENT_RESUBMISSIONS_TABLE,
            {"resubmission_uid": pending["resubmission_uid"]},
            {
                "resubmission_snapshot": json.dumps(snapshot, default=str),
                "resubmission_created_at": now,
                "resubmission_reviewed_at": None,
                "resubmission_reviewed_by": None,
                "resubmission_admin_note": None,
            },
        )
        if not _db_write_succeeded(upd_res):
            return {
                "ok": False,
                "message": upd_res.get("message", "Failed to update resubmission"),
            }
        resubmission_uid = pending["resubmission_uid"]
    else:
        resubmission_uid = _new_resubmission_uid(db)
        if not resubmission_uid:
            return {"ok": False, "message": "Failed to generate resubmission UID"}
        ins_res = db.insert(
            _CONTENT_RESUBMISSIONS_TABLE,
            {
                "resubmission_uid": resubmission_uid,
                "resubmission_target_uid": wish_uid,
                "resubmission_snapshot": json.dumps(snapshot, default=str),
                "resubmission_status": "pending",
                "resubmission_created_at": now,
            },
        )
        if not _db_write_succeeded(ins_res):
            return {
                "ok": False,
                "message": ins_res.get("message", "Failed to create resubmission"),
            }

    mod_res = db.update(
        _PROFILE_WISH_TABLE,
        {"profile_wish_uid": wish_uid},
        {"profile_wish_moderated": MODERATED_PENDING_REVIEW},
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to queue seeking post for review"}

    return {
        "ok": True,
        "resubmission_uid": resubmission_uid,
        "snapshot": snapshot,
    }


def approve_wish_review(db, target_uid, admin_uid, note=None):
    wish = get_wish(db, target_uid)
    if not wish:
        return {"ok": False, "message": "Seeking post not found"}
    if _wish_moderated_value(wish) != MODERATED_PENDING_REVIEW:
        return {"ok": False, "message": "Seeking post is not pending admin review"}

    finalized, resubmission_row = _finalize_pending_resubmission(
        db, target_uid, admin_uid, note, "approved"
    )
    if not finalized:
        return {"ok": False, "message": "Failed to approve resubmission"}

    snapshot = {}
    if resubmission_row:
        snapshot = _parse_snapshot(resubmission_row.get("resubmission_snapshot")) or {}

    is_public = int(snapshot.get("isPublic", wish.get("profile_wish_is_public") or 1))
    mod_res = db.update(
        _PROFILE_WISH_TABLE,
        {"profile_wish_uid": target_uid},
        {
            "profile_wish_moderated": MODERATED_ACTIVE,
            "profile_wish_is_public": is_public,
        },
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to restore seeking post"}

    _dismiss_pending_reports(db, target_uid)
    return {"ok": True, "profile_wish_uid": target_uid}


def reject_wish_review(db, target_uid, admin_uid, note=None):
    wish = get_wish(db, target_uid)
    if not wish:
        return {"ok": False, "message": "Seeking post not found"}
    if _wish_moderated_value(wish) != MODERATED_PENDING_REVIEW:
        return {"ok": False, "message": "Seeking post is not pending admin review"}

    note_text = str(note or "").strip()
    if not note_text:
        return {
            "ok": False,
            "message": "Admin note is required when rejecting a seeking post",
        }

    finalized, _ = _finalize_pending_resubmission(
        db, target_uid, admin_uid, note_text, "rejected"
    )
    if not finalized:
        return {"ok": False, "message": "Failed to reject resubmission"}

    mod_res = db.update(
        _PROFILE_WISH_TABLE,
        {"profile_wish_uid": target_uid},
        {
            "profile_wish_moderated": MODERATED_TAKEN_DOWN,
            "profile_wish_is_public": 0,
        },
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to reject seeking post"}

    return {
        "ok": True,
        "profile_wish_uid": target_uid,
        "rejection_note": note_text,
    }


def acknowledge_wish_takedown(db, target_uid, requester_profile_uid):
    """
    Owner acknowledges a rejected / taken-down seeking post.
    Sets profile_wish_moderated = 3 so it is no longer returned to the user.
    """
    wish = get_wish(db, target_uid)
    if not wish:
        return {"ok": False, "message": "Seeking post not found", "code": 404}

    owner_uid = wish.get("profile_wish_profile_personal_id")
    if not requester_profile_uid or str(requester_profile_uid) != str(owner_uid):
        return {
            "ok": False,
            "message": "Only the seeking post owner can acknowledge a takedown",
            "code": 403,
        }

    moderated = _wish_moderated_value(wish)
    if moderated == MODERATED_ACKNOWLEDGED:
        return {
            "ok": True,
            "profile_wish_uid": target_uid,
            "already_acknowledged": True,
        }

    if moderated != MODERATED_TAKEN_DOWN:
        return {
            "ok": False,
            "message": "Only rejected / taken-down seeking posts can be acknowledged",
            "code": 400,
        }

    latest = _get_latest_resubmission(db, target_uid)
    if not latest or latest.get("resubmission_status") != "rejected":
        return {
            "ok": False,
            "message": "Seeking post must be rejected by an admin before acknowledgment",
            "code": 400,
        }

    mod_res = db.update(
        _PROFILE_WISH_TABLE,
        {"profile_wish_uid": target_uid},
        {
            "profile_wish_moderated": MODERATED_ACKNOWLEDGED,
            "profile_wish_is_public": 0,
        },
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to acknowledge seeking post", "code": 500}

    return {
        "ok": True,
        "profile_wish_uid": target_uid,
        "already_acknowledged": False,
    }
