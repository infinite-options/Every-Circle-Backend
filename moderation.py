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
TARGET_TYPE_USER = "user"
TARGET_TYPE_BUSINESS = "business"

_OFFERING_UID_PREFIX = "150"
_WISH_UID_PREFIX = "160"
_PROFILE_UID_PREFIX = "110"
_BUSINESS_UID_PREFIX = "200"
_PROFILE_EXPERTISE_TABLE = "every_circle.profile_expertise"
_PROFILE_WISH_TABLE = "every_circle.profile_wish"
_PROFILE_PERSONAL_TABLE = "every_circle.profile_personal"
_BUSINESS_TABLE = "every_circle.business"
_BUSINESS_USER_TABLE = "every_circle.business_user"
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


def get_owner_visible_reports(db, target_uid):
    """
    Category/message per report for the content owner (never exposes the reporter).

    Only pending reports are returned so dismissed flags (e.g. after admin
    approval) are not shown while content is under review or taken down.
    """
    query = """
        SELECT report_uid,
               report_reason_category,
               report_reason_text,
               report_created_at
        FROM every_circle.content_reports
        WHERE report_target_uid = %s
          AND report_status = 'pending'
        ORDER BY report_created_at ASC
    """
    res = db.execute(query, (target_uid,))
    rows = res.get("result") or []
    return [
        {
            "reportUid": row.get("report_uid"),
            "category": row.get("report_reason_category"),
            "message": row.get("report_reason_text"),
            "createdAt": _serialize_value(row.get("report_created_at")),
        }
        for row in rows
    ]


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


def _owner_profile_hides_content_from_viewer(
    owner_moderated,
    owner_uid,
    viewer_profile_uid=None,
    viewer_is_admin=False,
):
    """
    Hide a profile owner's offerings/seekings when the account is
    pending_review (2), taken_down (1), or acknowledged (3).
    Does not change offering/seeking moderation rows — filter-only.

    - pending_review / taken_down: hidden from other users; owner may still see them
    - acknowledged: hidden from everyone (including the owner), same as content ack
    """
    if viewer_is_admin:
        return False
    moderated = int(owner_moderated or 0)
    if moderated == MODERATED_ACKNOWLEDGED:
        return True
    if moderated in (MODERATED_TAKEN_DOWN, MODERATED_PENDING_REVIEW):
        if viewer_profile_uid and str(viewer_profile_uid) == str(owner_uid):
            return False
        return True
    return False


def is_offering_publicly_visible(
    row,
    viewer_profile_uid=None,
    viewer_is_admin=False,
    owner_moderated=None,
):
    # Acknowledged (user dismissed) offerings are never returned in profile lists.
    if _moderated_value(row) == MODERATED_ACKNOWLEDGED:
        return False
    if viewer_is_admin:
        return True
    owner_uid = row.get("profile_expertise_profile_personal_id")
    if owner_moderated is not None and _owner_profile_hides_content_from_viewer(
        owner_moderated,
        owner_uid,
        viewer_profile_uid=viewer_profile_uid,
        viewer_is_admin=viewer_is_admin,
    ):
        return False
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
        "isReturnable": row.get("profile_expertise_is_returnable"),
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
        "reports": get_owner_visible_reports(db, expertise_uid),
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
    """Return (target_type, row) for an offering, seeking, user, or business UID, else (None, None)."""
    target_uid = str(target_uid or "").strip()
    if target_uid.startswith(_OFFERING_UID_PREFIX):
        row = get_offering(db, target_uid)
        return (TARGET_TYPE_OFFERING, row) if row else (None, None)
    if target_uid.startswith(_WISH_UID_PREFIX):
        row = get_wish(db, target_uid)
        return (TARGET_TYPE_SEEKING, row) if row else (None, None)
    if target_uid.startswith(_BUSINESS_UID_PREFIX):
        row = get_business(db, target_uid)
        return (TARGET_TYPE_BUSINESS, row) if row else (None, None)
    if target_uid.startswith(_PROFILE_UID_PREFIX):
        row = get_user(db, target_uid)
        return (TARGET_TYPE_USER, row) if row else (None, None)
    return None, None


def apply_content_takedown_if_threshold(db, target_uid):
    """Apply takedown when flag threshold is reached for offering, seeking, user, or business content."""
    target_type, _ = resolve_content_target(db, target_uid)
    if target_type == TARGET_TYPE_OFFERING:
        return apply_takedown_if_threshold(db, target_uid)
    if target_type == TARGET_TYPE_SEEKING:
        return apply_wish_takedown_if_threshold(db, target_uid)
    if target_type == TARGET_TYPE_USER:
        return apply_user_takedown_if_threshold(db, target_uid)
    if target_type == TARGET_TYPE_BUSINESS:
        return apply_business_takedown_if_threshold(db, target_uid)
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


def is_wish_publicly_visible(
    row,
    viewer_profile_uid=None,
    viewer_is_admin=False,
    owner_moderated=None,
):
    if _wish_moderated_value(row) == MODERATED_ACKNOWLEDGED:
        return False
    if viewer_is_admin:
        return True
    owner_uid = row.get("profile_wish_profile_personal_id")
    if owner_moderated is not None and _owner_profile_hides_content_from_viewer(
        owner_moderated,
        owner_uid,
        viewer_profile_uid=viewer_profile_uid,
        viewer_is_admin=viewer_is_admin,
    ):
        return False
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
        "isReturnable": row.get("profile_wish_is_returnable"),
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
        "reports": get_owner_visible_reports(db, wish_uid),
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


def _user_moderated_value(row):
    if not row:
        return MODERATED_ACTIVE
    return int(row.get("profile_personal_moderated") or 0)


def get_user(db, profile_uid):
    profile_uid = str(profile_uid or "").strip()
    if not profile_uid.startswith(_PROFILE_UID_PREFIX):
        return None
    res = db.select(
        _PROFILE_PERSONAL_TABLE,
        where={"profile_personal_uid": profile_uid},
    )
    rows = res.get("result") or []
    return rows[0] if rows else None


def get_user_moderated_value(db, profile_uid):
    return _user_moderated_value(get_user(db, profile_uid))


def is_owner_available_for_public_interaction(db, owner_profile_uid):
    """Return False when the profile owner account is not active for public actions."""
    return get_user_moderated_value(db, owner_profile_uid) == MODERATED_ACTIVE


def build_user_snapshot(row):
    """JSON-serializable profile fields for content_resubmissions snapshots."""
    if not row:
        return {}

    snapshot = {
        "uid": row.get("profile_personal_uid"),
        "firstName": row.get("profile_personal_first_name"),
        "lastName": row.get("profile_personal_last_name"),
        "emailIsPublic": row.get("profile_personal_email_is_public"),
        "phoneNumber": row.get("profile_personal_phone_number"),
        "phoneNumberIsPublic": row.get("profile_personal_phone_number_is_public"),
        "city": row.get("profile_personal_city"),
        "state": row.get("profile_personal_state"),
        "country": row.get("profile_personal_country"),
        "locationIsPublic": row.get("profile_personal_location_is_public"),
        "latitude": _serialize_value(row.get("profile_personal_latitude")),
        "longitude": _serialize_value(row.get("profile_personal_longitude")),
        "image": row.get("profile_personal_image"),
        "imageIsPublic": row.get("profile_personal_image_is_public"),
        "tagLine": row.get("profile_personal_tag_line"),
        "tagLineIsPublic": row.get("profile_personal_tag_line_is_public"),
        "shortBio": row.get("profile_personal_short_bio"),
        "shortBioIsPublic": row.get("profile_personal_short_bio_is_public"),
        "resume": row.get("profile_personal_resume"),
        "resumeIsPublic": row.get("profile_personal_resume_is_public"),
    }
    return {k: v for k, v in snapshot.items() if v is not None}


def can_user_profile_be_edited(db, profile_uid, user=None):
    """Only active user profiles can be edited by the owner."""
    user = user or get_user(db, profile_uid)
    if not user:
        return False
    return _user_moderated_value(user) == MODERATED_ACTIVE


def build_user_moderation_metadata(db, profile_uid):
    user = get_user(db, profile_uid)
    moderated = _user_moderated_value(user)
    latest = _get_latest_resubmission(db, profile_uid)
    metadata = {
        "flagCount": count_pending_flags(db, profile_uid),
        "moderated": moderated,
        "status": _moderation_status_label(moderated, latest),
        "canEdit": can_user_profile_be_edited(db, profile_uid, user),
        "reports": get_owner_visible_reports(db, profile_uid),
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


def apply_user_takedown_if_threshold(db, target_uid):
    """
    If pending flag count reaches the configured threshold while the user profile is
    active, queue it for admin review without changing offering/seeking moderation.
    """
    flag_count = count_pending_flags(db, target_uid)
    if flag_count < _takedown_threshold():
        return False

    user = get_user(db, target_uid)
    if not user:
        return False
    if _user_moderated_value(user) != MODERATED_ACTIVE:
        return False

    snapshot = build_user_snapshot(user)
    resubmission_uid = _create_pending_resubmission(db, target_uid, snapshot)
    if not resubmission_uid:
        return False

    upd_res = db.update(
        _PROFILE_PERSONAL_TABLE,
        {"profile_personal_uid": target_uid},
        {"profile_personal_moderated": MODERATED_PENDING_REVIEW},
    )
    return _db_write_succeeded(upd_res)


def queue_user_for_review(db, profile_uid, editor_profile_uid):
    """Queue an edited moderated user profile for admin review."""
    user = get_user(db, profile_uid)
    if not user:
        return {"ok": False, "message": "User profile not found"}

    moderated = _user_moderated_value(user)
    if moderated not in (MODERATED_TAKEN_DOWN, MODERATED_PENDING_REVIEW):
        return {"ok": False, "message": "User profile is not under moderation"}

    if not can_user_profile_be_edited(db, profile_uid, user):
        return {
            "ok": False,
            "message": "This profile cannot be edited while it is taken down",
        }

    if editor_profile_uid and str(editor_profile_uid) != str(profile_uid):
        return {"ok": False, "message": "Only the profile owner can resubmit"}

    snapshot = build_user_snapshot(user)
    now = _now_str()
    pending = _get_latest_resubmission(db, profile_uid, status="pending")

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
                "resubmission_target_uid": profile_uid,
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
        _PROFILE_PERSONAL_TABLE,
        {"profile_personal_uid": profile_uid},
        {"profile_personal_moderated": MODERATED_PENDING_REVIEW},
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to queue user profile for review"}

    return {
        "ok": True,
        "resubmission_uid": resubmission_uid,
        "snapshot": snapshot,
    }


def approve_user_review(db, target_uid, admin_uid, note=None):
    user = get_user(db, target_uid)
    if not user:
        return {"ok": False, "message": "User profile not found"}
    if _user_moderated_value(user) != MODERATED_PENDING_REVIEW:
        return {"ok": False, "message": "User profile is not pending admin review"}

    finalized, _ = _finalize_pending_resubmission(
        db, target_uid, admin_uid, note, "approved"
    )
    if not finalized:
        return {"ok": False, "message": "Failed to approve resubmission"}

    mod_res = db.update(
        _PROFILE_PERSONAL_TABLE,
        {"profile_personal_uid": target_uid},
        {"profile_personal_moderated": MODERATED_ACTIVE},
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to restore user profile"}

    _dismiss_pending_reports(db, target_uid)
    return {"ok": True, "profile_personal_uid": target_uid}


def reject_user_review(db, target_uid, admin_uid, note=None):
    user = get_user(db, target_uid)
    if not user:
        return {"ok": False, "message": "User profile not found"}
    if _user_moderated_value(user) != MODERATED_PENDING_REVIEW:
        return {"ok": False, "message": "User profile is not pending admin review"}

    note_text = str(note or "").strip()
    if not note_text:
        return {
            "ok": False,
            "message": "Admin note is required when rejecting a user profile",
        }

    finalized, _ = _finalize_pending_resubmission(
        db, target_uid, admin_uid, note_text, "rejected"
    )
    if not finalized:
        return {"ok": False, "message": "Failed to reject resubmission"}

    mod_res = db.update(
        _PROFILE_PERSONAL_TABLE,
        {"profile_personal_uid": target_uid},
        {"profile_personal_moderated": MODERATED_TAKEN_DOWN},
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to reject user profile"}

    return {
        "ok": True,
        "profile_personal_uid": target_uid,
        "rejection_note": note_text,
    }


def acknowledge_user_takedown(db, target_uid, requester_profile_uid):
    """
    Owner acknowledges a rejected / taken-down user profile.
    Sets profile_personal_moderated = 3 so offerings/seekings are no longer returned.
    """
    user = get_user(db, target_uid)
    if not user:
        return {"ok": False, "message": "User profile not found", "code": 404}

    if not requester_profile_uid or str(requester_profile_uid) != str(target_uid):
        return {
            "ok": False,
            "message": "Only the profile owner can acknowledge a takedown",
            "code": 403,
        }

    moderated = _user_moderated_value(user)
    if moderated == MODERATED_ACKNOWLEDGED:
        return {
            "ok": True,
            "profile_personal_uid": target_uid,
            "already_acknowledged": True,
        }

    if moderated != MODERATED_TAKEN_DOWN:
        return {
            "ok": False,
            "message": "Only rejected / taken-down profiles can be acknowledged",
            "code": 400,
        }

    latest = _get_latest_resubmission(db, target_uid)
    if not latest or latest.get("resubmission_status") != "rejected":
        return {
            "ok": False,
            "message": "Profile must be rejected by an admin before acknowledgment",
            "code": 400,
        }

    mod_res = db.update(
        _PROFILE_PERSONAL_TABLE,
        {"profile_personal_uid": target_uid},
        {"profile_personal_moderated": MODERATED_ACKNOWLEDGED},
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to acknowledge user profile", "code": 500}

    return {
        "ok": True,
        "profile_personal_uid": target_uid,
        "already_acknowledged": False,
    }


def _business_moderated_value(row):
    if not row:
        return MODERATED_ACTIVE
    return int(row.get("business_moderated") or 0)


def get_business(db, business_uid):
    business_uid = str(business_uid or "").strip()
    if not business_uid.startswith(_BUSINESS_UID_PREFIX):
        return None
    res = db.select(
        _BUSINESS_TABLE,
        where={"business_uid": business_uid},
    )
    rows = res.get("result") or []
    return rows[0] if rows else None


def get_business_owner_profile_uids(db, business_uid):
    """Return every profile_personal_uid associated with this business via business_user."""
    business_uid = str(business_uid or "").strip()
    if not business_uid:
        return []
    query = """
        SELECT pp.profile_personal_uid
        FROM every_circle.business_user bu
        JOIN every_circle.users u ON u.user_uid = bu.bu_user_id
        JOIN every_circle.profile_personal pp ON pp.profile_personal_user_id = u.user_uid
        WHERE bu.bu_business_id = %s
    """
    res = db.execute(query, (business_uid,))
    rows = res.get("result") or []
    return [row.get("profile_personal_uid") for row in rows if row.get("profile_personal_uid")]


def get_business_owner_profile_uid(db, business_uid):
    """Convenience accessor returning the first associated owner profile UID, if any."""
    owners = get_business_owner_profile_uids(db, business_uid)
    return owners[0] if owners else None


def is_business_owner(db, business_uid, profile_uid):
    profile_uid = str(profile_uid or "").strip()
    if not profile_uid:
        return False
    return profile_uid in {str(u) for u in get_business_owner_profile_uids(db, business_uid)}


def is_business_publicly_visible(row, viewer_is_admin=False):
    if _business_moderated_value(row) == MODERATED_ACKNOWLEDGED:
        return False
    if viewer_is_admin:
        return True
    return _business_moderated_value(row) == MODERATED_ACTIVE


def build_business_snapshot(row):
    """JSON-serializable business fields for content_resubmissions snapshots."""
    if not row:
        return {}

    snapshot = {
        "uid": row.get("business_uid"),
        "name": row.get("business_name"),
        "location": row.get("business_location"),
        "locationIsPublic": row.get("business_location_is_public"),
        "addressLine1": row.get("business_address_line_1"),
        "addressLine2": row.get("business_address_line_2"),
        "city": row.get("business_city"),
        "state": row.get("business_state"),
        "country": row.get("business_country"),
        "zipCode": row.get("business_zip_code"),
        "phoneNumber": row.get("business_phone_number"),
        "phoneNumberIsPublic": row.get("business_phone_number_is_public"),
        "emailId": row.get("business_email_id"),
        "emailIsPublic": row.get("business_email_id_is_public"),
        "shortBio": row.get("business_short_bio"),
        "shortBioIsPublic": row.get("business_short_bio_is_public"),
        "tagLine": row.get("business_tag_line"),
        "tagLineIsPublic": row.get("business_tag_line_is_public"),
        "website": row.get("business_website"),
        "profileImg": row.get("business_profile_img"),
        "profileImgIsPublic": row.get("business_profile_img_is_public"),
        "latitude": _serialize_value(row.get("business_latitude")),
        "longitude": _serialize_value(row.get("business_longitude")),
        "isActive": row.get("business_is_active"),
    }
    return {k: v for k, v in snapshot.items() if v is not None}


def can_business_be_edited(db, business_uid, business=None):
    """Only active businesses can be edited by an owner."""
    business = business or get_business(db, business_uid)
    if not business:
        return False
    return _business_moderated_value(business) == MODERATED_ACTIVE


def build_business_moderation_metadata(db, business_uid):
    business = get_business(db, business_uid)
    moderated = _business_moderated_value(business)
    latest = _get_latest_resubmission(db, business_uid)
    metadata = {
        "flagCount": count_pending_flags(db, business_uid),
        "moderated": moderated,
        "status": _moderation_status_label(moderated, latest),
        "canEdit": can_business_be_edited(db, business_uid, business),
        "reports": get_owner_visible_reports(db, business_uid),
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


def apply_business_takedown_if_threshold(db, target_uid):
    """
    If pending flag count reaches the configured threshold while the business is
    active, hide it from the public and queue it for admin review.

    Returns True when the business is queued for review on this call.
    """
    flag_count = count_pending_flags(db, target_uid)
    if flag_count < _takedown_threshold():
        return False

    business = get_business(db, target_uid)
    if not business:
        return False
    if _business_moderated_value(business) != MODERATED_ACTIVE:
        return False

    snapshot = build_business_snapshot(business)
    resubmission_uid = _create_pending_resubmission(db, target_uid, snapshot)
    if not resubmission_uid:
        return False

    upd_res = db.update(
        _BUSINESS_TABLE,
        {"business_uid": target_uid},
        {
            "business_moderated": MODERATED_PENDING_REVIEW,
            "business_is_active": 0,
        },
    )
    return _db_write_succeeded(upd_res)


def queue_business_for_review(db, business_uid, editor_profile_uid):
    """
    When an owner edits a moderated business, snapshot the current content,
    queue it for admin review, and set moderated = 2 on the same row.
    """
    business = get_business(db, business_uid)
    if not business:
        return {"ok": False, "message": "Business not found"}

    moderated = _business_moderated_value(business)
    if moderated not in (MODERATED_TAKEN_DOWN, MODERATED_PENDING_REVIEW):
        return {"ok": False, "message": "Business is not under moderation"}

    if not can_business_be_edited(db, business_uid, business):
        return {
            "ok": False,
            "message": "This business cannot be edited while it is taken down",
        }

    if editor_profile_uid and not is_business_owner(db, business_uid, editor_profile_uid):
        return {"ok": False, "message": "Only a business owner can resubmit"}

    snapshot = build_business_snapshot(business)
    now = _now_str()
    pending = _get_latest_resubmission(db, business_uid, status="pending")

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
                "resubmission_target_uid": business_uid,
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
        _BUSINESS_TABLE,
        {"business_uid": business_uid},
        {"business_moderated": MODERATED_PENDING_REVIEW},
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to queue business for review"}

    return {
        "ok": True,
        "resubmission_uid": resubmission_uid,
        "snapshot": snapshot,
    }


def approve_business_review(db, target_uid, admin_uid, note=None):
    business = get_business(db, target_uid)
    if not business:
        return {"ok": False, "message": "Business not found"}
    if _business_moderated_value(business) != MODERATED_PENDING_REVIEW:
        return {"ok": False, "message": "Business is not pending admin review"}

    finalized, resubmission_row = _finalize_pending_resubmission(
        db, target_uid, admin_uid, note, "approved"
    )
    if not finalized:
        return {"ok": False, "message": "Failed to approve resubmission"}

    snapshot = {}
    if resubmission_row:
        snapshot = _parse_snapshot(resubmission_row.get("resubmission_snapshot")) or {}

    is_active = int(snapshot.get("isActive", business.get("business_is_active") or 1))
    mod_res = db.update(
        _BUSINESS_TABLE,
        {"business_uid": target_uid},
        {
            "business_moderated": MODERATED_ACTIVE,
            "business_is_active": is_active,
        },
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to restore business"}

    _dismiss_pending_reports(db, target_uid)
    return {"ok": True, "business_uid": target_uid}


def reject_business_review(db, target_uid, admin_uid, note=None):
    business = get_business(db, target_uid)
    if not business:
        return {"ok": False, "message": "Business not found"}
    if _business_moderated_value(business) != MODERATED_PENDING_REVIEW:
        return {"ok": False, "message": "Business is not pending admin review"}

    note_text = str(note or "").strip()
    if not note_text:
        return {"ok": False, "message": "Admin note is required when rejecting a business"}

    finalized, _ = _finalize_pending_resubmission(
        db, target_uid, admin_uid, note_text, "rejected"
    )
    if not finalized:
        return {"ok": False, "message": "Failed to reject resubmission"}

    mod_res = db.update(
        _BUSINESS_TABLE,
        {"business_uid": target_uid},
        {
            "business_moderated": MODERATED_TAKEN_DOWN,
            "business_is_active": 0,
        },
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to reject business"}

    return {
        "ok": True,
        "business_uid": target_uid,
        "rejection_note": note_text,
    }


def acknowledge_business_takedown(db, target_uid, requester_profile_uid):
    """
    Owner acknowledges a rejected / taken-down business.
    Sets business_moderated = 3 so it is no longer returned to the public.
    """
    business = get_business(db, target_uid)
    if not business:
        return {"ok": False, "message": "Business not found", "code": 404}

    if not requester_profile_uid or not is_business_owner(db, target_uid, requester_profile_uid):
        return {
            "ok": False,
            "message": "Only a business owner can acknowledge a takedown",
            "code": 403,
        }

    moderated = _business_moderated_value(business)
    if moderated == MODERATED_ACKNOWLEDGED:
        return {
            "ok": True,
            "business_uid": target_uid,
            "already_acknowledged": True,
        }

    if moderated != MODERATED_TAKEN_DOWN:
        return {
            "ok": False,
            "message": "Only rejected / taken-down businesses can be acknowledged",
            "code": 400,
        }

    latest = _get_latest_resubmission(db, target_uid)
    if not latest or latest.get("resubmission_status") != "rejected":
        return {
            "ok": False,
            "message": "Business must be rejected by an admin before acknowledgment",
            "code": 400,
        }

    mod_res = db.update(
        _BUSINESS_TABLE,
        {"business_uid": target_uid},
        {
            "business_moderated": MODERATED_ACKNOWLEDGED,
            "business_is_active": 0,
        },
    )
    if not _db_write_succeeded(mod_res):
        return {"ok": False, "message": "Failed to acknowledge business", "code": 500}

    return {
        "ok": True,
        "business_uid": target_uid,
        "already_acknowledged": False,
    }
