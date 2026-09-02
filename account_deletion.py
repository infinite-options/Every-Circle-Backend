"""Account deletion orchestrator (App Store / Play Store compliant).

Soft-deletes on user request (30-day grace), then permanently purges via cron:
hard-deletes auth and PII, retains financial ledger rows, and leaves an
anonymized profile_personal tombstone for referral-tree integrity.
"""

from datetime import datetime, timedelta, timezone

from flask import request
from flask_restful import Resource

from data_ec import connect, deleteFolder
from datetime_utils import format_utc_iso, utc_now_str
from profile_status import is_permanently_deleted, is_profile_deleted, is_soft_deleted
from business_info import _is_unclaimed_business_role
from wallet_service import freeze_wallet, get_wallet_row, unfreeze_wallet

_PROFILE_PERSONAL_TABLE = "every_circle.profile_personal"
_USERS_TABLE = "every_circle.users"
_DELETION_LOG_TABLE = "every_circle.account_deletion_log"

_PROFILE_PERSONAL_S3_PREFIX = "profile_personal"
_EXPERTISE_S3_PREFIX = "profile_expertise"
_WISH_S3_PREFIX = "profile_wish"
_EXPERIENCE_S3_PREFIX = "profile_experience"
_EDUCATION_S3_PREFIX = "profile_education"

GRACE_DAYS = 30


class AccountDeletionError(Exception):
    """Base error for account deletion failures."""


class ProfileNotFoundError(AccountDeletionError):
    """No profile exists for the given user/profile identifiers."""


class AccountAlreadyDeletedError(AccountDeletionError):
    """The profile is already soft-deleted or permanently purged."""


class AccountPurgeWindowExpiredError(AccountDeletionError):
    """Grace period ended; account must be treated as permanently deleted."""


def _delete_s3_folder(prefix, uid):
    try:
        deleteFolder(prefix, uid)
    except Exception as exc:
        print(f"[ACCOUNT DELETION S3] Error deleting {prefix}/{uid}: {exc}")


def _resolve_profile_row(db, user_uid, profile_uid=None):
    user_uid = str(user_uid or "").strip()
    profile_uid = str(profile_uid or "").strip() or None

    if profile_uid:
        rows = db.select(_PROFILE_PERSONAL_TABLE, where={"profile_personal_uid": profile_uid})
        if not rows.get("result"):
            raise ProfileNotFoundError(f"No profile found for profile_uid={profile_uid}")
        row = rows["result"][0]
        linked_user = str(row.get("profile_personal_user_id") or "").strip()
        if linked_user and user_uid and linked_user != user_uid:
            raise AccountDeletionError("profile_uid does not belong to user_uid")
        return row

    if not user_uid:
        raise ProfileNotFoundError("user_uid is required when profile_uid is omitted")

    rows = db.select(_PROFILE_PERSONAL_TABLE, where={"profile_personal_user_id": user_uid})
    if not rows.get("result"):
        raise ProfileNotFoundError(f"No profile found for user_uid={user_uid}")
    return rows["result"][0]


def _purge_scheduled_at_str(from_dt=None):
    base = from_dt or datetime.now(timezone.utc)
    if base.tzinfo is None:
        base = base.replace(tzinfo=timezone.utc)
    return (base + timedelta(days=GRACE_DAYS)).strftime("%Y-%m-%d %H:%M:%S")


def _delete_profile_personal_s3_assets(profile_uid):
    _delete_s3_folder(_PROFILE_PERSONAL_S3_PREFIX, profile_uid)


def _delete_listing_s3_assets(db, profile_uid):
    expertise_rows = db.select(
        "every_circle.profile_expertise",
        where={"profile_expertise_profile_personal_id": profile_uid},
    )
    for row in expertise_rows.get("result") or []:
        expertise_uid = row.get("profile_expertise_uid")
        if expertise_uid:
            _delete_s3_folder(_EXPERTISE_S3_PREFIX, expertise_uid)

    wish_rows = db.select(
        "every_circle.profile_wish",
        where={"profile_wish_profile_personal_id": profile_uid},
    )
    for row in wish_rows.get("result") or []:
        wish_uid = row.get("profile_wish_uid")
        if wish_uid:
            _delete_s3_folder(_WISH_S3_PREFIX, wish_uid)

    experience_rows = db.select(
        "every_circle.profile_experience",
        where={"profile_experience_profile_personal_id": profile_uid},
    )
    for row in experience_rows.get("result") or []:
        experience_uid = row.get("profile_experience_uid")
        if experience_uid:
            _delete_s3_folder(_EXPERIENCE_S3_PREFIX, experience_uid)

    education_rows = db.select(
        "every_circle.profile_education",
        where={"profile_education_profile_personal_id": profile_uid},
    )
    for row in education_rows.get("result") or []:
        education_uid = row.get("profile_education_uid")
        if education_uid:
            _delete_s3_folder(_EDUCATION_S3_PREFIX, education_uid)


def _expertise_has_transactions(db, expertise_uid):
    sold_check = db.execute(
        "SELECT 1 FROM every_circle.transactions_items WHERE ti_bs_id = %s LIMIT 1",
        (expertise_uid,),
    )
    return bool(sold_check.get("result"))


def _scrub_sold_expertise_pii(db, expertise_uid):
    db.execute(
        """
        UPDATE every_circle.profile_expertise
        SET profile_expertise_title = NULL,
            profile_expertise_description = NULL,
            profile_expertise_details = NULL,
            profile_expertise_image = NULL,
            profile_expertise_image_is_public = 0,
            profile_expertise_location = NULL,
            profile_expertise_latitude = NULL,
            profile_expertise_longitude = NULL,
            profile_expertise_city = NULL,
            profile_expertise_state = NULL,
            profile_expertise_zip = NULL,
            profile_expertise_is_public = 0,
            profile_expertise_is_deleted = 1
        WHERE profile_expertise_uid = %s
        """,
        (expertise_uid,),
        cmd="post",
    )


def _delete_listings(db, profile_uid):
    expertise_rows = db.select(
        "every_circle.profile_expertise",
        where={"profile_expertise_profile_personal_id": profile_uid},
    )
    for row in expertise_rows.get("result") or []:
        expertise_uid = row.get("profile_expertise_uid")
        if not expertise_uid:
            continue
        if _expertise_has_transactions(db, expertise_uid):
            _scrub_sold_expertise_pii(db, expertise_uid)
        else:
            db.delete(
                f"DELETE FROM every_circle.profile_expertise "
                f"WHERE profile_expertise_uid = '{expertise_uid}'"
            )

    db.delete(
        f"DELETE FROM every_circle.profile_wish "
        f"WHERE profile_wish_profile_personal_id = '{profile_uid}'"
    )


def _hide_public_listings(db, profile_uid):
    """Hide offerings/wishes during soft-delete without destroying rows."""
    db.execute(
        """
        UPDATE every_circle.profile_expertise
        SET profile_expertise_is_public = 0
        WHERE profile_expertise_profile_personal_id = %s
        """,
        (profile_uid,),
        cmd="post",
    )
    db.execute(
        """
        UPDATE every_circle.profile_wish
        SET profile_wish_is_public = 0
        WHERE profile_wish_profile_personal_id = %s
        """,
        (profile_uid,),
        cmd="post",
    )


def _delete_profile_children(db, profile_uid):
    db.delete(
        f"DELETE FROM every_circle.profile_experience "
        f"WHERE profile_experience_profile_personal_id = '{profile_uid}'"
    )
    db.delete(
        f"DELETE FROM every_circle.profile_education "
        f"WHERE profile_education_profile_personal_id = '{profile_uid}'"
    )
    db.delete(
        f"DELETE FROM every_circle.profile_link "
        f"WHERE profile_link_profile_personal_id = '{profile_uid}'"
    )
    db.execute(
        "DELETE FROM every_circle.social_link WHERE social_link_personal_profile_id = %s",
        (profile_uid,),
        cmd="post",
    )


def _delete_conversations_and_messages(db, profile_uid):
    conv_rows = db.execute(
        """
        SELECT conversation_uid
        FROM every_circle.conversations
        WHERE participant_a_uid = %s OR participant_b_uid = %s
        """,
        (profile_uid, profile_uid),
    )
    conv_uids = [
        row.get("conversation_uid")
        for row in (conv_rows.get("result") or [])
        if row.get("conversation_uid")
    ]
    if not conv_uids:
        return

    placeholders = ", ".join(["%s"] * len(conv_uids))
    db.execute(
        f"DELETE FROM every_circle.messages WHERE message_conversation_id IN ({placeholders})",
        tuple(conv_uids),
        cmd="post",
    )
    db.execute(
        f"DELETE FROM every_circle.conversations WHERE conversation_uid IN ({placeholders})",
        tuple(conv_uids),
        cmd="post",
    )


def _delete_views_and_blocks(db, profile_uid):
    db.execute(
        """
        DELETE FROM every_circle.profile_views
        WHERE view_profile_id = %s OR view_viewer_id = %s
        """,
        (profile_uid, profile_uid),
        cmd="post",
    )
    db.execute(
        """
        DELETE FROM every_circle.blocked_users
        WHERE blocker_uid = %s OR blocked_uid = %s
        """,
        (profile_uid, profile_uid),
        cmd="post",
    )


def _delete_owned_circles(db, profile_uid):
    db.delete(
        f"DELETE FROM every_circle.circles WHERE circle_profile_id = '{profile_uid}'"
    )


def _delete_ratings_on_profile(db, profile_uid):
    db.execute(
        "DELETE FROM every_circle.ratings WHERE rating_profile_id = %s",
        (profile_uid,),
        cmd="post",
    )


def _delete_feedback_for_user(db, user_uid, profile_uid):
    db.execute(
        """
        DELETE FROM every_circle.feedback
        WHERE feedback_user_uid IN (%s, %s)
        """,
        (user_uid, profile_uid),
        cmd="post",
    )


def _count_real_business_owners(db, business_uid, exclude_user_uid=None):
    rows = db.select(
        "every_circle.business_user",
        where={"bu_business_id": business_uid},
    )
    count = 0
    for row in rows.get("result") or []:
        if exclude_user_uid and str(row.get("bu_user_id") or "") == str(exclude_user_uid):
            continue
        if not _is_unclaimed_business_role(row.get("bu_role")):
            count += 1
    return count


def _remove_business_membership(db, user_uid):
    membership_rows = db.select(
        "every_circle.business_user",
        where={"bu_user_id": user_uid},
    )
    businesses_to_check = set()
    for row in membership_rows.get("result") or []:
        business_uid = row.get("bu_business_id")
        role = row.get("bu_role")
        if business_uid and not _is_unclaimed_business_role(role):
            businesses_to_check.add(business_uid)

    for business_uid in businesses_to_check:
        if _count_real_business_owners(db, business_uid, exclude_user_uid=user_uid) == 0:
            db.execute(
                "UPDATE every_circle.business SET business_is_active = 0 WHERE business_uid = %s",
                (business_uid,),
                cmd="post",
            )

    db.execute(
        "DELETE FROM every_circle.business_user WHERE bu_user_id = %s",
        (user_uid,),
        cmd="post",
    )


def _anonymize_profile_tombstone(db, profile_uid, deleted_at):
    db.execute(
        """
        UPDATE every_circle.profile_personal
        SET profile_personal_user_id = NULL,
            profile_personal_first_name = NULL,
            profile_personal_last_name = NULL,
            profile_personal_email_is_public = 0,
            profile_personal_phone_number = NULL,
            profile_personal_phone_number_is_public = 0,
            profile_personal_city = NULL,
            profile_personal_state = NULL,
            profile_personal_country = NULL,
            profile_personal_location_is_public = 0,
            profile_personal_latitude = NULL,
            profile_personal_longitude = NULL,
            profile_personal_home_address = NULL,
            profile_personal_image = NULL,
            profile_personal_image_is_public = 0,
            profile_personal_tag_line = NULL,
            profile_personal_tag_line_is_public = 0,
            profile_personal_short_bio = NULL,
            profile_personal_short_bio_is_public = 0,
            profile_personal_resume = NULL,
            profile_personal_resume_is_public = 0,
            profile_personal_notification_preference = NULL,
            profile_personal_location_preference = NULL,
            profile_personal_allow_banner_ads = 0,
            profile_personal_banner_ads_bounty = NULL,
            profile_personal_messages_off = 1,
            profile_personal_messages_off_at = NULL,
            profile_personal_messages_receive_from = NULL,
            profile_personal_messages_receive_types = NULL,
            profile_personal_messages_allow_transaction = 0,
            profile_personal_experience_is_public = 0,
            profile_personal_education_is_public = 0,
            profile_personal_expertise_is_public = 0,
            profile_personal_wishes_is_public = 0,
            profile_personal_business_is_public = 0,
            profile_personal_social_is_public = 0,
            profile_personal_nearby_lat = NULL,
            profile_personal_nearby_lng = NULL,
            profile_personal_nearby_updated_at = NULL,
            profile_personal_nearby_share_with = NULL,
            profile_personal_nearby_share_types = NULL,
            profile_personal_last_updated_at = %s,
            profile_personal_is_deleted = 1,
            profile_personal_deleted_at = %s,
            profile_personal_purge_scheduled_at = NULL
        WHERE profile_personal_uid = %s
        """,
        (deleted_at, deleted_at, profile_uid),
        cmd="post",
    )


def _delete_auth_user(db, user_uid):
    db.execute(
        f"DELETE FROM {_USERS_TABLE} WHERE user_uid = %s",
        (user_uid,),
        cmd="post",
    )


def _log_account_deletion(db, user_uid, profile_uid, deleted_at, *, user_email=None, user_social_id=None):
    try:
        db.execute(
            f"""
            INSERT INTO {_DELETION_LOG_TABLE}
                (user_uid, profile_personal_uid, deleted_at, user_email_id, user_social_id)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (user_uid, profile_uid, deleted_at, user_email, user_social_id),
            cmd="post",
        )
    except Exception as exc:
        print(f"[ACCOUNT DELETION] Failed to write deletion log: {exc}")
        try:
            db.execute(
                f"""
                INSERT INTO {_DELETION_LOG_TABLE}
                    (user_uid, profile_personal_uid, deleted_at)
                VALUES (%s, %s, %s)
                """,
                (user_uid, profile_uid, deleted_at),
                cmd="post",
            )
        except Exception as fallback_exc:
            print(f"[ACCOUNT DELETION] Failed to write deletion log (fallback): {fallback_exc}")


def soft_delete_user_account(db, user_uid, profile_uid=None):
    """Schedule account deletion (30-day grace). Keeps PII and auth until purge.

    Returns a confirmation dict suitable for API responses.
    """
    profile_row = _resolve_profile_row(db, user_uid, profile_uid)
    profile_uid = profile_row.get("profile_personal_uid")
    if not profile_uid:
        raise ProfileNotFoundError("profile_personal_uid missing on profile row")

    if is_profile_deleted(profile_row):
        raise AccountAlreadyDeletedError(f"Profile {profile_uid} is already deleted")

    user_rows = db.select(_USERS_TABLE, where={"user_uid": user_uid})
    if not user_rows.get("result"):
        raise ProfileNotFoundError(f"No auth user found for user_uid={user_uid}")

    deleted_at = utc_now_str()
    purge_at = _purge_scheduled_at_str()

    db.execute(
        """
        UPDATE every_circle.profile_personal
        SET profile_personal_is_deleted = 1,
            profile_personal_deleted_at = %s,
            profile_personal_purge_scheduled_at = %s
        WHERE profile_personal_uid = %s
        """,
        (deleted_at, purge_at, profile_uid),
        cmd="post",
    )

    _hide_public_listings(db, profile_uid)
    wallet_frozen = freeze_wallet(db, profile_uid)

    return {
        "deleted_at": format_utc_iso(deleted_at),
        "purge_scheduled_at": format_utc_iso(purge_at),
        "grace_days": GRACE_DAYS,
        "profile_personal_uid": profile_uid,
        "wallet_frozen": wallet_frozen,
        "personal_data_deleted": False,
        "financial_records_retained": True,
        "reactivation_available": True,
    }


def permanently_purge_account(db, user_uid, profile_uid=None):
    """Irreversibly purge PII/auth after the soft-delete grace window.

    Steps (order matters for FK integrity):
      1. Resolve profile_uid
      2. Delete S3 assets
      3. Delete/scrub listings and child profile tables
      4. Delete social/messaging, views, blocks, owned circles
      5. Remove business membership (deactivate sole-owner businesses)
      6. Freeze wallet
      7. Anonymize profile_personal tombstone
      8. Delete users row
      9. Log deletion event

    Returns a confirmation dict suitable for API responses.
    """
    profile_row = _resolve_profile_row(db, user_uid, profile_uid)
    profile_uid = profile_row.get("profile_personal_uid")
    if not profile_uid:
        raise ProfileNotFoundError("profile_personal_uid missing on profile row")

    if is_permanently_deleted(profile_row) and not str(
        profile_row.get("profile_personal_user_id") or ""
    ).strip():
        raise AccountAlreadyDeletedError(
            f"Profile {profile_uid} is already permanently deleted"
        )

    linked_user = str(profile_row.get("profile_personal_user_id") or "").strip()
    user_uid = linked_user or str(user_uid or "").strip()
    if not user_uid:
        raise ProfileNotFoundError(f"No auth user linked for profile_uid={profile_uid}")

    user_rows = db.select(_USERS_TABLE, where={"user_uid": user_uid})
    user_row = (user_rows.get("result") or [None])[0] or {}
    user_email = user_row.get("user_email_id")
    user_social_id = user_row.get("user_social_id")

    deleted_at = utc_now_str()

    _delete_profile_personal_s3_assets(profile_uid)
    _delete_listing_s3_assets(db, profile_uid)
    _delete_listings(db, profile_uid)
    _delete_profile_children(db, profile_uid)
    _delete_conversations_and_messages(db, profile_uid)
    _delete_views_and_blocks(db, profile_uid)
    _delete_owned_circles(db, profile_uid)
    _delete_ratings_on_profile(db, profile_uid)
    _delete_feedback_for_user(db, user_uid, profile_uid)
    _remove_business_membership(db, user_uid)

    wallet_frozen = freeze_wallet(db, profile_uid)
    _anonymize_profile_tombstone(db, profile_uid, deleted_at)
    if user_rows.get("result"):
        _delete_auth_user(db, user_uid)
    _log_account_deletion(
        db,
        user_uid,
        profile_uid,
        deleted_at,
        user_email=user_email,
        user_social_id=user_social_id,
    )

    return {
        "deleted_at": format_utc_iso(deleted_at),
        "profile_personal_uid": profile_uid,
        "wallet_frozen": wallet_frozen,
        "financial_records_retained": True,
        "personal_data_deleted": True,
    }


def delete_user_account(db, user_uid, profile_uid=None):
    """Soft-delete entry point used by DELETE /api/v1/account."""
    return soft_delete_user_account(db, user_uid, profile_uid=profile_uid)


def delete_user_account_with_connect(user_uid, profile_uid=None):
    """Convenience wrapper that opens its own DB connection."""
    with connect() as db:
        return delete_user_account(db, user_uid, profile_uid=profile_uid)


def reactivate_user_account(db, email, password):
    """Clear soft-delete flags and unfreeze wallet after explicit confirmation."""
    from auth import (
        _load_user_for_login,
        _normalize_email,
        _profile_for_user,
        verify_password,
    )

    email = _normalize_email(email)
    if not email or not password:
        raise AccountDeletionError("email and password are required")

    user = _load_user_for_login(db, email)
    if not user:
        raise ProfileNotFoundError("No account found for this email")

    profile = _profile_for_user(db, user["user_uid"])
    if not profile:
        raise ProfileNotFoundError(f"No profile found for user_uid={user['user_uid']}")

    if is_permanently_deleted(profile):
        raise AccountPurgeWindowExpiredError("Account deleted")

    if not is_soft_deleted(profile):
        raise AccountDeletionError("Account is not scheduled for deletion")

    if not verify_password(
        password,
        user.get("user_password_salt"),
        user.get("user_password_hash"),
    ):
        raise AccountDeletionError("Invalid email or password")

    profile_uid = profile.get("profile_personal_uid")
    reactivated_at = utc_now_str()

    db.execute(
        """
        UPDATE every_circle.profile_personal
        SET profile_personal_is_deleted = 0,
            profile_personal_deleted_at = NULL,
            profile_personal_purge_scheduled_at = NULL
        WHERE profile_personal_uid = %s
        """,
        (profile_uid,),
        cmd="post",
    )

    unfreeze_wallet(db, profile_uid)
    wallet_row = get_wallet_row(db, profile_uid)
    wallet_is_frozen = bool(wallet_row and int(wallet_row.get("wallet_is_frozen") or 0))

    return {
        "user": user,
        "profile": profile,
        "confirmation": {
            "reactivated_at": format_utc_iso(reactivated_at),
            "profile_personal_uid": profile_uid,
            "wallet_frozen": wallet_is_frozen,
        },
    }


class AccountDelete(Resource):
    """DELETE /api/v1/account — soft-delete the authenticated user's account (30-day grace)."""

    def delete(self):
        payload = request.get_json(silent=True) or {}

        if not payload.get("confirm_deletion"):
            return {
                "message": "confirm_deletion must be true to delete your account",
                "code": 400,
            }, 400

        from auth import bind_actor, get_current_user_uid, jwt_auth_required

        requested = payload.get("user_uid") or payload.get("profile_uid")
        if jwt_auth_required() and not requested:
            requested = get_current_user_uid()
        _, error = bind_actor(requested)
        if error:
            return error, error["code"]

        user_uid = get_current_user_uid() if jwt_auth_required() else payload.get("user_uid")
        if not user_uid and requested and str(requested).startswith("100-"):
            user_uid = requested

        profile_uid = payload.get("profile_uid")
        if profile_uid and str(profile_uid).startswith("110-"):
            profile_uid = str(profile_uid).strip()
        else:
            profile_uid = None

        try:
            with connect() as db:
                if not user_uid and profile_uid:
                    profile_row = _resolve_profile_row(db, "", profile_uid)
                    user_uid = profile_row.get("profile_personal_user_id")
                if not user_uid:
                    return {"message": "user_uid is required", "code": 400}, 400

                confirmation = soft_delete_user_account(
                    db,
                    str(user_uid).strip(),
                    profile_uid=profile_uid,
                )
        except ProfileNotFoundError as exc:
            return {"message": str(exc), "code": 404}, 404
        except AccountAlreadyDeletedError as exc:
            return {"message": str(exc), "code": 409}, 409
        except AccountDeletionError as exc:
            return {"message": str(exc), "code": 400}, 400
        except Exception as exc:
            print(f"AccountDelete error: {exc}")
            return {"message": "Internal Server Error", "code": 500}, 500

        return {
            "message": "Your account is scheduled for deletion.",
            "code": 200,
            "confirmation": confirmation,
        }, 200


class AccountReactivate(Resource):
    """POST /api/v1/account/reactivate — restore a soft-deleted account (public, password gate)."""

    def post(self):
        payload = request.get_json(silent=True) or {}

        if not payload.get("confirm_reactivation"):
            return {
                "message": "confirm_reactivation must be true to reactivate your account",
                "code": 400,
            }, 400

        email = payload.get("email")
        password = payload.get("password") or ""
        if not email or not password:
            return {"message": "email and password are required", "code": 400}, 400

        try:
            with connect() as db:
                result = reactivate_user_account(db, email, password)
            from auth import _auth_success

            body, code = _auth_success(result["user"], result["profile"])
            body["message"] = "Your account has been reactivated."
            body["confirmation"] = result["confirmation"]
            return body, code
        except AccountPurgeWindowExpiredError as exc:
            return {"message": str(exc), "code": 410}, 410
        except ProfileNotFoundError as exc:
            return {"message": str(exc), "code": 404}, 404
        except AccountDeletionError as exc:
            msg = str(exc)
            if msg == "Invalid email or password":
                return {"message": msg, "code": 401}, 401
            return {"message": msg, "code": 400}, 400
        except Exception as exc:
            print(f"AccountReactivate error: {exc}")
            return {"message": "Internal Server Error", "code": 500}, 500
