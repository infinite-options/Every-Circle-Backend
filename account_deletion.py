"""Account deletion orchestrator (App Store / Play Store compliant).

Flow:
  1. DELETE /api/v1/account soft-deletes immediately (sets deleted_at, freezes
     wallet, hides the profile) and starts a 30-day grace period.
  2. AccountDeletionPurgeJob / Zappa cron hard-deletes accounts whose
     profile_personal_deleted_at is at least 30 days in the past.

Hard delete removes auth and PII, retains financial ledger rows, and leaves an
anonymized profile_personal tombstone for referral-tree integrity.
"""

import traceback
from datetime import datetime, timedelta, timezone

from flask import request
from flask_restful import Resource

from data_ec import connect, deleteFolder
from datetime_utils import format_utc_iso, parse_stored_datetime, utc_now_str
from profile_status import is_profile_deleted
from business_info import _is_unclaimed_business_role
from wallet_service import freeze_wallet

_PROFILE_PERSONAL_TABLE = "every_circle.profile_personal"
_USERS_TABLE = "every_circle.users"
_DELETION_LOG_TABLE = "every_circle.account_deletion_log"

# Soft-delete grace period before the purge cron hard-deletes the account.
ACCOUNT_DELETION_GRACE_DAYS = 30

_PROFILE_PERSONAL_S3_PREFIX = "profile_personal"
_EXPERTISE_S3_PREFIX = "profile_expertise"
_WISH_S3_PREFIX = "profile_wish"
_EXPERIENCE_S3_PREFIX = "profile_experience"
_EDUCATION_S3_PREFIX = "profile_education"


class AccountDeletionError(Exception):
    """Base error for account deletion failures."""


class ProfileNotFoundError(AccountDeletionError):
    """No profile exists for the given user/profile identifiers."""


class AccountAlreadyDeletedError(AccountDeletionError):
    """The profile is already an anonymized deletion tombstone."""


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


def _mark_profile_scheduled_for_deletion(db, profile_uid, deleted_at):
    """Soft-delete: hide profile and start the grace period (keep PII + auth)."""
    db.execute(
        """
        UPDATE every_circle.profile_personal
        SET profile_personal_email_is_public = 0,
            profile_personal_phone_number_is_public = 0,
            profile_personal_location_is_public = 0,
            profile_personal_image_is_public = 0,
            profile_personal_tag_line_is_public = 0,
            profile_personal_short_bio_is_public = 0,
            profile_personal_resume_is_public = 0,
            profile_personal_allow_banner_ads = 0,
            profile_personal_messages_off = 1,
            profile_personal_messages_allow_transaction = 0,
            profile_personal_experience_is_public = 0,
            profile_personal_education_is_public = 0,
            profile_personal_expertise_is_public = 0,
            profile_personal_wishes_is_public = 0,
            profile_personal_business_is_public = 0,
            profile_personal_social_is_public = 0,
            profile_personal_last_updated_at = %s,
            profile_personal_is_deleted = 1,
            profile_personal_deleted_at = %s
        WHERE profile_personal_uid = %s
        """,
        (deleted_at, deleted_at, profile_uid),
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
            profile_personal_deleted_at = %s
        WHERE profile_personal_uid = %s
        """,
        (deleted_at, deleted_at, profile_uid),
        cmd="post",
    )


def _purge_due_at(deleted_at):
    dt = parse_stored_datetime(deleted_at)
    if dt is None:
        return None
    return dt + timedelta(days=ACCOUNT_DELETION_GRACE_DAYS)


def _is_scheduled_soft_delete(profile_row):
    """True when soft-deleted but auth user is still linked (grace period)."""
    if not is_profile_deleted(profile_row):
        return False
    return bool(str(profile_row.get("profile_personal_user_id") or "").strip())


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


def schedule_account_deletion(db, user_uid, profile_uid=None):
    """
    Soft-delete an account and start the 30-day grace period.

    Sets profile_personal_is_deleted / profile_personal_deleted_at, freezes the
    wallet, and hides the profile. Auth + PII remain until the purge cron runs.
    """
    profile_row = _resolve_profile_row(db, user_uid, profile_uid)
    profile_uid = profile_row.get("profile_personal_uid")
    if not profile_uid:
        raise ProfileNotFoundError("profile_personal_uid missing on profile row")

    if is_profile_deleted(profile_row):
        if _is_scheduled_soft_delete(profile_row):
            deleted_at = profile_row.get("profile_personal_deleted_at")
            purge_at = _purge_due_at(deleted_at)
            return {
                "scheduled": True,
                "already_scheduled": True,
                "deleted_at": format_utc_iso(deleted_at),
                "purge_after": format_utc_iso(purge_at) if purge_at else None,
                "grace_days": ACCOUNT_DELETION_GRACE_DAYS,
                "profile_personal_uid": profile_uid,
                "wallet_frozen": True,
            }
        raise AccountAlreadyDeletedError(f"Profile {profile_uid} is already deleted")

    linked_user = str(profile_row.get("profile_personal_user_id") or "").strip()
    user_uid = str(user_uid or linked_user or "").strip()
    if not user_uid:
        raise ProfileNotFoundError("user_uid is required to schedule account deletion")

    user_rows = db.select(_USERS_TABLE, where={"user_uid": user_uid})
    if not user_rows.get("result"):
        raise ProfileNotFoundError(f"No auth user found for user_uid={user_uid}")

    deleted_at = utc_now_str()
    wallet_frozen = freeze_wallet(db, profile_uid)
    _mark_profile_scheduled_for_deletion(db, profile_uid, deleted_at)
    purge_at = _purge_due_at(deleted_at)

    return {
        "scheduled": True,
        "already_scheduled": False,
        "deleted_at": format_utc_iso(deleted_at),
        "purge_after": format_utc_iso(purge_at) if purge_at else None,
        "grace_days": ACCOUNT_DELETION_GRACE_DAYS,
        "profile_personal_uid": profile_uid,
        "wallet_frozen": wallet_frozen,
    }


def delete_user_account(db, user_uid, profile_uid=None):
    """Permanently delete a user account and associated personal data.

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

    if is_profile_deleted(profile_row) and not _is_scheduled_soft_delete(profile_row):
        raise AccountAlreadyDeletedError(f"Profile {profile_uid} is already deleted")

    linked_user = str(profile_row.get("profile_personal_user_id") or "").strip()
    user_uid = str(user_uid or linked_user or "").strip()
    if not user_uid:
        raise ProfileNotFoundError("user_uid is required to delete account")

    user_rows = db.select(_USERS_TABLE, where={"user_uid": user_uid})
    if not user_rows.get("result"):
        raise ProfileNotFoundError(f"No auth user found for user_uid={user_uid}")
    user_row = user_rows["result"][0]
    user_email = user_row.get("user_email_id")
    user_social_id = user_row.get("user_social_id")

    # Preserve original soft-delete timestamp when purging a scheduled deletion.
    deleted_at = profile_row.get("profile_personal_deleted_at") or utc_now_str()
    if not parse_stored_datetime(deleted_at):
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


def find_accounts_due_for_purge(db, *, now=None, grace_days=ACCOUNT_DELETION_GRACE_DAYS):
    """Profiles soft-deleted at least grace_days ago that still have an auth user."""
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    cutoff = (now - timedelta(days=grace_days)).astimezone(timezone.utc).replace(
        tzinfo=None
    )
    cutoff_str = cutoff.strftime("%Y-%m-%d %H:%M:%S")
    q = db.execute(
        """
        SELECT
            profile_personal_uid,
            profile_personal_user_id,
            profile_personal_deleted_at
        FROM every_circle.profile_personal
        WHERE COALESCE(profile_personal_is_deleted, 0) = 1
          AND profile_personal_deleted_at IS NOT NULL
          AND profile_personal_deleted_at <= %s
          AND profile_personal_user_id IS NOT NULL
          AND TRIM(profile_personal_user_id) <> ''
        ORDER BY profile_personal_deleted_at ASC, profile_personal_uid ASC
        """,
        (cutoff_str,),
    )
    return q.get("result") or []


def summarize_account_purge_result(result):
    """Compact one-line-per-row summary for cron JSON and email."""
    return {
        "profile_personal_uid": result.get("profile_personal_uid"),
        "user_uid": result.get("user_uid"),
        "message": result.get("message", "unknown"),
        "deleted_at": result.get("deleted_at"),
    }


def format_account_deletion_purge_email(response, run_dt=None):
    """Plain-text email body for account-deletion purge cron."""
    dt = run_dt or datetime.today()
    failed = response.get("failed_deletions") or []
    purged = response.get("purged_accounts") or []
    skipped = response.get("skipped_accounts") or []
    is_failure = "cron fail" in response

    lines = [
        "=" * 72,
        "EVERY-CIRCLE ACCOUNT DELETION PURGE CRON",
        f"Run time: {dt}",
        f"Grace days: {ACCOUNT_DELETION_GRACE_DAYS}",
        "=" * 72,
        "",
    ]

    if is_failure:
        cron_fail = response.get("cron fail") or {}
        lines.extend(
            [
                "STATUS: FAILED",
                f"Reason: {cron_fail.get('message', 'Unknown error')}",
            ]
        )
        if purged:
            lines.append(
                f"Note: {len(purged)} account(s) were purged before failures occurred."
            )
    else:
        completed = response.get("Account Deletion Purge CRON Job completed") or {}
        lines.extend(
            [
                "STATUS: SUCCESS",
                f"Summary: {completed.get('message', 'Completed')}",
            ]
        )

    lines.extend(
        [
            "",
            "-" * 72,
            "SUMMARY",
            "-" * 72,
            f"  Eligible soft-deletes : {response.get('eligible_count', 0)}",
            f"  Purged                : {response.get('purged_count', 0)}",
            f"  Failed                : {response.get('failed_count', 0)}",
            f"  Skipped               : {response.get('skipped_count', 0)}",
            "",
        ]
    )

    if purged:
        lines.extend(["-" * 72, "PURGED", "-" * 72])
        for entry in purged:
            lines.append(
                f"  {entry.get('profile_personal_uid')}  user={entry.get('user_uid')}  "
                f"{entry.get('message')}"
            )
        lines.append("")

    if skipped:
        lines.extend(["-" * 72, "SKIPPED", "-" * 72])
        for entry in skipped:
            lines.append(
                f"  {entry.get('profile_personal_uid')}  user={entry.get('user_uid')}  "
                f"{entry.get('message')}"
            )
        lines.append("")

    if failed:
        lines.extend(["-" * 72, "FAILED", "-" * 72])
        for entry in failed:
            lines.append(
                f"  {entry.get('profile_personal_uid')}  user={entry.get('user_uid')}  "
                f"{entry.get('message')}"
            )
        lines.append("")

    lines.extend(
        [
            "-" * 72,
            "Re-run: GET /api/v1/account_deletion_purge_cron",
            "-" * 72,
        ]
    )
    return "\n".join(lines)


class AccountDeletionPurgeJob:
    """Hard-delete soft-deleted accounts past the 30-day grace period."""

    @classmethod
    def get(cls):
        response = {
            "purged_accounts": [],
            "failed_deletions": [],
            "skipped_accounts": [],
        }

        try:
            with connect() as db:
                eligible = find_accounts_due_for_purge(db)
                response["eligible_count"] = len(eligible)

                for row in eligible:
                    profile_uid = row.get("profile_personal_uid")
                    user_uid = str(row.get("profile_personal_user_id") or "").strip()
                    try:
                        if not user_uid:
                            response["skipped_accounts"].append(
                                summarize_account_purge_result(
                                    {
                                        "profile_personal_uid": profile_uid,
                                        "user_uid": None,
                                        "message": "missing profile_personal_user_id",
                                    }
                                )
                            )
                            continue

                        confirmation = delete_user_account(
                            db, user_uid, profile_uid=profile_uid
                        )
                        print(
                            f"Account deletion purge: purged profile={profile_uid} "
                            f"user={user_uid}"
                        )
                        response["purged_accounts"].append(
                            summarize_account_purge_result(
                                {
                                    "profile_personal_uid": profile_uid,
                                    "user_uid": user_uid,
                                    "message": "account purged",
                                    "deleted_at": confirmation.get("deleted_at"),
                                }
                            )
                        )
                    except AccountAlreadyDeletedError as exc:
                        response["skipped_accounts"].append(
                            summarize_account_purge_result(
                                {
                                    "profile_personal_uid": profile_uid,
                                    "user_uid": user_uid,
                                    "message": str(exc),
                                }
                            )
                        )
                    except Exception as exc:
                        print(
                            f"Account deletion purge failed for {profile_uid}: {exc}"
                        )
                        print(traceback.format_exc())
                        response["failed_deletions"].append(
                            summarize_account_purge_result(
                                {
                                    "profile_personal_uid": profile_uid,
                                    "user_uid": user_uid,
                                    "message": str(exc),
                                }
                            )
                        )

                response["purged_count"] = len(response["purged_accounts"])
                response["failed_count"] = len(response["failed_deletions"])
                response["skipped_count"] = len(response["skipped_accounts"])

                if response["failed_count"] > 0:
                    response["cron fail"] = {
                        "message": (
                            f"{response['failed_count']} account deletion(s) failed"
                        ),
                        "code": 500,
                    }
                else:
                    response["Account Deletion Purge CRON Job completed"] = {
                        "message": (
                            f"Account Deletion Purge CRON Job completed; "
                            f"{response['purged_count']} purged, "
                            f"{response['skipped_count']} skipped"
                        ),
                        "code": 200,
                    }

        except Exception as e:
            print(f"Error in AccountDeletionPurgeJob.get: {e}")
            print(traceback.format_exc())
            response["cron fail"] = {
                "message": f"Account Deletion Purge CRON Job failed: {e}",
                "code": 500,
            }

        return response


def delete_user_account_with_connect(user_uid, profile_uid=None):
    """Convenience wrapper that opens its own DB connection."""
    with connect() as db:
        return delete_user_account(db, user_uid, profile_uid=profile_uid)


class AccountDelete(Resource):
    """DELETE /api/v1/account — schedule permanent deletion after a 30-day grace period."""

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

                confirmation = schedule_account_deletion(
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
            "message": (
                f"Your account is scheduled for permanent deletion in "
                f"{ACCOUNT_DELETION_GRACE_DAYS} days."
            ),
            "code": 200,
            "confirmation": confirmation,
        }, 200
