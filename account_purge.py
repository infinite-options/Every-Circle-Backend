"""Cron job: permanently purge soft-deleted accounts after the 30-day grace window.

Used by AccountPurgeCron_CLASS (HTTP) — same external-cron pattern as escrow release.
"""

import traceback

from data_ec import connect
from datetime_utils import utc_now_str
from account_deletion import permanently_purge_account


class AccountPurgeJob:
    """Find soft-deleted profiles past purge_scheduled_at and hard-purge them."""

    @staticmethod
    def get():
        response = {
            "purged_count": 0,
            "failed_count": 0,
            "skipped_count": 0,
            "purged_accounts": [],
            "failed_accounts": [],
        }
        try:
            with connect() as db:
                now = utc_now_str()
                rows = db.execute(
                    """
                    SELECT profile_personal_uid, profile_personal_user_id,
                           profile_personal_purge_scheduled_at
                    FROM every_circle.profile_personal
                    WHERE COALESCE(profile_personal_is_deleted, 0) = 1
                      AND profile_personal_user_id IS NOT NULL
                      AND profile_personal_user_id != ''
                      AND profile_personal_purge_scheduled_at IS NOT NULL
                      AND profile_personal_purge_scheduled_at <= %s
                    ORDER BY profile_personal_purge_scheduled_at ASC
                    """,
                    (now,),
                )
                candidates = rows.get("result") or []
                response["candidate_count"] = len(candidates)

                for row in candidates:
                    profile_uid = row.get("profile_personal_uid")
                    user_uid = row.get("profile_personal_user_id")
                    try:
                        confirmation = permanently_purge_account(
                            db, user_uid, profile_uid=profile_uid
                        )
                        response["purged_accounts"].append(
                            {
                                "profile_personal_uid": profile_uid,
                                "user_uid": user_uid,
                                "deleted_at": confirmation.get("deleted_at"),
                            }
                        )
                        response["purged_count"] += 1
                    except Exception as exc:
                        print(
                            f"[ACCOUNT PURGE] Failed profile={profile_uid} "
                            f"user={user_uid}: {exc}"
                        )
                        traceback.print_exc()
                        response["failed_accounts"].append(
                            {
                                "profile_personal_uid": profile_uid,
                                "user_uid": user_uid,
                                "message": str(exc),
                            }
                        )
                        response["failed_count"] += 1

            response["code"] = 200 if response["failed_count"] == 0 else 207
            response["message"] = (
                f"Purged {response['purged_count']} account(s); "
                f"{response['failed_count']} failed"
            )
            return response
        except Exception as exc:
            traceback.print_exc()
            response["cron fail"] = {
                "message": str(exc),
                "code": 500,
            }
            response["code"] = 500
            return response


def format_account_purge_email(response, run_dt=None):
    """Plain-text email body for account purge cron."""
    from datetime import datetime

    dt = run_dt or datetime.today()
    is_failure = "cron fail" in response
    lines = [
        "=" * 72,
        "EVERY-CIRCLE ACCOUNT PURGE CRON",
        f"Run time: {dt}",
        "=" * 72,
        "",
    ]
    if is_failure:
        cron_fail = response.get("cron fail") or {}
        lines.extend(
            [
                "STATUS: FAILED",
                f"Reason: {cron_fail.get('message', 'Unknown error')}",
                "",
            ]
        )
    else:
        lines.extend(
            [
                "STATUS: COMPLETED",
                f"Candidates: {response.get('candidate_count', 0)}",
                f"Purged: {response.get('purged_count', 0)}",
                f"Failed: {response.get('failed_count', 0)}",
                "",
            ]
        )

    for entry in response.get("purged_accounts") or []:
        lines.append(
            f"  purged  {entry.get('profile_personal_uid')}  "
            f"user={entry.get('user_uid')}"
        )
    for entry in response.get("failed_accounts") or []:
        lines.append(
            f"  FAILED  {entry.get('profile_personal_uid')}  "
            f"{entry.get('message')}"
        )
    lines.append("")
    return "\n".join(lines)
