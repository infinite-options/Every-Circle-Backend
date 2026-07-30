"""
Auto-release seller return-window holds after wt_available_at.

Held partial_delivery_credit rows move from wallet_pending to useable and
flip to status posted. Lines with an open return request are skipped.

Used by SellerHoldReleaseCron_CLASS (Postman) and SellerHoldRelease_CRON (Zappa).
"""

import traceback
from datetime import datetime

from data_ec import connect
from datetime_utils import utc_now_str
from wallet_transactions_service import (
    WT_STATUS_HELD,
    WT_TYPE_PARTIAL_DELIVERY_CREDIT,
    _ensure_wallet_transactions_table,
    release_held_wallet_transaction,
)


def _suggested_action_for_error(message):
    msg = (message or "").lower()
    if "wallet not found" in msg:
        return (
            "Create or repair the wallet row for the profile listed in the error, "
            "then re-run the cron."
        )
    if "failed to release seller hold" in msg or "failed to update wallet" in msg:
        return (
            "Inspect the wallet row for that profile_id in every_circle.wallet "
            "and check API/DB logs."
        )
    if "failed to mark wallet_transactions" in msg:
        return (
            "Verify the wallet_transactions row still exists and is not locked, "
            "then re-run the cron."
        )
    if "failed to query" in msg:
        return "Database connectivity or SQL error — check RDS and Lambda/EC2 logs."
    return (
        "Check API logs, fix the root cause, then re-run "
        "GET /api/v1/seller_hold_release_cron."
    )


def summarize_hold_release_result(result):
    """Compact one-line-per-row summary for cron JSON and email."""
    wt_uid = result.get("wt_uid")
    if result.get("code") == 200 and not result.get("skipped"):
        return {
            "wt_uid": wt_uid,
            "wt_ti_id": result.get("wt_ti_id"),
            "message": "hold released",
            "moved_to_useable": result.get("moved_to_useable"),
        }
    return {
        "wt_uid": wt_uid,
        "wt_ti_id": result.get("wt_ti_id"),
        "message": result.get("message", "unknown"),
    }


def _format_row_line(entry):
    wt_uid = entry.get("wt_uid", "?")
    ti_id = entry.get("wt_ti_id") or "-"
    message = entry.get("message", "")
    return f"  {wt_uid}  ti={ti_id}  {message}"


def format_seller_hold_release_email(response, run_dt=None):
    """Plain-text email body for seller-hold cron success, partial, or full failure."""
    dt = run_dt or datetime.today()
    failed = response.get("failed_holds") or []
    released = response.get("released_holds") or []
    skipped = response.get("skipped_holds") or []
    is_failure = "cron fail" in response

    lines = [
        "=" * 72,
        "EVERY-CIRCLE SELLER HOLD RELEASE CRON",
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
            ]
        )
        if released:
            lines.append(
                f"Note: {len(released)} hold(s) were released before failures occurred."
            )
    else:
        completed = response.get("Seller Hold Release CRON Job completed") or {}
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
            f"  Eligible held rows : {response.get('eligible_count', 0)}",
            f"  Released           : {response.get('released_count', 0)}",
            f"  Failed             : {response.get('failed_count', 0)}",
            f"  Skipped            : {response.get('skipped_count', 0)}",
            "",
        ]
    )

    if released:
        lines.extend(["-" * 72, "RELEASED", "-" * 72])
        lines.extend(_format_row_line(e) for e in released)
        lines.append("")

    if skipped:
        lines.extend(["-" * 72, "SKIPPED", "-" * 72])
        lines.extend(_format_row_line(e) for e in skipped)
        lines.append("")

    if failed:
        lines.extend(["-" * 72, "FAILED", "-" * 72])
        for entry in failed:
            lines.append(_format_row_line(entry))
            action = _suggested_action_for_error(entry.get("message"))
            lines.append(f"    → {action}")
        lines.append("")

    lines.extend(
        [
            "-" * 72,
            "Re-run: GET /api/v1/seller_hold_release_cron",
            "-" * 72,
        ]
    )
    return "\n".join(lines)


def _open_return_covers_line(req, ti_uid):
    """True when an open TRR includes this sale line."""
    if not ti_uid or not req:
        return False
    if req.get("trr_ti_uid"):
        return req.get("trr_ti_uid") == ti_uid
    for entry in req.get("items") or []:
        if entry.get("transaction_item_uid") == ti_uid:
            return True
    return False


def _has_open_return_for_line(db, transaction_uid, ti_uid):
    """
    Skip release while any open return request covers this line.

    Uses the same open-status concept as returns (lazy import to avoid
    pulling transactions at module import time).
    """
    if not transaction_uid or not ti_uid:
        return False
    from transactions import _load_open_return_requests

    for req in _load_open_return_requests(db, transaction_uid):
        if _open_return_covers_line(req, ti_uid):
            return True
    return False


def _eligible_held_query():
    return """
        SELECT wt_uid, wt_profile_id, wt_buyer_id, wt_seller_id,
               wt_transaction_id, wt_ti_id, wt_type, wt_status,
               wt_qty, wt_amount, wt_available_at, wt_created_at
        FROM every_circle.wallet_transactions
        WHERE wt_status = %s
          AND wt_type = %s
          AND wt_available_at IS NOT NULL
          AND wt_available_at <= %s
        ORDER BY wt_available_at ASC, wt_created_at ASC
    """


def release_seller_hold_for_row(db, wt_row):
    """
    Release one held row if eligible. Skips when an open return covers the line.
    """
    wt_uid = wt_row.get("wt_uid") if wt_row else None
    ti_uid = wt_row.get("wt_ti_id") if wt_row else None
    transaction_uid = wt_row.get("wt_transaction_id") if wt_row else None

    if _has_open_return_for_line(db, transaction_uid, ti_uid):
        return {
            "code": 200,
            "skipped": True,
            "message": "Open return on line; hold not released",
            "wt_uid": wt_uid,
            "wt_ti_id": ti_uid,
            "wt_transaction_id": transaction_uid,
        }

    return release_held_wallet_transaction(db, wt_row)


def _log_hold_release(result):
    """Server-side detail for debugging; not included in cron JSON/email."""
    wt_uid = result.get("wt_uid")
    moved = float(result.get("moved_to_useable") or 0)
    profile = result.get("wt_profile_id") or "?"
    print(
        f"Seller hold release {wt_uid}: profile {profile} "
        f"moved ${moved:.4f} to useable"
    )


class SellerHoldReleaseJob:
    """Core seller-hold auto-release batch job (Postman + Zappa call this)."""

    @classmethod
    def get(cls):
        response = {
            "released_holds": [],
            "failed_holds": [],
            "skipped_holds": [],
        }

        try:
            with connect() as db:
                _ensure_wallet_transactions_table(db)
                now = utc_now_str()
                eligible_q = db.execute(
                    _eligible_held_query(),
                    (WT_STATUS_HELD, WT_TYPE_PARTIAL_DELIVERY_CREDIT, now),
                )
                if eligible_q.get("code") != 200:
                    response["cron fail"] = {
                        "message": eligible_q.get(
                            "message", "Failed to query eligible held rows"
                        ),
                        "code": eligible_q.get("code", 500),
                    }
                    return response

                eligible = eligible_q.get("result") or []
                response["eligible_count"] = len(eligible)

                for row in eligible:
                    result = release_seller_hold_for_row(db, row)
                    if result.get("skipped"):
                        response["skipped_holds"].append(
                            summarize_hold_release_result(result)
                        )
                    elif result.get("code") == 200:
                        _log_hold_release(result)
                        response["released_holds"].append(
                            summarize_hold_release_result(result)
                        )
                    else:
                        response["failed_holds"].append(
                            summarize_hold_release_result(result)
                        )

                response["released_count"] = len(response["released_holds"])
                response["failed_count"] = len(response["failed_holds"])
                response["skipped_count"] = len(response["skipped_holds"])

                if response["failed_count"] > 0:
                    response["cron fail"] = {
                        "message": (
                            f"{response['failed_count']} hold(s) failed to release"
                        ),
                        "code": 500,
                    }
                else:
                    response["Seller Hold Release CRON Job completed"] = {
                        "message": (
                            f"Seller Hold Release CRON Job completed; "
                            f"{response['released_count']} released, "
                            f"{response['skipped_count']} skipped"
                        ),
                        "code": 200,
                    }

        except Exception as e:
            print(f"Error in SellerHoldReleaseJob.get: {e}")
            print(traceback.format_exc())
            response["cron fail"] = {
                "message": f"Seller Hold Release CRON Job failed: {e}",
                "code": 500,
            }

        return response
