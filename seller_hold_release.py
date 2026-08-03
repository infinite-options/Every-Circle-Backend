"""
Auto-release seller return-window holds after wt_available_at.

Held partial_delivery_credit rows move from wallet_pending to useable and
flip to status posted. When an open return has active wallet reservations,
only the net amount (held − reserved refund) is released; bounty release
uses the same net logic (pending − reserved reclaim).

Used by SellerHoldReleaseCron_CLASS (Postman) and SellerHoldRelease_CRON (Zappa).
"""

import traceback
from datetime import datetime

from data_ec import connect
from datetime_utils import utc_now_str
from wallet_return_reservations import sum_active_proceeds_reservation
from wallet_transactions_service import (
    WT_STATUS_HELD,
    WT_TYPE_PARTIAL_DELIVERY_CREDIT,
    _ensure_wallet_transactions_table,
    _line_held_proceeds_total,
    release_held_wallet_transaction,
    release_partial_held_wallet_transaction,
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
            "partial": result.get("partial"),
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


def _line_net_releasable_proceeds(db, transaction_uid, ti_uid):
    """Held sale proceeds minus active return refund reservations on the line."""
    held = _line_held_proceeds_total(db, ti_uid)
    reserved = sum_active_proceeds_reservation(
        db, transaction_uid=transaction_uid, ti_uid=ti_uid
    )
    return max(0.0, held - reserved), held, reserved


def release_seller_hold_for_row(db, wt_row, *, release_budget=None):
    """
    Release one held row if eligible.

    When open return reservations exist on the line, releases only the net
    amount (held − reserved refund). Never releases amounts covered by active
    reservations.

    release_budget: optional dict keyed by (transaction_uid, ti_uid) tracking
    how much net releasable has already been moved this cron run.
    """
    wt_uid = wt_row.get("wt_uid") if wt_row else None
    ti_uid = wt_row.get("wt_ti_id") if wt_row else None
    transaction_uid = wt_row.get("wt_transaction_id") if wt_row else None

    net_releasable, held_total, reserved = _line_net_releasable_proceeds(
        db, transaction_uid, ti_uid
    )

    budget_key = (transaction_uid, ti_uid)
    already_released = 0.0
    if release_budget is not None:
        already_released = float(release_budget.get(budget_key) or 0)
    remaining_budget = max(0.0, net_releasable - already_released)

    if remaining_budget <= 0 and reserved > 0:
        return {
            "code": 200,
            "skipped": True,
            "message": "Proceeds fully reserved for open return",
            "wt_uid": wt_uid,
            "wt_ti_id": ti_uid,
            "wt_transaction_id": transaction_uid,
            "held_total": held_total,
            "reserved_refund": reserved,
        }

    row_amount = float(wt_row.get("wt_amount") or 0)
    release_amount = min(row_amount, remaining_budget) if reserved > 0 else row_amount

    if release_amount <= 0:
        return {
            "code": 200,
            "skipped": True,
            "message": "No net proceeds to release",
            "wt_uid": wt_uid,
            "wt_ti_id": ti_uid,
            "wt_transaction_id": transaction_uid,
        }

    if release_amount < row_amount:
        result = release_partial_held_wallet_transaction(db, wt_row, release_amount)
    else:
        result = release_held_wallet_transaction(db, wt_row)

    if (
        release_budget is not None
        and result.get("code") == 200
        and not result.get("skipped")
    ):
        moved = float(result.get("moved_to_useable") or release_amount)
        release_budget[budget_key] = already_released + moved

    return result


def release_seller_holds_for_line(db, transaction_uid, ti_uid):
    """
    Release all eligible held rows for a sale line (e.g. after reservation clear).

    Respects active reservations and wt_available_at eligibility.
    """
    _ensure_wallet_transactions_table(db)
    if not transaction_uid or not ti_uid:
        return {"code": 400, "message": "transaction_uid and ti_uid are required"}

    now = utc_now_str()
    q = db.execute(
        """
        SELECT wt_uid, wt_profile_id, wt_buyer_id, wt_seller_id,
               wt_transaction_id, wt_ti_id, wt_type, wt_status,
               wt_qty, wt_amount, wt_available_at, wt_created_at,
               wt_idempotency_key, wt_received_qty_after, wt_unit_cost,
               wt_currency
        FROM every_circle.wallet_transactions
        WHERE wt_status = %s
          AND wt_type = %s
          AND wt_ti_id = %s
          AND wt_transaction_id = %s
          AND wt_available_at IS NOT NULL
          AND wt_available_at <= %s
        ORDER BY wt_available_at ASC, wt_created_at ASC
        """,
        (
            WT_STATUS_HELD,
            WT_TYPE_PARTIAL_DELIVERY_CREDIT,
            ti_uid,
            transaction_uid,
            now,
        ),
    )
    rows = q.get("result") or []
    released = []
    skipped = []
    release_budget = {}
    for row in rows:
        result = release_seller_hold_for_row(db, row, release_budget=release_budget)
        if result.get("skipped"):
            skipped.append(summarize_hold_release_result(result))
        elif result.get("code") == 200:
            released.append(summarize_hold_release_result(result))
        else:
            skipped.append(summarize_hold_release_result(result))

    return {
        "code": 200,
        "transaction_uid": transaction_uid,
        "ti_uid": ti_uid,
        "released_holds": released,
        "skipped_holds": skipped,
    }


def _eligible_held_query():
    return """
        SELECT wt_uid, wt_profile_id, wt_buyer_id, wt_seller_id,
               wt_transaction_id, wt_ti_id, wt_type, wt_status,
               wt_qty, wt_amount, wt_available_at, wt_created_at,
               wt_idempotency_key, wt_received_qty_after, wt_unit_cost,
               wt_currency
        FROM every_circle.wallet_transactions
        WHERE wt_status = %s
          AND wt_type = %s
          AND wt_available_at IS NOT NULL
          AND wt_available_at <= %s
        ORDER BY wt_available_at ASC, wt_created_at ASC
    """


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
                release_budget = {}

                for row in eligible:
                    result = release_seller_hold_for_row(
                        db, row, release_budget=release_budget
                    )
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
