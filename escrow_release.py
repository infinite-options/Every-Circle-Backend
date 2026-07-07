"""
Auto-release escrow after a configurable number of days.

Used by EscrowReleaseCron_CLASS (Postman) and EscrowRelease_CRON (Zappa).
Manual buyer confirmation can call release_escrow_for_transaction() separately.
"""

import traceback
from collections import defaultdict
from datetime import datetime

from data_ec import connect
from wallet_ids import EC_WALLET_ID, resolve_wallet_profile_id

ESCROW_RELEASE_DAYS = 5


def _get_wallet_row(db, profile_id):
    wallet_id = resolve_wallet_profile_id(profile_id)
    wallet_q = db.execute(
        """
        SELECT
            wallet_profile_id,
            wallet_pending,
            wallet_useable_balance,
            wallet_actual_balance,
            wallet_lifetime_earning
        FROM every_circle.wallet
        WHERE wallet_profile_id = %s
        """,
        (wallet_id,),
    )
    wallets = wallet_q.get("result") or []
    return wallets[0] if wallets else None


def _apply_wallet_release(db, wallet, profile_id, amount):
    """Update an existing wallet row; uses stored PK (may differ from bounty profile_id)."""
    stored_id = wallet.get("wallet_profile_id")
    pending = float(wallet.get("wallet_pending") or 0)
    useable = float(wallet.get("wallet_useable_balance") or 0)
    actual = float(wallet.get("wallet_actual_balance") or 0)
    lifetime = float(wallet.get("wallet_lifetime_earning") or 0)

    from_pending = min(amount, pending)
    updates = {
        "wallet_pending": pending - from_pending,
        "wallet_useable_balance": useable + amount,
    }

    if amount > from_pending and actual < amount:
        updates["wallet_actual_balance"] = actual + (amount - from_pending)
        updates["wallet_lifetime_earning"] = lifetime + (amount - from_pending)

    update_wallet = db.update(
        "every_circle.wallet",
        {"wallet_profile_id": stored_id},
        updates,
    )
    if update_wallet.get("code") != 200:
        return {
            "code": update_wallet.get("code", 500),
            "message": update_wallet.get(
                "message", f"Failed to update wallet for {profile_id}"
            ),
            "wallet_profile_id": profile_id,
            "wallet_pk": stored_id,
        }

    return {
        "code": 200,
        "wallet_profile_id": profile_id,
        "wallet_pk": stored_id,
        "bounty_total": amount,
        "moved_to_useable": amount,
        "from_pending": from_pending,
        "wallet_created": False,
    }


def _suggested_action_for_error(message):
    msg = (message or "").lower()
    if "wallet not found" in msg:
        return (
            "Create or repair the wallet row for the profile listed in the error, "
            "then re-run the cron."
        )
    if "failed to create wallet" in msg:
        return (
            "Check wallet table constraints and that profile_id is valid, "
            "then re-run the cron."
        )
    if "failed to update wallet" in msg or "failed to release wallet" in msg:
        return (
            "Inspect the wallet row for that profile_id in every_circle.wallet "
            "and check API/DB logs."
        )
    if "failed to clear transaction_in_escrow" in msg:
        return (
            "Verify the transaction row exists in every_circle.transactions "
            "and is not locked."
        )
    if "transaction not found" in msg:
        return "Confirm transaction_uid still exists in every_circle.transactions."
    if "failed to query" in msg or "failed to load bounties" in msg:
        return "Database connectivity or SQL error — check RDS and Lambda/EC2 logs."
    if "duplicate entry" in msg and "wallet" in msg:
        return (
            f"Wallet row conflict for platform wallet ({EC_WALLET_ID}) — "
            "verify every_circle.wallet and re-run the cron."
        )
    return "Check API logs, fix the root cause, then re-run GET /api/v1/escrow_release_cron."


def summarize_escrow_result(result):
    """Compact one-line-per-transaction summary for cron JSON and email."""
    tx_uid = result.get("transaction_uid")
    if result.get("code") == 200 and not result.get("skipped"):
        return {"transaction_uid": tx_uid, "message": "bounty released"}
    return {
        "transaction_uid": tx_uid,
        "message": result.get("message", "unknown"),
    }


def _format_tx_line(entry):
    tx_uid = entry.get("transaction_uid", "?")
    message = entry.get("message", "")
    return f"  {tx_uid}  {message}"


def format_escrow_release_email(response, run_dt=None):
    """Plain-text email body for escrow cron success, partial, or full failure."""
    dt = run_dt or datetime.today()
    failed_txs = response.get("failed_transactions") or []
    released_txs = response.get("released_transactions") or []
    skipped_txs = response.get("skipped_transactions") or []
    is_failure = "cron fail" in response

    lines = [
        "=" * 72,
        "EVERY-CIRCLE ESCROW RELEASE CRON",
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
        if released_txs:
            lines.append(
                f"Note: {len(released_txs)} transaction(s) were released before failures occurred."
            )
    else:
        completed = response.get("Escrow Release CRON Job completed") or {}
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
            f"  Escrow release window : {response.get('escrow_release_days', '?')} days",
            f"  Eligible transactions : {response.get('eligible_count', 0)}",
            f"  Released              : {response.get('released_count', 0)}",
            f"  Failed                : {response.get('failed_count', 0)}",
            f"  Skipped               : {response.get('skipped_count', 0)}",
        ]
    )

    if failed_txs:
        lines.extend(["", "-" * 72, "FAILED TRANSACTIONS (by error)", "-" * 72])

        by_message = defaultdict(list)
        for failure in failed_txs:
            by_message[failure.get("message", "Unknown error")].append(
                failure.get("transaction_uid", "?")
            )

        for idx, (message, tx_ids) in enumerate(by_message.items(), start=1):
            lines.extend(
                [
                    "",
                    f"[Failure group {idx}]",
                    f"  Error   : {message}",
                    f"  Count   : {len(tx_ids)}",
                    "  Action  : " + _suggested_action_for_error(message),
                    "  Transactions:",
                ]
            )
            display_ids = sorted(tx for tx in tx_ids if tx)
            for tx_uid in display_ids[:25]:
                lines.append(f"    - {tx_uid}")
            if len(display_ids) > 25:
                lines.append(f"    ... and {len(display_ids) - 25} more")

    if released_txs:
        lines.extend(
            [
                "",
                "-" * 72,
                f"RELEASED TRANSACTIONS ({len(released_txs)})",
                "-" * 72,
            ]
        )
        display = released_txs[:50]
        for entry in display:
            lines.append(_format_tx_line(entry))
        if len(released_txs) > 50:
            lines.append(f"  ... and {len(released_txs) - 50} more")

    if skipped_txs:
        lines.extend(
            [
                "",
                "-" * 72,
                f"SKIPPED TRANSACTIONS ({len(skipped_txs)})",
                "-" * 72,
            ]
        )
        display = skipped_txs[:25]
        for entry in display:
            lines.append(_format_tx_line(entry))
        if len(skipped_txs) > 25:
            lines.append(f"  ... and {len(skipped_txs) - 25} more")

    lines.extend(["", "-" * 72, "NEXT STEPS", "-" * 72])
    if failed_txs:
        lines.extend(
            [
                "  1. Review each failure group above and follow the suggested action.",
                "  2. After fixing data or code, re-run:",
                "       GET /api/v1/escrow_release_cron",
                "  3. Unreleased transactions still have transaction_in_escrow = 1",
                "     and will be picked up on the next run.",
            ]
        )
    else:
        lines.append("  No action required.")

    lines.append("")
    return "\n".join(lines)


def _eligible_transactions_query(days):
    return """
        SELECT transaction_uid, transaction_datetime
        FROM every_circle.transactions
        WHERE transaction_in_escrow = 1
          AND transaction_datetime < NOW() - INTERVAL %s DAY
          AND COALESCE(transaction_return_requested, 0) = 0
        ORDER BY transaction_datetime ASC
    """


def _release_bounty_to_wallet(db, profile_id, amount):
    """
    Move escrowed bounty into wallet_useable_balance.

    Creates a wallet when missing (legacy transactions). If pending is 0 but
    bounty was never credited at purchase time, credits useable/actual balances.
    """
    if not profile_id or amount <= 0:
        return {
            "code": 200,
            "wallet_profile_id": profile_id,
            "bounty_total": amount,
            "moved_to_useable": 0,
            "wallet_created": False,
        }

    wallet = _get_wallet_row(db, profile_id)

    if not wallet:
        insert_id = resolve_wallet_profile_id(profile_id)
        insert_result = db.insert(
            "every_circle.wallet",
            {
                "wallet_profile_id": insert_id,
                "wallet_actual_balance": amount,
                "wallet_pending": 0,
                "wallet_useable_balance": amount,
                "wallet_reserve": 0,
                "wallet_lifetime_earning": amount,
                "wallet_lifetime_spent": 0,
            },
        )
        if insert_result.get("code") != 200:
            insert_msg = insert_result.get("message", "")
            if "duplicate entry" in insert_msg.lower():
                wallet = _get_wallet_row(db, profile_id)
                if wallet:
                    return _apply_wallet_release(db, wallet, profile_id, amount)
            return {
                "code": insert_result.get("code", 500),
                "message": insert_result.get(
                    "message", f"Failed to create wallet for {profile_id}"
                ),
                "wallet_profile_id": profile_id,
            }
        return {
            "code": 200,
            "wallet_profile_id": profile_id,
            "wallet_pk": insert_id,
            "bounty_total": amount,
            "moved_to_useable": amount,
            "wallet_created": True,
        }

    return _apply_wallet_release(db, wallet, profile_id, amount)


def release_escrow_for_transaction(db, transaction_uid, reason="auto_5_day"):
    """
    Move bounty from wallet_pending to wallet_useable_balance and clear escrow.
    Idempotent: no-op if transaction is not in escrow.
    """
    tx_q = db.execute(
        """
        SELECT transaction_uid, transaction_in_escrow, transaction_return_requested
        FROM every_circle.transactions
        WHERE transaction_uid = %s
        """,
        (transaction_uid,),
    )
    tx_rows = tx_q.get("result") or []
    if not tx_rows:
        return {
            "code": 404,
            "message": "Transaction not found",
            "transaction_uid": transaction_uid,
            "reason": reason,
        }

    tx_row = tx_rows[0]
    if int(tx_row.get("transaction_in_escrow") or 0) != 1:
        return {
            "code": 200,
            "message": "Transaction already out of escrow",
            "transaction_uid": transaction_uid,
            "reason": reason,
            "skipped": True,
        }

    if int(tx_row.get("transaction_return_requested") or 0) == 1:
        return {
            "code": 409,
            "message": "Return in progress; escrow not released",
            "transaction_uid": transaction_uid,
            "reason": reason,
            "skipped": True,
        }

    bounty_q = db.execute(
        """
        SELECT tb.tb_profile_id, SUM(tb.tb_amount) AS bounty_total
        FROM every_circle.transactions_bounty tb
        INNER JOIN every_circle.transactions_items ti ON tb.tb_ti_id = ti.ti_uid
        WHERE ti.ti_transaction_id = %s
        GROUP BY tb.tb_profile_id
        """,
        (transaction_uid,),
    )
    if bounty_q.get("code") != 200:
        return {
            "code": bounty_q.get("code", 500),
            "message": bounty_q.get("message", "Failed to load bounties"),
            "transaction_uid": transaction_uid,
            "reason": reason,
        }

    wallet_updates = []
    for row in bounty_q.get("result") or []:
        profile_id = row.get("tb_profile_id")
        amount = float(row.get("bounty_total") or 0)
        if not profile_id or amount <= 0:
            continue

        wallet_result = _release_bounty_to_wallet(db, profile_id, amount)
        if wallet_result.get("code") != 200:
            return {
                "code": wallet_result.get("code", 500),
                "message": wallet_result.get(
                    "message", f"Failed to release wallet for {profile_id}"
                ),
                "transaction_uid": transaction_uid,
                "reason": reason,
            }

        wallet_updates.append(wallet_result)

    update_tx = db.update(
        "every_circle.transactions",
        {"transaction_uid": transaction_uid},
        {"transaction_in_escrow": 0},
    )
    if update_tx.get("code") != 200:
        return {
            "code": update_tx.get("code", 500),
            "message": update_tx.get(
                "message", "Failed to clear transaction_in_escrow"
            ),
            "transaction_uid": transaction_uid,
            "reason": reason,
        }

    return {
        "code": 200,
        "message": "Escrow released",
        "transaction_uid": transaction_uid,
        "reason": reason,
        "wallet_updates": wallet_updates,
    }


def _log_wallet_release(transaction_uid, wallet_updates):
    """Server-side detail for debugging; not included in cron JSON/email."""
    if not wallet_updates:
        print(f"Escrow release {transaction_uid}: escrow cleared, no bounty rows")
        return
    for w in wallet_updates:
        created = " [wallet created]" if w.get("wallet_created") else ""
        print(
            f"Escrow release {transaction_uid}: profile {w.get('wallet_profile_id')} "
            f"moved ${float(w.get('moved_to_useable') or 0):.4f} to useable{created}"
        )


class EscrowReleaseJob:
    """Core escrow auto-release batch job (Postman + Zappa call this)."""

    @classmethod
    def get(cls, days=None):
        release_days = ESCROW_RELEASE_DAYS if days is None else int(days)
        response = {
            "escrow_release_days": release_days,
            "released_transactions": [],
            "failed_transactions": [],
            "skipped_transactions": [],
        }

        try:
            with connect() as db:
                eligible_q = db.execute(
                    _eligible_transactions_query(release_days),
                    (release_days,),
                )
                if eligible_q.get("code") != 200:
                    response["cron fail"] = {
                        "message": eligible_q.get(
                            "message", "Failed to query eligible transactions"
                        ),
                        "code": eligible_q.get("code", 500),
                    }
                    return response

                eligible = eligible_q.get("result") or []
                response["eligible_count"] = len(eligible)

                for row in eligible:
                    tx_uid = row.get("transaction_uid")
                    result = release_escrow_for_transaction(
                        db, tx_uid, reason="auto_5_day"
                    )
                    if result.get("skipped"):
                        response["skipped_transactions"].append(
                            summarize_escrow_result(result)
                        )
                    elif result.get("code") == 200:
                        _log_wallet_release(tx_uid, result.get("wallet_updates"))
                        response["released_transactions"].append(
                            summarize_escrow_result(result)
                        )
                    else:
                        response["failed_transactions"].append(
                            summarize_escrow_result(result)
                        )

                response["released_count"] = len(response["released_transactions"])
                response["failed_count"] = len(response["failed_transactions"])
                response["skipped_count"] = len(response["skipped_transactions"])

                if response["failed_count"] > 0:
                    response["cron fail"] = {
                        "message": (
                            f"{response['failed_count']} transaction(s) failed to release"
                        ),
                        "code": 500,
                    }
                else:
                    response["Escrow Release CRON Job completed"] = {
                        "message": (
                            f"Escrow Release CRON Job completed; "
                            f"{response['released_count']} released, "
                            f"{response['skipped_count']} skipped"
                        ),
                        "code": 200,
                    }

        except Exception as e:
            print(f"Error in EscrowReleaseJob.get: {e}")
            print(traceback.format_exc())
            response["cron fail"] = {
                "message": f"Escrow Release CRON Job failed: {e}",
                "code": 500,
            }

        return response
