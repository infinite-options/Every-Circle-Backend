"""
Wallet ledger: unified history of balance-affecting events for a profile.

GET /api/v1/wallet_ledger/<profile_id>
"""

import traceback

from flask import request
from flask_restful import Resource

from data_ec import connect
from datetime_utils import enrich_datetime_fields
from wallet_ids import resolve_wallet_profile_id
from wallet_service import _round_money, _to_float, get_wallet_row


def _request_timezone():
    return request.args.get("timezone") or request.args.get("tz")


def _parse_pagination():
    try:
        limit = int(request.args.get("limit", 100))
    except (TypeError, ValueError):
        limit = 100
    try:
        offset = int(request.args.get("offset", 0))
    except (TypeError, ValueError):
        offset = 0
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    return limit, offset


def _entry_type_label(entry_type):
    labels = {
        "bounty_earned": "Bounty earned",
        "bounty_reversal": "Bounty reversed",
        "sale_proceeds": "Sale proceeds",
        "sale_proceeds_held": "Sale proceeds (pending)",
        "sale_proceeds_clawback": "Sale proceeds clawback",
        "wallet_payment": "Wallet payment",
        "wallet_refund": "Wallet refund",
    }
    return labels.get(entry_type, entry_type.replace("_", " ").title())


def _normalize_bounty_entry(row):
    amount = _round_money(row.get("amount"))
    tx_type = (row.get("transaction_type") or "sale").lower()
    if amount < 0 or tx_type == "return":
        entry_type = "bounty_reversal"
        availability = "useable"
        useable_delta = amount
    else:
        entry_type = "bounty_earned"
        if row.get("ti_bounty_released_at"):
            availability = "useable"
            useable_delta = amount
        else:
            order_qty = int(row.get("ti_bs_qty") or 0)
            received_qty = int(row.get("ti_received_qty") or 0)
            if order_qty > 0 and received_qty >= order_qty:
                # Verified but return-window hold not yet released
                availability = "pending"
                useable_delta = 0.0
            else:
                availability = "pending"
                useable_delta = 0.0

    counterparty = row.get("counterparty_name") or "Unknown"
    if entry_type == "bounty_reversal":
        description = f"Bounty reversed on return — {counterparty}"
    elif availability == "pending" and int(row.get("ti_received_qty") or 0) >= int(
        row.get("ti_bs_qty") or 0
    ) and int(row.get("ti_bs_qty") or 0) > 0:
        description = f"Bounty earned (pending return window) — {counterparty}"
    else:
        description = f"Bounty earned — {counterparty}"

    return {
        "entry_id": f"bounty:{row.get('ti_uid')}:{row.get('transaction_uid')}",
        "entry_source": "transactions_bounty",
        "entry_type": entry_type,
        "entry_type_label": _entry_type_label(entry_type),
        "amount": amount,
        "useable_delta": useable_delta,
        "availability": availability,
        "currency": "USD",
        "transaction_uid": row.get("transaction_uid"),
        "transaction_original_uid": row.get("transaction_original_uid"),
        "transaction_type": tx_type,
        "entry_datetime": row.get("transaction_datetime"),
        "description": description,
        "counterparty_name": counterparty,
        "purchaser_name": row.get("purchaser_name"),
        "ti_uid": row.get("ti_uid"),
        "ti_received_qty": row.get("ti_received_qty"),
        "ti_bs_qty": row.get("ti_bs_qty"),
        "bounty_released_at": row.get("ti_bounty_released_at"),
    }


def _normalize_wallet_transaction_entry(row):
    amount = _round_money(row.get("wt_amount"))
    wt_type = row.get("wt_type") or ""
    wt_status = row.get("wt_status") or "posted"

    if wt_type == "return_clawback":
        entry_type = "sale_proceeds_clawback"
    elif wt_status == "held":
        entry_type = "sale_proceeds_held"
    else:
        entry_type = "sale_proceeds"

    availability = "pending" if wt_status == "held" else "useable"
    useable_delta = amount if availability == "useable" else 0.0

    buyer = row.get("buyer_name") or row.get("wt_buyer_id") or "buyer"
    if entry_type == "sale_proceeds_clawback":
        description = f"Sale proceeds clawback — order from {buyer}"
    elif entry_type == "sale_proceeds_held":
        description = f"Sale proceeds (pending return window) — {buyer}"
    else:
        description = f"Sale proceeds — {buyer}"

    entry_dt = row.get("wt_created_at") or row.get("transaction_datetime")

    return {
        "entry_id": f"wt:{row.get('wt_uid')}",
        "entry_source": "wallet_transactions",
        "entry_type": entry_type,
        "entry_type_label": _entry_type_label(entry_type),
        "amount": amount,
        "useable_delta": useable_delta,
        "availability": availability,
        "currency": row.get("wt_currency") or "USD",
        "transaction_uid": row.get("wt_transaction_id"),
        "transaction_original_uid": None,
        "transaction_type": "sale",
        "entry_datetime": entry_dt,
        "description": description,
        "counterparty_name": buyer,
        "purchaser_name": buyer,
        "wt_uid": row.get("wt_uid"),
        "wt_type": wt_type,
        "wt_status": wt_status,
        "wt_note": row.get("wt_note"),
        "wt_available_at": row.get("wt_available_at"),
    }


def _normalize_wallet_spend_entry(row):
    stored = _to_float(row.get("transaction_wallet_amount"))
    # Purchases store positive wallet_amount; returns store negative restored amount.
    amount = _round_money(-stored)
    tx_type = (row.get("transaction_type") or "sale").lower()
    entry_type = "wallet_refund" if tx_type == "return" or amount > 0 else "wallet_payment"

    counterparty = row.get("counterparty_name") or "Unknown"
    if entry_type == "wallet_refund":
        description = f"Wallet refund — {counterparty}"
    else:
        description = f"Wallet payment — {counterparty}"

    return {
        "entry_id": f"wallet_spend:{row.get('transaction_uid')}",
        "entry_source": "transactions",
        "entry_type": entry_type,
        "entry_type_label": _entry_type_label(entry_type),
        "amount": amount,
        "useable_delta": amount,
        "availability": "useable",
        "currency": "USD",
        "transaction_uid": row.get("transaction_uid"),
        "transaction_original_uid": row.get("transaction_original_uid"),
        "transaction_type": tx_type,
        "entry_datetime": row.get("transaction_datetime"),
        "description": description,
        "counterparty_name": counterparty,
        "purchaser_name": None,
        "wallet_amount_stored": stored,
    }


def _fetch_bounty_ledger_rows(db, profile_id):
    from wallet_service import ensure_bounty_release_column

    ensure_bounty_release_column(db)
    q = db.execute(
        """
        SELECT
            ti.ti_uid,
            t.transaction_uid,
            t.transaction_datetime,
            t.transaction_type,
            t.transaction_original_uid,
            COALESCE(ti.ti_received_qty, 0) AS ti_received_qty,
            ti.ti_bs_qty,
            ti.ti_bounty_released_at,
            SUM(tb.tb_amount) AS amount,
            CONCAT(p.profile_personal_first_name, ' ', p.profile_personal_last_name)
                AS purchaser_name,
            IF(
                t.transaction_business_id LIKE '110%%',
                CONCAT(pp.profile_personal_first_name, ' ', pp.profile_personal_last_name),
                b.business_name
            ) AS counterparty_name
        FROM every_circle.transactions_bounty tb
        INNER JOIN every_circle.transactions_items ti ON tb.tb_ti_id = ti.ti_uid
        INNER JOIN every_circle.transactions t ON ti.ti_transaction_id = t.transaction_uid
        LEFT JOIN every_circle.business b ON t.transaction_business_id = b.business_uid
        LEFT JOIN every_circle.profile_personal pp
            ON t.transaction_business_id = pp.profile_personal_uid
        LEFT JOIN every_circle.profile_personal p
            ON t.transaction_profile_id = p.profile_personal_uid
        WHERE tb.tb_profile_id = %s
        GROUP BY
            ti.ti_uid,
            t.transaction_uid,
            t.transaction_datetime,
            t.transaction_type,
            t.transaction_original_uid,
            ti.ti_received_qty,
            ti.ti_bs_qty,
            ti.ti_bounty_released_at,
            p.profile_personal_first_name,
            p.profile_personal_last_name,
            pp.profile_personal_first_name,
            pp.profile_personal_last_name,
            b.business_name
        HAVING ABS(SUM(tb.tb_amount)) > 0.0001
        ORDER BY t.transaction_datetime DESC, ti.ti_uid
        """,
        (profile_id,),
    )
    return q.get("result") or []


def _fetch_wallet_transaction_ledger_rows(db, profile_id):
    wallet_id = resolve_wallet_profile_id(profile_id)
    q = db.execute(
        """
        SELECT
            wt.wt_uid,
            wt.wt_profile_id,
            wt.wt_buyer_id,
            wt.wt_seller_id,
            wt.wt_transaction_id,
            wt.wt_ti_id,
            wt.wt_type,
            wt.wt_status,
            wt.wt_amount,
            wt.wt_currency,
            wt.wt_note,
            wt.wt_available_at,
            wt.wt_created_at,
            t.transaction_datetime,
            CONCAT(bp.profile_personal_first_name, ' ', bp.profile_personal_last_name)
                AS buyer_name
        FROM every_circle.wallet_transactions wt
        LEFT JOIN every_circle.transactions t
            ON wt.wt_transaction_id = t.transaction_uid
        LEFT JOIN every_circle.profile_personal bp
            ON wt.wt_buyer_id = bp.profile_personal_uid
        WHERE wt.wt_profile_id IN (%s, %s)
        """,
        (profile_id, wallet_id),
    )
    return q.get("result") or []


def _fetch_wallet_spend_ledger_rows(db, profile_id):
    q = db.execute(
        """
        SELECT
            t.transaction_uid,
            t.transaction_datetime,
            t.transaction_type,
            t.transaction_original_uid,
            t.transaction_wallet_amount,
            IF(
                t.transaction_business_id LIKE '110%%',
                CONCAT(pp.profile_personal_first_name, ' ', pp.profile_personal_last_name),
                b.business_name
            ) AS counterparty_name
        FROM every_circle.transactions t
        LEFT JOIN every_circle.business b ON t.transaction_business_id = b.business_uid
        LEFT JOIN every_circle.profile_personal pp
            ON t.transaction_business_id = pp.profile_personal_uid
        WHERE t.transaction_profile_id = %s
          AND COALESCE(t.transaction_wallet_amount, 0) != 0
        """,
        (profile_id,),
    )
    return q.get("result") or []


def _sort_key(entry):
    dt = entry.get("entry_datetime")
    if dt is None:
        return ("", entry.get("entry_id", ""))
    return (str(dt), entry.get("entry_id", ""))


def _attach_running_balances(entries):
    """Oldest-first running totals; attach balance_after on each entry."""
    chronological = sorted(entries, key=_sort_key)
    running_actual = 0.0
    running_useable = 0.0
    for entry in chronological:
        running_actual = _round_money(running_actual + _to_float(entry.get("amount")))
        running_useable = _round_money(
            running_useable + _to_float(entry.get("useable_delta"))
        )
        entry["balance_after"] = running_actual
        entry["useable_balance_after"] = running_useable
    return entries


def get_wallet_ledger(db, profile_id, *, limit=100, offset=0):
    bounty_rows = _fetch_bounty_ledger_rows(db, profile_id)
    wt_rows = _fetch_wallet_transaction_ledger_rows(db, profile_id)
    spend_rows = _fetch_wallet_spend_ledger_rows(db, profile_id)

    entries = []
    entries.extend(_normalize_bounty_entry(r) for r in bounty_rows)
    entries.extend(_normalize_wallet_transaction_entry(r) for r in wt_rows)
    entries.extend(_normalize_wallet_spend_entry(r) for r in spend_rows)

    entries = [e for e in entries if abs(_to_float(e.get("amount"))) > 0.0001]
    entries.sort(key=_sort_key, reverse=True)
    total_entries = len(entries)

    _attach_running_balances(entries)

    page = entries[offset : offset + limit]

    wallet = get_wallet_row(db, profile_id)
    wallet_summary = None
    if wallet:
        wallet_summary = {
            "wallet_profile_id": wallet.get("wallet_profile_id"),
            "wallet_useable_balance": _round_money(
                wallet.get("wallet_useable_balance")
            ),
            "wallet_pending": _round_money(wallet.get("wallet_pending")),
            "wallet_actual_balance": _round_money(
                wallet.get("wallet_actual_balance")
            ),
            "wallet_lifetime_earning": _round_money(
                wallet.get("wallet_lifetime_earning")
            ),
            "wallet_lifetime_spent": _round_money(wallet.get("wallet_lifetime_spent")),
        }

    return {
        "code": 200,
        "message": "Wallet ledger retrieved successfully",
        "profile_id": profile_id,
        "wallet": wallet_summary,
        "total_entries": total_entries,
        "limit": limit,
        "offset": offset,
        "data": page,
    }


class WalletLedger(Resource):
    """
    GET /api/v1/wallet_ledger/<profile_id>

    Returns a checking-account-style ledger of events that affect wallet balance:
      - Bounty credits and reversals (transactions_bounty)
      - Seller sale proceeds and clawbacks (wallet_transactions)
      - Wallet payments and refunds at checkout (transactions.transaction_wallet_amount)

    Query params: limit (default 100, max 500), offset, timezone/tz
    """

    def get(self, profile_id):
        if not profile_id:
            return {"code": 400, "message": "profile_id is required"}, 400

        limit, offset = _parse_pagination()
        tz_name = _request_timezone()

        try:
            with connect() as db:
                result = get_wallet_ledger(db, profile_id, limit=limit, offset=offset)

            enriched = []
            for row in result.get("data") or []:
                if isinstance(row, dict):
                    enriched.append(
                        enrich_datetime_fields(dict(row), "entry_datetime", tz_name)
                    )
                else:
                    enriched.append(row)
            result["data"] = enriched
            if tz_name:
                result["timezone"] = tz_name
            result["datetime_storage"] = "UTC"

            return result, 200
        except Exception as e:
            print(f"Error in WalletLedger GET: {e}")
            print(traceback.format_exc())
            return {"code": 500, "message": f"An error occurred: {e}"}, 500
