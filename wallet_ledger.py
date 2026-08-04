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
from wallet_service import _round_money, _to_float, build_wallet_summary, get_wallet_row, line_is_fully_verified


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
        "sale_proceeds_hold": "Sale proceeds reserved (return)",
        "bounty_reclaim_reserved": "Bounty reserved (return)",
        "wallet_payment": "Wallet payment",
        "wallet_refund": "Wallet refund",
    }
    return labels.get(entry_type, entry_type.replace("_", " ").title())


def _bounty_line_fully_verified(row):
    order_qty = int(row.get("ti_bs_qty") or 0)
    received_qty = int(row.get("ti_received_qty") or 0)
    return line_is_fully_verified(received_qty, order_qty)


def _normalize_bounty_entry(row):
    amount = _round_money(row.get("amount"))
    tx_type = (row.get("transaction_type") or "sale").lower()
    fully_verified = _bounty_line_fully_verified(row)
    received_qty = int(row.get("ti_received_qty") or 0)
    order_qty = int(row.get("ti_bs_qty") or 0)
    if amount < 0 or tx_type == "return":
        entry_type = "bounty_reversal"
        availability = "useable"
        useable_delta = amount
    else:
        entry_type = "bounty_earned"
        if row.get("ti_bounty_released_at") and fully_verified:
            availability = "useable"
            useable_delta = amount
        else:
            availability = "pending"
            useable_delta = 0.0

    counterparty = row.get("counterparty_name") or "Unknown"
    if entry_type == "bounty_reversal":
        description = f"Bounty reversed on return — {counterparty}"
    elif availability == "useable":
        description = f"Bounty earned — {counterparty}"
    elif fully_verified and order_qty > 0:
        description = f"Bounty earned (pending return window) — {counterparty}"
    elif received_qty > 0 and order_qty > 0:
        description = (
            f"Bounty earned (pending verification, "
            f"{received_qty}/{order_qty}) — {counterparty}"
        )
    else:
        description = f"Bounty earned (pending verification) — {counterparty}"

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
    received_qty = int(row.get("wt_received_qty_after") or row.get("ti_received_qty") or 0)
    order_qty = int(row.get("ti_bs_qty") or 0)
    if entry_type == "sale_proceeds_clawback":
        description = f"Sale proceeds clawback — order from {buyer}"
    elif entry_type == "sale_proceeds_held":
        if order_qty > 0 and received_qty < order_qty:
            description = (
                f"Sale proceeds (pending return window, "
                f"{received_qty}/{order_qty} verified) — {buyer}"
            )
        else:
            description = f"Sale proceeds (pending return window) — {buyer}"
    elif order_qty > 0 and received_qty < order_qty:
        description = (
            f"Sale proceeds ({received_qty}/{order_qty} verified) — {buyer}"
        )
    else:
        description = f"Sale proceeds — {buyer}"

    entry_dt = row.get("wt_created_at") or row.get("transaction_datetime")
    tx_uid = row.get("wt_transaction_id")
    entry_id = (
        f"wt:sale:{tx_uid}:{wt_status}"
        if wt_type == "partial_delivery_credit"
        else f"wt:{row.get('wt_uid')}"
    )

    return {
        "entry_id": entry_id,
        "entry_source": "wallet_transactions",
        "entry_type": entry_type,
        "entry_type_label": _entry_type_label(entry_type),
        "amount": amount,
        "useable_delta": useable_delta,
        "availability": availability,
        "currency": row.get("wt_currency") or "USD",
        "transaction_uid": tx_uid,
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
        "ti_received_qty": received_qty or None,
        "ti_bs_qty": order_qty or None,
    }


def _aggregate_sale_proceeds_rows(rows):
    """
    Collapse incremental partial_delivery_credit rows (one per verify) into a
    single ledger line per sale + status. Underlying wallet_transactions rows
    remain for audit; unit progress lives on transactions_items.
    """
    credits_by_key = {}
    other_rows = []

    for row in rows:
        if (row.get("wt_type") or "") != "partial_delivery_credit":
            other_rows.append(row)
            continue
        tx_uid = row.get("wt_transaction_id")
        status = row.get("wt_status") or "posted"
        key = (tx_uid, status)
        credits_by_key.setdefault(key, []).append(row)

    aggregated = []
    for (_tx_uid, _status), group in credits_by_key.items():
        group.sort(
            key=lambda r: (
                str(r.get("wt_created_at") or ""),
                str(r.get("wt_uid") or ""),
            )
        )
        latest = dict(group[-1])
        latest["wt_amount"] = _round_money(
            sum(_to_float(r.get("wt_amount")) for r in group)
        )
        ti_totals = {}
        for row in group:
            ti_id = row.get("wt_ti_id")
            if not ti_id:
                continue
            ti_totals[ti_id] = {
                "bs": int(row.get("ti_bs_qty") or 0),
                "received": max(
                    int(row.get("ti_received_qty") or 0),
                    int(row.get("wt_received_qty_after") or 0),
                    ti_totals.get(ti_id, {}).get("received", 0),
                ),
            }
        order_qty = sum(item["bs"] for item in ti_totals.values())
        received_qty = sum(item["received"] for item in ti_totals.values())
        if order_qty > 0:
            latest["ti_bs_qty"] = order_qty
            latest["ti_received_qty"] = received_qty
            latest["wt_received_qty_after"] = received_qty
        else:
            latest["wt_received_qty_after"] = max(
                int(r.get("wt_received_qty_after") or 0) for r in group
            )
        aggregated.append(latest)

    return other_rows + aggregated


def _normalize_reservation_entry(row):
    wt_type = row.get("wt_type") or ""
    amount_raw = _round_money(row.get("wt_amount"))
    buyer = row.get("buyer_name") or row.get("wt_buyer_id") or "buyer"
    trr_uid = row.get("wt_note")

    if wt_type == "bounty_reclaim_reservation":
        entry_type = "bounty_reclaim_reserved"
        description = f"Bounty reserved for pending return — {buyer}"
    else:
        entry_type = "sale_proceeds_clawback"
        description = f"Sale proceeds clawback (pending return) — {buyer}"

    signed_amount = _round_money(-amount_raw)
    tx_uid = row.get("wt_transaction_id")

    return {
        "entry_id": f"reservation:{row.get('wt_uid')}",
        "entry_source": "wallet_transactions",
        "entry_type": entry_type,
        "entry_type_label": _entry_type_label(entry_type),
        "amount": signed_amount,
        "useable_delta": 0.0,
        "availability": "pending",
        "currency": row.get("wt_currency") or "USD",
        "transaction_uid": tx_uid,
        "transaction_original_uid": None,
        "transaction_type": "sale",
        "entry_datetime": row.get("wt_created_at") or row.get("transaction_datetime"),
        "description": description,
        "counterparty_name": buyer,
        "purchaser_name": buyer,
        "wt_uid": row.get("wt_uid"),
        "wt_type": "return_clawback" if entry_type == "sale_proceeds_clawback" else wt_type,
        "wt_status": "held" if entry_type == "sale_proceeds_clawback" else row.get("wt_status"),
        "trr_uid": trr_uid,
        "ti_uid": row.get("wt_ti_id"),
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


def _seller_business_ids(db, profile_id):
    """Business uids whose wallet resolves to this profile (personal + owned businesses)."""
    ids = {str(profile_id or "").strip()}
    ids.discard("")
    q = db.execute(
        """
        SELECT bu.bu_business_id
        FROM every_circle.business_user bu
        INNER JOIN every_circle.users u ON bu.bu_user_id = u.user_uid
        INNER JOIN every_circle.profile_personal pp
            ON pp.profile_personal_user_id = u.user_uid
        WHERE pp.profile_personal_uid = %s
        """,
        (profile_id,),
    )
    for row in q.get("result") or []:
        business_id = row.get("bu_business_id")
        if business_id:
            ids.add(str(business_id))
    return list(ids)


def _fetch_uncredited_seller_proceeds_rows(db, profile_id):
    """
    Sales where seller-eligible proceeds are not yet fully reflected in
    wallet_transactions (pre-verification or partially credited).
    """
    from wallet_transactions_service import (
        _posted_credits_total,
        compute_seller_eligible_total,
    )

    seller_ids = _seller_business_ids(db, profile_id)
    if not seller_ids:
        return []

    placeholders = ", ".join(["%s"] * len(seller_ids))
    q = db.execute(
        f"""
        SELECT
            t.transaction_uid,
            t.transaction_datetime,
            t.transaction_in_escrow,
            SUM(COALESCE(ti.ti_bs_qty, 0)) AS order_qty,
            SUM(COALESCE(ti.ti_received_qty, 0)) AS received_qty,
            CONCAT(bp.profile_personal_first_name, ' ', bp.profile_personal_last_name)
                AS buyer_name
        FROM every_circle.transactions t
        INNER JOIN every_circle.transactions_items ti
            ON ti.ti_transaction_id = t.transaction_uid
        LEFT JOIN every_circle.profile_personal bp
            ON t.transaction_profile_id = bp.profile_personal_uid
        WHERE COALESCE(t.transaction_type, 'sale') = 'sale'
          AND t.transaction_business_id IN ({placeholders})
        GROUP BY
            t.transaction_uid,
            t.transaction_datetime,
            t.transaction_in_escrow,
            bp.profile_personal_first_name,
            bp.profile_personal_last_name
        ORDER BY t.transaction_datetime DESC
        """,
        tuple(seller_ids),
    )

    rows = []
    for sale in q.get("result") or []:
        tx_uid = sale.get("transaction_uid")
        if not tx_uid:
            continue
        eligible = compute_seller_eligible_total(db, tx_uid)
        credited = _posted_credits_total(db, tx_uid)
        uncredited = _round_money(eligible - _to_float(credited))
        if uncredited <= 0.0001:
            continue
        rows.append(
            {
                **sale,
                "uncredited_amount": uncredited,
                "seller_eligible_total": eligible,
                "credited_total": credited,
            }
        )
    return rows


def _normalize_uncredited_seller_proceeds_entry(row):
    """Projected sale proceeds not yet delivery-credited to wallet_transactions."""
    amount = _round_money(row.get("uncredited_amount"))
    received_qty = int(row.get("received_qty") or 0)
    order_qty = int(row.get("order_qty") or 0)
    buyer = row.get("buyer_name") or "buyer"
    tx_uid = row.get("transaction_uid")

    if order_qty > 0 and received_qty <= 0:
        description = f"Sale proceeds (pending verification) — {buyer}"
    elif order_qty > 0 and received_qty < order_qty:
        description = (
            f"Sale proceeds (pending verification, "
            f"{received_qty}/{order_qty}) — {buyer}"
        )
    else:
        description = f"Sale proceeds (pending) — {buyer}"

    return {
        "entry_id": f"pending_proceeds:{tx_uid}",
        "entry_source": "seller_proceeds_pending",
        "entry_type": "sale_proceeds_held",
        "entry_type_label": _entry_type_label("sale_proceeds_held"),
        "amount": amount,
        "useable_delta": 0.0,
        "availability": "pending",
        "currency": "USD",
        "transaction_uid": tx_uid,
        "transaction_original_uid": None,
        "transaction_type": "sale",
        "entry_datetime": row.get("transaction_datetime"),
        "description": description,
        "counterparty_name": buyer,
        "purchaser_name": buyer,
        "ti_received_qty": received_qty or None,
        "ti_bs_qty": order_qty or None,
    }


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
            wt.wt_received_qty_after,
            t.transaction_datetime,
            ti.ti_bs_qty,
            COALESCE(ti.ti_received_qty, 0) AS ti_received_qty,
            CONCAT(bp.profile_personal_first_name, ' ', bp.profile_personal_last_name)
                AS buyer_name
        FROM every_circle.wallet_transactions wt
        LEFT JOIN every_circle.transactions t
            ON wt.wt_transaction_id = t.transaction_uid
        LEFT JOIN every_circle.transactions_items ti
            ON wt.wt_ti_id = ti.ti_uid
        LEFT JOIN every_circle.profile_personal bp
            ON wt.wt_buyer_id = bp.profile_personal_uid
        WHERE wt.wt_profile_id IN (%s, %s)
          AND wt.wt_type NOT IN ('return_refund_reservation', 'bounty_reclaim_reservation')
          AND wt.wt_status NOT IN ('reserved', 'cleared')
        """,
        (profile_id, wallet_id),
    )
    rows = q.get("result") or []
    return _aggregate_sale_proceeds_rows(rows)


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
        amount = _to_float(entry.get("amount"))
        # Projected (not yet delivery-credited) seller proceeds are shown in the
        # Pending column but are not yet in wallet.actual_balance.
        if (entry.get("entry_source") or "") != "seller_proceeds_pending":
            running_actual = _round_money(running_actual + amount)
        running_useable = _round_money(
            running_useable + _to_float(entry.get("useable_delta"))
        )
        entry["balance_after"] = running_actual
        entry["useable_balance_after"] = running_useable
    return entries


def get_wallet_ledger(db, profile_id, *, limit=100, offset=0):
    from wallet_return_reservations import fetch_reservation_ledger_rows

    bounty_rows = _fetch_bounty_ledger_rows(db, profile_id)
    wt_rows = _fetch_wallet_transaction_ledger_rows(db, profile_id)
    pending_proceeds_rows = _fetch_uncredited_seller_proceeds_rows(db, profile_id)
    spend_rows = _fetch_wallet_spend_ledger_rows(db, profile_id)
    reservation_rows = fetch_reservation_ledger_rows(db, profile_id)

    entries = []
    entries.extend(_normalize_bounty_entry(r) for r in bounty_rows)
    entries.extend(_normalize_wallet_transaction_entry(r) for r in wt_rows)
    entries.extend(
        _normalize_uncredited_seller_proceeds_entry(r) for r in pending_proceeds_rows
    )
    entries.extend(_normalize_reservation_entry(r) for r in reservation_rows)
    entries.extend(_normalize_wallet_spend_entry(r) for r in spend_rows)

    entries = [e for e in entries if abs(_to_float(e.get("amount"))) > 0.0001]
    entries.sort(key=_sort_key, reverse=True)
    total_entries = len(entries)

    _attach_running_balances(entries)

    page = entries[offset : offset + limit]

    wallet_summary = build_wallet_summary(db, profile_id)

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
      - Bounty credits and reversals (transactions_bounty) — pending until verified/released
      - Seller sale proceeds and clawbacks (wallet_transactions)
      - Projected seller proceeds not yet delivery-credited (pending verification)
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
