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
from wallet_service import _round_money, _to_float, build_wallet_summary, get_wallet_row, line_is_fully_verified, bounty_reversal_ledger_availability
from wallet_ledger_proceeds import _attach_status_note
from order_quantity_context import (
    line_quantity_context,
    order_quantity_context,
    compute_seller_proceeds_ledger_amounts,
    omit_ledger_proceeds_component_fields,
    quantity_context_fields,
    wallet_ledger_data_issue,
    wallet_ledger_event_amount,
)


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
        "sale_proceeds_original": "Sale proceeds",
        "sale_proceeds_cancel": "Sale proceeds",
        "sale_proceeds_return_clawback": "Sale proceeds",
        "sale_proceeds_verify_transfer": "Sale proceeds",
        "sale_proceeds_return_window_release": "Sale proceeds",
        "sale_proceeds_held": "Sale proceeds",
        "sale_proceeds_transfer": "Sale proceeds transfer",
        "sale_proceeds_adjustment": "Sale proceeds adjustment",
        "sale_proceeds_clawback": "Sale proceeds clawback",
        "sale_proceeds_hold": "Sale proceeds reserved (return)",
        "bounty_reclaim_reserved": "Bounty reserved (return)",
        "wallet_payment": "Wallet payment",
        "wallet_refund": "Wallet refund",
    }
    return labels.get(entry_type, entry_type.replace("_", " ").title())


def _bounty_line_fully_verified(row, *, denom=None):
    order_qty = int(denom if denom is not None else row.get("ti_bs_qty") or 0)
    received_qty = int(row.get("ti_received_qty") or 0)
    return line_is_fully_verified(received_qty, order_qty)


def _pending_return_window_description(buyer, net_verified_held):
    held = int(net_verified_held or 0)
    if held > 0:
        return (
            f"Sale proceeds (pending return window, {held} verified units) — {buyer}"
        )
    return f"Sale proceeds (pending return window) — {buyer}"


def _clawback_description(buyer, *, returned_qty=0):
    if int(returned_qty or 0) > 0:
        return (
            f"Sale proceeds clawback ({int(returned_qty)} returned) — "
            f"order from {buyer}"
        )
    return f"Sale proceeds clawback — order from {buyer}"


def _quantity_context_for_entry(db, row):
    tx_uid = row.get("wt_transaction_id") or row.get("transaction_uid")
    ti_uid = row.get("wt_ti_id") or row.get("ti_uid")
    if not tx_uid or not ti_uid:
        if tx_uid:
            return order_quantity_context(db, tx_uid)
        return None
    return line_quantity_context(db, tx_uid, ti_uid)


def _normalize_bounty_entry(db, row):
    from units_ledger import line_units_ledger, pending_bounty_units

    amount = _round_money(row.get("amount"))
    tx_type = (row.get("transaction_type") or "sale").lower()
    order_uid = row.get("transaction_original_uid") or row.get("transaction_uid")
    ti_uid = row.get("ti_uid")
    qty_ctx = None
    units = {}
    if order_uid and ti_uid:
        qty_ctx = line_quantity_context(db, order_uid, ti_uid, row=row)
        units = line_units_ledger(db, order_uid, ti_uid, row=row)

    received_qty = int(row.get("ti_received_qty") or 0)
    denom_qty = int(row.get("ti_bs_qty") or 0)
    if qty_ctx:
        received_qty = int(qty_ctx.get("verified_qty") or received_qty)
        denom_qty = int(qty_ctx.get("active_units") or qty_ctx.get("purchased_qty") or denom_qty)
    if units:
        received_qty = int(units.get("verified_qty") or received_qty)
        denom_qty = int(units.get("active_qty") or units.get("purchased_qty") or denom_qty)

    fully_verified = _bounty_line_fully_verified(row, denom=denom_qty)
    if amount < 0 or tx_type == "return":
        entry_type = "bounty_reversal"
        availability, useable_delta = bounty_reversal_ledger_availability(
            db, row, amount
        )
        display_amount = amount
    else:
        entry_type = "bounty_earned"
        if row.get("ti_bounty_released_at") and fully_verified:
            availability = "useable"
            useable_delta = amount
            display_amount = amount
        else:
            availability = "pending"
            useable_delta = 0.0
            pending_units = pending_bounty_units(units)
            if (
                not fully_verified
                and denom_qty > 0
                and pending_units < denom_qty
            ):
                display_amount = _round_money(
                    amount * (pending_units / float(denom_qty))
                )
            else:
                display_amount = amount

    counterparty = row.get("counterparty_name") or "Unknown"
    if entry_type == "bounty_reversal":
        description = f"Bounty reversed on return — {counterparty}"
    elif availability == "useable":
        description = f"Bounty earned — {counterparty}"
    elif fully_verified and denom_qty > 0:
        description = f"Bounty earned (pending return window) — {counterparty}"
    elif received_qty > 0 and denom_qty > 0:
        pending_units = pending_bounty_units(units)
        if pending_units > 0:
            description = (
                f"Bounty earned (pending verification, {pending_units} units) — "
                f"{counterparty}"
            )
        else:
            description = (
                f"Bounty earned (pending verification, "
                f"{received_qty}/{denom_qty}) — {counterparty}"
            )
    else:
        pending_units = pending_bounty_units(units)
        if pending_units > 0 and denom_qty > 0:
            description = (
                f"Bounty earned (pending verification, {pending_units} units) — "
                f"{counterparty}"
            )
        else:
            description = f"Bounty earned (pending verification) — {counterparty}"

    entry = {
        "entry_id": f"bounty:{row.get('ti_uid')}:{row.get('transaction_uid')}",
        "entry_source": "transactions_bounty",
        "entry_type": entry_type,
        "entry_type_label": _entry_type_label(entry_type),
        "amount": display_amount,
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
        "ti_received_qty": received_qty or None,
        "ti_bs_qty": denom_qty or None,
        "bounty_released_at": row.get("ti_bounty_released_at"),
    }
    if units:
        entry["units"] = units
    if qty_ctx:
        entry.update(quantity_context_fields(qty_ctx))
    return _attach_status_note(entry)


def _normalize_wallet_transaction_entry(db, row):
    amount = _round_money(row.get("wt_amount"))
    wt_type = row.get("wt_type") or ""
    wt_status = row.get("wt_status") or "posted"
    tx_uid = row.get("wt_transaction_id")

    if wt_type == "return_clawback":
        entry_type = "sale_proceeds_clawback"
    elif wt_type == "cancel_unshipped_adjustment":
        entry_type = "sale_proceeds_adjustment"
    elif wt_status == "held" and wt_type == "partial_delivery_credit":
        entry_type = "sale_proceeds_held"
    elif wt_status == "held":
        entry_type = "sale_proceeds_held"
    else:
        entry_type = "sale_proceeds"

    buyer = row.get("buyer_name") or row.get("wt_buyer_id") or "buyer"
    qty_ctx = row.get("_qty_ctx") or row.get("_proceeds_ctx")
    if qty_ctx is None:
        qty_ctx = _quantity_context_for_entry(db, row)

    cancelled_qty = int((qty_ctx or {}).get("cancelled_qty") or 0)
    returned_qty = int((qty_ctx or {}).get("returned_qty") or 0)
    net_verified_held = int((qty_ctx or {}).get("net_verified_held") or 0)
    verified_qty = int((qty_ctx or {}).get("verified_qty") or 0)
    active_units = int((qty_ctx or {}).get("active_units") or 0)

    event_fields = {}
    if tx_uid and wt_type in (
        "return_clawback",
        "cancel_unshipped_adjustment",
        "partial_delivery_credit",
    ):
        if entry_type == "sale_proceeds_held" and wt_type == "partial_delivery_credit":
            wallet_ledger_data_issue(
                "aggregated held partial_delivery_credit: using stored wt_amount only",
                order_uid=tx_uid,
                event=row,
            )
        else:
            event_sign = (
                -1
                if wt_type in ("return_clawback", "cancel_unshipped_adjustment")
                else 1
            )
            amount, event_fields = wallet_ledger_event_amount(
                db, tx_uid, row, sign=event_sign
            )

    if entry_type == "sale_proceeds_clawback":
        from wallet_transactions_service import return_clawback_ledger_availability

        try:
            claw_delta_qty = int(row.get("wt_qty") or 0)
        except (TypeError, ValueError):
            claw_delta_qty = 0
        if claw_delta_qty <= 0:
            wallet_ledger_data_issue(
                "return_clawback row missing wt_qty",
                order_uid=tx_uid,
                event=row,
            )
        description = return_clawback_ledger_description(
            db, row, delta_qty=claw_delta_qty
        )
        availability, useable_delta = return_clawback_ledger_availability(db, row)
    elif entry_type == "sale_proceeds_held":
        description = _pending_return_window_description(buyer, net_verified_held)
        availability = "pending"
        useable_delta = 0.0
    elif wt_type == "cancel_unshipped_adjustment":
        entry_type = "sale_proceeds_adjustment"
        description = (
            f"Sale proceeds reduced — {cancelled_qty} units cancelled before shipment"
            if cancelled_qty > 0
            else f"Sale proceeds reduced — units cancelled before shipment"
        )
        availability = "pending"
        useable_delta = 0.0
    else:
        description = f"Sale proceeds — {buyer}"
        availability = "useable"
        useable_delta = amount

    entry_dt = row.get("wt_created_at") or row.get("transaction_datetime")
    if entry_type == "sale_proceeds_adjustment":
        entry_id = f"wt:{row.get('wt_uid')}"
    elif (
        wt_type == "partial_delivery_credit"
    ):
        entry_id = f"wt:sale:{tx_uid}:{wt_status}"
    else:
        entry_id = f"wt:{row.get('wt_uid')}"

    entry = {
        "entry_id": entry_id,
        "entry_source": "wallet_transactions",
        "entry_type": entry_type,
        "entry_type_label": _entry_type_label(entry_type),
        "amount": amount,
        "useable_delta": useable_delta,
        "availability": availability,
        "currency": row.get("wt_currency") or "USD",
        "transaction_uid": tx_uid,
        "order_uid": tx_uid,
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
        "ti_uid": row.get("wt_ti_id"),
        "ti_received_qty": verified_qty or None,
        "ti_bs_qty": active_units or None,
    }
    if qty_ctx:
        entry.update(omit_ledger_proceeds_component_fields(quantity_context_fields(qty_ctx)))
    if event_fields:
        entry.update(event_fields)
    return _attach_status_note(entry)


def _aggregate_sale_proceeds_rows(db, rows):
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
        qty_ctx = order_quantity_context(db, _tx_uid) if _tx_uid else None
        if qty_ctx and _tx_uid:
            proceeds_ctx = compute_seller_proceeds_ledger_amounts(db, _tx_uid)
            latest["_qty_ctx"] = qty_ctx
            latest["_proceeds_ctx"] = proceeds_ctx
            latest["ti_received_qty"] = int(qty_ctx.get("verified_qty") or received_qty)
            latest["wt_received_qty_after"] = latest["ti_received_qty"]
        elif order_qty > 0:
            latest["ti_bs_qty"] = order_qty
            latest["ti_received_qty"] = received_qty
            latest["wt_received_qty_after"] = received_qty
        else:
            latest["wt_received_qty_after"] = max(
                int(r.get("wt_received_qty_after") or 0) for r in group
            )
        aggregated.append(latest)

    return other_rows + aggregated


def _normalize_reservation_entry(db, row):
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

    entry = {
        "entry_id": f"reservation:{row.get('wt_uid')}",
        "entry_source": "wallet_transactions",
        "entry_type": entry_type,
        "entry_type_label": _entry_type_label(entry_type),
        "amount": signed_amount,
        "useable_delta": 0.0,
        "availability": "pending",
        "currency": row.get("wt_currency") or "USD",
        "transaction_uid": tx_uid,
        "order_uid": tx_uid,
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
    if entry_type == "sale_proceeds_clawback" and tx_uid:
        event_row = {
            "wt_type": "return_clawback",
            "wt_ti_id": row.get("wt_ti_id"),
            "wt_qty": row.get("wt_qty"),
            "wt_amount": row.get("wt_amount"),
            "wt_idempotency_key": row.get("wt_idempotency_key")
            or (f"return_clawback_hold:{trr_uid}" if trr_uid else ""),
            "wt_note": trr_uid,
        }
        line_amount, event_fields = wallet_ledger_event_amount(
            db, tx_uid, event_row, sign=-1
        )
        entry.update(event_fields)
        entry["amount"] = line_amount
    return entry


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
            COALESCE(ti.ti_original_ti_uid, ti.ti_uid) AS source_ti_uid,
            t.transaction_uid,
            t.transaction_datetime,
            t.transaction_type,
            t.transaction_original_uid,
            COALESCE(ti.ti_received_qty, 0) AS ti_received_qty,
            ti.ti_bs_qty,
            ti.ti_bounty_released_at,
            MAX(orig_ti.ti_bounty_released_at) AS source_bounty_released_at,
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
        LEFT JOIN every_circle.transactions_items orig_ti
            ON orig_ti.ti_uid = COALESCE(ti.ti_original_ti_uid, ti.ti_uid)
        LEFT JOIN every_circle.business b ON t.transaction_business_id = b.business_uid
        LEFT JOIN every_circle.profile_personal pp
            ON t.transaction_business_id = pp.profile_personal_uid
        LEFT JOIN every_circle.profile_personal p
            ON t.transaction_profile_id = p.profile_personal_uid
        WHERE tb.tb_profile_id = %s
        GROUP BY
            ti.ti_uid,
            source_ti_uid,
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


def _fetch_seller_sale_uids(db, profile_id):
    seller_ids = _seller_business_ids(db, profile_id)
    if not seller_ids:
        return []
    placeholders = ", ".join(["%s"] * len(seller_ids))
    q = db.execute(
        f"""
        SELECT DISTINCT t.transaction_uid
        FROM every_circle.transactions t
        WHERE COALESCE(t.transaction_type, 'sale') = 'sale'
          AND t.transaction_business_id IN ({placeholders})
        """,
        tuple(seller_ids),
    )
    return [
        row.get("transaction_uid")
        for row in (q.get("result") or [])
        if row.get("transaction_uid")
    ]


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
            wt.wt_qty,
            wt.wt_idempotency_key,
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
    return _aggregate_sale_proceeds_rows(db, rows)


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


def _actual_balance_delta(entry):
    """
    Change to total on-hand (balance_after) for one ledger row.

    Pending→useable moves (verify / return-window release) set
    include_in_running_balance=False and only update useable_delta.
    """
    if entry.get("include_in_running_balance") is False:
        return 0.0
    if (entry.get("entry_source") or "") == "seller_proceeds_pending":
        return 0.0
    if entry.get("actual_balance_delta") is not None:
        return _to_float(entry.get("actual_balance_delta"))
    return _to_float(entry.get("amount"))


def _attach_running_balances(entries):
    """Oldest-first running totals; attach balance_after on each entry."""
    chronological = sorted(entries, key=_sort_key)
    running_actual = 0.0
    running_useable = 0.0
    for entry in chronological:
        running_actual = _round_money(running_actual + _actual_balance_delta(entry))
        running_useable = _round_money(
            running_useable + _to_float(entry.get("useable_delta"))
        )
        entry["balance_after"] = running_actual
        entry["useable_balance_after"] = running_useable
    return entries


def apply_ledger_entry_display(entry, tz_name=None):
    """Attach pending_delta, useable_delta, and display.* to one ledger entry."""
    from account_screen_v3_contract import (
        build_ledger_entry_display,
        ledger_entry_pool_deltas,
    )

    if not isinstance(entry, dict):
        return entry
    pending_delta, useable_delta = ledger_entry_pool_deltas(entry)
    out = dict(entry)
    out["pending_delta"] = pending_delta
    out["useable_delta"] = useable_delta
    out["display"] = build_ledger_entry_display(out, tz_name)
    return out


def get_wallet_ledger(db, profile_id, *, limit=100, offset=0):
    from wallet_return_reservations import fetch_reservation_ledger_rows
    from wallet_ledger_proceeds import build_proceeds_narrative_for_profile
    from wallet_transactions_service import (
        WT_TYPE_CANCEL_UNSHIPPED_ADJUSTMENT,
        WT_TYPE_PARTIAL_DELIVERY_CREDIT,
        WT_TYPE_RETURN_CLAWBACK,
    )
    from order_quantity_context import clear_ledger_quantity_caches

    clear_ledger_quantity_caches()

    # Preload quantity context for every seller sale once (biggest N+1 source).
    for order_uid in _fetch_seller_sale_uids(db, profile_id):
        order_quantity_context(db, order_uid)

    bounty_rows = _fetch_bounty_ledger_rows(db, profile_id)
    wt_rows = _fetch_wallet_transaction_ledger_rows(db, profile_id)
    spend_rows = _fetch_wallet_spend_ledger_rows(db, profile_id)
    reservation_rows = fetch_reservation_ledger_rows(db, profile_id)

    narrative_entries, narrative_order_uids = build_proceeds_narrative_for_profile(
        db, profile_id, wt_rows, []
    )
    narrative_wt_types = {
        WT_TYPE_PARTIAL_DELIVERY_CREDIT,
        WT_TYPE_RETURN_CLAWBACK,
        WT_TYPE_CANCEL_UNSHIPPED_ADJUSTMENT,
    }
    filtered_wt_rows = [
        r
        for r in wt_rows
        if not (
            r.get("wt_transaction_id") in narrative_order_uids
            and (r.get("wt_type") or "") in narrative_wt_types
        )
    ]

    entries = []
    entries.extend(_attach_status_note(e) for e in (_normalize_bounty_entry(db, r) for r in bounty_rows))
    entries.extend(_attach_status_note(e) for e in narrative_entries)
    entries.extend(
        _normalize_wallet_transaction_entry(db, r) for r in filtered_wt_rows
    )
    entries.extend(_normalize_reservation_entry(db, r) for r in reservation_rows)
    entries.extend(_normalize_wallet_spend_entry(r) for r in spend_rows)

    entries = [
        e
        for e in entries
        if abs(_to_float(e.get("amount"))) > 0.0001
        or (e.get("entry_type") or "") in (
            "sale_proceeds_transfer",
            "sale_proceeds_return_window_release",
            "sale_proceeds_verify_transfer",
        )
    ]
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
      - Seller sale proceeds narrative (original + per-event lines from wallet_transactions)
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
                    row = enrich_datetime_fields(dict(row), "entry_datetime", tz_name)
                    enriched.append(apply_ledger_entry_display(row, tz_name))
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
