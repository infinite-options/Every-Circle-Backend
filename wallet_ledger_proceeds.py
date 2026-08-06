"""
Chronological seller-proceeds ledger narrative per order.

Append-only audit trail: immutable original sale entry plus per-event lines
(cancel, return clawback, verify transfer, return-window release).
"""

from order_quantity_context import (
    compute_proceeds_buckets,
    compute_seller_proceeds_ledger_amounts,
    order_quantity_context,
    proceeds_breakdown_fields,
    quantity_context_fields,
)
from wallet_service import _round_money, _to_float
from wallet_transactions_service import (
    WT_STATUS_HELD,
    WT_STATUS_POSTED,
    WT_TYPE_CANCEL_UNSHIPPED_ADJUSTMENT,
    WT_TYPE_PARTIAL_DELIVERY_CREDIT,
    WT_TYPE_RETURN_CLAWBACK,
    return_clawback_ledger_availability,
    return_clawback_ledger_description,
)


def _entry_type_label(entry_type):
    labels = {
        "sale_proceeds_original": "Sale proceeds",
        "sale_proceeds_cancel": "Sale proceeds",
        "sale_proceeds_return_clawback": "Sale proceeds",
        "sale_proceeds_verify_transfer": "Sale proceeds",
        "sale_proceeds_return_window_release": "Sale proceeds",
    }
    return labels.get(entry_type, entry_type.replace("_", " ").title())


def _format_proceeds_bucket_summary(buckets):
    """Parenthetical bucket counts for sale-proceeds descriptions; omit zeros."""
    parts = []
    for key, label in (
        ("pending_shipment", "pending shipment"),
        ("pending_verification", "pending verification"),
        ("pending_return_window", "pending return window"),
    ):
        count = int(buckets.get(key) or 0)
        if count > 0:
            parts.append(f"{count} {label}")
    return ", ".join(parts)


def _original_status_note(buckets):
    summary = _format_proceeds_bucket_summary(buckets)
    return f"({summary})" if summary else ""


def _original_description(buyer, buckets):
    summary = _format_proceeds_bucket_summary(buckets)
    if summary:
        return f"Sale proceeds ({summary}) — {buyer}"
    return f"Sale proceeds — {buyer}"


def _sale_proceeds_status_note(description, *, counterparty_name=None):
    """
    Short status fragment for Order Detail chips — no prefix, no counterparty.
    Parses legacy full descriptions; prefer building status_note at source.
    """
    if not description:
        return ""
    desc = str(description).strip()
    paren_prefix = "Sale proceeds ("
    if desc.startswith(paren_prefix):
        close = desc.find(")")
        if close > len(paren_prefix) - 1:
            return f"({desc[len(paren_prefix):close]})"
    dash_prefixes = ("Sale proceeds — ", "Sale proceeds reduced — ")
    for dash_prefix in dash_prefixes:
        if desc.startswith(dash_prefix):
            fragment = desc[len(dash_prefix) :].strip()
            buyer = (counterparty_name or "").strip()
            if buyer and fragment == buyer:
                return ""
            return fragment
    return ""


def _attach_status_note(entry):
    """Set status_note on sale_proceeds_* entries when not already present."""
    if not isinstance(entry, dict):
        return entry
    entry_type = entry.get("entry_type") or ""
    if not entry_type.startswith("sale_proceeds"):
        return entry
    if entry.get("status_note") is not None:
        note = str(entry.get("status_note") or "").strip()
        if note:
            entry["status_note"] = note
        else:
            entry.pop("status_note", None)
        return entry
    note = _sale_proceeds_status_note(
        entry.get("description"),
        counterparty_name=entry.get("counterparty_name"),
    )
    if note:
        entry["status_note"] = note
    return entry


def _bucket_snapshot_from_totals(
    *,
    purchased,
    shipped,
    verified,
    cancelled,
    returned,
    pending_return_window=None,
):
    unverified_shipped = max((shipped - returned) - max(verified - returned, 0), 0)
    if pending_return_window is None:
        pending_return_window = max(0, verified - returned)
    pending_verification = unverified_shipped
    pending_shipment = max(0, purchased - cancelled - unverified_shipped - verified)
    active_qty = max(0, purchased - cancelled - returned)
    return {
        "purchased_qty": purchased,
        "cancelled_qty": cancelled,
        "returned_qty": returned,
        "verified_qty": verified,
        "shipped_qty": shipped,
        "unverified_shipped_qty": unverified_shipped,
        "pending_shipment": pending_shipment,
        "pending_verification": pending_verification,
        "pending_return_window": pending_return_window,
        "active_qty": active_qty,
    }


def _base_entry(
    order_uid,
    buyer_name,
    *,
    entry_id,
    entry_type,
    amount,
    entry_datetime,
    description,
    parent_entry_id=None,
    event_type=None,
    include_in_running_balance=True,
    **extra,
):
    entry = {
        "entry_id": entry_id,
        "entry_source": "wallet_transactions",
        "entry_type": entry_type,
        "entry_type_label": _entry_type_label(entry_type),
        "amount": _round_money(amount),
        "useable_delta": 0.0,
        "availability": "pending",
        "currency": "USD",
        "transaction_uid": order_uid,
        "transaction_original_uid": None,
        "transaction_type": "sale",
        "entry_datetime": entry_datetime,
        "description": description,
        "counterparty_name": buyer_name,
        "purchaser_name": buyer_name,
        "parent_entry_id": parent_entry_id,
        "event_type": event_type,
        "include_in_running_balance": include_in_running_balance,
    }
    entry.update(extra)
    status_note = extra.get("status_note")
    if status_note:
        entry["status_note"] = status_note
    return entry


def preload_wt_events_for_orders(db, order_uids):
    order_uids = [uid for uid in (order_uids or []) if uid]
    if not order_uids:
        return {}
    placeholders = ", ".join(["%s"] * len(order_uids))
    q = db.execute(
        f"""
        SELECT wt_transaction_id, wt_uid, wt_type, wt_status, wt_qty,
               wt_amount, wt_created_at, wt_idempotency_key, wt_received_qty_after,
               wt_ti_id, wt_available_at
        FROM every_circle.wallet_transactions
        WHERE wt_transaction_id IN ({placeholders})
          AND wt_type IN (%s, %s, %s)
          AND wt_status NOT IN ('reserved', 'cleared')
        ORDER BY wt_created_at ASC, wt_uid ASC
        """,
        tuple(order_uids)
        + (
            WT_TYPE_PARTIAL_DELIVERY_CREDIT,
            WT_TYPE_RETURN_CLAWBACK,
            WT_TYPE_CANCEL_UNSHIPPED_ADJUSTMENT,
        ),
    )
    by_order = {}
    for row in q.get("result") or []:
        tx_uid = row.get("wt_transaction_id")
        if tx_uid:
            by_order.setdefault(tx_uid, []).append(row)
    return by_order


def fetch_orders_buyer_and_datetime(db, order_uids):
    order_uids = [uid for uid in (order_uids or []) if uid]
    if not order_uids:
        return {}
    placeholders = ", ".join(["%s"] * len(order_uids))
    q = db.execute(
        f"""
        SELECT
            t.transaction_uid,
            t.transaction_datetime,
            CONCAT(bp.profile_personal_first_name, ' ', bp.profile_personal_last_name)
                AS buyer_name
        FROM every_circle.transactions t
        LEFT JOIN every_circle.profile_personal bp
            ON t.transaction_profile_id = bp.profile_personal_uid
        WHERE t.transaction_uid IN ({placeholders})
        """,
        tuple(order_uids),
    )
    meta = {}
    for row in q.get("result") or []:
        uid = row.get("transaction_uid")
        if uid:
            meta[uid] = (
                row.get("buyer_name") or "buyer",
                row.get("transaction_datetime"),
            )
    return meta


def build_order_proceeds_ledger_entries(
    db, order_uid, *, buyer_name, order_datetime, wt_events=None
):
    """
    Build seller-proceeds ledger entries for one sale order.

    One immutable original row (full order proceeds) plus append-only event rows.
    """
    if not order_uid:
        return []

    wt_events = wt_events or []
    qty = order_quantity_context(db, order_uid)
    proceeds = compute_seller_proceeds_ledger_amounts(db, order_uid, qty=qty)
    per_unit = _to_float(proceeds.get("seller_proceeds_per_unit") or 0)
    purchased = int(qty.get("purchased_qty") or 0)
    shipped = int(qty.get("shipped_qty") or 0)
    verified = int(qty.get("verified_qty") or 0)

    if purchased <= 0 or per_unit <= 0:
        return []

    buyer = buyer_name or "buyer"
    parent_id = f"wt:sale:{order_uid}:original"
    breakdown = {
        key: proceeds.get(key)
        for key in (
            "merchandise_amount",
            "sales_tax_amount",
            "shipping_amount",
            "bounty_amount",
            "amount",
            "per_unit_proceeds",
            "per_unit_merchandise",
            "per_unit_sales_tax",
            "per_unit_shipping",
            "per_unit_bounty",
            "purchased_qty",
        )
        if key in proceeds
    }
    full_order_proceeds = _round_money(
        proceeds.get("amount") if breakdown.get("amount") is not None else per_unit * purchased
    )
    current_buckets = compute_proceeds_buckets(proceeds)

    entries = []

    original = _base_entry(
        order_uid,
        buyer,
        entry_id=parent_id,
        entry_type="sale_proceeds_original",
        amount=full_order_proceeds,
        entry_datetime=order_datetime,
        description=_original_description(buyer, current_buckets),
        status_note=_original_status_note(current_buckets),
        parent_entry_id=None,
        event_type="order_placed",
        include_in_running_balance=True,
        cancelled_qty_delta=None,
        returned_qty_delta=None,
        verified_qty_delta=None,
        **quantity_context_fields(proceeds),
    )
    entries.append(original)

    cancelled_running = 0
    returned_running = 0

    for event in wt_events:
        wt_type = event.get("wt_type") or ""
        wt_status = event.get("wt_status") or ""
        event_dt = event.get("wt_created_at") or order_datetime
        wt_uid = event.get("wt_uid")
        delta_qty = int(event.get("wt_qty") or 0)

        if wt_type == WT_TYPE_CANCEL_UNSHIPPED_ADJUSTMENT and delta_qty > 0:
            cancelled_running += delta_qty
            cancel_amt = _round_money(-per_unit * delta_qty)
            stored_amt = _round_money(_to_float(event.get("wt_amount")))
            if stored_amt != 0 and abs(stored_amt - cancel_amt) <= 0.02:
                cancel_amt = stored_amt
            snap = _bucket_snapshot_from_totals(
                purchased=purchased,
                shipped=shipped,
                verified=verified,
                cancelled=cancelled_running,
                returned=returned_running,
            )
            cancel_note = f"{delta_qty} unit(s) cancelled before shipment"
            entries.append(
                _base_entry(
                    order_uid,
                    buyer,
                    entry_id=f"wt:{wt_uid}" if wt_uid else f"wt:sale:{order_uid}:cancel:{cancelled_running}",
                    entry_type="sale_proceeds_cancel",
                    amount=cancel_amt,
                    entry_datetime=event_dt,
                    description=f"Sale proceeds — {cancel_note}",
                    status_note=cancel_note,
                    parent_entry_id=parent_id,
                    event_type="cancel",
                    include_in_running_balance=True,
                    per_unit_proceeds=per_unit,
                    cancelled_qty_delta=delta_qty,
                    cancelled_qty_total=cancelled_running,
                    returned_qty_delta=None,
                    verified_qty_delta=None,
                    wt_uid=wt_uid,
                    wt_type=wt_type,
                    wt_status=wt_status,
                    **proceeds_breakdown_fields(
                        breakdown,
                        effective_qty=delta_qty,
                        purchased_qty=purchased,
                        sign=-1,
                    ),
                    **snap,
                )
            )

        elif wt_type == WT_TYPE_RETURN_CLAWBACK and delta_qty > 0:
            returned_running += delta_qty
            claw_amt = _round_money(-per_unit * delta_qty)
            stored_amt = _round_money(_to_float(event.get("wt_amount")))
            if stored_amt != 0 and abs(stored_amt - claw_amt) <= 0.02:
                claw_amt = stored_amt
            availability, useable_delta = return_clawback_ledger_availability(
                db, event
            )
            snap = _bucket_snapshot_from_totals(
                purchased=purchased,
                shipped=shipped,
                verified=verified,
                cancelled=cancelled_running,
                returned=returned_running,
            )
            claw_description = return_clawback_ledger_description(
                db, event, delta_qty=delta_qty
            )
            claw_entry = _base_entry(
                order_uid,
                buyer,
                entry_id=f"wt:{wt_uid}" if wt_uid else f"wt:sale:{order_uid}:clawback:{returned_running}",
                entry_type="sale_proceeds_return_clawback",
                amount=claw_amt,
                entry_datetime=event_dt,
                description=claw_description,
                status_note=_sale_proceeds_status_note(claw_description),
                parent_entry_id=parent_id,
                event_type="return",
                include_in_running_balance=True,
                per_unit_proceeds=per_unit,
                returned_qty_delta=delta_qty,
                returned_qty_total=returned_running,
                cancelled_qty_delta=None,
                verified_qty_delta=None,
                wt_uid=wt_uid,
                wt_type=wt_type,
                wt_status=wt_status,
                **proceeds_breakdown_fields(
                    breakdown,
                    effective_qty=delta_qty,
                    purchased_qty=purchased,
                    sign=-1,
                ),
                **snap,
            )
            claw_entry["availability"] = availability
            claw_entry["useable_delta"] = useable_delta
            entries.append(claw_entry)

        elif wt_type == WT_TYPE_PARTIAL_DELIVERY_CREDIT and delta_qty > 0:
            credit_amt = _round_money(per_unit * delta_qty)
            stored_amt = _round_money(_to_float(event.get("wt_amount")))
            if stored_amt > 0 and abs(stored_amt - credit_amt) > 0.02:
                credit_amt = stored_amt
            available_at = event.get("wt_available_at")
            snap = _bucket_snapshot_from_totals(
                purchased=purchased,
                shipped=shipped,
                verified=verified,
                cancelled=cancelled_running,
                returned=returned_running,
                pending_return_window=int(
                    compute_proceeds_buckets(proceeds).get("pending_return_window") or 0
                ),
            )

            if (
                wt_status == WT_STATUS_POSTED
                and not available_at
                and credit_amt > 0.0001
            ):
                verify_note = (
                    f"{delta_qty} unit(s) verified (${credit_amt:.2f} now useable)"
                )
                verify_entry = _base_entry(
                    order_uid,
                    buyer,
                    entry_id=(
                        f"wt:{wt_uid}:verify"
                        if wt_uid
                        else f"wt:sale:{order_uid}:verify:{delta_qty}"
                    ),
                    entry_type="sale_proceeds_verify_transfer",
                    amount=credit_amt,
                    entry_datetime=event_dt,
                    description=f"Sale proceeds — {verify_note}",
                    status_note=verify_note,
                    parent_entry_id=parent_id,
                    event_type="verify",
                    include_in_running_balance=False,
                    per_unit_proceeds=per_unit,
                    cancelled_qty_delta=None,
                    returned_qty_delta=None,
                    verified_qty_delta=delta_qty,
                    wt_uid=wt_uid,
                    wt_type=wt_type,
                    wt_status=wt_status,
                    **proceeds_breakdown_fields(
                        breakdown,
                        effective_qty=delta_qty,
                        purchased_qty=purchased,
                    ),
                    **snap,
                )
                verify_entry["availability"] = "useable"
                verify_entry["useable_delta"] = credit_amt
                entries.append(verify_entry)
            elif wt_status == WT_STATUS_POSTED and available_at and credit_amt > 0.0001:
                release_note = (
                    f"return window expired (${credit_amt:.2f} now useable)"
                )
                release_entry = _base_entry(
                    order_uid,
                    buyer,
                    entry_id=f"wt:{wt_uid}:release" if wt_uid else f"wt:sale:{order_uid}:release",
                    entry_type="sale_proceeds_return_window_release",
                    amount=0.0,
                    entry_datetime=event_dt,
                    description=f"Sale proceeds — {release_note}",
                    status_note=release_note,
                    parent_entry_id=parent_id,
                    event_type="release",
                    include_in_running_balance=False,
                    per_unit_proceeds=per_unit,
                    cancelled_qty_delta=None,
                    returned_qty_delta=None,
                    verified_qty_delta=None,
                    wt_uid=wt_uid,
                    wt_type=wt_type,
                    wt_status=wt_status,
                    **proceeds_breakdown_fields(
                        breakdown,
                        effective_qty=delta_qty,
                        purchased_qty=purchased,
                    ),
                    **snap,
                )
                release_entry["availability"] = "useable"
                release_entry["useable_delta"] = credit_amt
                entries.append(release_entry)

    return entries


def fetch_seller_sale_uids(db, profile_id):
    """Distinct sale order uids for businesses owned by this profile."""
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
    seller_ids = list(ids)
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


def build_proceeds_narrative_for_profile(db, profile_id, wt_rows, pending_rows):
    """
    Build all order-level proceeds ledger entries for a seller profile.

    Every seller sale gets an original entry; event rows come from wallet_transactions.
    """
    order_uids = fetch_seller_sale_uids(db, profile_id)
    if not order_uids:
        return [], set()

    order_uid_list = sorted(order_uids)
    order_meta = fetch_orders_buyer_and_datetime(db, order_uid_list)
    wt_events_by_order = preload_wt_events_for_orders(db, order_uid_list)

    entries = []
    for order_uid in order_uid_list:
        buyer, order_dt = order_meta.get(order_uid, ("buyer", None))
        entries.extend(
            build_order_proceeds_ledger_entries(
                db,
                order_uid,
                buyer_name=buyer,
                order_datetime=order_dt,
                wt_events=wt_events_by_order.get(order_uid, []),
            )
        )
    return entries, set(order_uids)


def orders_using_proceeds_narrative(db, profile_id, wt_rows, pending_rows):
    """All seller sale orders use the narrative builder."""
    return set(fetch_seller_sale_uids(db, profile_id))
