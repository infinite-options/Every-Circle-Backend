"""
Wallet reservations for open return requests.

When a buyer opens a return in-window, reserve seller sale proceeds and bounty
pool so those amounts cannot become spendable until the return is confirmed or
declined. Reservations are stored as wallet_transactions rows (status=reserved).

Bounty reclaim reservations mirror wallet_ledger (reduce actual + pending/useable
pools). Seller proceeds use held return_clawback rows + pending holds.
"""

import json

from datetime_utils import utc_now_str
from wallet_ids import resolve_wallet_profile_id
from wallet_service import (
    _round_money,
    _to_float,
    adjust_wallet_reserve,
    apply_pending_clawback_hold,
    credit_bounty_to_wallet,
    debit_bounty_from_wallet,
    get_wallet_row,
    release_pending_clawback_hold,
)
from wallet_transactions_service import (
    WT_STATUS_CLEARED,
    WT_STATUS_HELD,
    WT_STATUS_POSTED,
    WT_STATUS_RESERVED,
    WT_TYPE_BOUNTY_RECLAIM_RESERVATION,
    WT_TYPE_PARTIAL_DELIVERY_CREDIT,
    WT_TYPE_RETURN_CLAWBACK,
    WT_TYPE_RETURN_REFUND_RESERVATION,
    _ensure_wallet_transactions_table,
    _insert_return_clawback_row,
    _new_wallet_transaction_uid,
    _sale_line_reversal_context,
    compute_seller_proceeds_reversal_for_line,
    resolve_seller_wallet_profile_id,
    seller_proceeds_reversal_description,
)

_TTR_RESERVATION_COLUMNS_READY = False


def ensure_trr_reservation_columns(db):
    """Add TRR columns for persisted estimated refund + bounty reclaim."""
    global _TTR_RESERVATION_COLUMNS_READY
    if _TTR_RESERVATION_COLUMNS_READY:
        return
    for ddl in (
        "ALTER TABLE every_circle.transaction_return_requests "
        "ADD COLUMN trr_bounty_to_reclaim DECIMAL(18,4) NULL",
        "ALTER TABLE every_circle.transaction_return_requests "
        "ADD COLUMN trr_estimated_refund_json TEXT NULL",
    ):
        db.execute(ddl, cmd="post")
    _TTR_RESERVATION_COLUMNS_READY = True


def _fetch_reservation_by_key(db, idempotency_key):
    q = db.execute(
        """
        SELECT wt_uid, wt_profile_id, wt_amount, wt_status, wt_type,
               wt_transaction_id, wt_ti_id, wt_note
        FROM every_circle.wallet_transactions
        WHERE wt_idempotency_key = %s
        LIMIT 1
        """,
        (idempotency_key,),
    )
    rows = q.get("result") or []
    return rows[0] if rows else None


def _insert_proceeds_clawback_hold(
    db,
    *,
    trr_uid,
    profile_id,
    buyer_id,
    seller_id,
    transaction_id,
    ti_id,
    return_qty,
    return_shipped_qty=0,
    cancel_unshipped_qty=0,
    currency="USD",
):
    """
    Insert a held return_clawback row when buyer opens a return (delivered order).

    Idempotent per trr_uid. Reduces wallet_pending via apply_pending_clawback_hold.
    """
    try:
        return_shipped_qty = int(return_shipped_qty or 0)
        cancel_unshipped_qty = int(cancel_unshipped_qty or 0)
    except (TypeError, ValueError):
        return_shipped_qty = 0
        cancel_unshipped_qty = 0

    if return_shipped_qty <= 0 and cancel_unshipped_qty <= 0:
        try:
            total_qty = int(return_qty or 0)
        except (TypeError, ValueError):
            total_qty = 0
        if total_qty > 0:
            return_shipped_qty = total_qty

    ti_row, line_bounty = _sale_line_reversal_context(db, ti_id, transaction_id)
    clawback_amount = compute_seller_proceeds_reversal_for_line(
        ti_row,
        return_shipped_qty=return_shipped_qty,
        cancel_unshipped_qty=cancel_unshipped_qty,
        line_bounty_ledger=line_bounty,
    )
    if clawback_amount <= 0:
        return {"code": 200, "skipped": True, "amount": 0, "trr_uid": trr_uid}

    hold_qty = return_shipped_qty + cancel_unshipped_qty
    unit_cost = _round_money(clawback_amount / hold_qty) if hold_qty > 0 else 0.0
    idempotency_key = f"return_clawback_hold:{trr_uid}"
    ins = _insert_return_clawback_row(
        db,
        idempotency_key=idempotency_key,
        profile_id=profile_id,
        buyer_id=buyer_id,
        seller_id=seller_id,
        transaction_id=transaction_id,
        ti_id=ti_id,
        qty=hold_qty,
        unit_cost=unit_cost,
        amount=-clawback_amount,
        currency=currency,
        status=WT_STATUS_HELD,
        note=trr_uid,
    )
    if ins.get("code") != 200:
        return ins

    wallet_result = None
    if not ins.get("idempotent_replay"):
        wallet_result = apply_pending_clawback_hold(db, profile_id, clawback_amount)
        if wallet_result.get("code") != 200:
            return wallet_result

    return {
        "code": 200,
        "idempotent_replay": bool(ins.get("idempotent_replay")),
        "wt_uid": ins.get("wt_uid"),
        "amount": clawback_amount,
        "wt_type": WT_TYPE_RETURN_CLAWBACK,
        "wt_status": WT_STATUS_HELD,
        "wallet": wallet_result,
        "trr_uid": trr_uid,
        "return_shipped_qty": return_shipped_qty,
        "cancel_unshipped_qty": cancel_unshipped_qty,
        "description": seller_proceeds_reversal_description(
            return_shipped_qty,
            cancel_unshipped_qty,
            amount=clawback_amount,
        ),
    }


def _apply_bounty_reservation_wallet(db, profile_id, amount):
    """
    Debit bounty pools for an open return reservation.

    Matches wallet_ledger bounty_reclaim_reserved: reduce actual/lifetime and
    prefer pending over useable (useable_delta stays 0 when pending covers it).
    """
    return debit_bounty_from_wallet(
        db, profile_id, amount, prefer_pending=True
    )


def _clear_bounty_reservation_wallet(db, profile_id, amount, *, finalize):
    """
    Undo or keep the bounty debit when a reservation is cleared.

    Legacy rows parked funds in wallet_reserve (useable→reserve). New rows debit
    actual immediately. Detect legacy via remaining wallet_reserve.
    """
    amount = _round_money(amount)
    if not profile_id or amount <= 0:
        return {"code": 200, "skipped": True}

    wallet = get_wallet_row(db, profile_id) or {}
    reserve = _to_float(wallet.get("wallet_reserve"))

    if reserve + 0.0001 >= amount:
        # Legacy: undo useable→reserve park first.
        undo = adjust_wallet_reserve(db, profile_id, -amount)
        if undo.get("code") != 200:
            return undo
        if finalize:
            # Permanent reclaim after undoing the park.
            return debit_bounty_from_wallet(
                db, profile_id, amount, prefer_pending=True
            )
        return {"code": 200, "legacy_reserve_cleared": True, "finalized": False}

    # New path: insert already debited actual. Finalize keeps debit; decline restores.
    if finalize:
        return {"code": 200, "kept_debit": True, "finalized": True}
    return credit_bounty_to_wallet(db, profile_id, amount)


def _insert_reservation_row(
    db,
    *,
    idempotency_key,
    profile_id,
    buyer_id,
    seller_id,
    transaction_id,
    ti_id,
    wt_type,
    amount,
    trr_uid,
    currency="USD",
):
    amount = _round_money(amount)
    if amount <= 0:
        return {"code": 200, "skipped": True, "amount": 0}

    existing = _fetch_reservation_by_key(db, idempotency_key)
    if existing:
        return {
            "code": 200,
            "idempotent_replay": True,
            "wt_uid": existing.get("wt_uid"),
            "amount": _round_money(existing.get("wt_amount")),
        }

    wt_uid = _new_wallet_transaction_uid(db)
    if not wt_uid:
        return {"code": 500, "message": "Failed to generate wt_uid for reservation"}

    now = utc_now_str()
    row = {
        "wt_uid": wt_uid,
        "wt_profile_id": profile_id,
        "wt_buyer_id": buyer_id or "",
        "wt_seller_id": seller_id or "",
        "wt_transaction_id": transaction_id,
        "wt_ti_id": ti_id or "",
        "wt_type": wt_type,
        "wt_status": WT_STATUS_RESERVED,
        "wt_qty": 0,
        "wt_received_qty_after": 0,
        "wt_unit_cost": 0,
        "wt_amount": amount,
        "wt_currency": (currency or "USD")[:8],
        "wt_idempotency_key": idempotency_key,
        "wt_note": trr_uid,
        "wt_available_at": None,
        "wt_created_at": now,
        "wt_updated_at": now,
    }
    ins = db.insert("every_circle.wallet_transactions", row)
    if ins.get("code") != 200:
        msg = (ins.get("message") or "").lower()
        if "duplicate entry" in msg:
            existing = _fetch_reservation_by_key(db, idempotency_key)
            if existing:
                return {
                    "code": 200,
                    "idempotent_replay": True,
                    "wt_uid": existing.get("wt_uid"),
                    "amount": _round_money(existing.get("wt_amount")),
                }
        return {
            "code": ins.get("code", 500),
            "message": ins.get("message", "Failed to insert reservation row"),
        }

    if wt_type == WT_TYPE_BOUNTY_RECLAIM_RESERVATION:
        wallet_result = _apply_bounty_reservation_wallet(db, profile_id, amount)
    else:
        # Legacy return_refund_reservation path (if any remain).
        wallet_result = adjust_wallet_reserve(db, profile_id, amount)
    if wallet_result.get("code") != 200:
        return wallet_result

    return {
        "code": 200,
        "idempotent_replay": False,
        "wt_uid": wt_uid,
        "amount": amount,
        "wallet": wallet_result,
    }


def _bounty_reclaim_splits_for_line(db, ti_uid, return_qty):
    """Return [{profile_id, amount}] bounty reclaim per recipient for a line."""
    if not ti_uid or return_qty <= 0:
        return []

    from transactions import _bounty_scale_for_line

    ti_q = db.execute(
        """
        SELECT ti.ti_bs_qty
        FROM every_circle.transactions_items ti
        WHERE ti.ti_uid = %s
        LIMIT 1
        """,
        (ti_uid,),
    )
    ti_rows = ti_q.get("result") or []
    if not ti_rows:
        return []
    original_qty = int(ti_rows[0].get("ti_bs_qty") or 0)
    scale = _bounty_scale_for_line(return_qty, original_qty)
    if scale is None or scale <= 0:
        return []

    bounty_q = db.execute(
        """
        SELECT tb_profile_id, tb_amount
        FROM every_circle.transactions_bounty
        WHERE tb_ti_id = %s AND tb_amount > 0.0001
        """,
        (ti_uid,),
    )
    splits = []
    for row in bounty_q.get("result") or []:
        profile_id = row.get("tb_profile_id")
        amt = _round_money(_to_float(row.get("tb_amount")) * scale)
        if profile_id and amt > 0:
            splits.append({"profile_id": profile_id, "amount": amt})
    return splits


def create_reservations_for_return_request(
    db,
    *,
    trr_uid,
    transaction_uid,
    ti_uid,
    refund_amount,
    bounty_to_reclaim,
    buyer_id,
    seller_id,
    return_qty=0,
    return_shipped_qty=None,
    cancel_unshipped_qty=None,
    currency="USD",
):
    """
    Reserve seller proceeds and bounty pool for one TRR row.
    Idempotent per trr_uid.
    """
    _ensure_wallet_transactions_table(db)
    ensure_trr_reservation_columns(db)

    seller_profile_id = resolve_seller_wallet_profile_id(db, seller_id)
    if not seller_profile_id:
        return {
            "code": 500,
            "message": f"Unable to resolve seller wallet for {seller_id!r}",
            "trr_uid": trr_uid,
        }

    results = {"trr_uid": trr_uid, "reservations": []}
    try:
        return_qty = int(return_qty or 0)
    except (TypeError, ValueError):
        return_qty = 0

    if return_qty > 0 and ti_uid:
        if return_shipped_qty is None or cancel_unshipped_qty is None:
            trr_q = db.execute(
                """
                SELECT trr_items_json, trr_ti_uid, trr_return_quantity,
                       trr_cancel_unshipped
                FROM every_circle.transaction_return_requests
                WHERE trr_uid = %s
                LIMIT 1
                """,
                (trr_uid,),
            )
            trr_rows = trr_q.get("result") or []
            if trr_rows:
                from transactions import _items_from_return_request_row

                items = _items_from_return_request_row(trr_rows[0])
                for entry in items:
                    if entry.get("transaction_item_uid") != ti_uid:
                        continue
                    if return_shipped_qty is None:
                        return_shipped_qty = entry.get("return_shipped_qty")
                    if cancel_unshipped_qty is None:
                        cancel_unshipped_qty = entry.get("cancel_unshipped_qty")
                    break
        try:
            return_shipped_qty = int(return_shipped_qty or 0)
        except (TypeError, ValueError):
            return_shipped_qty = 0
        try:
            cancel_unshipped_qty = int(cancel_unshipped_qty or 0)
        except (TypeError, ValueError):
            cancel_unshipped_qty = 0

        proceeds = _insert_proceeds_clawback_hold(
            db,
            trr_uid=trr_uid,
            profile_id=seller_profile_id,
            buyer_id=buyer_id,
            seller_id=seller_id,
            transaction_id=transaction_uid,
            ti_id=ti_uid,
            return_qty=return_qty,
            return_shipped_qty=return_shipped_qty,
            cancel_unshipped_qty=cancel_unshipped_qty,
            currency=currency,
        )
        if proceeds.get("code") != 200:
            return proceeds
        if not proceeds.get("skipped"):
            results["reservations"].append(proceeds)

    bounty_splits = _bounty_reclaim_splits_for_line(db, ti_uid, return_qty)
    if not bounty_splits and bounty_to_reclaim > 0 and ti_uid:
        bounty_splits = [{"profile_id": None, "amount": _round_money(bounty_to_reclaim)}]

    split_total = _round_money(sum(s["amount"] for s in bounty_splits))
    if bounty_to_reclaim > 0 and split_total <= 0:
        bounty_q = db.execute(
            """
            SELECT tb_profile_id, tb_amount
            FROM every_circle.transactions_bounty tb
            INNER JOIN every_circle.transactions_items ti ON tb.tb_ti_id = ti.ti_uid
            WHERE ti.ti_transaction_id = %s AND tb.tb_amount > 0.0001
            """,
            (transaction_uid,),
        )
        rows = bounty_q.get("result") or []
        if rows:
            order_bounty = sum(_to_float(r.get("tb_amount")) for r in rows)
            for row in rows:
                share = _round_money(
                    bounty_to_reclaim * (_to_float(row.get("tb_amount")) / order_bounty)
                )
                if share > 0:
                    bounty_splits.append(
                        {"profile_id": row.get("tb_profile_id"), "amount": share}
                    )

    for idx, split in enumerate(bounty_splits):
        profile_id = split.get("profile_id")
        amt = _round_money(split.get("amount"))
        if not profile_id or amt <= 0:
            continue
        key = f"return_reservation:{trr_uid}:bounty:{profile_id}:{idx}"
        bounty_res = _insert_reservation_row(
            db,
            idempotency_key=key,
            profile_id=profile_id,
            buyer_id=buyer_id,
            seller_id=seller_id,
            transaction_id=transaction_uid,
            ti_id=ti_uid,
            wt_type=WT_TYPE_BOUNTY_RECLAIM_RESERVATION,
            amount=amt,
            trr_uid=trr_uid,
            currency=currency,
        )
        if bounty_res.get("code") != 200:
            return bounty_res
        results["reservations"].append(bounty_res)

    results["code"] = 200
    return results


def persist_trr_refund_metadata(
    db,
    *,
    trr_uid,
    transaction_uid,
    ti_uid,
    return_qty,
    trr_estimated_total=None,
    orig_tx=None,
):
    """
    Persist trr_bounty_to_reclaim and trr_estimated_refund_json on an existing TRR.

    Used by backfill and anywhere open TRRs predate reservation metadata columns.
    """
    from transactions import (
        _bounty_to_reclaim_for_items,
        _bounty_to_reclaim_for_line,
        _estimated_refund_api_payload,
        _fetch_ti_row_for_bounty,
        _load_sale_for_return,
        _refund_breakdown_from_context,
        _validate_and_price_return_items,
    )

    ensure_trr_reservation_columns(db)
    if not trr_uid or not transaction_uid or not ti_uid:
        return {
            "code": 400,
            "message": "trr_uid, transaction_uid, and ti_uid are required",
        }

    if orig_tx is None:
        orig_tx = _load_sale_for_return(db, transaction_uid)
    if not orig_tx:
        return {
            "code": 404,
            "message": f"Sale not found: {transaction_uid}",
            "trr_uid": trr_uid,
        }

    try:
        return_qty = int(return_qty or 0)
    except (TypeError, ValueError):
        return_qty = 0

    ti_row = _fetch_ti_row_for_bounty(db, ti_uid, transaction_uid) or {}

    item_entry = {"transaction_item_uid": ti_uid, "return_quantity": return_qty}
    bounty_to_reclaim = _bounty_to_reclaim_for_line(
        db, transaction_uid, ti_uid, return_qty, ti_row=ti_row or None
    )

    refund_amount = _round_money(trr_estimated_total)
    estimated_refund = None

    ok, _err, ctx = _validate_and_price_return_items(
        db,
        transaction_uid,
        [item_entry],
        exclude_trr_uid=trr_uid,
        enforce_return_eligibility=False,
    )
    if ok and ctx:
        for line in ctx.get("lines_processed") or []:
            if line.get("original_ti_uid") != ti_uid:
                continue
            if not refund_amount:
                refund_amount = _round_money(
                    _to_float(line.get("line_subtotal"))
                    + _to_float(line.get("line_tax"))
                    + _to_float(line.get("line_shipping"))
                )
            estimated_refund = _estimated_refund_api_payload(
                {
                    "subtotal": line.get("line_subtotal", 0),
                    "taxes": line.get("line_tax", 0),
                    "shipping": line.get("line_shipping", 0),
                    "fees_allocated": 0,
                    "total_customer_credit": refund_amount,
                    "wallet_refund": 0,
                    "stripe_refund": refund_amount,
                }
            )
            break
        if estimated_refund is None:
            refund_meta = _refund_breakdown_from_context(orig_tx, ctx)
            estimated_refund = _estimated_refund_api_payload(refund_meta)
            if not refund_amount:
                refund_amount = _round_money(
                    estimated_refund.get("total")
                    or estimated_refund.get("total_customer_credit")
                )

    if estimated_refund is None and refund_amount > 0:
        estimated_refund = _estimated_refund_api_payload(
            {
                "subtotal": refund_amount,
                "taxes": 0,
                "shipping": 0,
                "fees_allocated": 0,
                "total_customer_credit": refund_amount,
                "wallet_refund": 0,
                "stripe_refund": refund_amount,
            }
        )

    upd = db.update(
        "every_circle.transaction_return_requests",
        {"trr_uid": trr_uid},
        {
            "trr_bounty_to_reclaim": bounty_to_reclaim,
            "trr_estimated_refund_json": json.dumps(estimated_refund)
            if estimated_refund
            else None,
            "trr_updated_at": utc_now_str(),
        },
    )
    if upd.get("code") != 200:
        return {
            "code": upd.get("code", 500),
            "message": upd.get("message", "Failed to persist TRR metadata"),
            "trr_uid": trr_uid,
        }

    return {
        "code": 200,
        "trr_uid": trr_uid,
        "bounty_to_reclaim": bounty_to_reclaim,
        "estimated_refund": estimated_refund,
        "refund_amount": refund_amount,
    }


def create_reservations_for_return_batch(
    db,
    *,
    orig_tx,
    trr_uids,
    ctx,
    refund_meta,
):
    """
    Create wallet reservations for a batch of TRRs created in one POST.
    Persists estimated_refund JSON + bounty_to_reclaim on each TRR row.
    """
    from transactions import (
        _bounty_to_reclaim_for_items,
        _bounty_to_reclaim_for_line,
        _estimated_refund_api_payload,
        _fetch_ti_row_for_bounty,
        _line_estimated_total,
    )

    _ensure_wallet_transactions_table(db)
    ensure_trr_reservation_columns(db)

    transaction_uid = orig_tx.get("transaction_uid")
    buyer_id = orig_tx.get("transaction_profile_id")
    seller_id = orig_tx.get("transaction_business_id")
    lines_by_ti = {
        line.get("original_ti_uid"): line for line in (ctx.get("lines_processed") or [])
    }

    batch_refund_payload = _estimated_refund_api_payload(refund_meta)

    outcomes = []
    for trr_uid in trr_uids or []:
        trr_q = db.execute(
            """
            SELECT trr_uid, trr_ti_uid, trr_return_quantity, trr_estimated_total
            FROM every_circle.transaction_return_requests
            WHERE trr_uid = %s
            LIMIT 1
            """,
            (trr_uid,),
        )
        trr_rows = trr_q.get("result") or []
        if not trr_rows:
            continue
        trr = trr_rows[0]
        ti_uid = trr.get("trr_ti_uid")
        try:
            return_qty = int(trr.get("trr_return_quantity") or 0)
        except (TypeError, ValueError):
            return_qty = 0

        line = lines_by_ti.get(ti_uid) or {}
        refund_amount = _round_money(
            trr.get("trr_estimated_total")
            or _line_estimated_total(orig_tx, ctx, line)
            or 0
        )

        ti_row = (line.get("snapshot") or {}) if line else {}
        if not ti_row.get("ti_uid") and ti_uid:
            ti_row = _fetch_ti_row_for_bounty(db, ti_uid, transaction_uid) or {}

        item_entry = {"transaction_item_uid": ti_uid, "return_quantity": return_qty}
        bounty_to_reclaim = _bounty_to_reclaim_for_line(
            db,
            transaction_uid,
            ti_uid,
            return_qty,
            ti_row=ti_row or None,
        )

        line_refund_payload = _estimated_refund_api_payload(
            {
                "subtotal": line.get("line_subtotal", 0),
                "taxes": line.get("line_tax", 0),
                "shipping": line.get("line_shipping", 0),
                "fees_allocated": 0,
                "total_customer_credit": refund_amount,
                "wallet_refund": 0,
                "stripe_refund": refund_amount,
            }
        )
        db.update(
            "every_circle.transaction_return_requests",
            {"trr_uid": trr_uid},
            {
                "trr_bounty_to_reclaim": bounty_to_reclaim,
                "trr_estimated_refund_json": json.dumps(line_refund_payload),
                "trr_updated_at": utc_now_str(),
            },
        )

        currency = (ti_row.get("ti_bs_cost_currency") or "USD") if ti_row else "USD"
        return_shipped_qty = int(line.get("return_shipped_qty") or 0)
        cancel_unshipped_qty = int(line.get("cancel_unshipped_qty") or 0)
        result = create_reservations_for_return_request(
            db,
            trr_uid=trr_uid,
            transaction_uid=transaction_uid,
            ti_uid=ti_uid,
            refund_amount=refund_amount,
            bounty_to_reclaim=bounty_to_reclaim,
            buyer_id=buyer_id,
            seller_id=seller_id,
            return_qty=return_qty,
            return_shipped_qty=return_shipped_qty,
            cancel_unshipped_qty=cancel_unshipped_qty,
            currency=currency,
        )
        if result.get("code") != 200:
            return result
        outcomes.append(result)

    return {
        "code": 200,
        "trr_uids": trr_uids,
        "estimated_refund": batch_refund_payload,
        "reservations": outcomes,
    }


def _active_clawback_hold_rows(db, trr_uids):
    if not trr_uids:
        return []
    placeholders = ", ".join(["%s"] * len(trr_uids))
    q = db.execute(
        f"""
        SELECT wt_uid, wt_profile_id, wt_amount, wt_type, wt_transaction_id,
               wt_ti_id, wt_note, wt_status
        FROM every_circle.wallet_transactions
        WHERE wt_type = %s
          AND wt_status = %s
          AND wt_note IN ({placeholders})
        """,
        tuple([WT_TYPE_RETURN_CLAWBACK, WT_STATUS_HELD] + list(trr_uids)),
    )
    return q.get("result") or []


def _active_reservation_rows(db, *, trr_uids=None, transaction_uid=None, ti_uid=None):
    clauses = ["wt_status = %s"]
    params = [WT_STATUS_RESERVED]
    if trr_uids:
        placeholders = ", ".join(["%s"] * len(trr_uids))
        clauses.append(f"wt_note IN ({placeholders})")
        params.extend(trr_uids)
    if transaction_uid:
        clauses.append("wt_transaction_id = %s")
        params.append(transaction_uid)
    if ti_uid:
        clauses.append("wt_ti_id = %s")
        params.append(ti_uid)
    q = db.execute(
        f"""
        SELECT wt_uid, wt_profile_id, wt_amount, wt_type, wt_transaction_id,
               wt_ti_id, wt_note
        FROM every_circle.wallet_transactions
        WHERE {" AND ".join(clauses)}
        """,
        tuple(params),
    )
    return q.get("result") or []


def sum_active_proceeds_reservation(db, *, transaction_uid=None, ti_uid=None):
    """Sum held return_clawback rows (negative) for open return requests."""
    clauses = ["wt_type = %s", "wt_status = %s"]
    params = [WT_TYPE_RETURN_CLAWBACK, WT_STATUS_HELD]
    if transaction_uid:
        clauses.append("wt_transaction_id = %s")
        params.append(transaction_uid)
    if ti_uid:
        clauses.append("wt_ti_id = %s")
        params.append(ti_uid)
    q = db.execute(
        f"""
        SELECT COALESCE(SUM(ABS(wt_amount)), 0) AS reserved
        FROM every_circle.wallet_transactions
        WHERE {" AND ".join(clauses)}
        """,
        tuple(params),
    )
    rows = q.get("result") or []
    if not rows:
        return 0.0
    return _round_money(rows[0].get("reserved"))


def sum_active_bounty_reservation(db, *, ti_uid=None, profile_id=None):
    clauses = ["wt_status = %s", "wt_type = %s"]
    params = [WT_STATUS_RESERVED, WT_TYPE_BOUNTY_RECLAIM_RESERVATION]
    if ti_uid:
        clauses.append("wt_ti_id = %s")
        params.append(ti_uid)
    if profile_id:
        wallet_id = resolve_wallet_profile_id(profile_id)
        clauses.append("wt_profile_id IN (%s, %s)")
        params.extend([profile_id, wallet_id])
    q = db.execute(
        f"""
        SELECT COALESCE(SUM(wt_amount), 0) AS reserved
        FROM every_circle.wallet_transactions
        WHERE {" AND ".join(clauses)}
        """,
        tuple(params),
    )
    rows = q.get("result") or []
    if not rows:
        return 0.0
    return _round_money(rows[0].get("reserved"))


def sum_active_bounty_reservation_for_line(db, ti_uid):
    return sum_active_bounty_reservation(db, ti_uid=ti_uid)


def clear_return_reservations(
    db,
    trr_uids,
    *,
    finalize=False,
):
    """
    Clear bounty reservations and held return_clawback rows for the given TRR uids.

    When finalize=False (decline), restores wallet_pending for clawback holds.
    """
    _ensure_wallet_transactions_table(db)
    uids = [u for u in (trr_uids or []) if u]
    if not uids:
        return {"code": 200, "cleared": 0, "skipped": True}

    now = utc_now_str()
    cleared = 0

    clawback_rows = _active_clawback_hold_rows(db, uids)
    new_clawback_status = WT_STATUS_POSTED if finalize else WT_STATUS_CLEARED
    for row in clawback_rows:
        wt_uid = row.get("wt_uid")
        profile_id = row.get("wt_profile_id")
        amount = _round_money(abs(_to_float(row.get("wt_amount"))))
        upd = db.update(
            "every_circle.wallet_transactions",
            {"wt_uid": wt_uid},
            {"wt_status": new_clawback_status, "wt_updated_at": now},
        )
        if upd.get("code") != 200:
            return {
                "code": upd.get("code", 500),
                "message": upd.get("message", "Failed to clear clawback hold"),
                "wt_uid": wt_uid,
            }
        if not finalize and amount > 0:
            release_result = release_pending_clawback_hold(db, profile_id, amount)
            if release_result.get("code") != 200:
                return release_result
        cleared += 1

    rows = _active_reservation_rows(db, trr_uids=uids)
    if not rows and not clawback_rows:
        return {"code": 200, "cleared": 0, "skipped": True}

    new_status = WT_STATUS_POSTED if finalize else WT_STATUS_CLEARED
    for row in rows:
        wt_uid = row.get("wt_uid")
        profile_id = row.get("wt_profile_id")
        amount = _round_money(row.get("wt_amount"))
        wt_type = row.get("wt_type") or ""
        upd = db.update(
            "every_circle.wallet_transactions",
            {"wt_uid": wt_uid},
            {"wt_status": new_status, "wt_updated_at": now},
        )
        if upd.get("code") != 200:
            return {
                "code": upd.get("code", 500),
                "message": upd.get("message", "Failed to clear reservation"),
                "wt_uid": wt_uid,
            }
        if wt_type == WT_TYPE_BOUNTY_RECLAIM_RESERVATION:
            wallet_result = _clear_bounty_reservation_wallet(
                db, profile_id, amount, finalize=finalize
            )
        else:
            wallet_result = adjust_wallet_reserve(db, profile_id, -amount)
        if wallet_result.get("code") != 200:
            return wallet_result
        cleared += 1

    return {"code": 200, "cleared": cleared, "finalized": finalize}


def release_pending_after_reservation_clear(db, transaction_uid, ti_uid):
    """
    After reservations are cleared, release any eligible held proceeds + bounty
    for the line if the return window has elapsed and no other open returns block.
    """
    from seller_hold_release import release_seller_holds_for_line

    return release_seller_holds_for_line(db, transaction_uid, ti_uid)


def fetch_reservation_ledger_rows(db, profile_id):
    """Active return reservations for wallet ledger display."""
    wallet_id = resolve_wallet_profile_id(profile_id)
    q = db.execute(
        """
        SELECT
            wt.wt_uid,
            wt.wt_profile_id,
            wt.wt_buyer_id,
            wt.wt_transaction_id,
            wt.wt_ti_id,
            wt.wt_type,
            wt.wt_status,
            wt.wt_amount,
            wt.wt_qty,
            wt.wt_idempotency_key,
            wt.wt_currency,
            wt.wt_note,
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
          AND wt.wt_status = %s
          AND wt.wt_type IN (%s, %s)
        ORDER BY wt.wt_created_at DESC
        """,
        (
            profile_id,
            wallet_id,
            WT_STATUS_RESERVED,
            WT_TYPE_RETURN_REFUND_RESERVATION,
            WT_TYPE_BOUNTY_RECLAIM_RESERVATION,
        ),
    )
    return q.get("result") or []
