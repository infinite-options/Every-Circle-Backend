"""Order line quantity context for proceeds, ledger descriptions, and rollups."""

from transactions import (
    _load_open_return_requests,
    _return_ledger_line_split,
    ensure_return_split_columns,
)

# Request-scoped caches (cleared at start of wallet_ledger build).
_ORDER_QTY_CACHE = {}
_LINE_QTY_CACHE = {}
_RETURN_SPLITS_CACHE = {}
_OPEN_RETURN_REQS_CACHE = {}


def clear_ledger_quantity_caches():
    _ORDER_QTY_CACHE.clear()
    _LINE_QTY_CACHE.clear()
    _RETURN_SPLITS_CACHE.clear()
    _OPEN_RETURN_REQS_CACHE.clear()


def _confirmed_return_splits_for_order(db, order_uid):
    if order_uid in _RETURN_SPLITS_CACHE:
        return _RETURN_SPLITS_CACHE[order_uid]

    ensure_return_split_columns(db)
    q = db.execute(
        """
        SELECT
            rti.ti_bs_qty,
            rti.ti_return_shipped_qty,
            rti.ti_cancel_unshipped_qty,
            rti.ti_original_ti_uid,
            rt.transaction_uid AS return_tx_uid
        FROM every_circle.transactions_items rti
        INNER JOIN every_circle.transactions rt
            ON rti.ti_transaction_id = rt.transaction_uid
        WHERE rt.transaction_original_uid = %s
          AND COALESCE(rt.transaction_type, 'return') = 'return'
        """,
        (order_uid,),
    )
    splits = {}
    for row in q.get("result") or []:
        ti_uid = row.get("ti_original_ti_uid")
        if not ti_uid:
            continue
        shipped, cancel = _return_ledger_line_split(db, row.get("return_tx_uid"), row)
        prev_shipped, prev_cancel = splits.get(ti_uid, (0, 0))
        splits[ti_uid] = (prev_shipped + shipped, prev_cancel + cancel)

    _RETURN_SPLITS_CACHE[order_uid] = splits
    return splits


def _open_return_requests_for_order(db, order_uid):
    if order_uid in _OPEN_RETURN_REQS_CACHE:
        return _OPEN_RETURN_REQS_CACHE[order_uid]
    reqs = _load_open_return_requests(db, order_uid)
    _OPEN_RETURN_REQS_CACHE[order_uid] = reqs
    return reqs


def _reserved_return_split_from_reqs(open_reqs, ti_uid, *, exclude_trr_uid=None):
    from transactions import _as_trr_uid_set, _items_from_return_request_row

    exclude = _as_trr_uid_set(exclude_trr_uid)
    return_shipped = 0
    cancel_unshipped = 0
    for req in open_reqs or []:
        if req.get("trr_uid") in exclude:
            continue
        items = req.get("items") or _items_from_return_request_row(req)
        cancel_only = bool(
            req.get("trr_cancel_unshipped")
            or req.get("cancel_unshipped")
            or req.get("pre_ship_cancel")
        )
        for entry in items:
            if entry.get("transaction_item_uid") != ti_uid:
                continue
            try:
                total = int(entry.get("return_quantity") or 0)
            except (TypeError, ValueError):
                continue
            if entry.get("return_shipped_qty") is not None:
                try:
                    return_shipped += int(entry.get("return_shipped_qty") or 0)
                except (TypeError, ValueError):
                    pass
            elif not cancel_only:
                return_shipped += total
            if entry.get("cancel_unshipped_qty") is not None:
                try:
                    cancel_unshipped += int(entry.get("cancel_unshipped_qty") or 0)
                except (TypeError, ValueError):
                    pass
            elif cancel_only:
                cancel_unshipped += total
    return return_shipped, cancel_unshipped


def _returnable_verified_qty_cached(
    db, order_uid, ti_uid, verified_qty, *, order_splits=None, open_reqs=None
):
    splits = order_splits if order_splits is not None else _confirmed_return_splits_for_order(
        db, order_uid
    )
    returned = int((splits.get(ti_uid) or (0, 0))[0])
    if open_reqs is None:
        open_reqs = _open_return_requests_for_order(db, order_uid)
    reserved_return, _cancel = _reserved_return_split_from_reqs(open_reqs, ti_uid)
    return max(int(verified_qty or 0) - returned - reserved_return, 0)


def line_quantity_context(db, order_uid, ti_uid, *, row=None, order_splits=None, open_reqs=None):
    """
    Authoritative per-line units for wallet / ledger math.

    Returns purchased, shipped, cancelled, returned, verified, active_units,
    shippable_units, remaining_to_ship.
    """
    cache_key = (order_uid, ti_uid)
    if order_splits is None and open_reqs is None and cache_key in _LINE_QTY_CACHE:
        return _LINE_QTY_CACHE[cache_key]

    if row is None:
        q = db.execute(
            """
            SELECT ti_uid, ti_bs_qty, COALESCE(ti_shipped_qty, 0) AS ti_shipped_qty,
                   COALESCE(ti_received_qty, 0) AS ti_received_qty
            FROM every_circle.transactions_items
            WHERE ti_uid = %s AND ti_transaction_id = %s
            """,
            (ti_uid, order_uid),
        )
        rows = q.get("result") or []
        row = rows[0] if rows else {}

    purchased = int(row.get("ti_bs_qty") or 0)
    shipped = int(row.get("ti_shipped_qty") or 0)
    verified = int(row.get("ti_received_qty") or 0)
    if order_splits is None:
        order_splits = _confirmed_return_splits_for_order(db, order_uid)
    returned, cancelled = order_splits.get(ti_uid, (0, 0))
    active_units = max(purchased - cancelled - returned, 0)
    shippable_units = max(purchased - cancelled, 0)
    remaining_to_ship = max(purchased - shipped - cancelled, 0)
    net_verified_not_returned = max(verified - returned, 0)
    unverified_shipped = max((shipped - returned) - net_verified_not_returned, 0)
    pending_verification_units = remaining_to_ship + max(0, shipped - verified)
    verified_returnable = _returnable_verified_qty_cached(
        db,
        order_uid,
        ti_uid,
        verified,
        order_splits=order_splits,
        open_reqs=open_reqs,
    )
    max_return_shipped_qty = max(0, min(shipped, verified) - returned)
    max_cancel_unshipped_qty = max(0, (purchased - shipped) - cancelled)
    ctx = {
        "ti_uid": ti_uid,
        "purchased_qty": purchased,
        "shipped_qty": shipped,
        "cancelled_qty": cancelled,
        "returned_qty": returned,
        "verified_qty": verified,
        "active_units": active_units,
        "shippable_units": shippable_units,
        "remaining_to_ship": remaining_to_ship,
        "unverified_shipped_qty": unverified_shipped,
        "verified_returnable_qty": verified_returnable,
        "net_verified_not_returned": net_verified_not_returned,
        "net_verified_held": net_verified_not_returned,
        "pending_verification_units": pending_verification_units,
        "max_return_shipped_qty": max_return_shipped_qty,
        "max_cancel_unshipped_qty": max_cancel_unshipped_qty,
    }
    if order_splits is None and open_reqs is None:
        _LINE_QTY_CACHE[cache_key] = ctx
    return ctx


def order_quantity_context(db, order_uid):
    """Sum line quantity context across all sale lines on an order."""
    if order_uid in _ORDER_QTY_CACHE:
        return _ORDER_QTY_CACHE[order_uid]

    q = db.execute(
        """
        SELECT ti_uid, ti_bs_qty, COALESCE(ti_shipped_qty, 0) AS ti_shipped_qty,
               COALESCE(ti_received_qty, 0) AS ti_received_qty
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s
        ORDER BY ti_uid ASC
        """,
        (order_uid,),
    )
    totals = {
        "purchased_qty": 0,
        "shipped_qty": 0,
        "cancelled_qty": 0,
        "returned_qty": 0,
        "verified_qty": 0,
        "active_units": 0,
        "shippable_units": 0,
        "remaining_to_ship": 0,
        "unverified_shipped_qty": 0,
        "verified_returnable_qty": 0,
        "net_verified_held": 0,
        "pending_verification_units": 0,
        "lines": [],
    }
    order_splits = _confirmed_return_splits_for_order(db, order_uid)
    open_reqs = _open_return_requests_for_order(db, order_uid)
    for row in q.get("result") or []:
        ti_uid = row.get("ti_uid")
        ctx = line_quantity_context(
            db,
            order_uid,
            ti_uid,
            row=row,
            order_splits=order_splits,
            open_reqs=open_reqs,
        )
        totals["lines"].append(ctx)
        for key in (
            "purchased_qty",
            "shipped_qty",
            "cancelled_qty",
            "returned_qty",
            "verified_qty",
            "active_units",
            "shippable_units",
            "remaining_to_ship",
            "unverified_shipped_qty",
            "verified_returnable_qty",
            "net_verified_held",
            "pending_verification_units",
        ):
            totals[key] += int(ctx.get(key) or 0)
    _ORDER_QTY_CACHE[order_uid] = totals
    return totals


def compute_seller_proceeds_per_unit(db, order_uid, *, qty=None, eligible_total=None):
    """
    Seller proceeds per purchased unit at order placement.

    (merchandise + shipping − bounty) / purchased_qty — excludes tax and platform fees.
    """
    from wallet_transactions_service import compute_seller_eligible_total

    if qty is None:
        qty = order_quantity_context(db, order_uid)
    purchased = int(qty.get("purchased_qty") or 0)
    total = (
        eligible_total
        if eligible_total is not None
        else compute_seller_eligible_total(db, order_uid)
    )
    if purchased <= 0 or total <= 0:
        return 0.0
    return round(total / purchased, 4)


def compute_seller_proceeds_ledger_amounts(db, order_uid, *, qty=None):
    """
    Split seller proceeds into pending-verification vs return-window buckets.

    per_unit = full-order seller pool / purchased_qty (fixed at placement)
    pending_verification_amount  = per_unit × pending_verification_units
    pending_return_window_amount = per_unit × net_verified_held
    """
    from wallet_transactions_service import compute_seller_eligible_total

    if qty is None:
        qty = order_quantity_context(db, order_uid)
    pending_verify_units = int(qty.get("pending_verification_units") or 0)
    net_verified_held = int(qty.get("net_verified_held") or 0)
    full_order_total = compute_seller_eligible_total(db, order_uid)
    per_unit = compute_seller_proceeds_per_unit(
        db, order_uid, qty=qty, eligible_total=full_order_total
    )
    return {
        **qty,
        "net_seller_proceeds": full_order_total,
        "per_active_unit_proceeds": per_unit,
        "seller_proceeds_per_unit": per_unit,
        "pending_verification_amount": round(per_unit * pending_verify_units, 4),
        "pending_return_window_amount": round(per_unit * net_verified_held, 4),
    }


def verification_denominator(ctx):
    """Units the buyer still owns (verification / bounty context)."""
    active = int(ctx.get("active_units") or 0)
    if active > 0:
        return active
    return int(ctx.get("purchased_qty") or 0)


def compute_proceeds_buckets(ctx):
    """
    Live bucket counts for seller-proceeds ledger (source of truth).

    pending_shipment + pending_verification + pending_return_window = active_qty
    """
    if not ctx:
        return {}
    purchased = int(ctx.get("purchased_qty") or 0)
    cancelled = int(ctx.get("cancelled_qty") or 0)
    returned = int(ctx.get("returned_qty") or 0)
    verified = int(ctx.get("verified_qty") or 0)
    shipped = int(ctx.get("shipped_qty") or 0)
    unverified_shipped = int(ctx.get("unverified_shipped_qty") or 0)
    pending_return_window = max(0, verified - returned)
    pending_verification = unverified_shipped
    pending_shipment = max(
        0, purchased - cancelled - unverified_shipped - verified
    )
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


def quantity_context_fields(ctx):
    """Optional structured qty fields for API responses."""
    if not ctx:
        return {}
    buckets = compute_proceeds_buckets(ctx)
    return {
        **buckets,
        "shippable_qty": ctx.get("shippable_units"),
        "verified_returnable_qty": ctx.get("verified_returnable_qty"),
        "pending_verification_units": ctx.get("pending_verification_units"),
        "net_verified_held": ctx.get("net_verified_held"),
        "per_unit_proceeds": ctx.get("seller_proceeds_per_unit"),
    }
