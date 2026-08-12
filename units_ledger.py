"""
Shared v2 unit ledger for buyer personal / Offering surfaces.

Used by account-screen personal, transaction receipt, and order detail so all
three agree on shipped / verified / returnable counts after any mutation.
"""

from order_quantity_context import (
    order_quantity_context,
    line_quantity_context,
    _open_return_requests_for_order,
    _confirmed_return_splits_for_order,
    _reserved_return_split_from_reqs,
)


def _order_fulfillment_method(db, order_uid):
    q = db.execute(
        """
        SELECT COALESCE(MAX(ti_fulfillment_method), 'ship') AS method
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s
        """,
        (order_uid,),
    )
    rows = q.get("result") or []
    return str(rows[0].get("method") if rows else "ship").strip().lower()


def _line_fulfillment_method(row):
    return str(row.get("ti_fulfillment_method") or row.get("fulfillment_method") or "ship").strip().lower()


def compute_unverified_shipped(*, shipped, verified):
    """
    Shipped units not yet buyer-verified.

    ti_shipped_qty / ti_received_qty are never reduced by returns/cancels.
    Completed physical returns come from the *verified* pool, so they must NOT
    be subtracted here — doing so double-counts and falsely zeroes Verify.
    """
    return max(0, int(shipped or 0) - int(verified or 0))


def compute_verifiable_remaining(
    *,
    shipped,
    verified,
    returned_shipped=0,
    return_in_progress_shipped=0,
):
    """
    Units the buyer may still confirm receipt of.

    Physical returns (completed or open) consume verified units first. Only the
    portion of an open return that exceeds net verified units can block verify.
    """
    unverified_shipped = compute_unverified_shipped(shipped=shipped, verified=verified)
    rip = max(0, int(return_in_progress_shipped or 0))
    if rip <= 0 or unverified_shipped <= 0:
        return unverified_shipped

    # Gross verified still includes units later returned; net = still with buyer.
    verified_not_returned = max(0, int(verified or 0) - int(returned_shipped or 0))
    return_on_unverified = max(0, rip - verified_not_returned)
    return_on_unverified = min(return_on_unverified, unverified_shipped)
    return max(0, unverified_shipped - return_on_unverified)


def _units_from_counts(
    *,
    purchased,
    shipped,
    verified,
    cancelled_pre_ship,
    returned_shipped,
    returned_unshipped,
    remaining_to_ship,
    return_in_progress_shipped,
    return_in_progress_unshipped,
    is_pickup_or_virtual,
):
    cancelled_pre_ship_in_progress = return_in_progress_unshipped

    if is_pickup_or_virtual:
        remaining_to_ship = 0
        shipped = verified

    # Do NOT subtract returned_shipped — returns come from verified, not unverified.
    unverified_shipped = compute_unverified_shipped(shipped=shipped, verified=verified)
    verifiable_remaining = compute_verifiable_remaining(
        shipped=shipped,
        verified=verified,
        returned_shipped=returned_shipped,
        return_in_progress_shipped=return_in_progress_shipped,
    )
    if is_pickup_or_virtual:
        receivable = max(
            purchased - cancelled_pre_ship - cancelled_pre_ship_in_progress,
            0,
        )
        verifiable_remaining = max(
            0, receivable - verified - return_in_progress_shipped
        )

    # active_qty: fulfillment chip denominator = purchased minus completed and
    # in-progress pre-ship cancels (Cancelling + Cancelled). Returns do not reduce it.
    active = max(
        purchased - cancelled_pre_ship - cancelled_pre_ship_in_progress,
        0,
    )

    remaining_returnable = max(
        active
        - returned_shipped
        - returned_unshipped
        - return_in_progress_shipped,
        0,
    )
    if is_pickup_or_virtual:
        remaining_returnable = max(
            verified - returned_shipped - return_in_progress_shipped, 0
        )

    max_return_shipped = min(
        remaining_returnable,
        max(0, verified - returned_shipped - return_in_progress_shipped),
    )
    unshipped_pool = max(
        purchased - shipped - cancelled_pre_ship - cancelled_pre_ship_in_progress,
        0,
    )
    max_cancel_unshipped = min(
        remaining_returnable,
        max(0, unshipped_pool - return_in_progress_unshipped),
    )
    if is_pickup_or_virtual:
        max_cancel_unshipped = max(
            0,
            purchased
            - cancelled_pre_ship
            - verified
            - return_in_progress_unshipped,
        )

    return {
        "purchased_qty": purchased,
        "cancelled_pre_ship_qty": cancelled_pre_ship,
        "cancelled_pre_ship_in_progress_qty": cancelled_pre_ship_in_progress,
        "shipped_qty": shipped,
        "remaining_to_ship_qty": remaining_to_ship,
        "verified_qty": verified,
        "unverified_shipped_qty": unverified_shipped,
        "verifiable_remaining_qty": verifiable_remaining,
        "returned_shipped_completed_qty": returned_shipped,
        "returned_unshipped_completed_qty": returned_unshipped,
        "return_in_progress_shipped_qty": return_in_progress_shipped,
        "return_in_progress_unshipped_qty": return_in_progress_unshipped,
        "active_qty": active,
        "remaining_returnable_qty": remaining_returnable,
        "max_return_shipped_qty": max_return_shipped,
        "max_cancel_unshipped_qty": max_cancel_unshipped,
    }


def order_fulfillment_method(db, order_uid):
    return _order_fulfillment_method(db, order_uid)


def line_units_ledger(
    db,
    order_uid,
    ti_uid,
    row=None,
    *,
    order_splits=None,
    open_reqs=None,
):
    """Per-line v2 unit buckets (must sum to order-level for multi-line orders)."""
    ctx = line_quantity_context(
        db,
        order_uid,
        ti_uid,
        row=row,
        order_splits=order_splits,
        open_reqs=open_reqs,
    )
    if row is None:
        q = db.execute(
            """
            SELECT ti_fulfillment_method
            FROM every_circle.transactions_items
            WHERE ti_uid = %s AND ti_transaction_id = %s
            """,
            (ti_uid, order_uid),
        )
        rows = q.get("result") or []
        row = rows[0] if rows else {}

    if open_reqs is None:
        open_reqs = _open_return_requests_for_order(db, order_uid)
    return_in_progress_shipped, return_in_progress_unshipped = (
        _reserved_return_split_from_reqs(open_reqs, ti_uid)
    )

    method = _line_fulfillment_method(row)
    return _units_from_counts(
        purchased=int(ctx.get("purchased_qty") or 0),
        shipped=int(ctx.get("shipped_qty") or 0),
        verified=int(ctx.get("verified_qty") or 0),
        cancelled_pre_ship=int(ctx.get("cancelled_qty") or 0),
        returned_shipped=int(ctx.get("returned_qty") or 0),
        returned_unshipped=0,
        remaining_to_ship=int(ctx.get("remaining_to_ship") or 0),
        return_in_progress_shipped=return_in_progress_shipped,
        return_in_progress_unshipped=return_in_progress_unshipped,
        is_pickup_or_virtual=method in ("pickup", "virtual"),
    )


def _held_return_window_units(db, order_uid):
    """Units with seller proceeds still held for return-window escrow (wallet source of truth)."""
    from wallet_transactions_service import (
        WT_STATUS_HELD,
        WT_TYPE_PARTIAL_DELIVERY_CREDIT,
    )

    if not order_uid:
        return 0
    q = db.execute(
        """
        SELECT COALESCE(SUM(wt_qty), 0) AS held_units
        FROM every_circle.wallet_transactions
        WHERE wt_transaction_id = %s
          AND wt_type = %s
          AND wt_status = %s
        """,
        (order_uid, WT_TYPE_PARTIAL_DELIVERY_CREDIT, WT_STATUS_HELD),
    )
    rows = q.get("result") or []
    if not rows:
        return 0
    return max(int(rows[0].get("held_units") or 0), 0)


def proceeds_buckets_from_sale_units(db, order_uid, *, qty_ctx=None):
    """
    Seller-proceeds bucket counts derived from v2 sale_units_ledger.

    Same canonical unit ledger as purchases.rows[].units / order-detail sale.units.
    pending_return_window uses wallet held rows when buyer verification is caught up.
    """
    if not order_uid:
        return {}

    units = sale_units_ledger(db, order_uid)
    if qty_ctx is None:
        qty_ctx = order_quantity_context(db, order_uid)

    pending_verification = int(units.get("verifiable_remaining_qty") or 0)
    pending_cancellation = int(units.get("cancelled_pre_ship_in_progress_qty") or 0)
    pending_shipment = int(units.get("remaining_to_ship_qty") or 0)
    return_in_progress_shipped = int(units.get("return_in_progress_shipped_qty") or 0)
    returned_shipped = int(units.get("returned_shipped_completed_qty") or 0)
    verified = int(units.get("verified_qty") or 0)
    shipped = int(units.get("shipped_qty") or 0)

    held_return_window = _held_return_window_units(db, order_uid)
    if held_return_window > 0:
        # Wallet held partial_delivery_credit rows (wt_available_at in future) win over
        # unit-ledger shortcuts — verified units can still be in the return window.
        pending_return_window = held_return_window
    elif pending_verification == 0 and return_in_progress_shipped == 0:
        if returned_shipped > 0 and (verified + returned_shipped) >= shipped:
            pending_return_window = 0
        else:
            pending_return_window = held_return_window
    else:
        pending_return_window = max(0, int(qty_ctx.get("net_verified_held") or 0))

    purchased = int(units.get("purchased_qty") or 0)
    cancelled = int(units.get("cancelled_pre_ship_qty") or 0)
    returned = returned_shipped + int(units.get("returned_unshipped_completed_qty") or 0)
    active_qty = int(units.get("active_qty") or 0)
    unverified_shipped = max(0, shipped - verified)

    return {
        "purchased_qty": purchased,
        "cancelled_qty": cancelled,
        "returned_qty": returned,
        "verified_qty": verified,
        "shipped_qty": shipped,
        "unverified_shipped_qty": unverified_shipped,
        "pending_shipment": pending_shipment,
        "pending_cancellation": pending_cancellation,
        "pending_verification": pending_verification,
        "pending_return_window": pending_return_window,
        "active_qty": active_qty,
    }


def sale_proceeds_original_availability(buckets):
    """Map live proceeds buckets to ledger availability on the immutable original row."""
    pending_total = sum(
        int(buckets.get(key) or 0)
        for key in (
            "pending_shipment",
            "pending_cancellation",
            "pending_verification",
            "pending_return_window",
        )
    )
    return "useable" if pending_total == 0 else "pending"


def sale_units_ledger(db, order_uid):
    """Order-level v2 unit buckets (matches account-screen sale row units)."""
    qty = order_quantity_context(db, order_uid)
    open_reqs = _open_return_requests_for_order(db, order_uid)
    fulfillment = _order_fulfillment_method(db, order_uid)
    is_pickup_or_virtual = fulfillment in ("pickup", "virtual")

    return_in_progress_shipped = 0
    return_in_progress_unshipped = 0
    for line in qty.get("lines") or []:
        ti_uid = line.get("ti_uid")
        rs, cu = _reserved_return_split_from_reqs(open_reqs, ti_uid)
        return_in_progress_shipped += rs
        return_in_progress_unshipped += cu

    return _units_from_counts(
        purchased=int(qty.get("purchased_qty") or 0),
        shipped=int(qty.get("shipped_qty") or 0),
        verified=int(qty.get("verified_qty") or 0),
        cancelled_pre_ship=int(qty.get("cancelled_qty") or 0),
        returned_shipped=int(qty.get("returned_qty") or 0),
        returned_unshipped=0,
        remaining_to_ship=int(qty.get("remaining_to_ship") or 0),
        return_in_progress_shipped=return_in_progress_shipped,
        return_in_progress_unshipped=return_in_progress_unshipped,
        is_pickup_or_virtual=is_pickup_or_virtual,
    )


def sync_line_units_api_fields(line):
    """
    Promote canonical units.* to line top-level for verify/return UIs.

    unverified_shipped_qty — shipped − verified (returns are not subtracted).
    verifiable_remaining_qty — units the buyer may confirm receipt of now.
    """
    if not isinstance(line, dict):
        return line
    units = line.get("units") or {}
    shipped = int(units.get("shipped_qty") or 0)
    verified = int(units.get("verified_qty") or 0)
    raw_unverified = max(0, shipped - verified)
    line["unverified_shipped_qty"] = int(
        units.get("unverified_shipped_qty") if units.get("unverified_shipped_qty") is not None else raw_unverified
    )
    line["verifiable_remaining_qty"] = int(units.get("verifiable_remaining_qty") or 0)
    for key in (
        "return_in_progress_shipped_qty",
        "return_in_progress_unshipped_qty",
        "max_return_shipped_qty",
        "max_cancel_unshipped_qty",
    ):
        if units.get(key) is not None:
            line[key] = int(units.get(key) or 0)
    return line


def attach_line_units_ledgers(db, order_uid, lines):
    """Add units to each sale line; shares one DB pass for splits/open requests."""
    if not lines:
        return lines
    order_splits = _confirmed_return_splits_for_order(db, order_uid)
    open_reqs = _open_return_requests_for_order(db, order_uid)
    out = []
    for line in lines:
        if not isinstance(line, dict):
            out.append(line)
            continue
        row = dict(line)
        ti_uid = row.get("ti_uid")
        if ti_uid:
            row["units"] = line_units_ledger(
                db,
                order_uid,
                ti_uid,
                row=row,
                order_splits=order_splits,
                open_reqs=open_reqs,
            )
            sync_line_units_api_fields(row)
        out.append(row)
    return out


def fulfillment_method(row):
    method = (
        row.get("fulfillment_method")
        or row.get("ti_fulfillment_method")
        or "ship"
    )
    return str(method).strip().lower()


def requires_shipping(row):
    if row.get("requires_shipping") is not None:
        return bool(row.get("requires_shipping"))
    return fulfillment_method(row) not in ("pickup", "virtual", "not_required")


def shippable_total(units):
    """Units used as the denominator for ship-progress fractions on sale rows."""
    active = int(units.get("active_qty") or 0)
    purchased = int(units.get("purchased_qty") or 0)
    return active if active > 0 else purchased


def pending_bounty_units(units):
    """
    Units whose bounty is still pending shipment and/or buyer verification.

    Same v2 ledger fields as purchases.rows[].units — single- and multi-unit orders
    use the identical shape (a qty-1 order is a multi-unit order of one).
    """
    if not units:
        return 0
    return int(units.get("remaining_to_ship_qty") or 0) + int(
        units.get("verifiable_remaining_qty") or 0
    )


def sale_display(row, units, *, include_qty=True):
    """Buyer purchase chip labels (FE renders verbatim)."""
    from order_display import build_sale_display

    return build_sale_display(row, units, audience="buyer", include_qty=include_qty)


def seller_sale_display(row, units, *, include_qty=True):
    """Seller / Offering Product Summary chip labels (FE renders verbatim)."""
    from order_display import build_sale_display

    return build_sale_display(row, units, audience="seller", include_qty=include_qty)


def enrich_sale_row_v2(db, row, *, audience="buyer"):
    """
    Shared v2 enrichment for sale list rows and order-detail sale headers.

    audience: "buyer" | "seller"
    """
    order_uid = row.get("transaction_uid")
    if not order_uid:
        return row

    out = dict(row)
    method = fulfillment_method(row)
    if method == "ship" and not row.get("ti_fulfillment_method"):
        method = order_fulfillment_method(db, order_uid)
    out["fulfillment_method"] = method
    out["requires_shipping"] = requires_shipping(out)

    units = sale_units_ledger(db, order_uid)
    out["units"] = units
    display_fn = seller_sale_display if audience == "seller" else sale_display
    out["display"] = display_fn(
        out, units, include_qty=(audience == "seller")
    )
    if audience == "seller":
        sync_legacy_unit_fields(out, units)
    return out


def sync_legacy_unit_fields(row, units):
    """Align legacy count fields with units ledger (avoid shippable_item_count confusion)."""
    if not isinstance(row, dict) or not units:
        return row
    row["ti_shipped_qty"] = int(units.get("shipped_qty") or 0)
    row["unshipped_item_count"] = int(units.get("remaining_to_ship_qty") or 0)
    row["purchased_units"] = int(units.get("purchased_qty") or 0)
    row["shipped_item_count"] = int(units.get("shipped_qty") or 0)
    row["ti_received_qty"] = int(units.get("verified_qty") or 0)
    row["received_item_count"] = int(units.get("verified_qty") or 0)
    row.pop("shippable_item_count", None)
    return row
