"""
FE contract helpers for GET /api/v1/account-screen/personal/:profileId (schema v2).

Mobile reads purchases.rows[] only — no purchases.data[], no order_list_hydration merge.
Every row must ship units + display; missing chip labels → FE shows NA.
"""

from order_display import is_awaiting_seller, is_cancel_request

_ORDER_LEVEL_FINANCIAL_KEYS = frozenset(
    {
        "transaction_total",
        "transaction_amount",
        "transaction_taxes",
        "transaction_fees",
        "transaction_shipping",
        "order_bounty_paid",
    }
)

# Legacy list fields stripped from v2 rows (sale rows). Return/cancel rows keep
# refund money fields and pending_return payloads — see _legacy_keys_for_row().
_LEGACY_ROW_KEYS = frozenset(
    {
        "pending_return",
        "pending_returns",
        "transaction_return_items",
        "is_return",
        "is_pending_return",
        "needs_shipping",
        "needs_shipment",
        "has_in_transit",
        "has_shippable_items",
        "purchased_units",
        "received_units",
        "received_item_count",
        "delivered_item_count",
        "shippable_item_count",
        "shipped_item_count",
        "unshipped_item_count",
        "all_items_shipped",
        "all_items_received",
        "ti_shipped_qty",
        "ti_received_qty",
        "ti_bs_return_window_days",
        "fulfillment_status",
        "transaction_return_status",
        "transaction_refund_status",
        "parent_sale_resolve_error",
        "lines",
    }
)

# Return/cancel rows keep refund + pending payloads for Purchases Amount column.
_RETURN_ROW_PRESERVED_KEYS = frozenset(
    {
        "pending_return",
        "estimated_refund",
        "is_return",
        "is_pending_return",
        "transaction_total",
        "transaction_amount",
        "transaction_taxes",
        "transaction_fees",
        "transaction_shipping",
        "refund_amount",
        "refund_total",
        "return_total",
        "returned_total",
        "estimated_total",
        "bounty_to_reclaim",
    }
)


def _legacy_keys_for_row(kind):
    keys = set(_LEGACY_ROW_KEYS)
    if kind in ("return", "pending_return"):
        keys -= _RETURN_ROW_PRESERVED_KEYS
    return keys

_UNITS_DEFAULTS = {
    "purchased_qty": 0,
    "cancelled_pre_ship_qty": 0,
    "cancelled_pre_ship_in_progress_qty": 0,
    "shipped_qty": 0,
    "remaining_to_ship_qty": 0,
    "verified_qty": 0,
    "verifiable_remaining_qty": 0,
    "active_qty": 0,
    "max_return_shipped_qty": 0,
    "max_cancel_unshipped_qty": 0,
}


def _return_line_totals(return_lines):
    qty = shipped = cancel = 0
    for line in return_lines or []:
        try:
            qty += int(line.get("return_quantity") or 0)
        except (TypeError, ValueError):
            pass
        try:
            shipped += int(line.get("return_shipped_qty") or 0)
        except (TypeError, ValueError):
            pass
        try:
            cancel += int(line.get("cancel_unshipped_qty") or 0)
        except (TypeError, ValueError):
            pass
    return qty, shipped, cancel


def _units_for_return_row(row):
    """Minimal units block for return / pending_return rows."""
    lines = row.get("return_lines") or []
    qty, shipped, cancel = _return_line_totals(lines)
    if not qty:
        try:
            qty = int(row.get("return_quantity_total") or row.get("ti_bs_qty") or 0)
        except (TypeError, ValueError):
            qty = 0
    units = dict(_UNITS_DEFAULTS)
    units["purchased_qty"] = qty
    units["active_qty"] = qty
    units["return_shipped_qty"] = shipped
    units["return_unshipped_qty"] = cancel
    if cancel and not shipped:
        units["cancelled_pre_ship_in_progress_qty"] = cancel
    return units


def _ensure_display(row):
    display = dict(row.get("display") or {})
    if not display.get("delivered_label"):
        display["delivered_label"] = "—"
    if not display.get("received_label"):
        display["received_label"] = "—"
    if "qty" not in display:
        units = row.get("units") or {}
        display["qty"] = int(
            units.get("active_qty") or units.get("purchased_qty") or row.get("ti_bs_qty") or 0
        )
    return display


def _modal_copy_fields(row):
    """Recommended return/cancel modal copy — omitted sections stay blank on FE."""
    kind = row.get("row_kind")
    if kind not in ("return", "pending_return"):
        return {}

    req_view = dict(row)
    req_view.setdefault("trr_return_transaction_uid", None if kind == "pending_return" else row.get("transaction_uid"))
    cancel = is_cancel_request(req_view) or bool(row.get("is_cancel_before_ship"))
    awaiting = kind == "pending_return" and is_awaiting_seller(req_view)

    copy = {}
    if row.get("display_status"):
        copy["status_banner"] = row["display_status"]
        copy["banner_text"] = row["display_status"]

    if cancel:
        copy["is_cancel_before_ship"] = True
        copy["items_section_title"] = "Cancellation items"
        copy["return_items_section_title"] = "Cancellation items"
        copy["cancel_confirm_note"] = (
            "These units were never shipped. Confirm to cancel and refund the buyer."
        )
    else:
        copy["items_section_title"] = "Return items"
        copy["return_items_section_title"] = "Return items"

    if awaiting:
        copy["awaiting_seller_confirm"] = True
        copy["seller_approval_required"] = True
        if cancel:
            copy["pending_seller_note"] = (
                "Confirm or decline this pre-ship cancellation request."
            )
            copy["seller_action_note"] = copy["pending_seller_note"]
        else:
            copy["pending_seller_note"] = (
                "Confirm or decline this return request."
            )
            copy["seller_action_note"] = copy["pending_seller_note"]

    note = row.get("transaction_return_note") or row.get("trr_note")
    if note and "pending_seller_note" not in copy:
        copy["seller_action_note"] = note

    return copy


def finalize_account_screen_row(row):
    """Ensure one purchases.rows[] / seller_transactions[] entry meets v2 contract."""
    if not isinstance(row, dict):
        return row

    out = dict(row)
    kind = out.get("row_kind") or (
        "pending_return"
        if out.get("is_pending_return")
        else "return"
        if out.get("is_return") or (out.get("transaction_type") or "").lower() == "return"
        else "sale_line"
        if out.get("ti_uid") and (out.get("transaction_type") or "sale").lower() == "sale"
        else "sale"
    )
    out["row_kind"] = kind

    if kind in ("return", "pending_return"):
        out.setdefault("units", _units_for_return_row(out))
        for key in ("return_status", "refund_status", "display_status"):
            if out.get(key) is None and kind == "pending_return":
                out.setdefault(key, "")
    elif kind == "sale_line":
        units = dict(_UNITS_DEFAULTS)
        units.update(out.get("units") or {})
        out["units"] = units
        for key in _ORDER_LEVEL_FINANCIAL_KEYS:
            out.pop(key, None)
    elif kind == "sale":
        units = dict(_UNITS_DEFAULTS)
        units.update(out.get("units") or {})
        out["units"] = units

    out["display"] = _ensure_display(out)
    out.update(_modal_copy_fields(out))

    for key in _legacy_keys_for_row(kind):
        out.pop(key, None)

    return out


def finalize_account_screen_rows(rows):
    return [finalize_account_screen_row(r) for r in (rows or []) if isinstance(r, dict)]


def build_purchases_v2_section(*, code, message, rows):
    """purchases block for personal account-screen — rows only, no data[]."""
    finalized = finalize_account_screen_rows(rows)
    section = {
        "code": code if code is not None else 200,
        "count": len(finalized),
        "rows": finalized,
    }
    if message:
        section["message"] = message
    return section
