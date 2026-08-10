"""
Account Screen API v3 contract helpers.

Authoritative money.* and display.* blocks — the frontend must not reconstruct
totals from legacy transaction_* fields or cross-array joins.
"""

from datetime import datetime

from datetime_utils import parse_stored_datetime
from line_commerce_fields import (
    order_money_from_line_snapshots,
    return_money_from_line_snapshots,
    round_money,
)
_ORDER_ROW_KINDS = frozenset({"sale", "sale_line", "order"})
_RETURN_ROW_KINDS = frozenset({"return", "pending_return"})
_V2_STRIP_KEYS = frozenset(
    {
        "transaction_total",
        "transaction_amount",
        "transaction_taxes",
        "transaction_fees",
        "transaction_shipping",
        "order_bounty_paid",
        "refund_amount",
        "refund_total",
        "return_total",
        "returned_total",
        "estimated_refund",
        "estimated_total",
        "line_total",
        "pending_return",
        "pending_returns",
        "open_returns",
        "completed_return_uids",
        "return_lines",
        "is_return",
        "is_pending_return",
        "needs_shipping",
        "needs_shipment",
        "has_in_transit",
        "has_shippable_items",
        "fulfillment_method",
        "requires_shipping",
        "return_status",
        "refund_status",
        "display_status",
        "transaction_return_status",
        "transaction_refund_status",
        "order_line_count",
        "line_uid",
        "offering_uid",
        "unit_price",
        "line_bounty_paid",
        "bounty_to_reclaim",
        "cancel_unshipped",
        "pre_ship_cancel",
        "is_cancel_before_ship",
        "trr_transaction_uid",
        "original_transaction_uid",
        "transaction_type",
        "count",
        "code",
        "message",
    }
)


def format_money_label(amount):
    """Signed USD display string, e.g. '$99.00' or '-$66.00'."""
    value = round_money(amount)
    if value < 0:
        return f"-${abs(value):,.2f}"
    return f"${value:,.2f}"


def format_signed_pool_label(delta):
    """Pending/useable column label: '+$24.00', '−$66.00', or '—'."""
    try:
        value = round_money(delta)
    except (TypeError, ValueError):
        return "—"
    if abs(value) < 0.0001:
        return "—"
    formatted = f"${abs(value):,.2f}"
    if value > 0:
        return f"+{formatted}"
    return f"−{formatted}"


def _ledger_float(value):
    if value is None:
        return 0.0
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def ledger_entry_pool_deltas(entry):
    """
    Signed pending-pool and useable-pool impacts for one ledger row.

    Prefer explicit pending_delta / useable_delta when present; otherwise derive
    from amount, availability, and include_in_running_balance.
    """
    if not isinstance(entry, dict):
        return 0.0, 0.0

    amount = round_money(_ledger_float(entry.get("amount")))

    if entry.get("useable_delta") is not None:
        useable_delta = round_money(_ledger_float(entry.get("useable_delta")))
    else:
        useable_delta = 0.0

    if entry.get("pending_delta") is not None:
        pending_delta = round_money(_ledger_float(entry.get("pending_delta")))
        return pending_delta, useable_delta

    availability = (entry.get("availability") or "").strip().lower()
    include_in_balance = entry.get("include_in_running_balance") is not False

    if useable_delta != 0:
        if include_in_balance:
            pending_delta = round_money(amount - useable_delta)
        else:
            pending_delta = 0.0
    elif availability == "pending" and amount != 0:
        pending_delta = amount
    elif amount != 0 and include_in_balance:
        pending_delta = amount
    else:
        pending_delta = 0.0

    return pending_delta, useable_delta


def build_ledger_entry_display(entry, tz_name=None):
    """Wallet ledger table display.* block — pending/useable column labels."""
    pending_delta, useable_delta = ledger_entry_pool_deltas(entry)
    return {
        "date_label": format_date_label(entry.get("entry_datetime"), tz_name) or "—",
        "pending_amount_label": format_signed_pool_label(pending_delta),
        "useable_amount_label": format_signed_pool_label(useable_delta),
        "type_label": entry.get("entry_type_label") or "—",
    }


def normalize_tb_percentage_display(pct):
    """
    Bounty share for API display: integer 0–100.

    Persisted tb_percentage is a pool fraction (e.g. 0.4 = 40%); legacy rows may
    already store whole percents (40).
    """
    if pct is None or str(pct).strip() == "":
        return None
    try:
        val = float(pct)
    except (TypeError, ValueError):
        return None
    if 0 < abs(val) <= 1:
        val *= 100
    return int(round(val))


def format_tb_percent_label(pct):
    """Display label for bounty share, e.g. '40%'."""
    whole = normalize_tb_percentage_display(pct)
    return f"{whole}%" if whole is not None else "—"


def format_date_label(dt_value, tz_name=None):
    """MMM D in profile timezone."""
    dt = parse_stored_datetime(dt_value)
    if dt is None:
        return None
    if tz_name:
        try:
            from zoneinfo import ZoneInfo

            dt = dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            pass
    return dt.strftime("%b ") + str(dt.day)


def build_v3_units(row):
    """Map v2 units ledger to v3 {purchased_qty, return_shipped_qty, cancel_unshipped_qty}."""
    kind = row.get("row_kind")
    units = row.get("units") or {}

    if kind in _RETURN_ROW_KINDS:
        shipped = int(units.get("return_shipped_qty") or 0)
        cancel = int(units.get("cancel_unshipped_qty") or units.get("return_unshipped_qty") or 0)
        if not shipped and not cancel:
            shipped = int(units.get("purchased_qty") or 0)
        qty = shipped + cancel
        if not qty:
            qty = int(row.get("return_quantity_total") or row.get("ti_bs_qty") or 0)
        return {
            "purchased_qty": 0,
            "return_shipped_qty": shipped or qty,
            "cancel_unshipped_qty": cancel,
        }

    purchased = int(
        units.get("purchased_qty")
        or units.get("active_qty")
        or row.get("ti_bs_qty")
        or 0
    )
    return {
        "purchased_qty": purchased,
        "return_shipped_qty": 0,
        "cancel_unshipped_qty": 0,
    }


def build_order_money(row, *, sale_line=None):
    """money block for order rows — stored line snapshots only."""
    return order_money_from_line_snapshots(row)


def build_return_money(row, *, sale_line=None):
    """money block for return rows — prorate original sale line snapshots."""
    return return_money_from_line_snapshots(row, sale_line=sale_line)


def build_row_money(row, *, sale_line=None):
    kind = row.get("row_kind")
    if kind in _RETURN_ROW_KINDS:
        return build_return_money(row, sale_line=sale_line)
    return build_order_money(row)


def _return_line_display_qty(line):
    """Units in one return line (physical return + pre-ship cancel)."""
    if not isinstance(line, dict):
        return 0
    try:
        rq = line.get("return_quantity")
        if rq is not None:
            v = int(rq or 0)
            if v > 0:
                return v
    except (TypeError, ValueError):
        pass
    try:
        shipped = int(line.get("return_shipped_qty") or 0)
        cancel = int(line.get("cancel_unshipped_qty") or 0)
    except (TypeError, ValueError):
        shipped = cancel = 0
    if shipped or cancel:
        return shipped + cancel
    for key in ("ti_bs_qty", "quantity"):
        if line.get(key) is not None:
            try:
                return int(line.get(key) or 0)
            except (TypeError, ValueError):
                pass
    return 0


def _return_row_display_qty(row, units=None):
    """Total return qty for one product on a purchases list row."""
    lines = row.get("return_lines") or []
    if lines:
        total = sum(_return_line_display_qty(line) for line in lines)
        if total:
            return total
    if units is None:
        units = build_v3_units(row)
    try:
        shipped = int(units.get("return_shipped_qty") or 0)
        cancel = int(units.get("cancel_unshipped_qty") or 0)
    except (TypeError, ValueError):
        shipped = cancel = 0
    if shipped or cancel:
        return shipped + cancel
    try:
        return int(row.get("return_quantity_total") or 0)
    except (TypeError, ValueError):
        return 0


def build_v3_display(row, money, *, audience="buyer", tz_name=None):
    """Backend-owned display labels for list rows."""
    kind = row.get("row_kind")
    v2_display = row.get("display") or {}
    units = build_v3_units(row)
    dt = row.get("transaction_datetime") or row.get("entry_datetime")

    if kind in _RETURN_ROW_KINDS:
        qty = _return_row_display_qty(row, units)
        cancelled = int(units.get("cancel_unshipped_qty") or 0)
        if kind == "pending_return":
            type_label = "Cancel" if row.get("is_cancel_before_ship") else "Return"
        else:
            type_label = "Return"
        amount = money.get("customer_credit")
    else:
        qty = int(units.get("purchased_qty") or 0)
        cancelled = 0
        type_label = "Order"
        amount = money.get("customer_total")

    qty = max(int(qty or 0), 0)
    display = {
        "date_label": format_date_label(dt, tz_name) or v2_display.get("date_label") or "—",
        "qty": qty,
        "qty_label": str(qty) if qty else "—",
        "cancelled_label": str(cancelled) if cancelled else "—",
        "delivered_label": v2_display.get("delivered_label") or "—",
        "received_label": v2_display.get("received_label") or "—",
        "amount_label": format_money_label(amount) if amount is not None else "NA",
        "type_label": type_label,
    }
    if v2_display.get("received_action"):
        display["received_action"] = v2_display["received_action"]
    elif audience == "buyer" and kind not in _RETURN_ROW_KINDS:
        display["received_action"] = "status"

    if audience == "seller":
        kind = map_row_kind_v3(row.get("row_kind"))
        if kind == "return":
            reclaim = (
                row.get("bounty_to_reclaim")
                or (row.get("bounty") or {}).get("bounty_to_reclaim")
            )
            if reclaim:
                display["bounty_label"] = format_money_label(-round_money(reclaim))
            else:
                bounty_amt = (
                    row.get("line_bounty_paid")
                    or row.get("order_bounty_paid")
                    or row.get("bounty_paid")
                    or (row.get("bounty") or {}).get("order_bounty_paid")
                    or 0
                )
                display["bounty_label"] = format_money_label(bounty_amt)
        else:
            bounty_amt = (
                row.get("line_bounty_paid")
                or row.get("order_bounty_paid")
                or row.get("bounty_paid")
                or (row.get("bounty") or {}).get("order_bounty_paid")
                or 0
            )
            display["bounty_label"] = format_money_label(bounty_amt)
        display["days_open"] = v2_display.get("days_open") or "—"

    return display


def build_v3_actions(row):
    kind = row.get("row_kind")
    is_return = kind in _RETURN_ROW_KINDS
    return {
        "can_open_order_detail": kind in _ORDER_ROW_KINDS,
        "can_open_return_detail": is_return,
    }


def build_return_logistics(row):
    kind = row.get("row_kind")
    if kind not in _RETURN_ROW_KINDS:
        return None

    return_status = row.get("return_status") or ""
    refund_status = row.get("refund_status") or ""
    display_status = row.get("display_status") or ""

    if kind == "pending_return":
        if not return_status:
            return_status = "returning"
        if not refund_status:
            refund_status = "pending"
        if not display_status:
            display_status = "Pending"

    if not return_status and not refund_status and not display_status:
        return None

    return {
        "return_status": return_status or None,
        "refund_status": refund_status or None,
        "display_status": display_status or None,
    }


def map_row_kind_v3(kind):
    if kind in ("sale", "sale_line"):
        return "order"
    if kind == "pending_return":
        return "return"
    return kind or "order"


def strip_v2_row_fields(row):
    out = dict(row)
    for key in _V2_STRIP_KEYS:
        out.pop(key, None)
    return out


def attention_level_for_row(row):
    """Seller attention chip priority: red > orange > purple."""
    if not isinstance(row, dict):
        return None
    if row.get("needs_shipping") or row.get("needs_shipment"):
        return "red"
    units = row.get("units") or {}
    if int(units.get("verifiable_remaining_qty") or 0) > 0:
        return "orange"
    if row.get("is_pending_return") or row.get("open_returns"):
        return "purple"
    if row.get("row_kind") == "pending_return":
        return "purple"
    return None


def attention_priority(level):
    order = {"red": 3, "orange": 2, "purple": 1}
    return order.get(level, 0)
