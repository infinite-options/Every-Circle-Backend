"""
Per-line account-screen rows for multi-SKU orders.

Expands one aggregated sale list row into one row per transactions_items line so
Offering / Purchases UIs can show separate chips per offering (150-*).
"""

from line_commerce_fields import attach_line_commerce_fields
from order_display import build_sale_display
from order_quantity_context import _open_return_requests_for_order
from transactions import (
    _clear_parent_sale_return_status,
    _line_bounty_totals,
    _to_float,
)
from units_ledger import (
    fulfillment_method,
    line_units_ledger,
    requires_shipping,
    sync_legacy_unit_fields,
)

_ORDER_LEVEL_FINANCIAL_KEYS = (
    "transaction_total",
    "transaction_amount",
    "transaction_taxes",
    "transaction_fees",
    "transaction_shipping",
    "order_bounty_paid",
)


def _round_money(value):
    return round(_to_float(value), 2)


def _line_total(line_row):
    qty = int(line_row.get("ti_bs_qty") or 0)
    unit = _to_float(line_row.get("ti_bs_cost"))
    return _round_money(unit * qty)


def _scoped_return_line_fields(out, return_lines):
    """Top-level ti_uid / ti_bs_id so FE can attach return rows to an offering."""
    if not return_lines:
        return out
    line = return_lines[0]
    ti_uid = line.get("ti_original_ti_uid") or line.get("ti_uid")
    ti_bs_id = line.get("ti_bs_id")
    if ti_uid:
        out["ti_uid"] = ti_uid
        out["line_uid"] = ti_uid
    if ti_bs_id:
        out["ti_bs_id"] = ti_bs_id
        out["offering_uid"] = ti_bs_id
    name = line.get("item_name")
    if name:
        out["purchased_item"] = name
    return out


def load_order_sale_lines(db, order_uid):
    """Sale lines for list expansion (one row per ti_uid)."""
    if not order_uid:
        return []
    q = db.execute(
        """
        SELECT
            ti.ti_uid,
            ti.ti_bs_id,
            ti.ti_bs_qty,
            ti.ti_bs_cost,
            COALESCE(ti.ti_shipped_qty, 0) AS ti_shipped_qty,
            COALESCE(ti.ti_received_qty, 0) AS ti_received_qty,
            ti.ti_fulfillment_method,
            ti.ti_shipping_not_required,
            ti.ti_shipping_amount,
            ti.ti_line_shipping_amount,
            ti.ti_listing_shipping,
            ti.ti_shipping_refundable,
            bs.bs_bounty,
            bs.bs_bounty_type,
            pe.profile_expertise_bounty,
            pe.profile_expertise_bounty_type,
            CASE
                WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_service_name
                WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_title
                WHEN ti.ti_bs_id LIKE '165-%%' THEN pw.profile_wish_title
                ELSE ti.ti_bs_id
            END AS item_name
        FROM every_circle.transactions_items ti
        LEFT JOIN every_circle.business_services bs ON ti.ti_bs_id = bs.bs_uid
        LEFT JOIN every_circle.profile_expertise pe ON ti.ti_bs_id = pe.profile_expertise_uid
        LEFT JOIN every_circle.wish_response wr ON ti.ti_bs_id = wr.wish_response_uid
        LEFT JOIN every_circle.profile_wish pw ON wr.wr_profile_wish_id = pw.profile_wish_uid
        WHERE ti.ti_transaction_id = %s
        ORDER BY ti.ti_uid ASC
        """,
        (order_uid,),
    )
    return q.get("result") or []


def split_row_by_return_lines(row):
    """
    Split a pending/return list row with multiple return_lines into one row per line.

    Preserves trr_uid per line when trr_uids[] is parallel to return_lines.
    """
    if not isinstance(row, dict):
        return [row]
    lines = row.get("return_lines") or []
    if len(lines) <= 1:
        out = dict(row)
        if lines:
            line = lines[0]
            out["ti_uid"] = line.get("ti_uid") or line.get("ti_original_ti_uid")
            out["ti_bs_id"] = line.get("ti_bs_id")
            out["purchased_item"] = line.get("item_name") or out.get("purchased_item")
        return [out]

    trr_uids = row.get("trr_uids") or []
    results = []
    for idx, line in enumerate(lines):
        entry = dict(row)
        entry["return_lines"] = [line]
        ti_uid = line.get("ti_uid") or line.get("ti_original_ti_uid")
        entry["ti_uid"] = ti_uid
        entry["ti_bs_id"] = line.get("ti_bs_id")
        entry["purchased_item"] = line.get("item_name") or entry.get("purchased_item")
        line_trr = line.get("trr_uid") or (
            trr_uids[idx] if idx < len(trr_uids) else None
        )
        if line_trr:
            entry["trr_uid"] = line_trr
            entry["trr_uids"] = [line_trr]
            entry["transaction_uid"] = line_trr
        try:
            line_rq = int(line.get("return_quantity") or 0)
        except (TypeError, ValueError):
            line_rq = 0
        entry["return_quantity_total"] = line_rq
        entry["ti_bs_qty"] = line_rq
        if line_rq and entry.get("refund_amount") and int(row.get("return_quantity_total") or 0) > line_rq:
            ratio = line_rq / float(row.get("return_quantity_total"))
            entry["refund_amount"] = round(_to_float(entry.get("refund_amount")) * ratio, 4)
            entry["estimated_total"] = round(_to_float(entry.get("estimated_total")) * ratio, 4)
            entry["bounty_to_reclaim"] = round(
                _to_float(entry.get("bounty_to_reclaim")) * ratio, 4
            )
        base_uid = entry.get("trr_uid") or entry.get("transaction_uid") or ""
        entry["row_uid"] = f"{base_uid}:{ti_uid}" if ti_uid else base_uid
        results.append(entry)
    return results


def _open_return_summaries_for_line(db, sale_row, order_uid, ti_uid):
    from account_screen_purchases_v2 import _open_return_summary
    from transactions import _is_open_return

    summaries = []
    for req in _open_return_requests_for_order(db, order_uid) or []:
        if not _is_open_return(req.get("return_status"), req.get("refund_status")):
            continue
        if req.get("trr_return_transaction_uid"):
            continue
        items = req.get("items") or []
        if items and not any(
            (e.get("transaction_item_uid") or e.get("ti_uid")) == ti_uid for e in items
        ):
            continue
        summary = _open_return_summary(db, sale_row, req)
        if summary:
            summaries.append(summary)
    return summaries


def transform_sale_line_row(
    db,
    sale_row,
    line_row,
    *,
    audience="seller",
    order_line_count=1,
    line_bounty_paid=0.0,
):
    """One account-screen row scoped to a single sale line (offering/SKU)."""
    order_uid = sale_row.get("transaction_uid")
    ti_uid = line_row.get("ti_uid")
    out = dict(sale_row)

    out["ti_uid"] = ti_uid
    out["ti_bs_id"] = line_row.get("ti_bs_id")
    out["ti_bs_qty"] = int(line_row.get("ti_bs_qty") or 0)
    out["purchased_item"] = line_row.get("item_name") or out.get("purchased_item")
    out["unit_price"] = line_row.get("ti_bs_cost")
    out["line_total"] = _line_total(line_row)
    out["line_bounty_paid"] = _round_money(line_bounty_paid)
    out["ti_fulfillment_method"] = line_row.get("ti_fulfillment_method")
    out["ti_shipped_qty"] = int(line_row.get("ti_shipped_qty") or 0)
    out["ti_received_qty"] = int(line_row.get("ti_received_qty") or 0)
    for key in (
        "ti_shipping_amount",
        "ti_line_shipping_amount",
        "ti_listing_shipping",
        "ti_shipping_refundable",
        "bs_bounty",
        "bs_bounty_type",
        "profile_expertise_bounty",
        "profile_expertise_bounty_type",
    ):
        if line_row.get(key) is not None:
            out[key] = line_row.get(key)

    out["order_uid"] = order_uid
    out["transaction_uid"] = order_uid
    out["line_uid"] = ti_uid
    out["offering_uid"] = line_row.get("ti_bs_id")
    out["order_line_count"] = order_line_count

    header = dict(out)
    method = line_row.get("ti_fulfillment_method") or fulfillment_method(out)
    header["fulfillment_method"] = method
    header["requires_shipping"] = requires_shipping(header)

    units = line_units_ledger(db, order_uid, ti_uid, row=line_row)
    out["units"] = units
    out["fulfillment_method"] = method
    out["requires_shipping"] = header["requires_shipping"]
    out["display"] = build_sale_display(header, units, audience=audience)

    if audience == "seller":
        sync_legacy_unit_fields(out, units)
        _clear_parent_sale_return_status(out)
    else:
        open_returns = _open_return_summaries_for_line(db, out, order_uid, ti_uid)
        if open_returns:
            out["open_returns"] = open_returns
            _clear_parent_sale_return_status(out)

    out["row_kind"] = "sale_line"
    out["row_uid"] = ti_uid

    for key in _ORDER_LEVEL_FINANCIAL_KEYS:
        out.pop(key, None)

    attach_line_commerce_fields(
        out,
        line_bounty_paid=line_bounty_paid,
    )

    for key in (
        "needs_shipping",
        "needs_shipment",
        "has_in_transit",
        "has_shippable_items",
        "received_units",
        "delivered_item_count",
        "all_items_shipped",
        "all_items_received",
        "pending_return",
        "pending_returns",
    ):
        out.pop(key, None)

    return out


def expand_sale_row_to_line_rows(db, sale_row, *, audience="seller"):
    """Return one or more line-scoped sale rows for a legacy aggregated sale row."""
    order_uid = sale_row.get("transaction_uid")
    if not order_uid:
        return [sale_row]
    if (sale_row.get("transaction_type") or "sale").lower() != "sale":
        return [sale_row]
    if sale_row.get("is_return") or sale_row.get("is_pending_return"):
        return [sale_row]

    lines = load_order_sale_lines(db, order_uid)
    if not lines:
        return [sale_row]

    count = len(lines)
    ti_uids = [line.get("ti_uid") for line in lines if line.get("ti_uid")]
    bounty_map = _line_bounty_totals(db, ti_uids)
    return [
        transform_sale_line_row(
            db,
            sale_row,
            line,
            audience=audience,
            order_line_count=count,
            line_bounty_paid=bounty_map.get(line.get("ti_uid"), 0.0),
        )
        for line in lines
    ]
