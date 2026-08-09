"""
Account-screen personal purchases schema v2.

Each purchases.rows[] entry is a Purchases-table row with an explicit unit ledger
so the frontend never infers shipped / verified / return splits.
"""

from units_ledger import sale_units_ledger, sale_display, fulfillment_method, requires_shipping
from order_display import build_return_ledger_display
from account_screen_v2_contract import _units_for_return_row
from account_screen_line_rows import _scoped_return_line_fields
from order_quantity_context import _open_return_requests_for_order
from transactions import (
    _is_return_list_row,
    _is_open_return,
    _return_request_public_payload,
    _clear_parent_sale_return_status,
    _return_ledger_line_split,
    _pending_return_payload_for_sale,
    _resolve_parent_sale_uid,
    _to_float,
)


def _round_money(value):
    return round(_to_float(value), 2)


def _normalize_completed_return_money(out):
    """
    Expose customer refund credit on completed return/cancel list rows.

    FE resolveReturnRowMoney() reads transaction_total (preferred) or component
    fields — values are positive magnitudes; the client displays them as negative.
    """
    total = abs(_to_float(out.get("transaction_total")))
    if total <= 0:
        total = abs(_to_float(out.get("refund_amount")))
    if total <= 0:
        total = abs(_to_float(out.get("refund_total") or out.get("return_total")))

    amount = abs(_to_float(out.get("transaction_amount")))
    taxes = abs(_to_float(out.get("transaction_taxes")))
    shipping = abs(_to_float(out.get("transaction_shipping")))
    if total <= 0 and (amount > 0 or taxes > 0 or shipping > 0):
        total = amount + taxes + shipping

    if total > 0:
        out["transaction_total"] = _round_money(total)
        out["refund_total"] = out["transaction_total"]
        out["return_total"] = out["transaction_total"]
        out["refund_amount"] = out["transaction_total"]
    if amount > 0:
        out["transaction_amount"] = _round_money(amount)
    if taxes > 0:
        out["transaction_taxes"] = _round_money(taxes)
    if shipping > 0 or out.get("transaction_shipping") is not None:
        out["transaction_shipping"] = _round_money(shipping)
    fees = abs(_to_float(out.get("transaction_fees")))
    if fees > 0:
        out["transaction_fees"] = _round_money(fees)
    return out


def _attach_pending_return_money(db, out):
    """
    Wrap estimated_refund on pending return/cancel rows for FE Amount column.

    Synthetic list rows store estimated_refund at the top level; purchases v2
    exposes it under pending_return.estimated_refund.
    """
    pending_return = dict(out.get("pending_return") or {})
    estimated = out.get("estimated_refund") or pending_return.get("estimated_refund")

    if not estimated and out.get("trr_uid"):
        sale_uid = (
            out.get("trr_transaction_uid")
            or out.get("order_uid")
            or out.get("original_transaction_uid")
        )
        if sale_uid:
            reqs = _open_return_requests_for_order(db, sale_uid) or []
            match = next(
                (r for r in reqs if r.get("trr_uid") == out.get("trr_uid")),
                None,
            )
            if match:
                payload = _pending_return_payload_for_sale(
                    db,
                    {"transaction_uid": sale_uid, **out},
                    match,
                    compact=True,
                )
                if payload:
                    pending_return = {**pending_return, **payload}
                    estimated = payload.get("estimated_refund")

    if estimated:
        pending_return["estimated_refund"] = estimated
    for key in (
        "trr_uid",
        "trr_uids",
        "note",
        "trr_note",
        "items",
        "bounty_to_reclaim",
        "cancel_unshipped",
        "pre_ship_cancel",
        "is_cancel_before_ship",
        "seller_note",
    ):
        if out.get(key) is not None and key not in pending_return:
            pending_return[key] = out.get(key)

    credit = abs(
        _to_float(
            (estimated or {}).get("total_customer_credit")
            or (estimated or {}).get("total")
            or out.get("estimated_total")
            or pending_return.get("estimated_total")
        )
    )
    if credit > 0:
        pending_return.setdefault("estimated_total", _round_money(credit))
        out["transaction_total"] = _round_money(credit)
        out["refund_amount"] = out["transaction_total"]
        out["refund_total"] = out["transaction_total"]

    if pending_return.get("estimated_refund") or credit > 0:
        out["pending_return"] = pending_return
    return out


def _fulfillment_method(row):
    return fulfillment_method(row)


def _requires_shipping(row):
    return requires_shipping(row)


def _sale_display(row, units):
    return sale_display(row, units, include_qty=True)


def _return_line_with_split(db, line, *, return_tx_uid=None, cancel_only=False):
    """Ensure return_quantity = return_shipped_qty + cancel_unshipped_qty."""
    out = dict(line)
    ti_uid = out.get("ti_original_ti_uid") or out.get("ti_uid")
    if ti_uid:
        out.setdefault("ti_original_ti_uid", ti_uid)

    try:
        rq = int(out.get("return_quantity") or abs(int(out.get("quantity") or 0)) or 0)
    except (TypeError, ValueError):
        rq = 0
    out["return_quantity"] = rq

    has_split = (
        out.get("return_shipped_qty") is not None
        or out.get("cancel_unshipped_qty") is not None
    )
    if not has_split and return_tx_uid:
        shipped, cancel = _return_ledger_line_split(db, return_tx_uid, out)
        out["return_shipped_qty"] = shipped
        out["cancel_unshipped_qty"] = cancel
    elif not has_split:
        if cancel_only:
            out["return_shipped_qty"] = 0
            out["cancel_unshipped_qty"] = rq
        else:
            out["return_shipped_qty"] = rq
            out["cancel_unshipped_qty"] = 0
    else:
        out["return_shipped_qty"] = int(out.get("return_shipped_qty") or 0)
        out["cancel_unshipped_qty"] = int(out.get("cancel_unshipped_qty") or 0)

    return out


def _open_return_summary(db, sale_row, pending_req):
    """Slim open-return block for sale row (not a full pending_return duplicate)."""
    payload = _pending_return_payload_for_sale(db, sale_row, pending_req, compact=True)
    if not payload:
        return None

    api = _return_request_public_payload(pending_req)
    items = []
    cancel_only = bool(
        pending_req.get("cancel_unshipped") or pending_req.get("pre_ship_cancel")
    )
    for entry in payload.get("items") or []:
        items.append(
            _return_line_with_split(
                db,
                {
                    "ti_uid": entry.get("ti_uid") or entry.get("transaction_item_uid"),
                    "ti_original_ti_uid": entry.get("ti_original_ti_uid")
                    or entry.get("transaction_item_uid")
                    or entry.get("ti_uid"),
                    "return_quantity": entry.get("return_quantity"),
                    "return_shipped_qty": entry.get("return_shipped_qty"),
                    "cancel_unshipped_qty": entry.get("cancel_unshipped_qty"),
                },
                cancel_only=cancel_only,
            )
        )

    estimated = payload.get("estimated_refund") or {}
    summary = {
        "trr_uid": pending_req.get("trr_uid"),
        "return_status": api.get("return_status"),
        "refund_status": api.get("refund_status"),
        "display_status": api.get("display_status"),
        "note": payload.get("note") or pending_req.get("trr_note"),
        "items": items,
    }
    if api.get("display"):
        summary["display"] = api["display"]
    if cancel_only:
        summary["cancel_unshipped"] = True
        summary["pre_ship_cancel"] = True
        summary["is_cancel_before_ship"] = True
    if estimated:
        summary["estimated_refund"] = estimated
    if estimated.get("total") is not None:
        summary["estimated_refund_total"] = estimated.get("total")
    return summary


def _completed_return_uids(db, order_uid):
    q = db.execute(
        """
        SELECT transaction_uid
        FROM every_circle.transactions
        WHERE transaction_original_uid = %s
          AND COALESCE(transaction_type, 'return') = 'return'
        ORDER BY transaction_datetime ASC
        """,
        (order_uid,),
    )
    return [
        row.get("transaction_uid")
        for row in (q.get("result") or [])
        if row.get("transaction_uid")
    ]


def _transform_sale_row(db, row):
    order_uid = row.get("transaction_uid")
    out = dict(row)
    out["row_kind"] = "sale"
    out["row_uid"] = order_uid
    out["order_uid"] = order_uid
    out["transaction_uid"] = order_uid

    method = _fulfillment_method(row)
    out["fulfillment_method"] = method
    out["requires_shipping"] = _requires_shipping(row)

    units = sale_units_ledger(db, order_uid)
    out["units"] = units

    open_reqs = [
        req
        for req in (_open_return_requests_for_order(db, order_uid) or [])
        if _is_open_return(req.get("return_status"), req.get("refund_status"))
        and not req.get("trr_return_transaction_uid")
    ]
    open_returns = []
    for req in open_reqs:
        summary = _open_return_summary(db, row, req)
        if summary:
            open_returns.append(summary)
    if open_returns:
        out["open_returns"] = open_returns
        _clear_parent_sale_return_status(out)

    out["display"] = _sale_display(out, units)

    completed = _completed_return_uids(db, order_uid)
    if completed:
        out["completed_return_uids"] = completed

    # v2: drop duplicate pending payloads (pending rows carry full detail)
    for key in (
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
    ):
        out.pop(key, None)

    return out


def _transform_return_row(db, row):
    out = dict(row)
    out["row_kind"] = "return"
    tx_uid = out.get("transaction_uid")
    out["row_uid"] = tx_uid
    sale_uid, _ = _resolve_parent_sale_uid(out, context="v2 return row")
    if sale_uid:
        out["order_uid"] = sale_uid
        out["trr_transaction_uid"] = sale_uid

    cancel_only = bool(out.get("cancel_unshipped") or out.get("pre_ship_cancel"))
    lines = out.get("return_lines") or out.get("lines") or []
    return_lines = [
        _return_line_with_split(
            db, line, return_tx_uid=tx_uid, cancel_only=cancel_only
        )
        for line in lines
    ]
    out["return_lines"] = return_lines
    out.pop("lines", None)
    _scoped_return_line_fields(out, return_lines)

    qty = sum(int(l.get("return_quantity") or 0) for l in return_lines)
    display = build_return_ledger_display(out, qty=qty or abs(int(out.get("return_quantity_total") or 0)))
    out["display"] = display
    out["units"] = _units_for_return_row(out)
    out["is_return"] = 1
    _normalize_completed_return_money(out)

    for key in (
        "is_pending_return",
        "ti_shipped_qty",
        "shippable_item_count",
        "shipped_item_count",
        "unshipped_item_count",
        "pending_return",
        "lines",
    ):
        out.pop(key, None)

    return out


def _transform_pending_return_row(db, row):
    out = dict(row)
    out["row_kind"] = "pending_return"
    trr_uid = out.get("trr_uid") or (
        (out.get("trr_uids") or [None])[0]
        if out.get("trr_uids")
        else None
    )
    out["row_uid"] = trr_uid or out.get("transaction_uid")
    sale_uid = (
        out.get("trr_transaction_uid")
        or out.get("order_uid")
        or out.get("original_transaction_uid")
    )
    if sale_uid:
        out["order_uid"] = sale_uid
        out["trr_transaction_uid"] = sale_uid

    cancel_only = bool(out.get("cancel_unshipped") or out.get("pre_ship_cancel"))
    lines = out.get("return_lines") or []
    return_lines = []
    for line in lines:
        line_cancel = cancel_only
        if not line_cancel:
            try:
                shipped = int(line.get("return_shipped_qty") or 0)
                cancel = int(line.get("cancel_unshipped_qty") or 0)
            except (TypeError, ValueError):
                shipped = cancel = 0
            if cancel > 0 and shipped == 0:
                line_cancel = True
        return_lines.append(
            _return_line_with_split(db, line, cancel_only=line_cancel)
        )
    out["return_lines"] = return_lines
    _scoped_return_line_fields(out, return_lines)

    qty = sum(int(l.get("return_quantity") or 0) for l in return_lines)
    req_view = dict(out)
    req_view.setdefault("trr_uid", trr_uid)
    req_view["trr_return_transaction_uid"] = None
    api = _return_request_public_payload(
        req_view, qty=qty or abs(int(out.get("return_quantity_total") or 0))
    )
    out.update(
        {
            k: api[k]
            for k in ("return_status", "refund_status", "display_status")
            if k in api
        }
    )
    if api.get("cancel_unshipped"):
        out["cancel_unshipped"] = True
        out["pre_ship_cancel"] = True
        out["is_cancel_before_ship"] = True

    out["display"] = api.get("display") or build_return_ledger_display(
        out, qty=qty or abs(int(out.get("return_quantity_total") or 0))
    )
    out["units"] = _units_for_return_row(out)

    if out.get("ti_uid"):
        out["line_uid"] = out["ti_uid"]
        out["offering_uid"] = out.get("ti_bs_id")
    if out.get("order_uid") and out.get("ti_uid"):
        out["row_uid"] = out.get("row_uid") or f"{trr_uid}:{out['ti_uid']}"

    out["is_pending_return"] = True
    out["is_return"] = 1
    _attach_pending_return_money(db, out)

    for key in (
        "pending_returns",
        "transaction_return_items",
        "estimated_refund",
        "estimated_total",
        "lines",
    ):
        out.pop(key, None)

    return out


def build_purchases_v2_rows(db, rows):
    """Transform legacy purchase list rows into schema v2 purchases.rows[]."""
    if not rows:
        return []

    v2_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("is_pending_return"):
            v2_rows.append(_transform_pending_return_row(db, row))
        elif _is_return_list_row(row):
            v2_rows.append(_transform_return_row(db, row))
        else:
            v2_rows.append(_transform_sale_row(db, row))

    v2_rows.sort(
        key=lambda r: str(r.get("transaction_datetime") or ""),
        reverse=True,
    )
    return v2_rows
