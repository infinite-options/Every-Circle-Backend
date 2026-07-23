"""Trimmed per-order payloads for account-screen list hydration."""

from order_detail import build_order_payload
from transactions import _is_return_list_row, _resolve_parent_sale_uid, _omit_empty

_SALE_HEADER_KEYS = frozenset(
    {
        "transaction_uid",
        "fulfillment_status",
        "shipping_status",
        "unshipped_item_count",
        "shipped_item_count",
        "shippable_item_count",
        "all_items_shipped",
        "return_status",
        "refund_status",
        "transaction_return_status",
        "transaction_refund_status",
        "display_status",
        "cancel_unshipped",
        "pre_ship_cancel",
        "received_item_count",
        "delivered_item_count",
    }
)

_SALE_LINE_KEYS = frozenset(
    {
        "ti_uid",
        "ti_bs_id",
        "ti_bs_qty",
        "ti_bs_cost",
        "bs_service_name",
        "bs_service_desc",
        "item_name",
        "fulfillment_status",
        "ti_fulfillment_status",
        "shipped_qty",
        "ti_shipped_qty",
        "remaining_to_ship",
        "returned_qty",
        "ti_received_qty",
    }
)

_RETURN_HEADER_KEYS = frozenset(
    {
        "transaction_uid",
        "return_status",
        "refund_status",
        "transaction_return_status",
        "transaction_refund_status",
        "display_status",
    }
)

_RETURN_LINE_KEYS = frozenset(
    {
        "ti_uid",
        "return_quantity",
        "ti_bs_qty",
        "item_name",
        "bs_service_name",
    }
)

_PENDING_RETURN_KEYS = frozenset({"trr_uid", "items"})

_PENDING_ITEM_KEYS = frozenset(
    {
        "ti_uid",
        "transaction_item_uid",
        "return_quantity",
        "item_name",
    }
)

_TOP_KEYS = frozenset(
    {
        "order_uid",
        "display_status",
        "stripe_refund",
        "sale",
        "returns",
        "pending_returns",
        "pending_return",
        "pending_return_items",
        "transaction_return_items",
    }
)


def _omit_empty_deep(value):
    if isinstance(value, dict):
        out = {}
        for key, val in value.items():
            trimmed = _omit_empty_deep(val)
            if trimmed is None or trimmed == [] or trimmed == {}:
                continue
            out[key] = trimmed
        return out
    if isinstance(value, list):
        items = [_omit_empty_deep(item) for item in value]
        return [item for item in items if item is not None and item != {}]
    return value


def _pick_fields(obj, allowed):
    if not isinstance(obj, dict):
        return obj
    return {key: obj[key] for key in allowed if key in obj}


def _trim_sale_line(line):
    if not isinstance(line, dict):
        return line
    out = _pick_fields(line, _SALE_LINE_KEYS)
    name = line.get("item_name")
    if name and "bs_service_name" not in out:
        out["bs_service_name"] = name
    return out


def _trim_return_line(line):
    if not isinstance(line, dict):
        return line
    out = _pick_fields(line, _RETURN_LINE_KEYS)
    name = line.get("item_name")
    if name and "bs_service_name" not in out:
        out["bs_service_name"] = name
    return out


def _trim_pending_return(req):
    if not isinstance(req, dict):
        return req
    out = _pick_fields(req, _PENDING_RETURN_KEYS)
    items = req.get("items") or []
    if items:
        trimmed_items = []
        for item in items:
            if not isinstance(item, dict):
                continue
            entry = _pick_fields(item, _PENDING_ITEM_KEYS)
            if not entry.get("ti_uid") and entry.get("transaction_item_uid"):
                entry["ti_uid"] = entry["transaction_item_uid"]
            trimmed_items.append(entry)
        if trimmed_items:
            out["items"] = trimmed_items
    return out


def trim_order_list_hydration(full_payload):
    """Subset of order-detail fields needed by account-screen list chips."""
    if not isinstance(full_payload, dict):
        return None

    sale = full_payload.get("sale") or {}
    sale_out = _pick_fields(sale, _SALE_HEADER_KEYS)
    lines = [_trim_sale_line(line) for line in (sale.get("lines") or [])]
    if lines:
        sale_out["lines"] = lines

    returns_out = []
    for ret in full_payload.get("returns") or []:
        if not isinstance(ret, dict):
            continue
        entry = _pick_fields(ret, _RETURN_HEADER_KEYS)
        ret_lines = [_trim_return_line(line) for line in (ret.get("lines") or [])]
        if ret_lines:
            entry["lines"] = ret_lines
        returns_out.append(entry)

    pending_returns = [
        _trim_pending_return(req)
        for req in (full_payload.get("pending_returns") or [])
        if req
    ]
    pending_return = _trim_pending_return(full_payload.get("pending_return"))

    out = _pick_fields(full_payload, _TOP_KEYS)
    if sale_out:
        out["sale"] = sale_out
    if returns_out:
        out["returns"] = returns_out
    if pending_returns:
        out["pending_returns"] = pending_returns
    if pending_return:
        out["pending_return"] = pending_return

    for optional in ("pending_return_items", "transaction_return_items"):
        val = full_payload.get(optional)
        if val:
            out[optional] = val

    return _omit_empty_deep(out)


def _list_shipping_complete(row):
    shippable = int(row.get("shippable_item_count") or 0)
    if shippable <= 0:
        return True

    unshipped = int(row.get("unshipped_item_count") or 0)
    delivered = int(row.get("delivered_item_count") or 0)
    if unshipped == 0 and delivered >= shippable:
        return True
    if int(row.get("all_items_shipped") or 0) == 1 and (
        row.get("fulfillment_status") == "delivered"
    ):
        return True
    return False


def _row_has_return_signals(row):
    if not isinstance(row, dict):
        return False
    if _is_return_list_row(row):
        return True
    if row.get("return_status") or row.get("refund_status"):
        return True
    if row.get("display_status"):
        return True
    if row.get("transaction_return_requested"):
        return True
    if row.get("pending_return"):
        return True
    if row.get("return_lines") or row.get("lines"):
        return True
    refund = (row.get("refund_status") or "").strip().lower()
    if refund in ("stripe_fail", "stripe_failed"):
        return True
    return False


def _row_needs_shipping_hydration(row):
    shippable = int(row.get("shippable_item_count") or 0)
    if shippable <= 0 and not int(row.get("has_shippable_items") or 0):
        return False
    return not _list_shipping_complete(row)


def _row_needs_received_hydration(row):
    if _is_return_list_row(row):
        return False
    if int(row.get("transaction_in_escrow") or 0) != 1:
        return False
    if row.get("received_item_count") is not None:
        return False
    return True


def _sale_uid_for_hydration(row):
    sale_uid, _err = _resolve_parent_sale_uid(row, context="order_list_hydration")
    if sale_uid:
        return sale_uid
    if not _is_return_list_row(row):
        return row.get("transaction_uid")
    return None


def personal_row_needs_hydration(row):
    if not isinstance(row, dict):
        return False
    if _row_has_return_signals(row):
        return True
    if _row_needs_shipping_hydration(row):
        return True
    return False


def business_row_needs_hydration(row):
    if not isinstance(row, dict):
        return False
    if _row_needs_shipping_hydration(row):
        return True
    if _row_needs_received_hydration(row):
        return True
    return False


def collect_hydration_order_uids(rows, *, mode):
    needs_fn = (
        personal_row_needs_hydration
        if mode == "personal"
        else business_row_needs_hydration
    )
    order_uids = []
    seen = set()
    for row in rows or []:
        if not needs_fn(row):
            continue
        sale_uid = _sale_uid_for_hydration(row)
        if sale_uid and sale_uid not in seen:
            seen.add(sale_uid)
            order_uids.append(sale_uid)
    return order_uids


def _rows_from_section(section):
    if not isinstance(section, dict):
        return []
    data = section.get("data")
    return data if isinstance(data, list) else []


def build_order_list_hydration(db, rows, *, mode):
    """
    Map order_uid -> trimmed order detail for account-screen rows that need it.
    """
    order_uids = collect_hydration_order_uids(rows, mode=mode)
    if not order_uids:
        return {}

    hydration = {}
    for order_uid in order_uids:
        full_payload = build_order_payload(db, order_uid)
        if not full_payload:
            continue
        trimmed = trim_order_list_hydration(full_payload)
        if trimmed:
            hydration[order_uid] = trimmed
    return hydration


def attach_order_list_hydration(response, db, *, mode):
    """Add order_list_hydration to an account-screen response when needed."""
    if mode == "personal":
        rows = _rows_from_section(response.get("purchases")) + _rows_from_section(
            response.get("seller_transactions")
        )
    else:
        rows = _rows_from_section(response.get("seller_transactions"))

    hydration = build_order_list_hydration(db, rows, mode=mode)
    if hydration:
        response["order_list_hydration"] = hydration
    return response
