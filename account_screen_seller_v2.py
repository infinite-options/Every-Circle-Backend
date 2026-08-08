"""
Account-screen seller_transactions schema v2 (personal / Offering sales).

Same unit ledger as buyer purchases v2; seller-specific display labels.
"""

from units_ledger import enrich_sale_row_v2
from account_screen_purchases_v2 import (
    _transform_return_row,
    _transform_pending_return_row,
)
from transactions import _is_return_list_row, _clear_parent_sale_return_status


def _transform_seller_sale_row(db, row):
    order_uid = row.get("transaction_uid")
    out = enrich_sale_row_v2(db, row, audience="seller")
    out["row_kind"] = "sale"
    out["row_uid"] = order_uid
    out["order_uid"] = order_uid

    if row.get("pending_return") or row.get("pending_returns") or row.get("open_returns"):
        _clear_parent_sale_return_status(out)

    for key in (
        "needs_shipping",
        "needs_shipment",
        "has_in_transit",
        "has_shippable_items",
        "received_units",
        "delivered_item_count",
        "all_items_shipped",
        "all_items_received",
    ):
        out.pop(key, None)

    return out


def build_seller_transactions_v2_rows(db, rows):
    """Transform legacy seller list rows into schema v2 seller_transactions[]."""
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
            v2_rows.append(_transform_seller_sale_row(db, row))

    v2_rows.sort(
        key=lambda r: str(r.get("transaction_datetime") or ""),
        reverse=True,
    )
    return v2_rows
