"""
Account-screen seller_transactions schema v2 (personal + business / Offering sales).

Every sale emits one sale_line row per transactions_items line (ti_uid).
"""

from account_screen_line_rows import expand_sale_row_to_line_rows, split_row_by_return_lines
from account_screen_purchases_v2 import (
    _transform_return_row,
    _transform_pending_return_row,
)
from transactions import _is_return_list_row


def _transform_seller_sale_row(db, row):
    """Single-line shortcut (legacy path when expansion not used)."""
    lines = expand_sale_row_to_line_rows(db, row, audience="seller")
    return lines[0] if lines else row


def build_seller_transactions_v2_rows(db, rows):
    """Transform legacy seller list rows into schema v2 seller_transactions[]."""
    if not rows:
        return []

    v2_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if row.get("is_pending_return"):
            for split in split_row_by_return_lines(row):
                v2_rows.append(_transform_pending_return_row(db, split))
        elif _is_return_list_row(row):
            for split in split_row_by_return_lines(row):
                v2_rows.append(_transform_return_row(db, split))
        else:
            v2_rows.extend(expand_sale_row_to_line_rows(db, row, audience="seller"))

    v2_rows.sort(
        key=lambda r: (
            str(r.get("transaction_datetime") or ""),
            str(r.get("order_uid") or r.get("transaction_uid") or ""),
            str(r.get("ti_uid") or r.get("row_uid") or ""),
        ),
        reverse=True,
    )
    return v2_rows
