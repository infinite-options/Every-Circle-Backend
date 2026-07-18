"""Checkout shipping helpers for every_circle.transactions_shipping."""

from datetime_utils import utc_now_str

_SHIPPING_REQUIRED_FIELDS = (
    "first_name",
    "last_name",
    "address_line_1",
    "city",
    "state",
    "zip",
)


def normalize_shipping_address(shipping):
    """Parse checkout shipping_address. Returns (row_fields_or_None, error_or_None)."""
    if shipping is None:
        return None, None
    if not isinstance(shipping, dict):
        return None, "shipping_address must be an object"

    has_any = any(
        str(shipping.get(field) or "").strip()
        for field in _SHIPPING_REQUIRED_FIELDS + ("address_line_2",)
    )
    if not has_any:
        return None, None

    missing = [
        field
        for field in _SHIPPING_REQUIRED_FIELDS
        if not str(shipping.get(field) or "").strip()
    ]
    if missing:
        return None, f"shipping_address missing: {', '.join(missing)}"

    line2 = str(shipping.get("address_line_2") or "").strip()
    return {
        "ts_first_name": str(shipping["first_name"]).strip(),
        "ts_last_name": str(shipping["last_name"]).strip(),
        "ts_address_line_1": str(shipping["address_line_1"]).strip(),
        "ts_address_line_2": line2 or None,
        "ts_city": str(shipping["city"]).strip(),
        "ts_state": str(shipping["state"]).strip(),
        "ts_zip": str(shipping["zip"]).strip(),
    }, None


def shipping_address_response(shipping_row):
    if not shipping_row or not shipping_row.get("ts_uid"):
        return None
    return {
        "first_name": shipping_row.get("ts_first_name"),
        "last_name": shipping_row.get("ts_last_name"),
        "address_line_1": shipping_row.get("ts_address_line_1"),
        "address_line_2": shipping_row.get("ts_address_line_2"),
        "city": shipping_row.get("ts_city"),
        "state": shipping_row.get("ts_state"),
        "zip": shipping_row.get("ts_zip"),
    }


def insert_transaction_shipping(db, transaction_uid, shipping_fields):
    uid_resp = db.call(procedure="new_transaction_shipping_uid")
    if not uid_resp.get("result"):
        return {
            "code": 500,
            "message": "Failed to generate transaction shipping UID",
        }

    shipping_row = {
        "ts_uid": uid_resp["result"][0]["new_id"],
        "ts_transaction_id": transaction_uid,
        "ts_created_at": utc_now_str(),
        **shipping_fields,
    }
    insert_resp = db.insert("every_circle.transactions_shipping", shipping_row)
    if insert_resp.get("code") != 200:
        return {
            "code": insert_resp.get("code", 500),
            "message": insert_resp.get(
                "message", "Failed to insert transaction shipping"
            ),
        }

    return {
        "code": 200,
        "ts_uid": shipping_row["ts_uid"],
        "shipping_address": shipping_address_response(shipping_row),
    }


def load_shipping_for_transaction(db, transaction_uid):
    """Load shipping for a sale transaction_uid. Returns dict or None."""
    if not transaction_uid:
        return None
    result = db.execute(
        """
        SELECT
            ts_uid,
            ts_transaction_id,
            ts_first_name,
            ts_last_name,
            ts_address_line_1,
            ts_address_line_2,
            ts_city,
            ts_state,
            ts_zip,
            ts_created_at
        FROM every_circle.transactions_shipping
        WHERE ts_transaction_id = %s
        LIMIT 1
        """,
        (transaction_uid,),
    )
    rows = result.get("result") or []
    return rows[0] if rows else None


def load_shipping_by_order_uids(db, order_uids):
    """Map sale/order uid -> shipping row for batch attach on list endpoints."""
    uids = []
    seen = set()
    for uid in order_uids or []:
        if not uid or uid in seen:
            continue
        seen.add(uid)
        uids.append(uid)
    if not uids:
        return {}

    placeholders = ", ".join(["%s"] * len(uids))
    result = db.execute(
        f"""
        SELECT
            ts_uid,
            ts_transaction_id,
            ts_first_name,
            ts_last_name,
            ts_address_line_1,
            ts_address_line_2,
            ts_city,
            ts_state,
            ts_zip,
            ts_created_at
        FROM every_circle.transactions_shipping
        WHERE ts_transaction_id IN ({placeholders})
        """,
        tuple(uids),
    )
    out = {}
    for row in result.get("result") or []:
        out[row.get("ts_transaction_id")] = row
    return out


def shipping_payload_from_row(shipping_row):
    """API-facing fields for a shipping row (or empty defaults if None)."""
    if not shipping_row:
        return {
            "requires_shipping": False,
            "ts_uid": None,
            "shipping_address": None,
        }
    return {
        "requires_shipping": True,
        "ts_uid": shipping_row.get("ts_uid"),
        "shipping_address": shipping_address_response(shipping_row),
    }


def attach_shipping_to_transaction_rows(db, rows):
    """
    Attach requires_shipping / shipping_address / ts_uid onto list rows.

    Uses trr_transaction_uid / transaction_original_uid so return rows inherit
    the original sale address.
    """
    if not rows:
        return rows

    def _sale_key(row):
        if not isinstance(row, dict):
            return None
        return (
            row.get("trr_transaction_uid")
            or row.get("transaction_original_uid")
            or row.get("transaction_uid")
        )

    order_uids = [_sale_key(row) for row in rows]
    shipping_map = load_shipping_by_order_uids(db, order_uids)
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = _sale_key(row)
        if not key:
            print(
                "Error: attach_shipping_to_transaction_rows could not resolve "
                f"parent sale uid for transaction_uid={row.get('transaction_uid')!r}"
            )
            continue
        row.update(shipping_payload_from_row(shipping_map.get(key)))
    return rows


# --- Line-item fulfillment (transactions_items columns) ---

FULFILLMENT_STATUS_NOT_REQUIRED = "not_required"
FULFILLMENT_STATUS_NOT_SHIPPED = "not_shipped"
FULFILLMENT_STATUS_IN_TRANSIT = "in_transit"
FULFILLMENT_STATUS_DELIVERED = "delivered"

FULFILLMENT_STATUSES = frozenset(
    {
        FULFILLMENT_STATUS_NOT_REQUIRED,
        FULFILLMENT_STATUS_NOT_SHIPPED,
        FULFILLMENT_STATUS_IN_TRANSIT,
        FULFILLMENT_STATUS_DELIVERED,
    }
)

# Seller may set these via PUT fulfillment_updates
SELLER_FULFILLMENT_STATUSES = frozenset(
    {
        FULFILLMENT_STATUS_NOT_SHIPPED,
        FULFILLMENT_STATUS_IN_TRANSIT,
    }
)

# Column limits for shipment history fields on transactions_items
TI_TRACKING_CARRIER_MAX_LEN = 64
TI_TRACKING_NUMBER_MAX_LEN = 128


def append_fulfillment_field(
    existing, new_value, *, separator=" | ", max_len=None
):
    """
    Append a new shipment detail onto an existing value for partial shipments.

    - Empty/whitespace new_value → keep existing unchanged
    - Empty existing → store new_value
    - Identical segment already present → keep existing (no duplicate append)
    - Otherwise join with separator; if max_len is set and exceeded, keep the
      newest portion (right side) so latest tracking info is retained
    """
    existing_s = (str(existing).strip() if existing is not None else "") or ""
    new_s = (str(new_value).strip() if new_value is not None else "") or ""
    if not new_s:
        return existing_s or None
    if not existing_s:
        merged = new_s
    else:
        parts = [p.strip() for p in existing_s.split(separator) if p.strip()]
        if new_s in parts:
            merged = existing_s
        else:
            merged = f"{existing_s}{separator}{new_s}"
    if max_len is not None and len(merged) > max_len:
        merged = merged[-max_len:]
    return merged or None


def fulfillment_fields_from_row(row):
    """API-facing fulfillment fields from a transactions_items row."""
    if not row:
        return {
            "fulfillment_status": FULFILLMENT_STATUS_NOT_REQUIRED,
            "ti_fulfillment_status": FULFILLMENT_STATUS_NOT_REQUIRED,
            "shipped_quantity": 0,
            "shipped_qty": 0,
            "ti_shipped_qty": 0,
            "shipped_at": None,
            "tracking_carrier": None,
            "tracking_number": None,
            "ti_tracking_carrier": None,
            "ti_tracking_number": None,
            "fulfillment_note": None,
        }
    status = row.get("ti_fulfillment_status") or FULFILLMENT_STATUS_NOT_REQUIRED
    shipped_qty = int(row.get("ti_shipped_qty") or 0)
    carrier = row.get("ti_tracking_carrier")
    tracking = row.get("ti_tracking_number")
    return {
        "fulfillment_status": status,
        "ti_fulfillment_status": status,
        "shipped_quantity": shipped_qty,
        "shipped_qty": shipped_qty,
        "ti_shipped_qty": shipped_qty,
        "shipped_at": row.get("ti_shipped_at"),
        "tracking_carrier": carrier,
        "tracking_number": tracking,
        "ti_tracking_carrier": carrier,
        "ti_tracking_number": tracking,
        "fulfillment_note": row.get("ti_fulfillment_note"),
    }


def fulfillment_select_sql(alias="ti"):
    """SQL fragment selecting fulfillment columns."""
    return f"""
        COALESCE({alias}.ti_fulfillment_status, 'not_required') AS ti_fulfillment_status,
        COALESCE({alias}.ti_shipped_qty, 0) AS ti_shipped_qty,
        {alias}.ti_shipped_at,
        {alias}.ti_tracking_carrier,
        {alias}.ti_tracking_number,
        {alias}.ti_fulfillment_note
    """


def fulfillment_list_summary_sql(alias="ti"):
    """Aggregates for buyer/seller transaction list rows (GROUP BY transaction)."""
    return f"""
        SUM(
            CASE
                WHEN COALESCE({alias}.ti_fulfillment_status, 'not_required')
                     IN ('not_shipped', 'in_transit', 'delivered')
                THEN 1 ELSE 0
            END
        ) AS shippable_item_count,
        SUM(
            CASE
                WHEN COALESCE({alias}.ti_fulfillment_status, 'not_required')
                     IN ('not_shipped', 'in_transit', 'delivered')
                     AND (
                         COALESCE({alias}.ti_fulfillment_status, 'not_required') = 'delivered'
                         OR COALESCE({alias}.ti_shipped_qty, 0)
                            >= CAST({alias}.ti_bs_qty AS UNSIGNED)
                     )
                THEN 1 ELSE 0
            END
        ) AS shipped_item_count,
        SUM(
            CASE
                WHEN COALESCE({alias}.ti_fulfillment_status, 'not_required')
                     IN ('not_shipped', 'in_transit', 'delivered')
                     AND COALESCE({alias}.ti_fulfillment_status, 'not_required') <> 'delivered'
                     AND COALESCE({alias}.ti_shipped_qty, 0)
                         < CAST({alias}.ti_bs_qty AS UNSIGNED)
                THEN 1 ELSE 0
            END
        ) AS unshipped_item_count,
        SUM(
            CASE
                WHEN COALESCE({alias}.ti_fulfillment_status, 'not_required') = 'delivered'
                THEN 1 ELSE 0
            END
        ) AS delivered_item_count,
        MAX(
            CASE
                WHEN COALESCE({alias}.ti_fulfillment_status, 'not_required') = 'in_transit'
                THEN 1 ELSE 0
            END
        ) AS has_in_transit,
        MAX(
            CASE
                WHEN COALESCE({alias}.ti_fulfillment_status, 'not_required')
                     IN ('not_shipped', 'in_transit', 'delivered')
                THEN 1 ELSE 0
            END
        ) AS has_shippable_items
    """


def apply_order_fulfillment_summary(rows):
    """
    Normalize order-level fulfillment fields on seller/buyer list rows.

    Expects shippable_item_count / shipped_item_count / unshipped_item_count from SQL.
    Adds needs_shipping, all_items_shipped, fulfillment_status (order rollup).
    """
    if not rows:
        return rows

    for row in rows:
        if not isinstance(row, dict):
            continue

        shippable = int(row.get("shippable_item_count") or 0)
        shipped = int(row.get("shipped_item_count") or 0)
        unshipped = int(row.get("unshipped_item_count") or 0)
        delivered = int(row.get("delivered_item_count") or 0)

        # Return rows inherit sale shipping address via order_uid, but fulfillment
        # counts come from the return's own lines (usually not_required).
        row["shippable_item_count"] = shippable
        row["shipped_item_count"] = shipped
        row["unshipped_item_count"] = unshipped
        row["delivered_item_count"] = delivered
        row["needs_shipping"] = 1 if unshipped > 0 else 0
        row["needs_shipment"] = row["needs_shipping"]
        row["all_items_shipped"] = 1 if shippable > 0 and unshipped == 0 else 0
        row["has_shippable_items"] = 1 if shippable > 0 else int(
            row.get("has_shippable_items") or 0
        )

        if shippable <= 0:
            row["fulfillment_status"] = FULFILLMENT_STATUS_NOT_REQUIRED
        elif unshipped <= 0 and delivered >= shippable:
            row["fulfillment_status"] = FULFILLMENT_STATUS_DELIVERED
        elif unshipped <= 0:
            row["fulfillment_status"] = FULFILLMENT_STATUS_IN_TRANSIT
        elif shipped <= 0:
            row["fulfillment_status"] = FULFILLMENT_STATUS_NOT_SHIPPED
        else:
            row["fulfillment_status"] = FULFILLMENT_STATUS_IN_TRANSIT

    return rows
