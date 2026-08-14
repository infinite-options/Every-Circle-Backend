from aiohttp import payload
from flask_restful import Resource
from datetime import datetime, timedelta, timezone
import os
import traceback
from flask import request, jsonify
import json
import re
import requests as http_requests

from flask_jwt_extended import get_jwt_identity, verify_jwt_in_request

from data_ec import connect, processImage
from moderation import (
    MODERATED_ACTIVE,
    get_business,
    is_business_publicly_visible,
    is_owner_available_for_public_interaction,
)
from user_path_connection import ConnectionsPath
from wallet_ids import EC_WALLET_ID
from wallet_service import (
    bounty_was_released_to_useable_at,
    credit_bounty_to_wallet,
    credit_useable_from_refund,
    debit_bounty_from_wallet,
    debit_useable_for_purchase,
    transfer_wallet_refund_to_buyer,
)
from wallet_transactions_service import (
    clawback_seller_proceeds_on_return,
    credit_partial_delivery,
    credit_seller_proceeds_at_checkout,
    resolve_seller_wallet_profile_id,
    _parse_unit_cost,
)
from datetime_utils import utc_now_str, enrich_datetime_fields, parse_stored_datetime
from transaction_shipping import (
    normalize_shipping_address,
    insert_transaction_shipping,
    attach_shipping_to_transaction_rows,
    apply_order_fulfillment_summary,
    sync_list_rows_fulfillment_from_context,
    fulfillment_list_summary_sql,
    ensure_fulfillment_list_rollups,
    append_fulfillment_field,
    FULFILLMENT_STATUS_NOT_REQUIRED,
    FULFILLMENT_STATUS_NOT_SHIPPED,
    FULFILLMENT_STATUS_IN_TRANSIT,
    FULFILLMENT_STATUS_DELIVERED,
    SELLER_FULFILLMENT_STATUSES,
    TI_TRACKING_CARRIER_MAX_LEN,
    TI_TRACKING_NUMBER_MAX_LEN,
)

# Return logistics (item physical state)
RETURN_STATUS_RETURNING = "returning"  # buyer initiated return
RETURN_STATUS_RETURNED = "returned"  # seller received the item
RETURN_STATUS_CANCELLED = "cancelled"  # pre-ship cancel (never shipped)

# Refund / money state
REFUND_STATUS_PENDING = "pending"  # buyer waiting for money
REFUND_STATUS_REFUNDED = "refunded"  # money returned
REFUND_STATUS_REJECTED = "rejected"  # seller/admin will not refund

_VALID_RETURN_STATUSES = (
    RETURN_STATUS_RETURNING,
    RETURN_STATUS_RETURNED,
    RETURN_STATUS_CANCELLED,
)
_VALID_REFUND_STATUSES = (
    REFUND_STATUS_PENDING,
    REFUND_STATUS_REFUNDED,
    REFUND_STATUS_REJECTED,
)

RETURN_INELIGIBLE_NOT_RETURNABLE = "Not returnable"
RETURN_INELIGIBLE_OUTSIDE_WINDOW = "Outside return window"



def _parse_limited_quantity(raw_qty):
    """Return int when stock is tracked; None means unlimited / not tracked."""
    if raw_qty is None:
        return None
    s = str(raw_qty).strip().lower()
    if s in ("", "unlimited", "null", "none"):
        return None
    try:
        qty = int(float(s))
        return qty if qty >= 0 else None
    except (TypeError, ValueError):
        return None


def _purchase_qty(item):
    try:
        qty = int(item.get("quantity") or 1)
        return qty if qty >= 1 else 1
    except (TypeError, ValueError):
        return 1


def _validate_purchase_quantity(available_qty, purchased_qty):
    """Fast-fail when a listing has limited stock and the cart qty exceeds it."""
    if available_qty is None:
        return None
    if available_qty < purchased_qty:
        err = {
            "message": "Insufficient stock",
            "code": 409,
            "remaining": available_qty,
        }
        return (err, 409)
    return None




def _increment_tracked_quantity(db, table, uid_column, qty_column, uid, qty):
    """Restore limited stock after a failed checkout line insert."""
    qty = int(qty or 0)
    if qty <= 0:
        return
    db.execute(
        f"""
        UPDATE every_circle.{table}
        SET {qty_column} = CAST(CAST({qty_column} AS SIGNED) + %s AS CHAR)
        WHERE {uid_column} = %s
          AND {qty_column} IS NOT NULL
          AND TRIM(CAST({qty_column} AS CHAR)) <> ''
          AND LOWER(TRIM(CAST({qty_column} AS CHAR))) NOT IN ('unlimited', 'null', 'none')
        """,
        (qty, uid),
        cmd="post",
    )


def _validate_business_service_available(db, bs_data):
    """Gate checkout on business service + parent business visibility."""
    if not bs_data:
        return {"message": "Business service not found", "code": 404}

    if int(bs_data.get("bs_is_visible") or 0) != 1:
        return {"message": "Product is not available", "code": 403}

    status = str(bs_data.get("bs_status") or "active").strip().lower()
    if status in ("out_of_stock", "inactive", "deleted", "removed"):
        return {"message": "Product is not available", "code": 403}

    business_uid = bs_data.get("bs_business_id")
    if business_uid:
        business = get_business(db, business_uid)
        if not business or not is_business_publicly_visible(business):
            return {"message": "Business is not available", "code": 403}

    return None





def _parse_line_shipping_amount(value):
    """Parse shipping dollar amount from checkout payload."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return round(float(value), 2)
    s = str(value).strip().replace("$", "").replace(",", "")
    if not s or s.lower() in ("null", "none"):
        return None
    try:
        return round(float(s), 2)
    except (TypeError, ValueError):
        return None


def _normalize_shipping_refundable(value, default=0):
    """Snapshot ti_shipping_refundable as 0 or 1."""
    if value is None or value == "":
        return 1 if default else 0
    return 1 if _as_returnable_flag(value, default=bool(default)) else 0




def _apply_line_shipping_snapshot(tx_item, item, bs_data=None):
    """
    Set ti_shipping_amount / ti_shipping_refundable from checkout item payload,
    falling back to business_services when omitted.

    ti_shipping_amount is stored as the per-unit shipping charge for the line.
    """
    raw_amt = item.get("ti_shipping_amount_per_unit")
    if raw_amt is None:
        raw_amt = item.get("shipping_amount")
    if raw_amt is None:
        raw_amt = item.get("ti_shipping_amount")

    raw_ref = item.get("shipping_refundable")
    if raw_ref is None:
        raw_ref = item.get("ti_shipping_refundable")

    if raw_amt is not None and raw_amt != "":
        tx_item["ti_shipping_amount"] = _parse_line_shipping_amount(raw_amt)
    elif bs_data is not None:
        _product_data_67 = bs_data
        if not _product_data_67:
            amt = None
        else:
            sh = _product_data_67.get('bs_shipping')
            if sh is None or str(sh).strip() == '':
                sh = _product_data_67.get('profile_expertise_shipping')
            if sh is None or str(sh).strip() == '':
                amt = None
            else:
                low = str(sh).strip().lower()
                if low == 'free':
                    amt = 0.0
                elif low in ('buyer fixed', 'buyer_fixed'):
                    _amt_68 = _parse_line_shipping_amount(_product_data_67.get('bs_shipping_amount') if _product_data_67.get('bs_shipping_amount') is not None else _product_data_67.get('profile_expertise_shipping_amount'))
                    amt = 0.0 if _amt_68 is None else _amt_68
                else:
                    amt = None
        if amt is not None:
            tx_item["ti_shipping_amount"] = amt

    if raw_ref is not None and raw_ref != "":
        tx_item["ti_shipping_refundable"] = _normalize_shipping_refundable(raw_ref)
    elif bs_data is not None:
        product_data = bs_data
        default = 0
        if not product_data:
            _r__shipping_refundable_from_product_9 = default
        else:
            raw = product_data.get('bs_shipping_refundable')
            if raw is None or raw == '':
                raw = product_data.get('profile_expertise_shipping_refundable')
            _r__shipping_refundable_from_product_9 = _normalize_shipping_refundable(raw, default=default)
        tx_item["ti_shipping_refundable"] = _r__shipping_refundable_from_product_9
    else:
        tx_item["ti_shipping_refundable"] = 0


def _money_close(a, b, tol=0.02):
    return abs(round(_to_float(a), 2) - round(_to_float(b), 2)) <= tol



def _listing_tax_config(bs_data):
    if not bs_data:
        return 0, 0.0
    if bs_data.get("bs_is_taxable") is not None:
        return bs_data.get("bs_is_taxable"), bs_data.get("bs_tax_rate")
    if bs_data.get("profile_expertise_is_taxable") is not None:
        return bs_data.get("profile_expertise_is_taxable"), bs_data.get(
            "profile_expertise_tax_rate"
        )
    return bs_data.get("profile_wish_is_taxable"), bs_data.get("profile_wish_tax_rate")


def _apply_line_tax_snapshot(tx_item, item, bs_data=None):
    """Persist per-line tax snapshot from checkout payload."""
    raw_tax = item.get("line_tax_amount")
    if raw_tax is None or raw_tax == "":
        raw_tax = item.get("ti_line_tax_amount")
    if raw_tax is not None and raw_tax != "":
        amount = round(_to_float(raw_tax), 2)
        tx_item["ti_line_tax_amount"] = amount
        tx_item["ti_tax_amount"] = amount

    raw_rate = item.get("ti_tax_rate")
    if raw_rate is not None and raw_rate != "":
        tx_item["ti_bs_tax_rate"] = raw_rate
    elif bs_data is not None and tx_item.get("ti_bs_tax_rate") is None:
        _taxable, rate = _listing_tax_config(bs_data)
        if rate is not None:
            tx_item["ti_bs_tax_rate"] = rate
        if _taxable is not None:
            tx_item["ti_bs_is_taxable"] = _taxable


VALID_FULFILLMENT_METHODS = ("ship", "pickup", "virtual")


def _is_no_shipping_fulfillment(method):
    """Pickup and virtual lines skip shipping address, charges, and ship workflow."""
    return method in ("pickup", "virtual")



_RETURN_SPLIT_COLUMNS_READY = False


def ensure_return_split_columns(db):
    """Add split return qty columns on return ledger lines (idempotent)."""
    global _RETURN_SPLIT_COLUMNS_READY
    if _RETURN_SPLIT_COLUMNS_READY:
        return
    db.execute(
        "ALTER TABLE every_circle.transactions_items "
        "ADD COLUMN ti_return_shipped_qty INT NULL",
        cmd="post",
    )
    db.execute(
        "ALTER TABLE every_circle.transactions_items "
        "ADD COLUMN ti_cancel_unshipped_qty INT NULL",
        cmd="post",
    )
    _RETURN_SPLIT_COLUMNS_READY = True


def _confirmed_return_split(db, order_uid, ti_uid):
    """
    Confirmed return ledger split for a sale line.
    Returns (return_shipped_qty, cancel_unshipped_qty).
    """
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
          AND rti.ti_original_ti_uid = %s
        """,
        (order_uid, ti_uid),
    )
    return_shipped = 0
    cancel_unshipped = 0
    for row in q.get("result") or []:
        shipped, cancel = _return_ledger_line_split(db, row.get("return_tx_uid"), row)
        return_shipped += shipped
        cancel_unshipped += cancel
    return return_shipped, cancel_unshipped


def _returnable_verified_qty(
    db, order_uid, ti_uid, verified_qty, exclude_trr_uid=None
):
    """
    Verified units still eligible for post-ship physical return.
    Physical returns may only come from the verified pool.
    """
    returned = _already_returned_qty(db, order_uid, ti_uid)
    reserved_return, _cancel = _reserved_return_split(
        db, order_uid, ti_uid, exclude_trr_uid=exclude_trr_uid
    )
    return max(int(verified_qty or 0) - returned - reserved_return, 0)




def _returnable_qty_remaining(
    db, order_uid, ti_uid, order_qty, exclude_trr_uid=None
):
    """Units still returnable on a line after ledger returns/cancels and open reservations."""
    returned, cancelled = _confirmed_return_split(db, order_uid, ti_uid)
    reserved = _reserved_return_qty(
        db, order_uid, ti_uid, exclude_trr_uid=exclude_trr_uid
    )
    return max(int(order_qty or 0) - returned - cancelled - reserved, 0)


def _parse_listing_mode_flags(raw):
    """
    Parse profile_expertise_mode / profile_wish_mode like the FE:
    optional Virtual, Delivered, and/or In-Person (comma-separated).
    """
    if raw is None or str(raw).strip() == "":
        return {"virtual": False, "delivered": False, "inPerson": False}
    s = str(raw).strip().lower()
    if "virtual or in-person" in s or "virtual or in person" in s:
        return {"virtual": True, "delivered": False, "inPerson": True}
    if s == "virtual":
        return {"virtual": True, "delivered": False, "inPerson": False}
    if s in ("in-person", "in person"):
        return {"virtual": False, "delivered": False, "inPerson": True}
    if s in ("delivered", "delivery"):
        return {"virtual": False, "delivered": True, "inPerson": False}
    has_virtual = bool(re.search(r"\bvirtual\b", s))
    has_delivered = bool(re.search(r"\bdeliver(ed|y)\b", s))
    has_in_person = bool(re.search(r"in-?\s*person", s))
    return {"virtual": has_virtual, "delivered": has_delivered, "inPerson": has_in_person}


def _mode_flags_from_product(product_data):
    if not product_data:
        return _parse_listing_mode_flags(None)
    raw = (
        product_data.get("bs_mode")
        or product_data.get("profile_expertise_mode")
        or product_data.get("profile_wish_mode")
    )
    return _parse_listing_mode_flags(raw)



def _normalize_listing_mode(raw):
    """
    Map profile_expertise_mode / profile_wish_mode / bs_mode to virtual | ship | pickup | both.

    New modes: Virtual, Delivered, In-Person (any combination). Legacy strings
    (in-person, virtual or in-person) are still accepted.
    """
    flags = _parse_listing_mode_flags(raw)
    virtual = flags["virtual"]
    delivered = flags["delivered"]
    in_person = flags["inPerson"]
    active = int(virtual) + int(delivered) + int(in_person)
    if active == 0:
        return "virtual"
    if active == 1:
        if delivered:
            return "ship"
        if in_person:
            return "pickup"
        return "virtual"
    return "both"


def _listing_shipping_type(product_data):
    if not product_data:
        return None
    sh = product_data.get("bs_shipping")
    if sh is None or str(sh).strip() == "":
        sh = product_data.get("profile_expertise_shipping")
    if sh is None or str(sh).strip() == "":
        return None
    return str(sh).strip()


def _has_shipping_config(product_data):
    return _listing_shipping_type(product_data) is not None












def _parse_return_item_quantities(entry, *, line_unshipped=False, order_cancel=False):
    """
    Parse return_quantity and optional return_shipped_qty / cancel_unshipped_qty.

    When split fields are omitted, infer from context:
      - order cancel / line never shipped → all units are cancel_unshipped
      - otherwise → treat as return_shipped (physical return)
    Returns (return_qty, return_shipped_qty, cancel_unshipped_qty, has_explicit_split)
    or None when invalid.
    """
    if not isinstance(entry, dict):
        return None

    has_shipped = entry.get("return_shipped_qty") is not None
    has_cancel = entry.get("cancel_unshipped_qty") is not None

    if has_shipped or has_cancel:
        try:
            return_shipped = int(entry.get("return_shipped_qty") or 0)
            cancel_unshipped = int(entry.get("cancel_unshipped_qty") or 0)
        except (TypeError, ValueError):
            return None
        if return_shipped < 0 or cancel_unshipped < 0:
            return None
        return_qty = return_shipped + cancel_unshipped
        if entry.get("return_quantity") is not None:
            try:
                declared = int(entry.get("return_quantity"))
            except (TypeError, ValueError):
                return None
            if declared != return_qty:
                return None
        return return_qty, return_shipped, cancel_unshipped, True

    try:
        return_qty = int(entry.get("return_quantity"))
    except (TypeError, ValueError):
        return None
    if return_qty < 1:
        return None

    if order_cancel or line_unshipped:
        return return_qty, 0, return_qty, False
    return return_qty, return_qty, 0, False


def _refund_shipping_for_line(
    ti_row,
    return_qty,
    *,
    return_shipped_qty=0,
    cancel_unshipped_qty=0,
):
    """
    Shipping credit for a product line.

    Per-unit model (Buyer Fixed): per_unit × eligible return qty.
    Flat line model: full line shipping only when the entire line is returned.
    """
    from line_commerce_fields import is_per_unit_shipping_model, line_shipping_charge

    if not ti_row:
        return 0.0

    try:
        rq = int(return_qty or 0)
    except (TypeError, ValueError):
        rq = 0
    if rq <= 0:
        return 0.0

    refundable = (
        _normalize_shipping_refundable(ti_row.get("ti_shipping_refundable"), default=0) == 1
    )
    if refundable:
        eligible_qty = rq
    else:
        eligible_qty = int(cancel_unshipped_qty or 0)

    if eligible_qty <= 0:
        return 0.0

    if is_per_unit_shipping_model(ti_row):
        per_unit = _to_float(ti_row.get("ti_shipping_amount"))
        if per_unit <= 0:
            return 0.0
        return round(per_unit * eligible_qty, 2)

    line_ship = line_shipping_charge(ti_row)
    if line_ship <= 0:
        return 0.0
    original_qty = int(ti_row.get("ti_bs_qty") or 0)
    if original_qty <= 0 or rq < original_qty:
        return 0.0
    return round(line_ship, 2)


def _items_all_cancel_only(items_payload, *, order_cancel=False):
    """True when every line is cancel-only (return_shipped_qty == 0 everywhere)."""
    if not items_payload:
        return False
    for entry in items_payload:
        parsed = _parse_return_item_quantities(entry, order_cancel=order_cancel)
        if not parsed:
            return False
        _rq, return_shipped, cancel_unshipped, _has_split = parsed
        if return_shipped != 0 or cancel_unshipped <= 0:
            return False
    return True


def _as_returnable_flag(value, default=True):
    """Interpret 0/1/true/false; NULL/empty uses default (legacy lines = returnable)."""
    if value is None or value == "":
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    s = str(value).strip().lower()
    if s in ("0", "false", "no", "off"):
        return False
    if s in ("1", "true", "yes", "on"):
        return True
    return default


def _normalize_is_returnable(value, default=1):
    """Snapshot value for ti_bs_is_returnable (0 or 1)."""
    return 1 if _as_returnable_flag(value, default=bool(default)) else 0


def line_return_eligibility(ti_row, now=None):
    """
    Compute return eligibility for a sale line from snapshotted policy fields.

    Returns dict with:
      return_eligible (bool)
      return_ineligible_reason ("Not returnable" | "Outside return window" | None)
      ti_bs_is_returnable (0|1)
      ti_bs_return_window_days
    """
    is_returnable_raw = ti_row.get("ti_bs_is_returnable")
    is_returnable = _as_returnable_flag(is_returnable_raw, default=True)
    window_days = ti_row.get("ti_bs_return_window_days")

    result = {
        "ti_bs_is_returnable": 1 if is_returnable else 0,
        "ti_bs_return_window_days": window_days,
        "return_eligible": True,
        "return_ineligible_reason": None,
    }

    if not is_returnable:
        result["return_eligible"] = False
        result["return_ineligible_reason"] = RETURN_INELIGIBLE_NOT_RETURNABLE
        return result

    received_at = parse_stored_datetime(ti_row.get("ti_received_at"))
    if received_at is None or window_days is None or str(window_days).strip() == "":
        return result

    try:
        days = int(window_days)
    except (TypeError, ValueError):
        return result

    now = now or datetime.now(timezone.utc)
    deadline = received_at + timedelta(days=days)
    if now > deadline:
        result["return_eligible"] = False
        result["return_ineligible_reason"] = RETURN_INELIGIBLE_OUTSIDE_WINDOW

    return result


def _display_return_status(return_status, refund_status):
    """FE label: e.g. 'Returning - Pending' / 'Cancelled - Refunded'."""
    r = (return_status or "").strip().capitalize()
    f = (refund_status or "").strip().capitalize()
    if r and f:
        return f"{r} - {f}"
    return r or f or None


def _normalize_status_pair(return_status=None, refund_status=None):
    """
    Normalize legacy single-field values into (return_status, refund_status).

    Legacy:
      pending/declined/accepted/refunded/resolved
    Current:
      return_status: returning | returned | cancelled
      refund_status: pending | refunded | rejected
    """
    rs = (return_status or "").strip().lower()
    fs = (refund_status or "").strip().lower()
    if rs in ("canceled",):
        rs = RETURN_STATUS_CANCELLED

    if rs in _VALID_RETURN_STATUSES and fs in _VALID_REFUND_STATUSES:
        return rs, fs

    legacy = {
        "pending": (RETURN_STATUS_RETURNING, REFUND_STATUS_PENDING),
        "declined": (RETURN_STATUS_RETURNING, REFUND_STATUS_REJECTED),
        "accepted": (RETURN_STATUS_RETURNED, REFUND_STATUS_PENDING),
        "refunded": (RETURN_STATUS_RETURNED, REFUND_STATUS_REFUNDED),
        "resolved": (RETURN_STATUS_RETURNED, REFUND_STATUS_REJECTED),
        "rejected": (RETURN_STATUS_RETURNING, REFUND_STATUS_REJECTED),
        "returning": (RETURN_STATUS_RETURNING, fs or REFUND_STATUS_PENDING),
        "returned": (RETURN_STATUS_RETURNED, fs or REFUND_STATUS_PENDING),
        "cancelled": (RETURN_STATUS_CANCELLED, fs or REFUND_STATUS_PENDING),
        "canceled": (RETURN_STATUS_CANCELLED, fs or REFUND_STATUS_PENDING),
    }
    if rs in legacy:
        return legacy[rs]

    if fs in _VALID_REFUND_STATUSES:
        return rs or RETURN_STATUS_RETURNING, fs

    return None, None


def _is_cancel_unshipped_request(req):
    """True when this TRR is a pre-ship cancel (not a physical return)."""
    from order_display import is_cancel_request

    return is_cancel_request(req)


def _status_payload(return_status, refund_status):
    rs, fs = _normalize_status_pair(return_status, refund_status)
    return {
        "return_status": rs,
        "refund_status": fs,
        "transaction_return_status": rs,  # logistics (aligned with FE Return column)
        "transaction_refund_status": fs,  # money (aligned with FE Received column)
        "display_status": _display_return_status(rs, fs),
    }


def _list_status_payload(return_status, refund_status):
    """Compact status for account-list rows (no aliased duplicates)."""
    rs, fs = _normalize_status_pair(return_status, refund_status)
    return {
        "return_status": rs,
        "refund_status": fs,
        "display_status": _display_return_status(rs, fs),
    }


def _awaiting_seller_confirm(req):
    """True when return request is open and not yet ledgered."""
    from order_display import is_awaiting_seller

    return is_awaiting_seller(req)


def _display_status_label(return_status, refund_status, *, cancel_unshipped=False):
    """Human label; e.g. 'Cancelling - Pending'. Prefer order_display.return_request_display_status."""
    from order_display import return_request_display_status

    if isinstance(return_status, dict):
        return return_request_display_status(return_status)
    f = (refund_status or "").strip().capitalize()
    if cancel_unshipped:
        r = "Cancelled"
    else:
        r = (return_status or "").strip().capitalize()
    if r and f:
        return f"{r} - {f}"
    return r or f or None


def _pending_return_chip_labels(req, refund_status):
    """Purchases / seller table chips — delegates to order_display."""
    from order_display import return_request_delivered_chip, return_request_received_chip

    return return_request_delivered_chip(req), return_request_received_chip(req)




_PARENT_SALE_RETURN_STATUS_KEYS = (
    "return_status",
    "refund_status",
    "display_status",
    "transaction_return_status",
    "transaction_refund_status",
)


def _has_open_pending_return(pending_req):
    """True when TRR is open and not yet ledgered (awaiting seller / refund)."""
    if not pending_req or pending_req.get("trr_return_transaction_uid"):
        return False
    rs, fs = _normalize_status_pair(
        pending_req.get("return_status") or pending_req.get("trr_return_status"),
        pending_req.get("refund_status") or pending_req.get("trr_refund_status"),
    )
    if not rs:
        rs, fs = _normalize_status_pair(pending_req.get("trr_status"), None)
    return _is_open_return(rs, fs)


def _clear_parent_sale_return_status(row):
    """Remove return/refund status from parent sale rows while TRR is open."""
    if isinstance(row, dict):
        for key in _PARENT_SALE_RETURN_STATUS_KEYS:
            row.pop(key, None)
    return row


def _return_request_public_payload(req, *, qty=None):
    """
    Status + display.* for one TRR — pending_return rows, open_returns[], list rows.

    Single source of truth so orders/:uid, purchases.rows[], and seller_transactions[]
    agree for the same trr_uid.
    """
    from order_display import build_return_request_display

    api = build_return_request_display(req, qty=qty)
    if not api:
        return {}
    out = {
        "return_status": api.get("return_status"),
        "refund_status": api.get("refund_status"),
        "display_status": api.get("display_status"),
    }
    if api.get("display"):
        out["display"] = dict(api["display"])
    for flag in ("cancel_unshipped", "pre_ship_cancel", "is_cancel_before_ship"):
        if api.get(flag):
            out[flag] = True
    return out


def _is_return_list_row(row):
    if not isinstance(row, dict):
        return False
    if row.get("is_return") or row.get("is_pending_return"):
        return True
    return (row.get("transaction_type") or "sale").lower() == "return"


def _resolve_parent_sale_uid(row, *, context=""):
    """
    Parent purchase uid for a list/API row.

    Prefer:
      1) trr_transaction_uid  (return-request / synthetic pending return)
      2) transaction_original_uid  (completed return ledger)
      3) transaction_uid  (sale rows only)

    Logs an error when a return row cannot resolve a parent sale.
    Returns (sale_uid_or_None, error_message_or_None).
    """
    if not isinstance(row, dict):
        msg = f"Cannot resolve parent sale uid{(' (' + context + ')') if context else ''}: row is not a dict"
        print(f"Error: {msg}")
        return None, msg

    trr_sale = row.get("trr_transaction_uid") or (
        (row.get("pending_return") or {}).get("trr_transaction_uid")
        if isinstance(row.get("pending_return"), dict)
        else None
    )
    original = row.get("transaction_original_uid")
    self_uid = row.get("transaction_uid")
    is_return = _is_return_list_row(row)

    if trr_sale:
        return str(trr_sale), None
    if original:
        return str(original), None
    if not is_return and self_uid:
        return str(self_uid), None

    msg = (
        f"Cannot resolve parent sale uid{(' (' + context + ')') if context else ''}: "
        f"missing trr_transaction_uid/transaction_original_uid "
        f"(transaction_uid={self_uid!r}, type={row.get('transaction_type')!r})"
    )
    print(f"Error: {msg}")
    return None, msg


def _omit_empty(obj):
    """Drop keys whose values are None or empty lists/dicts (shallow)."""
    if not isinstance(obj, dict):
        return obj
    out = {}
    for k, v in obj.items():
        if v is None:
            continue
        if v == [] or v == {}:
            continue
        out[k] = v
    return out


def _request_timezone():
    return request.args.get("timezone") or request.args.get("tz")


def _enrich_transaction_rows(rows):
    tz_name = _request_timezone()
    enriched = []
    for row in rows or []:
        if isinstance(row, dict):
            enriched.append(
                enrich_datetime_fields(dict(row), "transaction_datetime", tz_name)
            )
        else:
            enriched.append(row)
    return enriched


def _strip_currency(value):
    """Remove $ and commas from currency values before storing."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.replace("$", "").replace(",", "").strip()
    return value


def _parse_money_amount(value):
    """
    Parse a cost/currency string to float.

    Handles offering/product formats like 300/each, $25/hr, 35 total, as well
    as plain numbers.
    """
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return round(float(value), 2)
    s = str(value).replace("$", "").replace(",", "").strip()
    if not s:
        return None
    try:
        return round(float(s), 2)
    except (TypeError, ValueError):
        pass
    import re

    match = re.match(r"^([+-]?\d+(?:\.\d+)?)", s)
    if match:
        return round(float(match.group(1)), 2)
    return None


def _normalize_stored_cost(value):
    """Persist ti_bs_cost as a numeric string suitable for refunds and SQL."""
    parsed = _parse_money_amount(value)
    if parsed is None:
        stripped = _strip_currency(value)
        return stripped if stripped not in (None, "") else None
    if parsed == int(parsed):
        return str(int(parsed))
    return f"{parsed:.2f}"


def _to_float(value):
    if value is None:
        return 0.0
    parsed = _parse_money_amount(value)
    if parsed is not None:
        return parsed
    try:
        return float(_strip_currency(value))
    except (TypeError, ValueError):
        return 0.0


def _build_selected_options(item):
    """Normalize selected service options from checkout payload."""
    selected = item.get("selected_choices") or {}
    labels = item.get("selected_choice_labels") or {}
    choice_items = item.get("selected_choice_items") or []

    options = []
    if choice_items:
        for opt in choice_items:
            group = (opt.get("groupTitle") or opt.get("group_title") or "").strip()
            bso_uid = (
                opt.get("bso_uid")
                or opt.get("id")
                or selected.get(group)
            )
            options.append(
                {
                    "group_title": group,
                    "bso_uid": bso_uid,
                    "label": opt.get("label") or labels.get(group),
                    "extra_cost": _to_float(opt.get("extra_cost")),
                }
            )
    elif selected:
        for group, bso_uid in selected.items():
            options.append(
                {
                    "group_title": group,
                    "bso_uid": bso_uid,
                    "label": labels.get(group),
                    "extra_cost": 0.0,
                }
            )
    return options or None




def _parse_selected_options_field(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
    return []


def _bounty_scale_for_line(return_qty, original_qty):
    """Scale bounty reversal for a partial line return (return_qty / original_qty)."""
    if original_qty <= 0:
        return None
    rq = int(return_qty)
    oq = int(original_qty)
    if rq <= 0 or rq > oq:
        return None
    return rq / float(oq)


def _tax_amount_for_line(line_subtotal, ti_bs_is_taxable, ti_bs_tax_rate):
    if not ti_bs_is_taxable:
        return 0.0
    rate = _to_float(ti_bs_tax_rate)
    if rate <= 0:
        return 0.0
    # Rates may be stored as whole percent (e.g. 8.25) or fraction (0.0825).
    if rate > 1:
        rate = rate / 100.0
    return round(line_subtotal * rate, 4)


CHARITY_PROFILE_ID = "charity"
_BOUNTY_NETWORK_POOL = 0.40
_BOUNTY_NETWORK_MAX_PERSON = 0.20




def _bounty_pct_amount(effective_bounty, percentage):
    return {
        "tb_percentage": str(percentage),
        "tb_amount": round(percentage * effective_bounty, 4),
    }


def _charity_share_is_payable(charity_amount, charity_pct):
    """Skip charity bounty rows when there is nothing meaningful to pay."""
    if not charity_amount or charity_amount <= 0:
        return False
    try:
        return float(charity_pct) > 0
    except (TypeError, ValueError):
        return charity_amount > 0





def _get_authenticated_profile_id():
    try:
        verify_jwt_in_request(optional=True)
        identity = get_jwt_identity()
        if identity:
            return str(identity)
    except Exception:
        pass

    body = request.get_json(silent=True) or {}
    profile_id = body.get("profile_id")
    return str(profile_id) if profile_id else None


def _resolve_transaction_item(db, transaction_uid, transaction_item_uid):
    """Resolve a line item by ti_uid first, then ti_bs_id (same pattern as returns)."""
    ti_q = db.execute(
        """
        SELECT ti_uid, ti_bs_id, ti_bs_qty,
               COALESCE(ti_received_qty, 0) AS ti_received_qty,
               COALESCE(ti_fulfillment_status, 'not_required') AS ti_fulfillment_status,
               COALESCE(ti_shipped_qty, 0) AS ti_shipped_qty,
               COALESCE(ti_shipping_not_required, 0) AS ti_shipping_not_required,
               ti_fulfillment_method,
               ti_shipped_at, ti_tracking_carrier, ti_tracking_number,
               ti_fulfillment_note
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s AND ti_uid = %s
        """,
        (transaction_uid, transaction_item_uid),
    )
    ti_rows = ti_q.get("result") or []
    if ti_rows:
        return ti_rows[0]

    ti_q = db.execute(
        """
        SELECT ti_uid, ti_bs_id, ti_bs_qty,
               COALESCE(ti_received_qty, 0) AS ti_received_qty,
               COALESCE(ti_fulfillment_status, 'not_required') AS ti_fulfillment_status,
               COALESCE(ti_shipped_qty, 0) AS ti_shipped_qty,
               COALESCE(ti_shipping_not_required, 0) AS ti_shipping_not_required,
               ti_fulfillment_method,
               ti_shipped_at, ti_tracking_carrier, ti_tracking_number,
               ti_fulfillment_note
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s AND ti_bs_id = %s
        """,
        (transaction_uid, transaction_item_uid),
    )
    ti_rows = ti_q.get("result") or []
    return ti_rows[0] if ti_rows else None


def _apply_return_item_split(item, *, cancel_only=False):
    """Normalize return_quantity = return_shipped_qty + cancel_unshipped_qty on one item."""
    if not isinstance(item, dict):
        return item
    ti_uid = item.get("transaction_item_uid") or item.get("ti_uid")
    if ti_uid:
        item.setdefault("transaction_item_uid", ti_uid)
        item.setdefault("ti_uid", ti_uid)
    try:
        rq = int(item.get("return_quantity") or 0)
    except (TypeError, ValueError):
        rq = 0
    has_shipped = item.get("return_shipped_qty") is not None
    has_cancel = item.get("cancel_unshipped_qty") is not None
    if has_shipped or has_cancel:
        try:
            shipped = int(item.get("return_shipped_qty") or 0)
        except (TypeError, ValueError):
            shipped = 0
        try:
            unshipped = int(item.get("cancel_unshipped_qty") or 0)
        except (TypeError, ValueError):
            unshipped = 0
    elif cancel_only:
        shipped, unshipped = 0, rq
    else:
        shipped, unshipped = rq, 0
    item["return_shipped_qty"] = shipped
    item["cancel_unshipped_qty"] = unshipped
    return item


def _items_from_return_request_row(row):
    """
    Build the single-item (or legacy multi-item) list for a return-request row.
    Prefer columnar trr_ti_uid / trr_return_quantity; fall back to trr_items_json.
    """
    cancel_only = bool(
        int(row.get("trr_cancel_unshipped") or 0) == 1
        or row.get("cancel_unshipped")
        or row.get("pre_ship_cancel")
    )
    ti_uid = row.get("trr_ti_uid")
    json_items = []
    try:
        json_items = json.loads(row.get("trr_items_json") or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        json_items = []
    if ti_uid:
        try:
            qty = int(row.get("trr_return_quantity") or 0)
        except (TypeError, ValueError):
            qty = 0
        item = {
            "transaction_item_uid": ti_uid,
            "return_quantity": qty,
        }
        if isinstance(json_items, list) and json_items:
            first = json_items[0] or {}
            if first.get("return_shipped_qty") is not None:
                try:
                    item["return_shipped_qty"] = int(first.get("return_shipped_qty") or 0)
                except (TypeError, ValueError):
                    item["return_shipped_qty"] = 0
            if first.get("cancel_unshipped_qty") is not None:
                try:
                    item["cancel_unshipped_qty"] = int(first.get("cancel_unshipped_qty") or 0)
                except (TypeError, ValueError):
                    item["cancel_unshipped_qty"] = 0
        return [_apply_return_item_split(item, cancel_only=cancel_only)]
    try:
        items = json.loads(row.get("trr_items_json") or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    if not isinstance(items, list):
        return []
    return [
        _apply_return_item_split(entry, cancel_only=cancel_only)
        for entry in items
        if isinstance(entry, dict)
    ]


def _hydrate_return_request_row(row):
    """Normalize item fields + dual status onto a request row."""
    if not row:
        return None
    row["items"] = _items_from_return_request_row(row)
    if row.get("trr_ti_uid"):
        row["transaction_item_uid"] = row.get("trr_ti_uid")
        try:
            row["return_quantity"] = int(row.get("trr_return_quantity") or 0)
        except (TypeError, ValueError):
            row["return_quantity"] = 0
    elif row["items"]:
        first = row["items"][0] or {}
        row["transaction_item_uid"] = first.get("transaction_item_uid")
        try:
            row["return_quantity"] = int(first.get("return_quantity") or 0)
        except (TypeError, ValueError):
            row["return_quantity"] = 0
    else:
        row["transaction_item_uid"] = None
        row["return_quantity"] = 0
    if row.get("trr_return_status") or row.get("trr_refund_status"):
        rs, fs = _normalize_status_pair(
            row.get("trr_return_status"),
            row.get("trr_refund_status"),
        )
    else:
        rs, fs = _normalize_status_pair(row.get("trr_status"), None)
    row["return_status"] = rs
    row["refund_status"] = fs
    try:
        cancel_flag = int(row.get("trr_cancel_unshipped") or 0) == 1
    except (TypeError, ValueError):
        cancel_flag = bool(row.get("trr_cancel_unshipped"))
    if not cancel_flag and rs == RETURN_STATUS_CANCELLED:
        cancel_flag = True
    row["cancel_unshipped"] = cancel_flag
    row["pre_ship_cancel"] = cancel_flag
    row["is_cancel_before_ship"] = cancel_flag
    row["seller_note"] = row.get("trr_seller_note")
    row["note"] = row.get("trr_note")
    return row


_TRR_SELECT_COLS = """
    trr_uid, trr_transaction_uid, trr_profile_id,
    trr_ti_uid, trr_return_quantity, trr_items_json, trr_note, trr_seller_note,
    trr_status, trr_return_status, trr_refund_status, trr_cancel_unshipped,
    trr_estimated_total, trr_return_transaction_uid,
    trr_stripe_refund_id, trr_created_at, trr_updated_at,
    trr_bounty_to_reclaim, trr_estimated_refund_json
"""


def _already_returned_qty(db, order_uid, ti_uid):
    """Post-ship physical returns only (excludes pre-ship cancels)."""
    returned, _cancelled = _confirmed_return_split(db, order_uid, ti_uid)
    return returned


def _cancelled_qty(db, order_uid, ti_uid):
    """Pre-ship cancels only (excludes post-ship physical returns)."""
    _returned, cancelled = _confirmed_return_split(db, order_uid, ti_uid)
    return cancelled


def _return_ledger_line_split(db, return_tx_uid, row):
    """Split qty for a confirmed return ledger line."""
    return_shipped = row.get("ti_return_shipped_qty")
    cancel_unshipped = row.get("ti_cancel_unshipped_qty")
    if return_shipped is not None or cancel_unshipped is not None:
        return int(return_shipped or 0), int(cancel_unshipped or 0)

    qty = abs(int(row.get("ti_bs_qty") or 0))
    ti_uid = row.get("ti_original_ti_uid")
    trr_q = db.execute(
        """
        SELECT trr_cancel_unshipped, trr_items_json, trr_ti_uid
        FROM every_circle.transaction_return_requests
        WHERE trr_return_transaction_uid = %s
        LIMIT 1
        """,
        (return_tx_uid,),
    )
    trr_rows = trr_q.get("result") or []
    if trr_rows:
        trr = trr_rows[0]
        try:
            items = json.loads(trr.get("trr_items_json") or "[]")
        except (TypeError, ValueError, json.JSONDecodeError):
            items = []
        for entry in items if isinstance(items, list) else []:
            if ti_uid and entry.get("transaction_item_uid") != ti_uid:
                continue
            if entry.get("return_shipped_qty") is not None:
                try:
                    shipped = int(entry.get("return_shipped_qty") or 0)
                except (TypeError, ValueError):
                    shipped = 0
            else:
                shipped = 0
            if entry.get("cancel_unshipped_qty") is not None:
                try:
                    cancel = int(entry.get("cancel_unshipped_qty") or 0)
                except (TypeError, ValueError):
                    cancel = 0
            else:
                cancel = 0
            if shipped or cancel:
                return shipped, cancel
        if int(trr.get("trr_cancel_unshipped") or 0) == 1:
            return 0, qty
    return qty, 0


def _as_trr_uid_set(exclude_trr_uid=None):
    """Normalize a single uid, list, or set into a set of trr_uid strings."""
    if not exclude_trr_uid:
        return set()
    if isinstance(exclude_trr_uid, (list, tuple, set)):
        return {u for u in exclude_trr_uid if u}
    return {exclude_trr_uid}


def _reserved_return_split(db, order_uid, ti_uid, exclude_trr_uid=None):
    """
    Qty reserved by open return requests, split by post-ship return vs pre-ship cancel.
    Returns (return_shipped_qty, cancel_unshipped_qty).
    """
    exclude = _as_trr_uid_set(exclude_trr_uid)
    open_reqs = _load_open_return_requests(db, order_uid)
    return_shipped = 0
    cancel_unshipped = 0
    for req in open_reqs:
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


def _reserved_return_qty(db, order_uid, ti_uid, exclude_trr_uid=None):
    """Total qty already claimed by other open return requests on this sale."""
    return_shipped, cancel_unshipped = _reserved_return_split(
        db, order_uid, ti_uid, exclude_trr_uid=exclude_trr_uid
    )
    return return_shipped + cancel_unshipped


def _load_sale_for_return(db, transaction_uid):
    tx_row_q = db.execute(
        """
        SELECT transaction_uid, transaction_profile_id, transaction_business_id,
               transaction_stripe_pi, transaction_total, transaction_amount,
               transaction_taxes, transaction_fees,
               COALESCE(transaction_wallet_amount, 0) AS transaction_wallet_amount,
               transaction_return_requested, transaction_return_note,
               COALESCE(transaction_type, 'sale') AS transaction_type
        FROM every_circle.transactions
        WHERE transaction_uid = %s
        """,
        (transaction_uid,),
    )
    rows = tx_row_q.get("result") or []
    return rows[0] if rows else None


def _validate_and_price_return_items(
    db, original_tx_uid, items_payload, exclude_trr_uid=None,
    enforce_return_eligibility=True,
):
    """
    Validate return lines and compute refund breakdown.
    Returns (ok, error_dict_or_None, context_dict_or_None).

    exclude_trr_uid: when confirming an existing request, do not count that
    request's (or batch's) own reserved qty against itself. Accepts one uid
    or a list/set of uids for multi-item wave confirm.

    enforce_return_eligibility: when True (create-return), reject lines that
    are not returnable or outside the snapshotted return window.
    """
    if not isinstance(items_payload, list) or len(items_payload) == 0:
        return False, {
            "message": "transaction_return_items must be a non-empty list",
            "code": 400,
        }, None

    subtotal_q = db.execute(
        """
        SELECT COALESCE(SUM(CAST(ti_bs_cost AS DECIMAL(18,6)) * ti_bs_qty), 0)
            AS order_subtotal
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s
        """,
        (original_tx_uid,),
    )
    order_subtotal_rows = subtotal_q.get("result") or []
    order_subtotal = _to_float(
        order_subtotal_rows[0].get("order_subtotal") if order_subtotal_rows else 0
    )

    refund_subtotal = 0.0
    refund_tax = 0.0
    refund_shipping = 0.0
    lines_processed = []
    seen_ti = set()

    for entry in items_payload:
        ti_uid = entry.get("transaction_item_uid")
        if not ti_uid:
            return False, {
                "message": "Each entry requires transaction_item_uid",
                "code": 400,
            }, None

        ti_q = db.execute(
            """
            SELECT ti_uid, ti_transaction_id, ti_bs_id, ti_bso_id, ti_bs_qty, ti_bs_cost,
                   ti_bs_cost_currency, ti_bs_sku, ti_bs_is_taxable, ti_bs_tax_rate,
                   ti_bs_refund_policy, ti_bs_return_window_days, ti_bs_is_returnable,
                   ti_received_at, ti_selected_options, ti_special_instructions,
                   ti_choices_extra_cost, ti_shipping_amount, ti_shipping_refundable,
                   COALESCE(ti_shipped_qty, 0) AS ti_shipped_qty,
                   COALESCE(ti_received_qty, 0) AS ti_received_qty,
                   COALESCE(ti_fulfillment_method, '') AS ti_fulfillment_method,
                   COALESCE(ti_fulfillment_status, 'not_required') AS ti_fulfillment_status,
                   COALESCE(ti_shipping_not_required, 0) AS ti_shipping_not_required
            FROM every_circle.transactions_items
            WHERE ti_uid = %s AND ti_transaction_id = %s
            """,
            (ti_uid, original_tx_uid),
        )
        ti_rows = ti_q.get("result") or []
        if not ti_rows:
            return False, {
                "message": f"Transaction item not found on this sale: {ti_uid}",
                "code": 404,
            }, None

        ti_row = ti_rows[0]
        _ti_row_84 = ti_row
        if not _ti_row_84:
            uses_ship = False
        else:
            method = str(_ti_row_84.get('ti_fulfillment_method') or '').strip().lower()
            if method in ('pickup', 'virtual'):
                uses_ship = False
            elif method == 'ship':
                uses_ship = True
            elif int(_ti_row_84.get('ti_shipping_not_required') or 0) == 1:
                uses_ship = False
            else:
                status = str(_ti_row_84.get('ti_fulfillment_status') or FULFILLMENT_STATUS_NOT_REQUIRED).strip().lower()
                if status == FULFILLMENT_STATUS_NOT_REQUIRED:
                    uses_ship = False
                else:
                    uses_ship = status in ('not_shipped', 'in_transit', 'delivered', 'partial', 'partially_shipped')
        if uses_ship:
            line_unshipped = int(ti_row.get("ti_shipped_qty") or 0) == 0
        else:
            line_unshipped = int(ti_row.get("ti_received_qty") or 0) == 0
        parsed = _parse_return_item_quantities(
            entry,
            line_unshipped=line_unshipped,
            order_cancel=bool(entry.get("_order_cancel")),
        )
        if not parsed:
            return False, {
                "message": (
                    f"Invalid return quantities for item {ti_uid}; require "
                    "return_quantity or return_shipped_qty + cancel_unshipped_qty"
                ),
                "code": 400,
            }, None

        rq, return_shipped_qty, cancel_unshipped_qty, _has_split = parsed
        if rq < 1:
            return False, {
                "message": f"Invalid return_quantity for item {ti_uid}",
                "code": 400,
            }, None
        if ti_uid in seen_ti:
            return False, {
                "message": f"Duplicate transaction_item_uid: {ti_uid}",
                "code": 400,
            }, None
        seen_ti.add(ti_uid)

        if enforce_return_eligibility:
            eligibility = line_return_eligibility(ti_row)
            if not eligibility["return_eligible"]:
                reason = eligibility["return_ineligible_reason"]
                if reason == RETURN_INELIGIBLE_NOT_RETURNABLE:
                    message = "Item is not returnable"
                elif reason == RETURN_INELIGIBLE_OUTSIDE_WINDOW:
                    message = "Item is outside the return window"
                else:
                    message = "Item is not eligible for return"
                return False, {
                    "message": message,
                    "code": 422,
                    "transaction_item_uid": ti_uid,
                    "return_ineligible_reason": reason,
                }, None

        original_qty = int(ti_row.get("ti_bs_qty") or 0)
        received_qty = int(ti_row.get("ti_received_qty") or 0)
        shipped_qty = int(ti_row.get("ti_shipped_qty") or 0)
        returnable_remaining = _returnable_qty_remaining(
            db, original_tx_uid, ti_uid, original_qty, exclude_trr_uid=exclude_trr_uid
        )

        if uses_ship:
            _db_27 = db
            order_uid = original_tx_uid
            _ti_uid_25 = ti_uid
            _shipped_qty_28 = shipped_qty
            verified_qty = received_qty
            _exclude_trr_uid_26 = exclude_trr_uid
            returned = _already_returned_qty(_db_27, order_uid, _ti_uid_25)
            reserved_return, _cancel = _reserved_return_split(_db_27, order_uid, _ti_uid_25, exclude_trr_uid=_exclude_trr_uid_26)
            cap = min(int(_shipped_qty_28 or 0), int(verified_qty or 0))
            max_return_shipped = max(cap - returned - reserved_return, 0)
            _db_31 = db
            _order_uid_33 = original_tx_uid
            _ti_uid_29 = ti_uid
            purchased_qty = original_qty
            _shipped_qty_32 = shipped_qty
            _exclude_trr_uid_30 = exclude_trr_uid
            cancelled = _cancelled_qty(_db_31, _order_uid_33, _ti_uid_29)
            _reserved_return, reserved_cancel = _reserved_return_split(_db_31, _order_uid_33, _ti_uid_29, exclude_trr_uid=_exclude_trr_uid_30)
            pool = max(int(purchased_qty or 0) - int(_shipped_qty_32 or 0) - cancelled, 0)
            max_cancel = max(pool - reserved_cancel, 0)
            left_seller_qty = max_return_shipped
            remaining_not_left = max_cancel
            left_label = "verified returnable"
            not_left_label = "unshipped"
        else:
            left_seller_qty = min(received_qty, returnable_remaining)
            _db_53 = db
            _order_uid_51 = original_tx_uid
            _ti_uid_49 = ti_uid
            order_qty = original_qty
            _received_qty_50 = received_qty
            _exclude_trr_uid_52 = exclude_trr_uid
            order_qty = int(order_qty or 0)
            _received_qty_50 = int(_received_qty_50 or 0)
            unreceived_pool = max(order_qty - _received_qty_50, 0)
            total_remaining = _returnable_qty_remaining(_db_53, _order_uid_51, _ti_uid_49, order_qty, exclude_trr_uid=_exclude_trr_uid_52)
            remaining_not_left = min(unreceived_pool, total_remaining)
            left_label = "received"
            not_left_label = "unreceived"

        if return_shipped_qty > left_seller_qty:
            return False, {
                "message": (
                    f"return_shipped_qty exceeds max returnable verified qty for {ti_uid} "
                    f"(requested {return_shipped_qty}, max {left_seller_qty}; "
                    f"min(shipped, verified)=({shipped_qty}, {received_qty}))"
                ),
                "code": 400,
            }, None
        if cancel_unshipped_qty > remaining_not_left:
            return False, {
                "message": (
                    f"cancel_unshipped_qty exceeds max cancel-unshipped qty for {ti_uid} "
                    f"(requested {cancel_unshipped_qty}, max {remaining_not_left})"
                ),
                "code": 400,
            }, None

        already_returned = _already_returned_qty(db, original_tx_uid, ti_uid)
        already_cancelled = _cancelled_qty(db, original_tx_uid, ti_uid)
        reserved = _reserved_return_qty(
            db,
            original_tx_uid,
            ti_uid,
            exclude_trr_uid=exclude_trr_uid,
        )
        remaining = original_qty - already_returned - already_cancelled - reserved
        if rq > remaining:
            return False, {
                "message": (
                    f"return_quantity exceeds remaining returnable qty for {ti_uid} "
                    f"(requested {rq}, remaining {remaining})"
                ),
                "code": 400,
            }, None

        unit_cost = _parse_unit_cost(ti_row.get("ti_bs_cost"))
        scale = _bounty_scale_for_line(rq, original_qty)
        if scale is None:
            return False, {
                "message": (
                    f"return_quantity must be between 1 and {original_qty} for {ti_uid}"
                ),
                "code": 400,
            }, None

        line_subtotal = round(unit_cost * rq, 4)
        from line_commerce_fields import _line_tax_snapshot

        prorated_row = dict(ti_row)
        prorated_row["ti_bs_qty"] = rq
        stored_line_tax = _line_tax_snapshot(ti_row)
        orig_qty = int(ti_row.get("ti_bs_qty") or 0)
        if stored_line_tax is not None and orig_qty > 0:
            line_tax = round(_to_float(stored_line_tax) * rq / orig_qty, 4)
        else:
            line_tax = _tax_amount_for_line(
                line_subtotal,
                ti_row.get("ti_bs_is_taxable"),
                ti_row.get("ti_bs_tax_rate"),
            )
        line_shipping = _refund_shipping_for_line(
            ti_row,
            rq,
            return_shipped_qty=return_shipped_qty,
            cancel_unshipped_qty=cancel_unshipped_qty,
        )
        refund_subtotal += line_subtotal
        refund_tax += line_tax
        refund_shipping += line_shipping

        lines_processed.append(
            {
                "original_ti_uid": ti_uid,
                "ti_bs_id": ti_row.get("ti_bs_id"),
                "return_quantity": rq,
                "return_shipped_qty": return_shipped_qty,
                "cancel_unshipped_qty": cancel_unshipped_qty,
                "original_quantity": original_qty,
                "already_returned": already_returned,
                "unit_cost": unit_cost,
                "line_subtotal": line_subtotal,
                "line_tax": line_tax,
                "line_shipping": line_shipping,
                "snapshot": ti_row,
            }
        )

    return True, None, {
        "order_subtotal": order_subtotal,
        "refund_subtotal": refund_subtotal,
        "refund_tax": refund_tax,
        "refund_shipping": refund_shipping,
        "lines_processed": lines_processed,
    }


def _refund_breakdown_from_context(orig_tx, ctx):
    order_subtotal = ctx["order_subtotal"]
    refund_subtotal = ctx["refund_subtotal"]
    refund_tax = ctx["refund_tax"]
    refund_shipping = _to_float(ctx.get("refund_shipping"))
    orig_fees = abs(_to_float(orig_tx.get("transaction_fees")))
    fee_ratio = refund_subtotal / order_subtotal if order_subtotal > 0 else 0.0
    # Card processing fees are non-refundable but are NOT part of merchandise/tax/shipping.
    # Refund = subtotal + tax + shipping only — do not subtract fees again (that double-counts).
    refund_fees = 0.0
    refund_grand = round(refund_subtotal + refund_tax + refund_shipping, 4)

    orig_total = abs(_to_float(orig_tx.get("transaction_total")))
    wallet_paid = abs(_to_float(orig_tx.get("transaction_wallet_amount")))
    has_card = bool(
        _normalize_stripe_payment_intent_id(orig_tx.get("transaction_stripe_pi"))
    )
    # Full wallet checkout with missing/zero column: no card PI means all wallet.
    if wallet_paid <= 0 and not has_card and orig_total > 0:
        wallet_paid = orig_total
    if orig_total > 0 and wallet_paid > 0:
        wallet_paid = min(wallet_paid, orig_total)
        wallet_ratio = wallet_paid / orig_total
        wallet_refund = round(refund_grand * wallet_ratio, 4)
        stripe_refund = round(refund_grand - wallet_refund, 4)
    else:
        wallet_refund = 0.0
        stripe_refund = refund_grand

    return {
        "subtotal": round(refund_subtotal, 4),
        "taxes": round(refund_tax, 4),
        "shipping": round(refund_shipping, 4),
        "fees_allocated": 0.0,
        "fees_not_refunded": round(orig_fees * fee_ratio, 4),
        "total_customer_credit": round(refund_grand, 4),
        "fee_allocation_ratio": round(fee_ratio, 6),
        "original_order_subtotal": round(order_subtotal, 4),
        "refund_fees": refund_fees,
        "refund_shipping": refund_shipping,
        "refund_grand": refund_grand,
        "fee_ratio": fee_ratio,
        "wallet_paid": round(wallet_paid, 4),
        "wallet_refund": wallet_refund,
        "stripe_refund": stripe_refund,
    }


def _estimated_refund_api_payload(refund_meta, *, compact=False):
    """
    FE-facing estimated_refund object.

    Uses ``total`` (not total_customer_credit). ``estimated_total`` at the
    response root should match ``estimated_refund.total``.

    Includes wallet vs card split so FE createRefund only hits Stripe for the
    card portion (wallet-paid amounts restore to useable balance).
    """
    fees = round(_to_float(refund_meta.get("fees_allocated")), 4)
    total = round(_to_float(refund_meta.get("total_customer_credit")), 4)
    wallet_refund = round(_to_float(refund_meta.get("wallet_refund")), 4)
    stripe_refund = round(
        _to_float(
            refund_meta.get("stripe_refund")
            if refund_meta.get("stripe_refund") is not None
            else total
        ),
        4,
    )
    payload = {
        "subtotal": round(_to_float(refund_meta.get("subtotal")), 4),
        "taxes": round(_to_float(refund_meta.get("taxes")), 4),
        "shipping_refund": round(
            _to_float(refund_meta.get("shipping") or refund_meta.get("refund_shipping")),
            4,
        ),
        "total": total,
        "total_customer_credit": total,
        "wallet_refund": wallet_refund,
        "stripe_refund": stripe_refund,
    }
    if not compact or fees:
        payload["fees_allocated"] = fees
    return payload




def _normalize_stripe_payment_intent_id(raw):
    """
    Accept a PaymentIntent id (pi_…) or a client secret (pi_…_secret_…).
    Returns the pi_… id, or None if empty/invalid.
    """
    if raw is None:
        return None
    value = str(raw).strip()
    if not value:
        return None
    if "_secret_" in value:
        value = value.split("_secret_", 1)[0].strip()
    if not value.startswith("pi_"):
        return None
    return value




def _load_return_request_by_uid(db, trr_uid):
    if not trr_uid:
        return None
    q = db.execute(
        f"""
        SELECT {_TRR_SELECT_COLS}
        FROM every_circle.transaction_return_requests
        WHERE trr_uid = %s
        """,
        (trr_uid,),
    )
    rows = q.get("result") or []
    return _hydrate_return_request_row(rows[0]) if rows else None


def _load_return_requests_for_sale(db, transaction_uid):
    """All return-request rows for a sale, newest first."""
    if not transaction_uid:
        return []
    q = db.execute(
        f"""
        SELECT {_TRR_SELECT_COLS}
        FROM every_circle.transaction_return_requests
        WHERE trr_transaction_uid = %s
        ORDER BY trr_created_at DESC, trr_updated_at DESC
        """,
        (transaction_uid,),
    )
    return [
        _hydrate_return_request_row(row)
        for row in (q.get("result") or [])
        if row
    ]


def _is_open_return(return_status, refund_status):
    """True when a return wave is in flight (awaiting seller/refund action)."""
    return (return_status, refund_status) in (
        (RETURN_STATUS_RETURNING, REFUND_STATUS_PENDING),
        (RETURN_STATUS_RETURNED, REFUND_STATUS_PENDING),
        (RETURN_STATUS_CANCELLED, REFUND_STATUS_PENDING),
    )


def _items_all_unshipped(db, order_uid, items_payload):
    """True when every requested sale line has ti_shipped_qty == 0."""
    if not items_payload:
        return False
    for entry in items_payload:
        ti_uid = entry.get("transaction_item_uid")
        if not ti_uid:
            return False
        ti_row = _resolve_transaction_item(db, order_uid, ti_uid)
        if not ti_row:
            return False
        if int(ti_row.get("ti_shipped_qty") or 0) > 0:
            return False
    return True


def _remaining_to_ship_qty(
    db, order_uid, ti_uid, order_qty, shipped_qty, exclude_trr_uid=None, *, ti_row=None
):
    """
    Units still shippable after shipped qty and pre-ship cancels/reservations.
    Post-ship physical returns do not reduce remaining_to_ship.

    Uses effective shipped (max ti_shipped_qty, ti_received_qty) on shipping-required
    lines so buyer verification implies delivery even when seller never clicked ship.
    remaining_to_ship = max(purchased - effective_shipped - cancelled - reserved_cancel, 0)
    """
    if ti_row is None and ti_uid and order_uid:
        ti_row = _resolve_transaction_item(db, order_uid, ti_uid) or {}
    else:
        ti_row = dict(ti_row or {})
    ti_row.setdefault("ti_bs_qty", order_qty)
    ti_row.setdefault("ti_shipped_qty", shipped_qty)

    from transaction_shipping import effective_shipped_qty_for_line

    effective_shipped = effective_shipped_qty_for_line(ti_row)
    cancelled = _cancelled_qty(db, order_uid, ti_uid)
    _reserved_return, reserved_cancel = _reserved_return_split(
        db, order_uid, ti_uid, exclude_trr_uid=exclude_trr_uid
    )
    max_cancel = max(int(order_qty or 0) - effective_shipped - cancelled, 0)
    reserved_cancel = min(int(reserved_cancel or 0), max_cancel)
    return max(
        int(order_qty or 0) - effective_shipped - cancelled - reserved_cancel,
        0,
    )



def _load_open_return_requests(db, transaction_uid):
    """Open (in-flight) return requests for a sale, newest first."""
    return [
        req
        for req in _load_return_requests_for_sale(db, transaction_uid)
        if _is_open_return(req.get("return_status"), req.get("refund_status"))
    ]


def _load_return_request(db, transaction_uid):
    """
    Back-compat: prefer the newest open request for a sale; else newest any.
    Prefer _load_return_request_by_uid / _load_open_return_requests for new code.
    """
    open_reqs = _load_open_return_requests(db, transaction_uid)
    if open_reqs:
        return open_reqs[0]
    all_reqs = _load_return_requests_for_sale(db, transaction_uid)
    return all_reqs[0] if all_reqs else None


def _parse_trr_uids_from_payload(payload):
    """Prefer trr_uids[]; fall back to single trr_uid / return_request_uid."""
    if not payload:
        return []
    raw = payload.get("trr_uids")
    if isinstance(raw, list) and len(raw) > 0:
        seen = set()
        ordered = []
        for u in raw:
            if u and u not in seen:
                seen.add(u)
                ordered.append(u)
        return ordered
    single = payload.get("trr_uid") or payload.get("return_request_uid")
    return [single] if single else []


def _load_return_request_wave(db, transaction_uid, trr_uids):
    """
    Load multiple return-request rows for batch confirm/decline.
    All must belong to transaction_uid and share the same trr_created_at wave.
    Returns (requests_list, error_dict_or_None).
    """
    if not trr_uids:
        return None, {"message": "trr_uids is required", "code": 400}

    seen = set()
    ordered = []
    for u in trr_uids:
        if u and u not in seen:
            seen.add(u)
            ordered.append(u)

    requests = []
    for uid in ordered:
        req = _load_return_request_by_uid(db, uid)
        if not req:
            return None, {
                "message": f"Return request not found: {uid}",
                "code": 404,
                "trr_uid": uid,
            }
        if transaction_uid and req.get("trr_transaction_uid") != transaction_uid:
            return None, {
                "message": f"trr_uid {uid} does not belong to this transaction_uid",
                "code": 400,
                "trr_uid": uid,
            }
        requests.append(req)

    if len(requests) > 1:
        waves = {str(r.get("trr_created_at") or "") for r in requests}
        if len(waves) > 1:
            return None, {
                "message": (
                    "trr_uids must belong to the same return wave "
                    "(same trr_created_at)"
                ),
                "code": 400,
                "trr_uids": ordered,
            }
    return requests, None


def _resolve_return_request(db, transaction_uid, trr_uid=None):
    """
    Resolve which request to act on.
    If trr_uid given, load it (must belong to sale).
    Else if exactly one open request, use it.
    Else if multiple open, require trr_uid or trr_uids.
    """
    if trr_uid:
        req = _load_return_request_by_uid(db, trr_uid)
        if not req:
            return None, {
                "message": f"Return request not found: {trr_uid}",
                "code": 404,
            }
        if transaction_uid and req.get("trr_transaction_uid") != transaction_uid:
            return None, {
                "message": "trr_uid does not belong to this transaction_uid",
                "code": 400,
            }
        return req, None

    open_reqs = _load_open_return_requests(db, transaction_uid)
    if len(open_reqs) == 1:
        return open_reqs[0], None
    if len(open_reqs) > 1:
        return None, {
            "message": (
                "Multiple open return requests; pass trr_uid or trr_uids "
                "to select one request or a whole wave"
            ),
            "code": 400,
            "open_trr_uids": [r.get("trr_uid") for r in open_reqs],
        }
    # Fall back to newest closed request (legacy single-row callers)
    req = _load_return_request(db, transaction_uid)
    if not req:
        return None, {
            "message": "No pending return request found for this transaction",
            "code": 404,
        }
    return req, None


def _pair_for_sale(orig_tx, pending=None):
    """
    Resolve current (return_status, refund_status) from the return-request row.
    Sale transactions no longer store return_status.
    """
    if pending:
        rs, fs = _normalize_status_pair(
            pending.get("return_status") or pending.get("trr_return_status"),
            pending.get("refund_status") or pending.get("trr_refund_status"),
        )
        if rs and fs:
            return rs, fs
        rs, fs = _normalize_status_pair(pending.get("trr_status"), None)
        if rs and fs:
            return rs, fs
    return None, None


def _line_estimated_total(orig_tx, ctx, line):
    """Estimated customer credit for a single return line (merchandise + tax + shipping)."""
    line_ctx = {
        "order_subtotal": ctx["order_subtotal"],
        "refund_subtotal": line["line_subtotal"],
        "refund_tax": line["line_tax"],
        "refund_shipping": line.get("line_shipping", 0),
    }
    return _refund_breakdown_from_context(orig_tx, line_ctx)["total_customer_credit"]




def _sale_has_other_open_returns(db, transaction_uid, exclude_trr_uid=None):
    exclude = _as_trr_uid_set(exclude_trr_uid)
    for req in _load_open_return_requests(db, transaction_uid):
        if req.get("trr_uid") in exclude:
            continue
        return True
    return False



def _update_return_statuses(
    db,
    transaction_uid,
    return_status,
    refund_status,
    *,
    trr_uid=None,
    trr_uids=None,
    return_requested=None,
    return_note=None,
    seller_note=None,
    return_transaction_uid=None,
    stripe_refund_id=None,
):
    """
    Update targeted return-request row(s) by trr_uid / trr_uids.

    On the sale transaction, only maintain transaction_return_requested.
    Note / return_status / seller_note live on transaction_return_requests:
      trr_note, trr_return_status, trr_seller_note
      (sale uid is trr_transaction_uid)
    """
    uids = []
    if trr_uids:
        uids = [u for u in trr_uids if u]
    elif trr_uid:
        uids = [trr_uid]

    sale_return_requested = return_requested
    if sale_return_requested == 0 and _sale_has_other_open_returns(
        db, transaction_uid, exclude_trr_uid=uids
    ):
        sale_return_requested = 1

    if sale_return_requested is not None:
        db.update(
            "every_circle.transactions",
            {"transaction_uid": transaction_uid},
            {"transaction_return_requested": sale_return_requested},
        )

    for uid in uids:
        _r__update_return_request_row_98 = None
        _db_101 = db
        _trr_uid_106 = uid
        _return_status_104 = return_status
        _refund_status_99 = refund_status
        _return_note_100 = return_note
        _seller_note_105 = seller_note
        _return_transaction_uid_103 = return_transaction_uid
        _stripe_refund_id_102 = stripe_refund_id
        if not _trr_uid_106:
            _r__update_return_request_row_98 = None
        else:
            req_fields = {'trr_status': _refund_status_99, 'trr_return_status': _return_status_104, 'trr_refund_status': _refund_status_99, 'trr_updated_at': utc_now_str()}
            if _return_note_100 is not None:
                req_fields['trr_note'] = _return_note_100
            if _seller_note_105 is not None:
                req_fields['trr_seller_note'] = _seller_note_105
            if _return_transaction_uid_103 is not None:
                req_fields['trr_return_transaction_uid'] = _return_transaction_uid_103
            if _stripe_refund_id_102 is not None:
                req_fields['trr_stripe_refund_id'] = _stripe_refund_id_102
            _db_101.update('every_circle.transaction_return_requests', {'trr_uid': _trr_uid_106}, req_fields)


def _finalize_pending_return(
    db,
    original_tx_uid,
    seller_note=None,
    stripe_refund_from_client=None,
    trr_uid=None,
    trr_uids=None,
):
    """
    Seller/admin confirmation: item(s) received (Returned) then ledger + Stripe.
    Flow: Returning-Pending → Returned-Pending → Returned-Refunded|Rejected
    Returns (http_body, http_status).

    Supports a single trr_uid or a same-wave trr_uids batch. Batch confirm
    merges items into one return ledger and issues Stripe once.

    If stripe_refund_from_client is provided (FE already called IO-Payments createRefund),
    use that result instead of calling Stripe from this backend.
    """
    orig_tx = _load_sale_for_return(db, original_tx_uid)
    if not orig_tx:
        return {"message": "Original transaction not found", "code": 404}, 404

    if (orig_tx.get("transaction_type") or "sale") != "sale":
        return {
            "message": "Returns can only be confirmed against a sale transaction",
            "code": 400,
        }, 400

    uid_list = [u for u in (trr_uids or []) if u]
    if not uid_list and trr_uid:
        uid_list = [trr_uid]

    if uid_list:
        requests, resolve_err = _load_return_request_wave(
            db, original_tx_uid, uid_list
        )
        if resolve_err:
            return resolve_err, resolve_err.get("code", 400)
    else:
        pending, resolve_err = _resolve_return_request(db, original_tx_uid, None)
        if resolve_err:
            return resolve_err, resolve_err.get("code", 400)
        requests = [pending]

    batch_uids = [r.get("trr_uid") for r in requests]
    primary_trr = batch_uids[0] if batch_uids else None
    is_batch = len(batch_uids) > 1

    items_payload = []
    for req in requests:
        items_payload.extend(req.get("items") or [])

    is_cancel = all(_is_cancel_unshipped_request(r) for r in requests) or (
        bool(requests) and _items_all_unshipped(db, original_tx_uid, items_payload)
    )
    logistics_status = (
        RETURN_STATUS_CANCELLED if is_cancel else RETURN_STATUS_RETURNED
    )

    confirm_allowed = (
        (RETURN_STATUS_RETURNING, REFUND_STATUS_PENDING),
        (RETURN_STATUS_RETURNING, REFUND_STATUS_REJECTED),
        (RETURN_STATUS_RETURNED, REFUND_STATUS_PENDING),
        (RETURN_STATUS_CANCELLED, REFUND_STATUS_PENDING),
        (RETURN_STATUS_CANCELLED, REFUND_STATUS_REJECTED),
    )

    for req in requests:
        uid = req.get("trr_uid")
        return_status, refund_status = _pair_for_sale(orig_tx, req)

        if refund_status == REFUND_STATUS_REFUNDED:
            return {
                "message": "Return already refunded",
                "code": 409,
                "trr_uid": uid,
                "trr_uids": batch_uids,
                **_status_payload(return_status, refund_status),
            }, 409
        if (
            return_status == RETURN_STATUS_RETURNED
            and refund_status == REFUND_STATUS_REJECTED
        ):
            return {
                "message": "Return was rejected; cannot refund",
                "code": 409,
                "trr_uid": uid,
                "trr_uids": batch_uids,
                **_status_payload(return_status, refund_status),
            }, 409
        if not req.get("items"):
            return {
                "message": "No pending return request found for this transaction",
                "code": 404,
                "trr_uid": uid,
                "trr_uids": batch_uids,
            }, 404
        if (return_status, refund_status) not in confirm_allowed:
            return {
                "message": (
                    "Return is not awaiting confirmation "
                    f"(status={_display_return_status(return_status, refund_status)})"
                ),
                "code": 409,
                "trr_uid": uid,
                "trr_uids": batch_uids,
                **_status_payload(return_status, refund_status),
            }, 409

    return_note = requests[0].get("trr_note") if requests else None

    # Cancel path: keep cancelled (no physical receipt). Physical: mark returned.
    _update_return_statuses(
        db,
        original_tx_uid,
        logistics_status,
        REFUND_STATUS_PENDING,
        trr_uids=batch_uids,
        return_requested=1,
        return_note=return_note,
        seller_note=seller_note,
    )

    ok, err, ctx = _validate_and_price_return_items(
        db,
        original_tx_uid,
        [
            {**entry, "_order_cancel": is_cancel}
            for entry in items_payload
        ],
        exclude_trr_uid=batch_uids,
        enforce_return_eligibility=False,
    )
    if not ok:
        return err, err.get("code", 400)

    refund_meta = _refund_breakdown_from_context(orig_tx, ctx)
    trr_by_ti = {}
    for req in requests:
        ti_uid = req.get("trr_ti_uid")
        if not ti_uid:
            for entry in req.get("items") or []:
                ti_uid = entry.get("transaction_item_uid")
                if ti_uid:
                    break
        if ti_uid and req.get("trr_uid"):
            trr_by_ti[ti_uid] = req.get("trr_uid")

    _db_168 = db
    _orig_tx_170 = orig_tx
    _ctx_167 = ctx
    _refund_meta_171 = refund_meta
    _return_note_173 = return_note
    _trr_by_ti_169 = trr_by_ti
    ensure_return_split_columns(_db_168)
    _original_tx_uid_172 = _orig_tx_170.get('transaction_uid')
    lines_processed = _ctx_167['lines_processed']
    refund_grand = _refund_meta_171['refund_grand']
    refund_subtotal = _refund_meta_171['subtotal']
    refund_tax = _refund_meta_171['taxes']
    refund_fees = _refund_meta_171['refund_fees']
    refund_shipping = _refund_meta_171.get('refund_shipping', 0) or 0
    fee_ratio = _refund_meta_171['fee_ratio']
    order_subtotal = _refund_meta_171['original_order_subtotal']
    wallet_refund = round(_to_float(_refund_meta_171.get('wallet_refund')), 4)
    new_uid_resp = _db_168.call(procedure='new_transaction_uid')
    if not new_uid_resp.get('result') or len(new_uid_resp['result']) == 0:
        _r__create_return_ledger_166 = (False, {'message': 'Failed to generate return transaction UID', 'code': 500}, None)
    else:
        new_transaction_uid = new_uid_resp['result'][0]['new_id']
        transactions_datetime = utc_now_str()
        new_transaction = {'transaction_uid': new_transaction_uid, 'transaction_datetime': transactions_datetime, 'transaction_profile_id': _orig_tx_170.get('transaction_profile_id'), 'transaction_business_id': _orig_tx_170.get('transaction_business_id'), 'transaction_stripe_pi': _orig_tx_170.get('transaction_stripe_pi'), 'transaction_total': f'{-refund_grand:.4f}', 'transaction_amount': f'{-refund_subtotal:.4f}', 'transaction_taxes': f'{-refund_tax:.4f}', 'transaction_fees': '0.0000', 'transaction_wallet_amount': f'{-wallet_refund:.4f}', 'transaction_in_escrow': 0, 'transaction_return_note': _return_note_173, 'transaction_type': 'return', 'transaction_original_uid': _original_tx_uid_172}
        if refund_shipping:
            new_transaction['transaction_shipping'] = f'{-refund_shipping:.4f}'
        tx_insert = _db_168.insert('every_circle.transactions', new_transaction)
        if tx_insert.get('code') != 200:
            _r__create_return_ledger_166 = (False, {'message': tx_insert.get('message', 'Failed to insert return transaction'), 'code': tx_insert.get('code', 500)}, None)
        else:
            bounty_insert_count = 0
            item_insert_count = 0
            response_lines = []
            total_seller_clawed = 0.0
            _r__create_return_ledger_166__returned = False
            for line in lines_processed:
                ti_row = line['snapshot']
                rq = line['return_quantity']
                original_qty = line['original_quantity']
                ti_bs_id = ti_row.get('ti_bs_id')
                ti_uid_resp = _db_168.call(procedure='new_transaction_item_uid')
                if not ti_uid_resp.get('result') or len(ti_uid_resp['result']) == 0:
                    _r__create_return_ledger_166 = (False, {'message': 'Failed to generate return line item UID', 'code': 500}, None)
                    _r__create_return_ledger_166__returned = True
                    break
                new_ti_uid = ti_uid_resp['result'][0]['new_id']
                neg_qty = -int(rq)
                tx_item = {'ti_uid': new_ti_uid, 'ti_transaction_id': new_transaction_uid, 'ti_original_ti_uid': line['original_ti_uid'], 'ti_bs_id': ti_bs_id, 'ti_bs_qty': neg_qty, 'ti_bs_cost': ti_row.get('ti_bs_cost'), 'ti_bs_cost_currency': ti_row.get('ti_bs_cost_currency'), 'ti_bs_sku': ti_row.get('ti_bs_sku'), 'ti_bs_is_taxable': ti_row.get('ti_bs_is_taxable'), 'ti_bs_tax_rate': ti_row.get('ti_bs_tax_rate'), 'ti_bs_refund_policy': ti_row.get('ti_bs_refund_policy'), 'ti_bs_return_window_days': ti_row.get('ti_bs_return_window_days'), 'ti_bs_is_returnable': _normalize_is_returnable(ti_row.get('ti_bs_is_returnable'))}
                if ti_row.get('ti_bso_id'):
                    tx_item['ti_bso_id'] = ti_row.get('ti_bso_id')
                if ti_row.get('ti_selected_options') is not None:
                    tx_item['ti_selected_options'] = ti_row.get('ti_selected_options')
                if ti_row.get('ti_special_instructions'):
                    tx_item['ti_special_instructions'] = ti_row.get('ti_special_instructions')
                if ti_row.get('ti_choices_extra_cost') is not None:
                    tx_item['ti_choices_extra_cost'] = ti_row.get('ti_choices_extra_cost')
                if ti_row.get('ti_shipping_amount') is not None:
                    tx_item['ti_shipping_amount'] = ti_row.get('ti_shipping_amount')
                if ti_row.get('ti_shipping_refundable') is not None:
                    tx_item['ti_shipping_refundable'] = ti_row.get('ti_shipping_refundable')
                return_shipped_qty = int(line.get('return_shipped_qty') or 0)
                cancel_unshipped_qty = int(line.get('cancel_unshipped_qty') or 0)
                tx_item['ti_return_shipped_qty'] = return_shipped_qty
                tx_item['ti_cancel_unshipped_qty'] = cancel_unshipped_qty
                ti_insert = _db_168.insert('every_circle.transactions_items', tx_item)
                if ti_insert.get('code') != 200:
                    _r__create_return_ledger_166 = (False, {'message': ti_insert.get('message', 'Failed to insert return transaction item'), 'code': ti_insert.get('code', 500)}, None)
                    _r__create_return_ledger_166__returned = True
                    break
                item_insert_count += 1
                scale = _bounty_scale_for_line(rq, original_qty) or 0.0
                bounty_q = _db_168.execute('\n            SELECT tb_uid, tb_profile_id, tb_percentage, tb_amount\n            FROM every_circle.transactions_bounty\n            WHERE tb_ti_id = %s\n            ', (line['original_ti_uid'],))
                bounty_rows = bounty_q.get('result') or []
                for br in bounty_rows:
                    raw_amt = _to_float(br.get('tb_amount'))
                    reversal = round(-scale * raw_amt, 4)
                    if reversal == 0:
                        continue
                    bounty_uid_resp = _db_168.call(procedure='new_transaction_bounty_uid')
                    if not bounty_uid_resp.get('result') or len(bounty_uid_resp['result']) == 0:
                        print('Warning: Failed to generate bounty UID for reversal')
                        continue
                    new_tb_uid = bounty_uid_resp['result'][0]['new_id']
                    tx_bounty = {'tb_uid': new_tb_uid, 'tb_ti_id': new_ti_uid, 'tb_profile_id': br.get('tb_profile_id'), 'tb_percentage': br.get('tb_percentage'), 'tb_amount': reversal}
                    bins = _db_168.insert('every_circle.transactions_bounty', tx_bounty)
                    if bins.get('code') == 200:
                        bounty_insert_count += 1
                        reversal_abs = abs(reversal)
                        if reversal_abs > 0:
                            prefer_pending = not bounty_was_released_to_useable_at(_db_168, line['original_ti_uid'], utc_now_str())
                            wallet_result = debit_bounty_from_wallet(_db_168, br.get('tb_profile_id'), reversal_abs, prefer_pending=prefer_pending)
                            if wallet_result.get('code') != 200:
                                print(f'Warning: Failed to debit wallet on return for {br.get('tb_profile_id')}: {wallet_result}')
                if _r__create_return_ledger_166__returned:
                    break
                return_shipped_qty = int(line.get('return_shipped_qty') or 0)
                cancel_unshipped_qty = int(line.get('cancel_unshipped_qty') or 0)
                claw_qty = return_shipped_qty
                clawback_result = None
                if claw_qty > 0:
                    line_trr_uid = (_trr_by_ti_169 or {}).get(line['original_ti_uid'])
                    clawback_result = clawback_seller_proceeds_on_return(_db_168, original_ti_uid=line['original_ti_uid'], return_ti_uid=new_ti_uid, return_qty=claw_qty, transaction_uid=_original_tx_uid_172, trr_uid=line_trr_uid)
                    if clawback_result.get('code') != 200:
                        print(f'Warning: Failed to claw back seller proceeds on return for {line['original_ti_uid']}: {clawback_result}')
                    else:
                        total_seller_clawed = round(total_seller_clawed + _to_float(clawback_result.get('clawed')), 4)
                cancel_adjust_result = None
                cancel_hold_result = None
                if cancel_unshipped_qty > 0 and (not (clawback_result and clawback_result.get('finalized_request_hold'))):
                    from wallet_transactions_service import _finalize_pending_clawback_holds, adjust_seller_proceeds_on_cancel_unshipped
                    line_trr_uid = (_trr_by_ti_169 or {}).get(line['original_ti_uid'])
                    if return_shipped_qty <= 0:
                        cancel_hold_result = _finalize_pending_clawback_holds(_db_168, original_ti_uid=line['original_ti_uid'], return_ti_uid=new_ti_uid, trr_uid=line_trr_uid)
                    if not (cancel_hold_result and _to_float(cancel_hold_result.get('clawed')) > 0):
                        cancel_adjust_result = adjust_seller_proceeds_on_cancel_unshipped(_db_168, original_ti_uid=line['original_ti_uid'], return_ti_uid=new_ti_uid, cancel_qty=cancel_unshipped_qty, transaction_uid=_original_tx_uid_172)
                    cancel_clawed = 0.0
                    if cancel_hold_result and cancel_hold_result.get('code') == 200:
                        cancel_clawed = _to_float(cancel_hold_result.get('clawed'))
                    elif cancel_adjust_result and cancel_adjust_result.get('code') == 200:
                        cancel_clawed = _to_float(cancel_adjust_result.get('adjusted'))
                    if cancel_clawed > 0:
                        total_seller_clawed = round(total_seller_clawed + cancel_clawed, 4)
                    if cancel_adjust_result and cancel_adjust_result.get('code') != 200:
                        print(f'Warning: Failed to adjust seller proceeds for cancel on {line['original_ti_uid']}: {cancel_adjust_result}')
                    elif cancel_hold_result and cancel_hold_result.get('code') not in (None, 200):
                        print(f'Warning: Failed to finalize cancel clawback hold on {line['original_ti_uid']}: {cancel_hold_result}')
                response_lines.append({'original_transaction_item_uid': line['original_ti_uid'], 'new_transaction_item_uid': new_ti_uid, 'return_quantity': rq, 'return_shipped_qty': line.get('return_shipped_qty', 0), 'cancel_unshipped_qty': line.get('cancel_unshipped_qty', 0), 'line_subtotal': line['line_subtotal'], 'line_tax': line['line_tax'], 'line_shipping': line.get('line_shipping', 0), 'seller_proceeds_clawback': clawback_result.get('clawed', 0) if clawback_result else 0})
            if not _r__create_return_ledger_166__returned:
                wallet_credit_result = None
                if wallet_refund > 0:
                    seller_profile_id = resolve_seller_wallet_profile_id(_db_168, _orig_tx_170.get('transaction_business_id'))
                    wallet_credit_result = transfer_wallet_refund_to_buyer(_db_168, buyer_profile_id=_orig_tx_170.get('transaction_profile_id'), seller_profile_id=seller_profile_id, amount=wallet_refund, seller_clawed_amount=total_seller_clawed)
                    print('wallet refund transfer on return: ', {'wallet_refund': wallet_refund, 'seller_clawed': total_seller_clawed, 'seller_profile_id': seller_profile_id, 'result': wallet_credit_result})
                    if wallet_credit_result.get('code') != 200:
                        _r__create_return_ledger_166 = (False, {'message': wallet_credit_result.get('message', 'Failed to transfer wallet refund to buyer'), 'code': wallet_credit_result.get('code', 500), 'wallet_transfer': wallet_credit_result}, None)
                _r__create_return_ledger_166 = (True, None, {'return_transaction_uid': new_transaction_uid, 'original_transaction_uid': _original_tx_uid_172, 'trr_transaction_uid': _original_tx_uid_172, 'estimated_refund': _estimated_refund_api_payload(_refund_meta_171), 'estimated_total': round(refund_grand, 4), 'wallet_refund': wallet_refund, 'wallet_credit': wallet_credit_result, 'seller_proceeds_clawed': total_seller_clawed, 'ledger_amounts_negative': {'transaction_total': new_transaction['transaction_total'], 'transaction_amount': new_transaction['transaction_amount'], 'transaction_taxes': new_transaction['transaction_taxes'], 'transaction_fees': new_transaction['transaction_fees'], 'transaction_shipping': new_transaction.get('transaction_shipping'), 'transaction_wallet_amount': new_transaction.get('transaction_wallet_amount')}, 'transaction_items_created': item_insert_count, 'bounty_reversal_rows_created': bounty_insert_count, 'lines': response_lines})
    ledger_ok, ledger_err, ledger_result = _r__create_return_ledger_166
    if not ledger_ok:
        return ledger_err, ledger_err.get("code", 500)

    stripe_refund_amount = round(
        _to_float(refund_meta.get("stripe_refund", refund_meta["refund_grand"])),
        4,
    )
    wallet_refund_amount = round(_to_float(refund_meta.get("wallet_refund")), 4)

    # Wallet-only (or wallet covers this return): never refund to the card.
    if stripe_refund_amount < 0.01:
        stripe_result = {
            "ok": True,
            "skipped": True,
            "refund_id": None,
            "message": "No card portion to refund (wallet covered refund)",
        }
    elif isinstance(stripe_refund_from_client, dict) and (
        "ok" in stripe_refund_from_client
        or stripe_refund_from_client.get("refund_id")
        or stripe_refund_from_client.get("skipped")
    ):
        stripe_result = {
            "ok": bool(
                stripe_refund_from_client.get("ok")
                or stripe_refund_from_client.get("skipped")
            ),
            "skipped": bool(stripe_refund_from_client.get("skipped")),
            "refund_id": stripe_refund_from_client.get("refund_id"),
            "message": stripe_refund_from_client.get("message"),
        }
    else:
        stripe_meta = {
            "order_uid": original_tx_uid,
            "trr_uid": primary_trr,
            "return_transaction_uid": ledger_result["return_transaction_uid"],
        }
        if is_batch:
            stripe_meta["trr_uids"] = ",".join(batch_uids)
        if is_cancel:
            stripe_meta["cancel_unshipped"] = "1"
        payment_intent_id = orig_tx.get('transaction_stripe_pi')
        amount_dollars = stripe_refund_amount
        metadata = stripe_meta
        payment_intent_id = _normalize_stripe_payment_intent_id(payment_intent_id)
        if not payment_intent_id:
            stripe_result = {'ok': False, 'skipped': True, 'message': 'No Stripe payment intent on sale'}
        else:
            mode = (os.getenv('STRIPE_MODE') or os.getenv('stripe_mode') or os.getenv('RDS_DB') or 'dev').lower()
            if mode in ('prod', 'production', 'live'):
                secret = os.getenv('stripe_secret_live_key')
            else:
                secret = os.getenv('stripe_secret_test_key') or os.getenv('stripe_secret_live_key')
            if not secret:
                stripe_result = {'ok': False, 'skipped': True, 'message': 'Stripe secret key not configured'}
            else:
                amount_cents = int(round(abs(_to_float(amount_dollars)) * 100))
                if amount_cents < 1:
                    stripe_result = {'ok': False, 'skipped': True, 'message': 'Refund amount too small'}
                else:
                    data = {'payment_intent': payment_intent_id, 'amount': str(amount_cents)}
                    if metadata:
                        for i, (k, v) in enumerate(metadata.items()):
                            if v is None:
                                continue
                            data[f'metadata[{k}]'] = str(v)
                    try:
                        resp = http_requests.post('https://api.stripe.com/v1/refunds', data=data, auth=(secret, ''), timeout=30)
                    except Exception as e:
                        stripe_result = {'ok': False, 'skipped': False, 'message': f'Stripe request failed: {e}'}
                    try:
                        body = resp.json()
                    except Exception:
                        body = {'raw': resp.text}
                    if resp.status_code >= 400:
                        stripe_result = {'ok': False, 'skipped': False, 'message': body.get('error', {}).get('message') if isinstance(body.get('error'), dict) else body.get('error') or f'Stripe HTTP {resp.status_code}', 'stripe_status': resp.status_code, 'stripe_response': body}
                    else:
                        stripe_result = {'ok': True, 'skipped': False, 'refund_id': body.get('id'), 'stripe_status': resp.status_code, 'stripe_response': body}

    if wallet_refund_amount >= 0.01:
        wallet_refund_ok = (
            isinstance(ledger_result.get("wallet_credit"), dict)
            and ledger_result["wallet_credit"].get("code") == 200
            and _to_float(ledger_result["wallet_credit"].get("credited")) >= 0.01
        )
    else:
        wallet_refund_ok = True

    refund_money_ok = bool(stripe_result.get("ok")) and wallet_refund_ok
    final_refund_status = (
        REFUND_STATUS_REFUNDED if refund_money_ok else REFUND_STATUS_REJECTED
    )

    from wallet_return_reservations import clear_return_reservations

    clear_result = clear_return_reservations(
        db, batch_uids, finalize=refund_money_ok
    )
    if clear_result.get("code") != 200:
        print(
            f"Warning: failed to clear return reservations for {batch_uids}: "
            f"{clear_result}"
        )

    _update_return_statuses(
        db,
        original_tx_uid,
        logistics_status,
        final_refund_status,
        trr_uids=batch_uids,
        return_requested=0 if final_refund_status == REFUND_STATUS_REFUNDED else 1,
        return_note=return_note,
        seller_note=seller_note,
        return_transaction_uid=ledger_result["return_transaction_uid"],
        stripe_refund_id=stripe_result.get("refund_id"),
    )

    if is_cancel:
        ok_msg = "Unshipped items cancelled and refund issued"
        fail_msg = "Unshipped items cancelled; refund not completed (Rejected)"
    else:
        ok_msg = "Item received and refund issued"
        fail_msg = "Item received; refund not completed (Rejected)"

    _seller_note_24 = seller_note
    n = (_seller_note_24 or '').strip().upper()
    if n in ('ECTEST', 'PMTEST', 'EC', 'PM'):
        _r__refund_business_code_from_note_23 = n
    else:
        _r__refund_business_code_from_note_23 = 'EC'
    response = {
        "message": ok_msg if refund_money_ok else fail_msg,
        "code": 200,
        "trr_uid": primary_trr,
        "trr_uids": batch_uids,
        "cancel_unshipped": is_cancel,
        "pre_ship_cancel": is_cancel,
        "is_cancel_before_ship": is_cancel,
        **_status_payload(logistics_status, final_refund_status),
        "stripe_refund": {
            "ok": bool(stripe_result.get("ok")),
            "skipped": bool(stripe_result.get("skipped")),
            "refund_id": stripe_result.get("refund_id"),
            "message": stripe_result.get("message"),
            "amount": stripe_refund_amount,
        },
        "wallet_refund": {
            "ok": wallet_refund_ok,
            "amount": wallet_refund_amount,
        },
        "seller_note": seller_note,
        "refund_business_code_hint": _r__refund_business_code_from_note_23,
    }
    response.update(ledger_result)
    return response, 200



class ReturnTransaction(Resource):
    """
    POST: buyer requests a return → creates one trr_uid row per item.
    Physical returns start as Returning - Pending.
    Unshipped / cancel_unshipped requests start as Cancelling - Pending (API chips).

    Does NOT write the return ledger or refund via Stripe. Seller confirms via
    ConfirmReturnTransaction (trr_uid or trr_uids) → Returned/Cancelled - *.

    Multiple concurrent open requests on the same sale are allowed as long as
    remaining returnable qty (ledger + other open reservations) is sufficient.
    """

    def post(self):
        print("In ReturnTransaction POST")
        response = {}

        try:
            payload = request.get_json()
            if not payload:
                response["message"] = "Request body is required"
                response["code"] = 400
                return response, 400

            profile_id = payload.get("profile_id")
            original_tx_uid = payload.get("transaction_uid")
            items_payload = payload.get("transaction_return_items") or []
            return_note = payload.get("transaction_return_note")
            cancel_flag = bool(
                payload.get("cancel_unshipped") or payload.get("pre_ship_cancel")
            )

            if not profile_id:
                response["message"] = "profile_id is required"
                response["code"] = 400
                return response, 400
            if not original_tx_uid:
                response["message"] = "transaction_uid is required"
                response["code"] = 400
                return response, 400
            if cancel_flag and not _items_all_cancel_only(
                items_payload, order_cancel=True
            ):
                response["message"] = (
                    "cancel_unshipped / pre_ship_cancel requires every line to "
                    "have return_shipped_qty = 0 and cancel_unshipped_qty > 0"
                )
                response["code"] = 400
                return response, 400

            with connect() as db:
                orig_tx = _load_sale_for_return(db, original_tx_uid)
                if not orig_tx:
                    response["message"] = "Original transaction not found"
                    response["code"] = 404
                    return response, 404

                if (orig_tx.get("transaction_type") or "sale") != "sale":
                    response["message"] = "Returns can only be requested for sale transactions"
                    response["code"] = 400
                    return response, 400

                if orig_tx.get("transaction_profile_id") != profile_id:
                    response["message"] = (
                        "profile_id does not match the buyer on this transaction"
                    )
                    response["code"] = 403
                    return response, 403

                pricing_items = []
                for entry in items_payload:
                    pricing_entry = dict(entry)
                    pricing_entry["_order_cancel"] = cancel_flag
                    pricing_items.append(pricing_entry)

                ok, err, ctx = _validate_and_price_return_items(
                    db, original_tx_uid, pricing_items
                )
                if not ok:
                    return err, err.get("code", 400)

                all_unshipped = _items_all_unshipped(
                    db, original_tx_uid, items_payload
                )
                is_cancel = (
                    cancel_flag
                    or _items_all_cancel_only(items_payload)
                    or all_unshipped
                )
                return_status = (
                    RETURN_STATUS_CANCELLED
                    if is_cancel
                    else RETURN_STATUS_RETURNING
                )

                refund_meta = _refund_breakdown_from_context(orig_tx, ctx)
                _db_139 = db
                transaction_uid = original_tx_uid
                _profile_id_137 = profile_id
                _items_payload_144 = items_payload
                note = return_note
                _return_status_140 = return_status
                refund_status = REFUND_STATUS_PENDING
                _orig_tx_141 = orig_tx
                _ctx_138 = ctx
                cancel_unshipped = is_cancel
                lines_by_ti = {line.get('original_ti_uid'): line for line in _ctx_138.get('lines_processed') or []}
                batch_created_at = utc_now_str()
                _trr_uids_136 = []
                _r__insert_return_requests_for_items_135__returned = False
                for _entry_143 in _items_payload_144:
                    ti_uid = _entry_143.get('transaction_item_uid')
                    line = lines_by_ti.get(ti_uid) or {'line_subtotal': 0.0, 'line_tax': 0.0}
                    stored_entry = {'transaction_item_uid': ti_uid, 'return_quantity': line.get('return_quantity') if line.get('return_quantity') is not None else _entry_143.get('return_quantity')}
                    if line.get('return_shipped_qty') is not None:
                        stored_entry['return_shipped_qty'] = line.get('return_shipped_qty')
                    elif _entry_143.get('return_shipped_qty') is not None:
                        stored_entry['return_shipped_qty'] = _entry_143.get('return_shipped_qty')
                    if line.get('cancel_unshipped_qty') is not None:
                        stored_entry['cancel_unshipped_qty'] = line.get('cancel_unshipped_qty')
                    elif _entry_143.get('cancel_unshipped_qty') is not None:
                        stored_entry['cancel_unshipped_qty'] = _entry_143.get('cancel_unshipped_qty')
                    estimated_total = round(_line_estimated_total(_orig_tx_141, _ctx_138, line), 2)
                    _db_131 = _db_139
                    _transaction_uid_126 = transaction_uid
                    _profile_id_130 = _profile_id_137
                    item_entry = stored_entry
                    _note_128 = note
                    _return_status_132 = _return_status_140
                    _refund_status_127 = refund_status
                    _estimated_total_133 = estimated_total
                    seller_note = None
                    created_at = batch_created_at
                    _cancel_unshipped_129 = cancel_unshipped
                    _db_8 = _db_131
                    uid_resp = _db_8.call(procedure='every_circle.transaction_return_requests_uid')
                    if not uid_resp.get('result') or len(uid_resp['result']) == 0:
                        _trr_uid_134 = None
                    else:
                        _trr_uid_134 = uid_resp['result'][0].get('new_id')
                    if not _trr_uid_134:
                        _r__insert_return_request_124 = ({'code': 500, 'message': 'Failed to generate return request UID'}, None)
                    else:
                        _ti_uid_125 = item_entry.get('transaction_item_uid')
                        try:
                            qty = int(item_entry.get('return_quantity') or 0)
                        except (TypeError, ValueError):
                            qty = 0
                        item_payload = [{'transaction_item_uid': _ti_uid_125, 'return_quantity': qty}]
                        if item_entry.get('return_shipped_qty') is not None:
                            item_payload[0]['return_shipped_qty'] = int(item_entry.get('return_shipped_qty') or 0)
                        if item_entry.get('cancel_unshipped_qty') is not None:
                            item_payload[0]['cancel_unshipped_qty'] = int(item_entry.get('cancel_unshipped_qty') or 0)
                        now = created_at or utc_now_str()
                        fields = {'trr_uid': _trr_uid_134, 'trr_transaction_uid': _transaction_uid_126, 'trr_profile_id': _profile_id_130, 'trr_ti_uid': _ti_uid_125, 'trr_return_quantity': qty, 'trr_items_json': json.dumps(item_payload), 'trr_note': _note_128, 'trr_seller_note': seller_note, 'trr_status': _refund_status_127, 'trr_return_status': _return_status_132, 'trr_refund_status': _refund_status_127, 'trr_cancel_unshipped': 1 if _cancel_unshipped_129 else 0, 'trr_estimated_total': _estimated_total_133, 'trr_return_transaction_uid': None, 'trr_stripe_refund_id': None, 'trr_created_at': now, 'trr_updated_at': now}
                        result = _db_131.insert('every_circle.transaction_return_requests', fields)
                        _r__insert_return_request_124 = (result, _trr_uid_134)
                    _insert_result_142, trr_uid = _r__insert_return_request_124
                    if not trr_uid or _insert_result_142.get('code') != 200:
                        _r__insert_return_requests_for_items_135 = (_insert_result_142 if _insert_result_142 else {'code': 500, 'message': 'Failed to save return request'}, _trr_uids_136)
                        _r__insert_return_requests_for_items_135__returned = True
                        break
                    _trr_uids_136.append(trr_uid)
                if not _r__insert_return_requests_for_items_135__returned:
                    _r__insert_return_requests_for_items_135 = ({'code': 200, 'message': 'ok'}, _trr_uids_136)
                insert_result, trr_uids = _r__insert_return_requests_for_items_135
                if not trr_uids or insert_result.get("code") != 200:
                    response["message"] = insert_result.get(
                        "message", "Failed to save return request"
                    )
                    response["code"] = insert_result.get("code", 500)
                    return response, response["code"]

                # Sale-level flags; each item row already has Cancelled/Returning + Pending.
                _update_return_statuses(
                    db,
                    original_tx_uid,
                    return_status,
                    REFUND_STATUS_PENDING,
                    trr_uid=trr_uids[0],
                    return_requested=1,
                    return_note=return_note,
                )

                reservation_result = None
                from wallet_return_reservations import (
                    create_reservations_for_return_batch,
                )

                reservation_result = create_reservations_for_return_batch(
                    db,
                    orig_tx=orig_tx,
                    trr_uids=trr_uids,
                    ctx=ctx,
                    refund_meta=refund_meta,
                )
                if reservation_result.get("code") != 200:
                    response["message"] = reservation_result.get(
                        "message", "Failed to create wallet reservations"
                    )
                    response["code"] = reservation_result.get("code", 500)
                    return response, response["code"]

                if is_cancel:
                    response["message"] = (
                        "Unshipped items cancelled successfully (Cancelled - Pending)"
                    )
                    response["next_step"] = (
                        "Seller confirms cancel/refund via "
                        "PUT /api/v1/transactions/return/confirm with trr_uid "
                        "or trr_uids (no physical receipt required)"
                    )
                else:
                    response["message"] = (
                        "Return requested successfully (Returning - Pending)"
                    )
                    response["next_step"] = (
                        "Seller confirms item receipt via "
                        "PUT /api/v1/transactions/return/confirm with trr_uid "
                        "or trr_uids (one confirmation per return wave)"
                    )
                response["code"] = 200
                response["trr_uids"] = trr_uids
                response["trr_uid"] = trr_uids[0]
                response["original_transaction_uid"] = original_tx_uid
                response["transaction_uid"] = original_tx_uid
                response["transaction_return_requested"] = 1
                response["cancel_unshipped"] = is_cancel
                response["pre_ship_cancel"] = is_cancel
                response["is_cancel_before_ship"] = is_cancel
                response.update(_status_payload(return_status, REFUND_STATUS_PENDING))
                estimated_refund = _estimated_refund_api_payload(refund_meta)
                response["estimated_refund"] = estimated_refund
                response["estimated_total"] = estimated_refund["total"]
                if reservation_result:
                    response["wallet_reservations"] = reservation_result.get(
                        "reservations"
                    )
                response["transaction_return_items"] = items_payload
                return response, 200

        except Exception as e:
            print(f"Error in ReturnTransaction POST: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500


class ConfirmReturnTransaction(Resource):
    """
    PUT: seller confirms returned goods received → Returned - Pending, then
         issues ledger + Stripe → Returned - Refunded|Rejected
         or pre-ship cancel → Cancelled - Pending → Cancelled - Refunded|Rejected
         or rejects the return request → Returning/Cancelled - Rejected.

    Each return-request row is one sale line item. Confirm/decline may act on
    a single trr_uid or a same-wave trr_uids batch (one ledger + one Stripe).

    Required:
      - transaction_uid (original sale)
      - seller_id (must match transaction_business_id)

    Recommended:
      - trr_uid or trr_uids (required when multiple open returns exist)

    Optional:
      - action: "confirm" (default) | "decline" | "set_refund_status"
      - transaction_return_seller_note
      - refund_status (for set_refund_status): refunded | stripe_fail | rejected
      - stripe_refund / stripe_refund_id (optional; FE createRefund result)
    """

    def put(self):
        print("In ConfirmReturnTransaction PUT")
        response = {}

        try:
            payload = request.get_json()
            if not payload:
                response["message"] = "Request body is required"
                response["code"] = 400
                return response, 400

            transaction_uid = payload.get("transaction_uid")
            trr_uids = _parse_trr_uids_from_payload(payload)
            trr_uid = trr_uids[0] if trr_uids else None
            seller_id = (
                payload.get("seller_id")
                or payload.get("business_uid")
                or payload.get("transaction_business_id")
            )
            action = (payload.get("action") or "confirm").lower()
            seller_note = payload.get("transaction_return_seller_note")

            if not transaction_uid:
                response["message"] = "transaction_uid is required"
                response["code"] = 400
                return response, 400
            if not seller_id:
                response["message"] = "seller_id is required"
                response["code"] = 400
                return response, 400

            with connect() as db:
                orig_tx = _load_sale_for_return(db, transaction_uid)
                if not orig_tx:
                    response["message"] = "Original transaction not found"
                    response["code"] = 404
                    return response, 404

                if str(orig_tx.get("transaction_business_id")) != str(seller_id):
                    response["message"] = (
                        "seller_id does not match the seller on this transaction"
                    )
                    response["code"] = 403
                    return response, 403

                if len(trr_uids) > 1:
                    requests, resolve_err = _load_return_request_wave(
                        db, transaction_uid, trr_uids
                    )
                    if resolve_err:
                        return resolve_err, resolve_err.get("code", 400)
                else:
                    pending, resolve_err = _resolve_return_request(
                        db, transaction_uid, trr_uid
                    )
                    if resolve_err:
                        return resolve_err, resolve_err.get("code", 400)
                    requests = [pending]

                batch_uids = [r.get("trr_uid") for r in requests]
                trr_uid = batch_uids[0] if batch_uids else None
                is_cancel = all(_is_cancel_unshipped_request(r) for r in requests)

                if action in ("decline", "reject"):
                    for req in requests:
                        cur_return, cur_refund = _pair_for_sale(orig_tx, req)
                        decline_ok = (
                            cur_return
                            in (RETURN_STATUS_RETURNING, RETURN_STATUS_CANCELLED)
                            and cur_refund == REFUND_STATUS_PENDING
                        )
                        if not decline_ok:
                            response["message"] = (
                                "Only Returning/Cancelled - Pending returns can be "
                                "rejected "
                                f"(status={_display_return_status(cur_return, cur_refund)})"
                            )
                            response["code"] = 409
                            response["trr_uid"] = req.get("trr_uid")
                            response["trr_uids"] = batch_uids
                            response.update(
                                _status_payload(cur_return, cur_refund)
                            )
                            return response, 409

                    decline_return = (
                        RETURN_STATUS_CANCELLED
                        if is_cancel
                        else RETURN_STATUS_RETURNING
                    )
                    _update_return_statuses(
                        db,
                        transaction_uid,
                        decline_return,
                        REFUND_STATUS_REJECTED,
                        trr_uids=batch_uids,
                        return_requested=1,
                        seller_note=seller_note,
                    )

                    from wallet_return_reservations import (
                        clear_return_reservations,
                        release_pending_after_reservation_clear,
                    )

                    clear_result = clear_return_reservations(db, batch_uids)
                    if clear_result.get("code") != 200:
                        response["message"] = clear_result.get(
                            "message", "Failed to clear wallet reservations"
                        )
                        response["code"] = clear_result.get("code", 500)
                        return response, response["code"]

                    if not _sale_has_other_open_returns(
                        db, transaction_uid, exclude_trr_uid=batch_uids
                    ):
                        ti_uids = set()
                        for req in requests:
                            if req.get("trr_ti_uid"):
                                ti_uids.add(req.get("trr_ti_uid"))
                            for entry in req.get("items") or []:
                                if entry.get("transaction_item_uid"):
                                    ti_uids.add(entry.get("transaction_item_uid"))
                        for ti_uid in ti_uids:
                            release_pending_after_reservation_clear(
                                db, transaction_uid, ti_uid
                            )

                    response["message"] = (
                        "Cancel rejected (Cancelled - Rejected)"
                        if is_cancel
                        else "Return rejected (Returning - Rejected)"
                    )
                    response["code"] = 200
                    response["transaction_uid"] = transaction_uid
                    response["trr_uid"] = trr_uid
                    response["trr_uids"] = batch_uids
                    response["cancel_unshipped"] = is_cancel
                    response.update(
                        _status_payload(decline_return, REFUND_STATUS_REJECTED)
                    )
                    return response, 200

                if action in ("set_refund_status", "set_status"):
                    requested = (
                        payload.get("refund_status")
                        or payload.get("transaction_refund_status")
                        or ""
                    ).strip().lower()
                    for req in requests:
                        cur_return, cur_refund = _pair_for_sale(orig_tx, req)
                        if cur_return not in (
                            RETURN_STATUS_RETURNED,
                            RETURN_STATUS_CANCELLED,
                        ):
                            response["message"] = (
                                "set_refund_status requires return already confirmed "
                                f"(status={_display_return_status(cur_return, cur_refund)})"
                            )
                            response["code"] = 409
                            response["trr_uid"] = req.get("trr_uid")
                            response["trr_uids"] = batch_uids
                            response.update(
                                _status_payload(cur_return, cur_refund)
                            )
                            return response, 409

                    logistics = (
                        RETURN_STATUS_CANCELLED
                        if is_cancel
                        else RETURN_STATUS_RETURNED
                    )

                    if requested in ("refunded",):
                        stripe_refund_id = (
                            payload.get("stripe_refund_id")
                            or (
                                payload.get("stripe_refund")
                                if isinstance(payload.get("stripe_refund"), dict)
                                else {}
                            ).get("refund_id")
                        )
                        _update_return_statuses(
                            db,
                            transaction_uid,
                            logistics,
                            REFUND_STATUS_REFUNDED,
                            trr_uids=batch_uids,
                            return_requested=0,
                            seller_note=seller_note,
                            stripe_refund_id=stripe_refund_id,
                        )
                        response["message"] = "Refund status updated to refunded"
                        response["code"] = 200
                        response["transaction_uid"] = transaction_uid
                        response["trr_uid"] = trr_uid
                        response["trr_uids"] = batch_uids
                        response["cancel_unshipped"] = is_cancel
                        response.update(
                            _status_payload(logistics, REFUND_STATUS_REFUNDED)
                        )
                        if stripe_refund_id:
                            response["stripe_refund"] = {
                                "ok": True,
                                "skipped": False,
                                "refund_id": stripe_refund_id,
                            }
                        return response, 200

                    if requested in (
                        "stripe_fail",
                        "stripe_failed",
                        "cc_issue",
                        "rejected",
                    ):
                        _update_return_statuses(
                            db,
                            transaction_uid,
                            logistics,
                            REFUND_STATUS_REJECTED,
                            trr_uids=batch_uids,
                            return_requested=1,
                            seller_note=seller_note,
                        )
                        response["message"] = "Refund status updated"
                        response["code"] = 200
                        response["transaction_uid"] = transaction_uid
                        response["trr_uid"] = trr_uid
                        response["trr_uids"] = batch_uids
                        response["cancel_unshipped"] = is_cancel
                        response.update(
                            _status_payload(logistics, REFUND_STATUS_REJECTED)
                        )
                        response["refund_status"] = "stripe_fail"
                        response["transaction_refund_status"] = "stripe_fail"
                        response["display_status"] = (
                            "Cancelled - CC Issue"
                            if is_cancel
                            else "Returned - CC Issue"
                        )
                        return response, 200
                    response["message"] = (
                        "refund_status must be refunded, stripe_fail, rejected, or equivalent"
                    )
                    response["code"] = 400
                    return response, 400

                if action != "confirm":
                    response["message"] = (
                        "action must be 'confirm', 'decline', or 'set_refund_status'"
                    )
                    response["code"] = 400
                    return response, 400

                stripe_from_client = payload.get("stripe_refund")
                if not isinstance(stripe_from_client, dict) and payload.get(
                    "stripe_refund_id"
                ):
                    stripe_from_client = {
                        "ok": True,
                        "skipped": False,
                        "refund_id": payload.get("stripe_refund_id"),
                        "message": "Refund id provided by client",
                    }

                return _finalize_pending_return(
                    db,
                    transaction_uid,
                    seller_note=seller_note,
                    stripe_refund_from_client=stripe_from_client,
                    trr_uids=batch_uids,
                )

        except Exception as e:
            print(f"Error in ConfirmReturnTransaction PUT: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500



def _query_buyer_purchase_list_rows(db, profile_id, *, order_uid=None):
    ensure_fulfillment_list_rollups(db)
    order_uid_filter = bool(order_uid)
    fulfillment_summary = fulfillment_list_summary_sql('ti')
    order_filter = ' AND t.transaction_uid = %s' if order_uid_filter else ''
    query = f"\n                    SELECT\n                    t.transaction_uid,\n                    t.transaction_original_uid,\n                    COALESCE(t.transaction_type, 'sale') AS transaction_type,\n                    (COALESCE(t.transaction_type, 'sale') = 'return') AS is_return,\n                    t.transaction_datetime,\n                    t.transaction_total,\n                    t.transaction_amount,\n                    t.transaction_taxes,\n                    t.transaction_fees,\n                    t.transaction_shipping,\n                    t.transaction_profile_id,\n                    t.transaction_in_escrow,\n                    t.transaction_return_requested,\n                    t.transaction_return_note,\n                    t.transaction_business_id AS seller_id,\n                    CASE\n                        WHEN ti.ti_bs_id LIKE '250-%%' THEN biz.business_name\n                        WHEN ti.ti_bs_id LIKE '150-%%' THEN\n                            CONCAT(expertise_pp.profile_personal_first_name, ' ', expertise_pp.profile_personal_last_name)\n                        WHEN ti.ti_bs_id LIKE '165-%%' THEN\n                            CONCAT(wish_pp.profile_personal_first_name, ' ', wish_pp.profile_personal_last_name)\n                        ELSE NULL\n                    END AS business_name,\n                    CASE\n                        WHEN ti.ti_bs_id LIKE '250-%%' THEN 'Business'\n                        WHEN ti.ti_bs_id LIKE '150-%%' THEN 'Offering'\n                        WHEN ti.ti_bs_id LIKE '165-%%' THEN 'Seeking'\n                        ELSE 'Unknown'\n                    END AS purchase_type,\n                    GROUP_CONCAT(\n                        CASE\n                            WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_service_name\n                            WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_title\n                            WHEN ti.ti_bs_id LIKE '165-%%' THEN pw.profile_wish_title\n                            ELSE 'See Receipt'\n                        END\n                        ORDER BY ti.ti_uid\n                        SEPARATOR ', '\n                    ) AS purchased_item,\n                    SUM(ti.ti_bs_qty) AS ti_bs_qty,\n                    MIN(ti.ti_uid) AS ti_uid,\n                    MIN(ti.ti_bs_cost) AS ti_bs_cost,\n                    MIN(pe.profile_expertise_cost) AS profile_expertise_cost,\n                    MIN(pe.profile_expertise_cost_currency) AS profile_expertise_cost_currency,\n                    MAX(ti.ti_fulfillment_method) AS ti_fulfillment_method,\n                    {fulfillment_summary}\n                    FROM every_circle.transactions t\n                    LEFT JOIN every_circle.transactions_items ti\n                    ON t.transaction_uid = ti.ti_transaction_id\n                    LEFT JOIN every_circle.business_services bs\n                    ON ti.ti_bs_id = bs.bs_uid\n                    LEFT JOIN every_circle.business biz\n                    ON bs.bs_business_id = biz.business_uid\n                    LEFT JOIN every_circle.profile_personal seller_pp\n                    ON t.transaction_business_id = seller_pp.profile_personal_user_id\n                    LEFT JOIN every_circle.profile_expertise pe\n                    ON ti.ti_bs_id = pe.profile_expertise_uid\n                    LEFT JOIN every_circle.profile_personal expertise_pp\n                    ON pe.profile_expertise_profile_personal_id = expertise_pp.profile_personal_uid\n                    LEFT JOIN every_circle.wish_response wr\n                    ON ti.ti_bs_id = wr.wish_response_uid\n                    LEFT JOIN every_circle.profile_wish pw\n                    ON wr.wr_profile_wish_id = pw.profile_wish_uid\n                    LEFT JOIN every_circle.profile_personal wish_pp\n                    ON pw.profile_wish_profile_personal_id = wish_pp.profile_personal_uid\n                    WHERE t.transaction_profile_id = %s{order_filter}\n                    GROUP BY\n                    t.transaction_uid,\n                    t.transaction_datetime,\n                    t.transaction_total,\n                    t.transaction_profile_id,\n                    seller_id,\n                    business_name,\n                    purchase_type\n                    ORDER BY t.transaction_datetime DESC, ti_uid ASC\n               "
    params = [profile_id]
    if order_uid:
        params.append(order_uid)
    result = db.execute(query, tuple(params))
    if result.get("code") != 200:
        return []
    return result.get("result") or []


def _finalize_buyer_purchase_list_rows(db, rows):
    """Enrich buyer purchase list rows with shipping, fulfillment, and return linkage."""
    from order_quantity_context import apply_list_verification_status, clear_ledger_quantity_caches
    from line_commerce_fields import format_offering_rate_display

    clear_ledger_quantity_caches()
    if not rows:
        return []
    rows = _enrich_transaction_rows(rows)
    for row in rows:
        if not isinstance(row, dict):
            continue
        cost = row.get("profile_expertise_cost") or row.get("ti_bs_cost")
        if cost is not None and str(cost).strip():
            row["offering_rate_display"] = format_offering_rate_display(cost)
    rows = attach_shipping_to_transaction_rows(db, rows)
    rows = apply_order_fulfillment_summary(rows)
    rows = sync_list_rows_fulfillment_from_context(db, rows)
    rows = apply_list_verification_status(db, rows)
    return _enrich_list_transaction_rows(db, rows)


def fetch_buyer_purchase_list_row(db, profile_id, order_uid):
    """One buyer purchase list row after post-write enrichment (account-screen v2 shape)."""
    if not profile_id or not order_uid:
        return None
    rows = _query_buyer_purchase_list_rows(db, profile_id, order_uid=order_uid)
    rows = _finalize_buyer_purchase_list_rows(db, rows)
    return rows[0] if rows else None


class Transactions(Resource):

    def get(self, profile_id=None):
        print(f"In Transactions GET with profile_id: {profile_id}")
        response = {}

        try:
            if not profile_id:
                response["message"] = "profile_id is required"
                response["code"] = 400
                return response, 400

            with connect() as db:
                rows = _finalize_buyer_purchase_list_rows(
                    db, _query_buyer_purchase_list_rows(db, profile_id)
                )
                from account_screen_purchases_v2 import build_purchases_v2_rows

                v2_rows = build_purchases_v2_rows(db, rows)
                response["message"] = "Purchase Transactions retrieved successfully"
                response["code"] = 200
                response["schema_version"] = 2
                response["data"] = v2_rows
                response["rows"] = v2_rows
                response["count"] = len(v2_rows)
                if _request_timezone():
                    response["timezone"] = _request_timezone()
                response["datetime_storage"] = "UTC"
                return response, 200

        except Exception as e:
            print(f"Error in Transactions GET: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500

    def post(self):
        print("In Transactions POST New")
        response = {}

        try:
            # Get JSON payload from request
            payload = request.get_json()
            print(payload)

            # Enter Data in Transactions Table
            # Validate required fields
            required_fields = [
                "profile_id",
                "total_amount_paid",
                "total_costs",
                "items",
            ]
            missing_fields = [
                field for field in required_fields if not payload.get(field)
                and payload.get(field) != 0
            ]

            if missing_fields:
                response["message"] = (
                    f"Missing required fields: {', '.join(missing_fields)}"
                )
                response["code"] = 400
                return response, 400
            print("No Missing Fields")

            raw = payload.get('wallet_amount') if payload.get('wallet_amount') is not None else payload.get('wallet_amount_applied')
            if raw is None or raw == '':
                wallet_amount = 0.0
            else:
                amount = round(_to_float(raw), 4)
                if amount < 0:
                    wallet_amount = None
                else:
                    wallet_amount = amount
            if wallet_amount is None:
                response["message"] = "wallet_amount must be a non-negative number"
                response["code"] = 400
                return response, 400

            total_amount_paid = round(_to_float(payload.get("total_amount_paid")), 4)
            if total_amount_paid < 0:
                response["message"] = "total_amount_paid must be non-negative"
                response["code"] = 400
                return response, 400

            if wallet_amount - total_amount_paid > 1e-9:
                response["message"] = (
                    "wallet_amount cannot exceed total_amount_paid"
                )
                response["code"] = 400
                return response, 400

            card_amount = round(total_amount_paid - wallet_amount, 4)
            stripe_pi = _normalize_stripe_payment_intent_id(
                payload.get("stripe_payment_intent")
            )
            if card_amount >= 0.01 and not stripe_pi:
                response["message"] = (
                    "stripe_payment_intent is required when card charge is greater than zero"
                )
                response["code"] = 400
                return response, 400

            shipping_fields, shipping_error = normalize_shipping_address(
                payload.get("shipping_address")
            )
            if shipping_error:
                response["message"] = shipping_error
                response["code"] = 400
                return response, 400

            # Extract required fields from payload
            transaction = {
                "transaction_profile_id": payload.get("profile_id"),
                "transaction_business_id": payload.get("business_id"),
                # Always store pi_… (never a client secret) so refunds can use this field
                "transaction_stripe_pi": stripe_pi,
                "transaction_total": payload.get("total_amount_paid"),
                "transaction_amount": payload.get("total_costs"),
                "transaction_taxes": payload.get("total_taxes"),
                "transaction_fees": payload.get("total_fees"),
                "transaction_wallet_amount": wallet_amount,
                "transaction_in_escrow": (
                    1 if payload.get("transaction_in_escrow") else 0
                ),
                "transaction_type": "sale",
            }

            with connect() as db:
                if stripe_pi:
                    _db_59 = db
                    _stripe_pi_58 = stripe_pi
                    if not _stripe_pi_58:
                        existing_sale = None
                    else:
                        rows = _db_59.execute("\n        SELECT transaction_uid, transaction_profile_id, transaction_business_id\n        FROM every_circle.transactions\n        WHERE transaction_stripe_pi = %s\n          AND COALESCE(transaction_type, 'sale') = 'sale'\n        LIMIT 1\n        ", (_stripe_pi_58,))
                        result = (rows or {}).get('result') or []
                        existing_sale = result[0] if result else None
                    if existing_sale:
                        response["message"] = "Transaction already recorded"
                        response["code"] = 200
                        response["transaction_uid"] = existing_sale.get(
                            "transaction_uid"
                        )
                        response["idempotent"] = True
                        return response, 200

                _db_191 = db
                items = payload.get('items', [])
                _payload_176 = payload
                _shipping_fields_175 = shipping_fields
                if not isinstance(items, list) or len(items) == 0:
                    _r__plan_checkout_174 = (False, {'message': 'items must be a non-empty list', 'code': 400}, None)
                else:
                    lines = []
                    _order_shipping_192 = 0.0
                    order_merchandise = 0.0
                    order_tax = 0.0
                    shipping_actual_pending = 0
                    any_ship = False
                    _r__plan_checkout_174__returned = False
                    for idx, _item_183 in enumerate(items):
                        if not isinstance(_item_183, dict):
                            continue
                        if not _item_183.get('bs_uid') and (not _item_183.get('expertise_uid')) and (not _item_183.get('wish_response_uid')):
                            continue
                        _db_152 = _db_191
                        _item_151 = _item_183
                        _ti_bs_id_148 = _item_151.get('bs_uid') or _item_151.get('expertise_uid') or _item_151.get('wish_response_uid')
                        if not _ti_bs_id_148:
                            _r__fetch_listing_for_checkout_item_145 = (None, None, None, False, {'message': 'Each item requires bs_uid, expertise_uid, or wish_response_uid', 'code': 400})
                        else:
                            is_wish = False
                            _listing_mode_147 = None
                            if str(_ti_bs_id_148).startswith('250'):
                                _bs_response_180 = _db_152.execute('SELECT * FROM every_circle.business_services WHERE bs_uid = %s', (_ti_bs_id_148,))
                                if not _bs_response_180.get('result'):
                                    _r__fetch_listing_for_checkout_item_145 = (None, _ti_bs_id_148, None, False, {'message': f'Business service not found: {_ti_bs_id_148}', 'code': 404})
                                else:
                                    _bs_data_153 = _bs_response_180['result'][0]
                                    _avail_err_184 = _validate_business_service_available(_db_152, _bs_data_153)
                                    if _avail_err_184:
                                        _r__fetch_listing_for_checkout_item_145 = (None, _ti_bs_id_148, None, False, _avail_err_184)
                                    else:
                                        _product_data_149 = _bs_data_153
                                        if not _product_data_149:
                                            _listing_mode_147 = None
                                        else:
                                            _raw_146 = _product_data_149.get('bs_mode')
                                            if _raw_146 is None or not str(_raw_146).strip():
                                                _listing_mode_147 = None
                                            else:
                                                mode_str = str(_raw_146).strip()
                                                _flags_150 = _parse_listing_mode_flags(mode_str)
                                                if not any(_flags_150.values()):
                                                    _listing_mode_147 = None
                                                else:
                                                    _listing_mode_147 = _normalize_listing_mode(mode_str)
                                        if _listing_mode_147 is None:
                                            _r__fetch_listing_for_checkout_item_145 = (None, _ti_bs_id_148, None, False, {'message': f'Business service {_ti_bs_id_148} has missing or invalid bs_mode (requires Virtual, Delivered, and/or In-Person)', 'code': 400})
                            elif str(_ti_bs_id_148).startswith('150'):
                                _bs_response_180 = _db_152.execute('SELECT * FROM every_circle.profile_expertise WHERE profile_expertise_uid = %s', (_ti_bs_id_148,))
                                if not _bs_response_180.get('result'):
                                    _r__fetch_listing_for_checkout_item_145 = (None, _ti_bs_id_148, None, False, {'message': f'Expertise not found: {_ti_bs_id_148}', 'code': 404})
                                else:
                                    _bs_data_153 = _bs_response_180['result'][0]
                                    if int(_bs_data_153.get('profile_expertise_moderated') or 0) != MODERATED_ACTIVE:
                                        _r__fetch_listing_for_checkout_item_145 = (None, _ti_bs_id_148, None, False, {'message': 'Offering is not available', 'code': 403})
                                    else:
                                        _owner_uid_178 = _bs_data_153.get('profile_expertise_profile_personal_id')
                                        if not is_owner_available_for_public_interaction(_db_152, _owner_uid_178):
                                            _r__fetch_listing_for_checkout_item_145 = (None, _ti_bs_id_148, None, False, {'message': 'Offering is not available', 'code': 403})
                                        else:
                                            _listing_mode_147 = _normalize_listing_mode(_bs_data_153.get('profile_expertise_mode'))
                            elif str(_ti_bs_id_148).startswith('165'):
                                is_wish = True
                                _bs_response_180 = _db_152.execute('\n            SELECT wish_response.wish_response_uid, profile_wish.*\n            FROM every_circle.profile_wish\n            LEFT JOIN every_circle.wish_response ON wr_profile_wish_id = profile_wish_uid\n            WHERE wish_response_uid = %s\n            ', (_item_151.get('wish_response_uid'),))
                                if not _bs_response_180.get('result'):
                                    _r__fetch_listing_for_checkout_item_145 = (None, _ti_bs_id_148, None, False, {'message': f'Wish not found: {_item_151.get('wish_response_uid')}', 'code': 404})
                                else:
                                    _bs_data_153 = _bs_response_180['result'][0]
                                    _listing_mode_147 = _normalize_listing_mode(_bs_data_153.get('profile_wish_mode'))
                            else:
                                _r__fetch_listing_for_checkout_item_145 = (None, _ti_bs_id_148, None, False, {'message': f'Invalid item id: {_ti_bs_id_148}', 'code': 400})
                            _r__fetch_listing_for_checkout_item_145 = (_bs_data_153, _ti_bs_id_148, _listing_mode_147, is_wish, None)
                        _bs_data_179, _ti_bs_id_187, listing_mode, _is_wish, _err_189 = _r__fetch_listing_for_checkout_item_145
                        if _err_189:
                            _r__plan_checkout_174 = (False, _err_189, None)
                            _r__plan_checkout_174__returned = True
                            break
                        _qty_188 = _purchase_qty(_item_183)
                        _item_88 = _item_183
                        _listing_mode_86 = listing_mode
                        _product_data_87 = _bs_data_179
                        _raw_177 = _item_88.get('fulfillment_method')
                        if _raw_177 is not None and str(_raw_177).strip():
                            _method_89 = str(_raw_177).strip().lower()
                            if _method_89 not in VALID_FULFILLMENT_METHODS:
                                _r__resolve_fulfillment_method_85 = (None, "Invalid fulfillment_method; must be 'ship', 'pickup', or 'virtual'")
                            else:
                                _r__resolve_fulfillment_method_85 = (_method_89, None)
                        elif _listing_mode_86 == 'both':
                            _r__resolve_fulfillment_method_85 = (None, 'fulfillment_method is required for dual-mode listings')
                        elif _listing_mode_86 == 'pickup':
                            _r__resolve_fulfillment_method_85 = ('pickup', None)
                        elif _listing_mode_86 == 'ship':
                            _r__resolve_fulfillment_method_85 = ('ship', None)
                        elif _listing_mode_86 == 'virtual':
                            _r__resolve_fulfillment_method_85 = ('virtual', None)
                        elif _has_shipping_config(_product_data_87):
                            _r__resolve_fulfillment_method_85 = ('ship', None)
                        else:
                            _r__resolve_fulfillment_method_85 = ('pickup', None)
                        _method_186, err_msg = _r__resolve_fulfillment_method_85
                        if _method_186 is None:
                            _r__plan_checkout_174 = (False, {'message': err_msg, 'code': 400}, None)
                            _r__plan_checkout_174__returned = True
                            break
                        declared_unit = _item_183.get('unit_price')
                        if declared_unit is not None and declared_unit != '':
                            unit_cost = round(_to_float(declared_unit), 2)
                        else:
                            _bs_data_10 = _bs_data_179
                            if not _bs_data_10:
                                unit_cost = 0.0
                            else:
                                _unit_cost__returned_182 = False
                                for key in ('bs_cost', 'profile_expertise_cost', 'profile_wish_cost'):
                                    if _bs_data_10.get(key) is not None:
                                        unit_cost = round(_to_float(_bs_data_10.get(key)), 2)
                                        _unit_cost__returned_182 = True
                                        break
                                if _r__plan_checkout_174__returned:
                                    break
                                if not _unit_cost__returned_182:
                                    unit_cost = 0.0
                        choices_extra = 0.0
                        if str(_ti_bs_id_187).startswith('250'):
                            choices_extra = _to_float(_item_183.get('choices_extra_cost') or 0)
                        line_merchandise = round(unit_cost * _qty_188 + choices_extra, 2)
                        order_merchandise += line_merchandise
                        line_tax_raw = _item_183.get('line_tax_amount')
                        if line_tax_raw is None or line_tax_raw == '':
                            _r__plan_checkout_174 = (False, {'message': f'line_tax_amount is required for item {_ti_bs_id_187}', 'code': 400}, None)
                            _r__plan_checkout_174__returned = True
                            break
                        line_tax = round(_to_float(line_tax_raw), 2)
                        is_taxable, catalog_rate = _listing_tax_config(_bs_data_179)
                        rate_raw = _item_183.get('ti_tax_rate')
                        tax_rate = _to_float(rate_raw) if rate_raw is not None and rate_raw != '' else _to_float(catalog_rate)
                        expected_tax = round(_tax_amount_for_line(line_merchandise, is_taxable, tax_rate), 2)
                        if not _money_close(line_tax, expected_tax):
                            _r__plan_checkout_174 = (False, {'message': f'line_tax_amount mismatch for item {_ti_bs_id_187} (expected {expected_tax:.2f}, got {line_tax:.2f})', 'code': 400}, None)
                            _r__plan_checkout_174__returned = True
                            break
                        order_tax += line_tax
                        _listing_mode_18 = listing_mode
                        _product_data_16 = _bs_data_179
                        _flags_17 = _mode_flags_from_product(_product_data_16)
                        if _flags_17['delivered'] or _flags_17['virtual']:
                            _r__listing_supports_ship_15 = True
                        elif _listing_mode_18 in ('ship', 'both'):
                            _r__listing_supports_ship_15 = True
                        elif _listing_mode_18 is None:
                            _r__listing_supports_ship_15 = _has_shipping_config(_product_data_16)
                        else:
                            _r__listing_supports_ship_15 = False
                        if _method_186 == 'ship' and (not _r__listing_supports_ship_15):
                            _r__plan_checkout_174 = (False, {'message': f'Ship fulfillment is not allowed for item {_ti_bs_id_187}', 'code': 400}, None)
                            _r__plan_checkout_174__returned = True
                            break
                        _listing_mode_22 = listing_mode
                        _product_data_20 = _bs_data_179
                        _flags_21 = _mode_flags_from_product(_product_data_20)
                        if _flags_21['inPerson']:
                            _r__listing_supports_pickup_19 = True
                        elif _listing_mode_22 in ('pickup', 'both'):
                            _r__listing_supports_pickup_19 = True
                        elif _listing_mode_22 is None:
                            _r__listing_supports_pickup_19 = not _has_shipping_config(_product_data_20)
                        else:
                            _r__listing_supports_pickup_19 = False
                        if _method_186 == 'pickup' and (not _r__listing_supports_pickup_19):
                            _r__plan_checkout_174 = (False, {'message': f'Pickup fulfillment is not allowed for item {_ti_bs_id_187}', 'code': 400}, None)
                            _r__plan_checkout_174__returned = True
                            break
                        _listing_mode_13 = listing_mode
                        _product_data_12 = _bs_data_179
                        flags = _mode_flags_from_product(_product_data_12)
                        if flags['virtual']:
                            _r__listing_supports_virtual_11 = True
                        elif _listing_mode_13 is None:
                            _r__listing_supports_virtual_11 = False
                        else:
                            _r__listing_supports_virtual_11 = _listing_mode_13 in ('virtual', 'both')
                        if _method_186 == 'virtual' and (not _r__listing_supports_virtual_11):
                            _r__plan_checkout_174 = (False, {'message': f'Virtual fulfillment is not allowed for item {_ti_bs_id_187}', 'code': 400}, None)
                            _r__plan_checkout_174__returned = True
                            break
                        fulfillment_method = _method_186
                        _product_data_74 = _bs_data_179
                        _qty_75 = _qty_188
                        if _is_no_shipping_fulfillment(fulfillment_method):
                            expected_line_ship = 0.0
                        else:
                            _sh_73 = _listing_shipping_type(_product_data_74)
                            if not _sh_73:
                                expected_line_ship = 0.0
                            else:
                                low = _sh_73.strip().lower()
                                if low == 'free':
                                    expected_line_ship = 0.0
                                elif low in ('buyer actual', 'buyer_actual'):
                                    expected_line_ship = 0.0
                                elif low in ('buyer fixed', 'buyer_fixed'):
                                    amt = _parse_line_shipping_amount(_product_data_74.get('bs_shipping_amount') if _product_data_74.get('bs_shipping_amount') is not None else _product_data_74.get('profile_expertise_shipping_amount'))
                                    _per_unit_185 = 0.0 if amt is None else amt
                                    expected_line_ship = round(_per_unit_185 * int(_qty_75 or 1), 2)
                                else:
                                    expected_line_ship = 0.0
                        per_unit_ship = _item_183.get('ti_shipping_amount_per_unit')
                        if per_unit_ship is None:
                            per_unit_ship = _item_183.get('ti_shipping_amount')
                        declared_raw = _item_183.get('line_shipping_amount')
                        if declared_raw is not None and declared_raw != '':
                            declared_line_ship = _parse_line_shipping_amount(declared_raw)
                            if declared_line_ship is None:
                                _r__plan_checkout_174 = (False, {'message': f'Invalid line_shipping_amount for item {_ti_bs_id_187}', 'code': 400}, None)
                                _r__plan_checkout_174__returned = True
                                break
                            if per_unit_ship is not None and per_unit_ship != '':
                                expected_from_unit = round(_to_float(per_unit_ship) * _qty_188, 2)
                                if not _money_close(declared_line_ship, expected_from_unit):
                                    _r__plan_checkout_174 = (False, {'message': f'line_shipping_amount must equal ti_shipping_amount_per_unit × quantity for item {_ti_bs_id_187} (expected {expected_from_unit:.2f}, got {declared_line_ship:.2f})', 'code': 400}, None)
                                    _r__plan_checkout_174__returned = True
                                    break
                            elif _method_186 == 'ship' and (not _money_close(declared_line_ship, expected_line_ship)):
                                _r__plan_checkout_174 = (False, {'message': f'line_shipping_amount mismatch for item {_ti_bs_id_187} (expected {expected_line_ship:.2f}, got {declared_line_ship:.2f})', 'code': 400}, None)
                                _r__plan_checkout_174__returned = True
                                break
                        else:
                            legacy_unit = _item_183.get('shipping_amount')
                            if legacy_unit is None:
                                legacy_unit = per_unit_ship
                            if legacy_unit is not None and legacy_unit != '':
                                unit_amt = _parse_line_shipping_amount(legacy_unit)
                                declared_line_ship = round((unit_amt or 0.0) * _qty_188, 2)
                                if _method_186 == 'ship' and (not _money_close(declared_line_ship, expected_line_ship)):
                                    _r__plan_checkout_174 = (False, {'message': f'Shipping amount mismatch for item {_ti_bs_id_187} (expected {expected_line_ship:.2f}, got {declared_line_ship:.2f})', 'code': 400}, None)
                                    _r__plan_checkout_174__returned = True
                                    break
                            else:
                                declared_line_ship = expected_line_ship
                        if _is_no_shipping_fulfillment(_method_186) and declared_line_ship != 0:
                            _r__plan_checkout_174 = (False, {'message': f'{_method_186.title()} lines must have line_shipping_amount 0 ({_ti_bs_id_187})', 'code': 400}, None)
                            _r__plan_checkout_174__returned = True
                            break
                        snr = _item_183.get('shipping_not_required')
                        if snr is not None and _is_no_shipping_fulfillment(_method_186):
                            if int(snr) != 1:
                                _r__plan_checkout_174 = (False, {'message': f'shipping_not_required must be 1 for {_method_186} lines ({_ti_bs_id_187})', 'code': 400}, None)
                                _r__plan_checkout_174__returned = True
                                break
                        if _method_186 == 'ship':
                            any_ship = True
                            _product_data_190 = _bs_data_179
                            _sh_181 = _listing_shipping_type(_product_data_190)
                            if not _sh_181:
                                _r__is_buyer_actual_shipping_1 = False
                            else:
                                _r__is_buyer_actual_shipping_1 = _sh_181.strip().lower() in ('buyer actual', 'buyer_actual')
                            if _r__is_buyer_actual_shipping_1:
                                shipping_actual_pending = 1
                        _order_shipping_192 += declared_line_ship
                        lines.append({'item_index': idx, 'ti_bs_id': _ti_bs_id_187, 'fulfillment_method': _method_186, 'line_shipping_amount': declared_line_ship, 'line_tax_amount': line_tax, 'line_merchandise': line_merchandise, 'unit_price': unit_cost, 'ti_tax_rate': tax_rate, 'ti_shipping_amount_per_unit': per_unit_ship, 'qty': _qty_188})
                    if not _r__plan_checkout_174__returned:
                        if not lines:
                            _r__plan_checkout_174 = (False, {'message': 'No valid items in checkout', 'code': 400}, None)
                        elif any_ship and (not _shipping_fields_175):
                            _r__plan_checkout_174 = (False, {'message': 'shipping_address is required when any item uses ship fulfillment', 'code': 400}, None)
                        else:
                            _order_shipping_192 = round(_order_shipping_192, 2)
                            order_merchandise = round(order_merchandise, 2)
                            order_tax = round(order_tax, 2)
                            if not _money_close(_to_float(_payload_176.get('total_costs')), order_merchandise):
                                _r__plan_checkout_174 = (False, {'message': f'total_costs mismatch (expected {order_merchandise:.2f}, got {_to_float(_payload_176.get('total_costs')):.2f})', 'code': 400}, None)
                            else:
                                payload_taxes = _payload_176.get('total_taxes')
                                if payload_taxes is not None and payload_taxes != '':
                                    if not _money_close(_to_float(payload_taxes), order_tax):
                                        _r__plan_checkout_174 = (False, {'message': f'total_taxes mismatch (expected {order_tax:.2f}, got {_to_float(payload_taxes):.2f})', 'code': 400}, None)
                                payload_shipping = _payload_176.get('total_shipping')
                                if payload_shipping is not None:
                                    if not _money_close(_to_float(payload_shipping), _order_shipping_192):
                                        _r__plan_checkout_174 = (False, {'message': f'total_shipping mismatch (expected {_order_shipping_192:.2f}, got {_to_float(payload_shipping):.2f})', 'code': 400}, None)
                                shipping_for_total = _order_shipping_192 if payload_shipping is None else round(_to_float(payload_shipping), 2)
                                expected_paid = round(_to_float(_payload_176.get('total_costs')) + _to_float(_payload_176.get('total_taxes')) + shipping_for_total + _to_float(_payload_176.get('total_fees')), 2)
                                if not _money_close(_to_float(_payload_176.get('total_amount_paid')), expected_paid):
                                    _r__plan_checkout_174 = (False, {'message': f'total_amount_paid mismatch (expected {expected_paid:.2f}, got {_to_float(_payload_176.get('total_amount_paid')):.2f})', 'code': 400}, None)
                                else:
                                    _r__plan_checkout_174 = (True, None, {'lines': lines, 'order_shipping': _order_shipping_192, 'order_merchandise': order_merchandise, 'order_tax': order_tax, 'any_ship': any_ship, 'shipping_actual_pending': shipping_actual_pending})
                plan_ok, plan_err, checkout_plan = _r__plan_checkout_174
                if not plan_ok:
                    return plan_err, plan_err.get("code", 400)

                if checkout_plan.get("shipping_actual_pending"):
                    transaction["transaction_shipping_actual_pending"] = 1

                plan_by_index = {
                    entry["item_index"]: entry for entry in checkout_plan["lines"]
                }

                wallet_debited = 0.0
                if wallet_amount >= 0.01:
                    wallet_debit = debit_useable_for_purchase(
                        db,
                        payload.get("profile_id"),
                        wallet_amount,
                    )
                    if wallet_debit.get("code") != 200:
                        response["message"] = wallet_debit.get(
                            "message", "Failed to apply wallet balance"
                        )
                        response["code"] = wallet_debit.get("code", 400)
                        response["wallet"] = wallet_debit
                        return response, response["code"]
                    wallet_debited = _to_float(wallet_debit.get("debited"))
                    response["wallet_applied"] = wallet_debit
                else:
                    response["wallet_applied"] = {
                        "code": 200,
                        "skipped": True,
                        "debited": 0.0,
                    }
                response["card_amount"] = card_amount
                response["wallet_amount"] = wallet_amount

                def _rollback_wallet_debit():
                    if wallet_debited < 0.01:
                        return
                    restore = credit_useable_from_refund(
                        db, payload.get("profile_id"), wallet_debited
                    )
                    if restore.get("code") != 200:
                        print(
                            "Warning: Failed to restore wallet after checkout failure: "
                            f"{restore}"
                        )

                # Generate new transaction UID
                transaction_stored_procedure_response = db.call(
                    procedure="new_transaction_uid"
                )
                if (
                    not transaction_stored_procedure_response.get("result")
                    or len(transaction_stored_procedure_response["result"]) == 0
                ):
                    _rollback_wallet_debit()
                    response["message"] = "Failed to generate transaction UID"
                    response["code"] = 500
                    return response, 500

                new_transaction_uid = transaction_stored_procedure_response["result"][0]["new_id"]
                transaction["transaction_uid"] = new_transaction_uid
                transactions_datetime = utc_now_str()
                transaction["transaction_datetime"] = transactions_datetime

                # Insert transaction
                transaction_response = db.insert(
                    "every_circle.transactions", transaction
                )
                print("transaction post response: ", transaction_response)

                if transaction_response.get("code") != 200:
                    _rollback_wallet_debit()
                    response["message"] = transaction_response.get(
                        "message", "Failed to insert transaction"
                    )
                    response["code"] = transaction_response.get("code", 500)
                    return response, response["code"]

                response["transaction"] = transaction_response
                response["transaction_uid"] = new_transaction_uid

                if checkout_plan.get("any_ship") and shipping_fields:
                    shipping_response = insert_transaction_shipping(
                        db, new_transaction_uid, shipping_fields
                    )
                    print("transaction_shipping post response: ", shipping_response)
                    if shipping_response.get("code") != 200:
                        response["message"] = shipping_response.get(
                            "message", "Failed to insert shipping address"
                        )
                        response["code"] = shipping_response.get("code", 500)
                        return response, response["code"]
                    response["ts_uid"] = shipping_response.get("ts_uid")
                    response["shipping_address"] = shipping_response.get(
                        "shipping_address"
                    )

                # Enter Data in Transactions_ItemsTable
                print("items: ", payload.get("items"))
                items_count = 0
                bounty_count = 0
                order_shipping_total = 0.0
                inventory_updates = []
                for item_idx, item in enumerate(payload.get("items", [])):
                    print(item)
                    # {'bs_uid': '250-000021', 'quantity': 9, 'recommender_profile_id': '110-000231'}

                    # Validate required item fields
                    if (
                        not item.get("bs_uid")
                        and not item.get("expertise_uid")
                        and not item.get("wish_response_uid")
                    ):
                        print(
                            f"Warning: Skipping item missing bs_uid or expertise_uid or wish_response_uid: {item}"
                        )
                        continue

                    # Generate new transaction item UID
                    transaction_item_stored_procedure_response = db.call(
                        procedure="new_transaction_item_uid"
                    )
                    if (
                        not transaction_item_stored_procedure_response.get("result")
                        or len(transaction_item_stored_procedure_response["result"])
                        == 0
                    ):
                        print(
                            f"Warning: Failed to generate transaction item UID for item: {item}"
                        )
                        continue

                    new_transaction_item_uid = (
                        transaction_item_stored_procedure_response["result"][0]["new_id"]
                    )
                    print(
                        "new_transaction_item_uid: ",
                        new_transaction_item_uid,
                        type(new_transaction_item_uid),
                    )

                    # Load transaction item data from payload
                    tx_item = {
                        "ti_uid": new_transaction_item_uid,
                        "ti_transaction_id": new_transaction_uid,
                        "ti_bs_id": item.get("bs_uid")
                        or item.get("expertise_uid")
                        or item.get("wish_response_uid"),
                        "ti_bs_qty": item.get("quantity"),
                    }
                    print("tx_item: ", tx_item)
                    ti_bs_id = tx_item.get("ti_bs_id")
                    # item_bounty_type = "per_item"
                    item_bounty_type = item.get("bounty_type", "per_item")
                    is_wish_item = False
                    stock_decrement = None

                    if ti_bs_id and str(ti_bs_id).startswith("250"):
                        print("ti_bs_id is a business service")
                        bs_query = """
                           SELECT *
                           FROM every_circle.business_services
                           WHERE bs_uid = %s
                       """
                        bs_response = db.execute(bs_query, ti_bs_id)
                        print("bs_response: ", bs_response)

                        if (
                            not bs_response.get("result")
                            or len(bs_response["result"]) == 0
                        ):
                            response["message"] = (
                                f"Business service not found: {item.get('bs_uid')}"
                            )
                            response["code"] = 404
                            return response, 404

                        bs_data = bs_response["result"][0]
                        avail_err = _validate_business_service_available(db, bs_data)
                        if avail_err:
                            _rollback_wallet_debit()
                            response["message"] = avail_err["message"]
                            response["code"] = avail_err["code"]
                            return response, avail_err["code"]

                        tx_item["ti_bs_cost"] = _normalize_stored_cost(bs_data.get("bs_cost"))
                        tx_item["ti_bs_cost_currency"] = bs_data.get("bs_cost_currency")
                        tx_item["ti_bs_sku"] = bs_data.get("bs_sku")
                        tx_item["ti_bs_is_taxable"] = bs_data.get("bs_is_taxable")
                        tx_item["ti_bs_tax_rate"] = bs_data.get("bs_tax_rate")
                        tx_item["ti_bs_refund_policy"] = bs_data.get("bs_refund_policy")
                        tx_item["ti_bs_return_window_days"] = bs_data.get(
                            "bs_return_window_days"
                        )
                        tx_item["ti_bs_is_returnable"] = _normalize_is_returnable(
                            bs_data.get("bs_is_returnable")
                        )
                        _apply_line_shipping_snapshot(tx_item, item, bs_data)
                        _apply_line_tax_snapshot(tx_item, item, bs_data)
                        item_bounty_type = (
                            bs_data.get("bs_bounty_type", "per_item") or "per_item"
                        )
                        print("tx_item: ", tx_item)

                        purchased_qty = _purchase_qty(item)
                        available_qty = _parse_limited_quantity(bs_data.get("bs_quantity"))
                        stock_err = _validate_purchase_quantity(
                            available_qty, purchased_qty
                        )
                        if stock_err:
                            _rollback_wallet_debit()
                            response.update(stock_err[0])
                            return response, stock_err[1]
                        stock_decrement = {
                            "table": "business_services",
                            "uid_column": "bs_uid",
                            "qty_column": "bs_quantity",
                            "uid": ti_bs_id,
                            "purchased_qty": purchased_qty,
                            "limited": available_qty is not None,
                        }

                    elif ti_bs_id and str(ti_bs_id).startswith("150"):
                        print("ti_bs_id is an expertise")
                        # Get other item details from expertise table using parameterized query
                        expertise_query = """
                           SELECT *
                           FROM every_circle.profile_expertise
                           WHERE profile_expertise_uid = %s
                       """
                        bs_response = db.execute(expertise_query, ti_bs_id)
                        print("expertise_response: ", bs_response)
                        # Check if expertise exists
                        if (
                            not bs_response.get("result")
                            or len(bs_response["result"]) == 0
                        ):
                            response["message"] = (
                                f"Expertise not found: {item.get('profile_expertise_uid')}"
                            )
                            response["code"] = 404
                            return response, 404

                        bs_data = bs_response["result"][0]
                        if (
                            int(bs_data.get("profile_expertise_moderated") or 0)
                            != MODERATED_ACTIVE
                        ):
                            response["message"] = "Offering is not available"
                            response["code"] = 403
                            return response, 403

                        owner_uid = bs_data.get("profile_expertise_profile_personal_id")
                        if not is_owner_available_for_public_interaction(db, owner_uid):
                            response["message"] = "Offering is not available"
                            response["code"] = 403
                            return response, 403

                        tx_item["ti_bs_cost"] = _normalize_stored_cost(
                            bs_data.get("profile_expertise_cost")
                        )
                        tx_item["ti_bs_cost_currency"] = bs_data.get(
                            "profile_expertise_cost_currency"
                        )
                        tx_item["ti_bs_sku"] = bs_data.get(
                            "profile_expertise_sku"
                        )  # Doesn't exist
                        tx_item["ti_bs_is_taxable"] = bs_data.get(
                            "profile_expertise_is_taxable"
                        )
                        tx_item["ti_bs_tax_rate"] = bs_data.get(
                            "profile_expertise_tax_rate"
                        )
                        tx_item["ti_bs_refund_policy"] = bs_data.get(
                            "profile_expertise_refund_policy"
                        )
                        tx_item["ti_bs_return_window_days"] = bs_data.get(
                            "profile_expertise_return_window_days"
                        )
                        tx_item["ti_bs_is_returnable"] = _normalize_is_returnable(
                            bs_data.get("profile_expertise_is_returnable")
                        )
                        _apply_line_shipping_snapshot(tx_item, item, bs_data)
                        _apply_line_tax_snapshot(tx_item, item, bs_data)
                        item_bounty_type = (
                            bs_data.get("profile_expertise_bounty_type", "per_item") or "per_item"
                        )
                        print("tx_item: ", tx_item)

                        purchased_qty = _purchase_qty(item)
                        available_qty = _parse_limited_quantity(
                            bs_data.get("profile_expertise_quantity")
                        )
                        stock_err = _validate_purchase_quantity(
                            available_qty, purchased_qty
                        )
                        if stock_err:
                            _rollback_wallet_debit()
                            response.update(stock_err[0])
                            return response, stock_err[1]
                        stock_decrement = {
                            "table": "profile_expertise",
                            "uid_column": "profile_expertise_uid",
                            "qty_column": "profile_expertise_quantity",
                            "uid": ti_bs_id,
                            "purchased_qty": purchased_qty,
                            "limited": available_qty is not None,
                        }

                    elif ti_bs_id and str(ti_bs_id).startswith("165"):
                        print("ti_bs_id is a wish")
                        is_wish_item = True
                        # Get other item details from wish table using parameterized query
                        wish_query = """
                           SELECT wish_response.wish_response_uid, profile_wish.*
                           FROM every_circle.profile_wish
                           LEFT JOIN every_circle.wish_response ON wr_profile_wish_id = profile_wish_uid
                           WHERE wish_response_uid = %s
                       """
                        bs_response = db.execute(
                            wish_query, (item.get("wish_response_uid"),)
                        )
                        print("wish_response: ", bs_response)
                        # Check if wish exists
                        if (
                            not bs_response.get("result")
                            or len(bs_response["result"]) == 0
                        ):
                            response["message"] = (
                                f"Wish not found: {item.get('wish_response_uid')}"
                            )
                            response["code"] = 404
                            return response, 404

                        bs_data = bs_response["result"][0]
                        if (
                            int(bs_data.get("profile_wish_moderated") or 0)
                            != MODERATED_ACTIVE
                        ):
                            response["message"] = "Seeking post is not available"
                            response["code"] = 403
                            return response, 403

                        owner_uid = bs_data.get("profile_wish_profile_personal_id")
                        if not is_owner_available_for_public_interaction(db, owner_uid):
                            response["message"] = "Seeking post is not available"
                            response["code"] = 403
                            return response, 403

                        tx_item["ti_bs_cost"] = _normalize_stored_cost(
                            bs_data.get("profile_wish_cost")
                        )
                        tx_item["ti_bs_cost_currency"] = bs_data.get(
                            "profile_wish_cost_currency"
                        )
                        tx_item["ti_bs_sku"] = bs_data.get(
                            "profile_wish_sku"
                        )  # Doesn't exist
                        tx_item["ti_bs_is_taxable"] = bs_data.get(
                            "profile_wish_is_taxable"
                        )
                        tx_item["ti_bs_tax_rate"] = bs_data.get("profile_wish_tax_rate")
                        tx_item["ti_bs_refund_policy"] = bs_data.get(
                            "profile_wish_refund_policy"
                        )
                        tx_item["ti_bs_return_window_days"] = bs_data.get(
                            "profile_wish_return_window_days"
                        )
                        tx_item["ti_bs_is_returnable"] = _normalize_is_returnable(
                            bs_data.get("profile_wish_is_returnable")
                        )
                        _apply_line_shipping_snapshot(tx_item, item)
                        _apply_line_tax_snapshot(tx_item, item, bs_data)
                        item_bounty_type = (
                            bs_data.get("profile_wish_bounty_type", "per_item") or "per_item"
                        )
                        print("tx_item: ", tx_item)

                        purchased_qty = _purchase_qty(item)
                        available_qty = _parse_limited_quantity(
                            bs_data.get("profile_wish_quantity")
                        )
                        stock_err = _validate_purchase_quantity(
                            available_qty, purchased_qty
                        )
                        if stock_err:
                            _rollback_wallet_debit()
                            response.update(stock_err[0])
                            return response, stock_err[1]
                        stock_decrement = {
                            "table": "profile_wish",
                            "uid_column": "profile_wish_uid",
                            "qty_column": "profile_wish_quantity",
                            "uid": bs_data.get("profile_wish_uid"),
                            "purchased_qty": purchased_qty,
                            "limited": available_qty is not None,
                        }

                    else:
                        print("ti_bs_id is not a valid ID")
                        continue

                    _r__apply_item_options_to_tx_item_92 = None
                    _tx_item_94 = tx_item
                    _item_95 = item
                    _ti_bs_id_93 = ti_bs_id
                    options = _build_selected_options(_item_95)
                    if options:
                        _tx_item_94['ti_selected_options'] = json.dumps(options)
                        _item_47 = _item_95
                        _options_48 = _build_selected_options(_item_47) or []
                        _uids_96 = []
                        _seen_97 = set()
                        for opt in _options_48:
                            bso_uid = (opt.get('bso_uid') or '').strip()
                            if not bso_uid or bso_uid in _seen_97:
                                continue
                            _seen_97.add(bso_uid)
                            _uids_96.append(bso_uid)
                        bso_ids = _uids_96
                        if bso_ids:
                            _tx_item_94['ti_bso_id'] = ','.join(bso_ids)
                    special = (_item_95.get('special_instructions') or '').strip()
                    if special:
                        _tx_item_94['ti_special_instructions'] = special
                    if _item_95.get('choices_extra_cost') is not None:
                        _tx_item_94['ti_choices_extra_cost'] = _to_float(_item_95.get('choices_extra_cost'))
                    unit_price = _item_95.get('unit_price')
                    if unit_price is not None:
                        _tx_item_94['ti_bs_cost'] = _normalize_stored_cost(unit_price)

                    line_plan = plan_by_index.get(item_idx)
                    if line_plan:
                        _r__apply_checkout_fulfillment_90 = None
                        _tx_item_91 = tx_item
                        plan = line_plan
                        product_data = bs_data
                        method = plan['fulfillment_method']
                        qty = int(plan.get('qty') or _tx_item_91.get('ti_bs_qty') or 1)
                        line_shipping = round(_to_float(plan.get('line_shipping_amount')), 2)
                        _tx_item_91['ti_fulfillment_method'] = method
                        _r__snapshot_listing_shipping_on_line_2 = None
                        _tx_item_4 = _tx_item_91
                        _product_data_3 = product_data
                        sh = _listing_shipping_type(_product_data_3)
                        if sh:
                            _tx_item_4['ti_listing_shipping'] = sh
                        if _is_no_shipping_fulfillment(method):
                            _tx_item_91['ti_fulfillment_status'] = FULFILLMENT_STATUS_NOT_REQUIRED
                            _tx_item_91['ti_shipping_not_required'] = 1
                            _tx_item_91['ti_line_shipping_amount'] = 0.0
                            _tx_item_91['ti_shipping_amount'] = 0.0
                            _r__apply_checkout_fulfillment_90 = None
                        else:
                            _tx_item_91['ti_fulfillment_status'] = FULFILLMENT_STATUS_NOT_SHIPPED
                            _tx_item_91['ti_shipping_not_required'] = 0
                            _tx_item_91['ti_line_shipping_amount'] = line_shipping
                            per_unit = round(line_shipping / qty, 2) if qty > 0 else 0.0
                            _tx_item_91['ti_shipping_amount'] = per_unit

                    line_qty = int(tx_item.get("ti_bs_qty") or 1)
                    if line_plan:
                        order_shipping_total += _to_float(
                            line_plan.get("line_shipping_amount")
                        )
                    else:
                        order_shipping_total += (
                            _to_float(tx_item.get("ti_shipping_amount") or 0) * line_qty
                        )

                    # Insert transaction item
                    # bs_query = """
                    #     SELECT *
                    #     FROM every_circle.business_services
                    #     WHERE bs_uid = %s
                    # """
                    # bs_response = db.execute(bs_query, (item.get('bs_uid'),))
                    # print("bs_response: ", bs_response)

                    # # Check if business service exists
                    # if not bs_response.get('result') or len(bs_response['result']) == 0:
                    #     response['message'] = f"Business service not found: {item.get('bs_uid')}"
                    #     response['code'] = 404
                    #     return response, 404

                    # bs_data = bs_response['result'][0]
                    # tx_item['ti_bs_cost'] = bs_data.get('bs_cost')
                    # tx_item['ti_bs_cost_currency'] = bs_data.get('bs_cost_currency')
                    # tx_item['ti_bs_sku'] = bs_data.get('bs_sku')
                    # tx_item['ti_bs_is_taxable'] = bs_data.get('bs_is_taxable')
                    # tx_item['ti_bs_tax_rate'] = bs_data.get('bs_tax_rate')
                    # tx_item['ti_bs_refund_policy'] = bs_data.get('bs_refund_policy')
                    # tx_item['ti_bs_return_window_days'] = bs_data.get('bs_return_window_days')
                    # print("tx_item: ", tx_item)

                    remaining_after = None
                    if stock_decrement:
                        _db_115 = db
                        table = stock_decrement['table']
                        uid_column = stock_decrement['uid_column']
                        qty_column = stock_decrement['qty_column']
                        _uid_112 = stock_decrement['uid']
                        _purchased_qty_116 = stock_decrement['purchased_qty']
                        _purchased_qty_116 = int(_purchased_qty_116 or 1)
                        decrement_result = _db_115.execute(f"\n        UPDATE every_circle.{table}\n        SET {qty_column} = CAST(CAST({qty_column} AS SIGNED) - %s AS CHAR)\n        WHERE {uid_column} = %s\n          AND {qty_column} IS NOT NULL\n          AND TRIM(CAST({qty_column} AS CHAR)) <> ''\n          AND LOWER(TRIM(CAST({qty_column} AS CHAR))) NOT IN ('unlimited', 'null', 'none')\n          AND CAST({qty_column} AS SIGNED) >= %s\n        ", (_purchased_qty_116, _uid_112, _purchased_qty_116), cmd='post')
                        if decrement_result.get('code') != 200:
                            err = {'message': 'Failed to update quantity', 'code': 500}
                            _r__decrement_tracked_quantity_111 = (False, None, (err, 500))
                        else:
                            db_result = decrement_result
                            change = (db_result or {}).get('change') or ''
                            try:
                                _r__rows_affected_6 = int(str(change).split()[0])
                            except (ValueError, IndexError):
                                _r__rows_affected_6 = 0
                            if _r__rows_affected_6 > 0:
                                check = _db_115.execute(f'SELECT {qty_column} FROM every_circle.{table} WHERE {uid_column} = %s', (_uid_112,))
                                _rows_114 = (check or {}).get('result') or []
                                _remaining_113 = _rows_114[0].get(qty_column) if _rows_114 else None
                                _r__decrement_tracked_quantity_111 = (True, _remaining_113, None)
                            else:
                                check = _db_115.execute(f'SELECT {qty_column} FROM every_circle.{table} WHERE {uid_column} = %s', (_uid_112,))
                                _rows_114 = (check or {}).get('result') or []
                                available = _parse_limited_quantity(_rows_114[0].get(qty_column) if _rows_114 else None)
                                if available is None:
                                    _r__decrement_tracked_quantity_111 = (True, None, None)
                                else:
                                    err = {'message': 'Insufficient stock', 'code': 409, 'remaining': available}
                                    _r__decrement_tracked_quantity_111 = (False, available, (err, 409))
                        _, remaining_after, dec_err = _r__decrement_tracked_quantity_111
                        if dec_err:
                            _rollback_wallet_debit()
                            for prior in inventory_updates:
                                if not prior.get("limited"):
                                    continue
                                _increment_tracked_quantity(
                                    db,
                                    prior["table"],
                                    prior["uid_column"],
                                    prior["qty_column"],
                                    prior["uid"],
                                    prior["purchased_qty"],
                                )
                            response.update(dec_err[0])
                            return response, dec_err[1]
                        _stock_decrement_62 = stock_decrement
                        _remaining_61 = remaining_after
                        if not _stock_decrement_62:
                            inv_entry = None
                        else:
                            _uid_63 = _stock_decrement_62.get('uid')
                            if not _uid_63:
                                inv_entry = None
                            else:
                                _entry_60 = {'product_uid': _uid_63, 'remaining': _remaining_61, 'quantity': _stock_decrement_62.get('purchased_qty')}
                                if _stock_decrement_62.get('table') == 'profile_expertise':
                                    _entry_60['profile_expertise_uid'] = _uid_63
                                elif _stock_decrement_62.get('table') == 'business_services':
                                    _entry_60['bs_uid'] = _uid_63
                                elif _stock_decrement_62.get('table') == 'profile_wish':
                                    _entry_60['profile_wish_uid'] = _uid_63
                                inv_entry = _entry_60
                        if inv_entry:
                            inventory_updates.append(
                                {
                                    **stock_decrement,
                                    **inv_entry,
                                }
                            )
                            print(
                                f"Decremented {stock_decrement['table']} "
                                f"{stock_decrement['uid']} to {remaining_after}"
                            )
                            _r__apply_business_sold_out_if_needed_54 = None
                            _db_57 = db
                            _stock_decrement_55 = stock_decrement
                            _remaining_after_56 = remaining_after
                            if not _stock_decrement_55 or _stock_decrement_55.get('table') != 'business_services':
                                _r__apply_business_sold_out_if_needed_54 = None
                            elif not _stock_decrement_55.get('limited'):
                                _r__apply_business_sold_out_if_needed_54 = None
                            else:
                                remaining = _parse_limited_quantity(_remaining_after_56)
                                if remaining is None or remaining > 0:
                                    _r__apply_business_sold_out_if_needed_54 = None
                                else:
                                    _db_57.update('every_circle.business_services', {'bs_uid': _stock_decrement_55['uid']}, {'bs_is_visible': 0, 'bs_status': 'out_of_stock', 'bs_updated_at': utc_now_str()})

                    # Insert transaction item
                    transaction_item_response = db.insert(
                        "every_circle.transactions_items", tx_item
                    )
                    print("transaction_item post response: ", transaction_item_response)

                    if transaction_item_response.get("code") == 200:
                        items_count += 1
                    else:
                        if stock_decrement and stock_decrement.get("limited"):
                            _increment_tracked_quantity(
                                db,
                                stock_decrement["table"],
                                stock_decrement["uid_column"],
                                stock_decrement["qty_column"],
                                stock_decrement["uid"],
                                stock_decrement["purchased_qty"],
                            )
                            inventory_updates.pop()
                        print(
                            f"Warning: Failed to insert transaction item: {transaction_item_response}"
                        )
                        continue

                    # Process bounty if applicable
                    bounty_amount = item.get("bounty", 0)
                    # item_bounty_type = item.get("bounty_type", "per_item")
                    if bounty_amount and float(bounty_amount) > 0:
                        quantity = item.get("quantity", 1) or 1
                        # Determine effective bounty based on type:
                        # 'total'    -> fixed bounty for the whole order (ignore quantity)
                        # 'per_item' -> bounty per unit, multiply by quantity
                        if item_bounty_type == "total":
                            effective_bounty = float(bounty_amount)
                            print(
                                f"Bounty type: total (fixed), bounty_amount: {bounty_amount}, effective_bounty: {effective_bounty}"
                            )
                        else:
                            effective_bounty = float(bounty_amount) * int(quantity)
                            print(
                                f"Bounty type: per_item, bounty_amount: {bounty_amount}, quantity: {quantity}, effective_bounty: {effective_bounty}"
                            )
                        print("Processing bounty: ", effective_bounty)

                        recommender_profile_id = item.get("recommender_profile_id")
                        if not recommender_profile_id:
                            print("Warning: No recommender_profile_id provided")
                            recommender_profile_id = payload.get("profile_id")

                        profile_id = payload.get("profile_id")
                        buyer_is_recommender = (
                            profile_id
                            and recommender_profile_id
                            and profile_id == recommender_profile_id
                        )
                        is_expertise_item = (
                            ti_bs_id and str(ti_bs_id).startswith("150")
                        )

                        if is_expertise_item:
                            seller_profile_id = (
                                bs_data.get("profile_expertise_profile_personal_id")
                                or payload.get("business_id")
                            )
                            path_from, path_to = seller_profile_id, profile_id
                        elif is_wish_item:
                            path_from, path_to = profile_id, recommender_profile_id
                        else:
                            path_from, path_to = profile_id, recommender_profile_id

                        _path_from_71 = path_from
                        _path_to_70 = path_to
                        if not _path_from_71 or not _path_to_70:
                            combined_path = None
                        else:
                            try:
                                connections_path = ConnectionsPath()
                                network_response, network_status = connections_path.get(_path_from_71, _path_to_70)
                                if network_status != 200 or not network_response.get('combined_path'):
                                    print(f'Warning: Could not find connection path {_path_from_71} -> {_path_to_70}. Status: {network_status}, Response: {network_response}')
                                    combined_path = None
                                else:
                                    _combined_path_72 = network_response['combined_path']
                                    print('network combined_path: ', _combined_path_72)
                                    combined_path = _combined_path_72
                            except Exception as _e_69:
                                print(f'Error getting connection path: {str(_e_69)}')
                                combined_path = None

                        known_participants = []
                        if is_expertise_item:
                            if profile_id:
                                known_participants.append(
                                    {
                                        "tb_profile_id": profile_id,
                                        **_bounty_pct_amount(effective_bounty, 0.40),
                                    }
                                )
                        elif is_wish_item:
                            if buyer_is_recommender:
                                if profile_id:
                                    known_participants.append(
                                        {
                                            "tb_profile_id": profile_id,
                                            **_bounty_pct_amount(effective_bounty, 0.40),
                                        }
                                    )
                            else:
                                if profile_id:
                                    known_participants.append(
                                        {
                                            "tb_profile_id": profile_id,
                                            **_bounty_pct_amount(effective_bounty, 0.20),
                                        }
                                    )
                                if recommender_profile_id:
                                    known_participants.append(
                                        {
                                            "tb_profile_id": recommender_profile_id,
                                            **_bounty_pct_amount(effective_bounty, 0.20),
                                        }
                                    )
                        else:
                            if buyer_is_recommender:
                                known_participants.append(
                                    {
                                        "tb_profile_id": profile_id,
                                        **_bounty_pct_amount(effective_bounty, 0.40),
                                    }
                                )
                            else:
                                if profile_id:
                                    known_participants.append(
                                        {
                                            "tb_profile_id": profile_id,
                                            **_bounty_pct_amount(effective_bounty, 0.20),
                                        }
                                    )
                                if recommender_profile_id:
                                    known_participants.append(
                                        {
                                            "tb_profile_id": recommender_profile_id,
                                            **_bounty_pct_amount(effective_bounty, 0.20),
                                        }
                                    )
                        known_participants.append(
                            {
                                "tb_profile_id": EC_WALLET_ID,
                                **_bounty_pct_amount(effective_bounty, 0.20),
                            }
                        )
                        seen = {
                            p["tb_profile_id"]
                            for p in known_participants
                            if p["tb_profile_id"]
                        }

                        _combined_path_36 = combined_path
                        _seen_37 = seen
                        if not _combined_path_36:
                            middle_nodes = []
                        else:
                            try:
                                uids = _combined_path_36.split(',')
                                middle = uids[1:-1] if len(uids) > 2 else []
                                middle_nodes = [uid for uid in middle if uid and uid not in _seen_37]
                            except Exception as _e_35:
                                print(f'Error processing network path: {str(_e_35)}')
                                middle_nodes = []
                        if is_expertise_item or is_wish_item:
                            _middle_uids_109 = middle_nodes
                            _effective_bounty_107 = effective_bounty
                            pool = round(_BOUNTY_NETWORK_POOL * _effective_bounty_107, 4)
                            max_per = round(_BOUNTY_NETWORK_MAX_PERSON * _effective_bounty_107, 4)
                            if not _middle_uids_109:
                                if pool <= 0:
                                    network_participants = []
                                else:
                                    network_participants = [{'tb_profile_id': CHARITY_PROFILE_ID, **_bounty_pct_amount(_effective_bounty_107, _BOUNTY_NETWORK_POOL)}]
                            else:
                                per_person = min(round(pool / len(_middle_uids_109), 4), max_per)
                                total_paid = round(per_person * len(_middle_uids_109), 4)
                                charity_amount = round(pool - total_paid, 4)
                                person_pct = round(per_person / _effective_bounty_107, 4) if _effective_bounty_107 else 0
                                _participants_110 = [{'tb_profile_id': _uid_108, 'tb_percentage': str(person_pct), 'tb_amount': per_person} for _uid_108 in _middle_uids_109]
                                if charity_amount > 0:
                                    charity_pct = round(charity_amount / _effective_bounty_107, 4) if _effective_bounty_107 else 0
                                    if _charity_share_is_payable(charity_amount, charity_pct):
                                        _participants_110.append({'tb_profile_id': CHARITY_PROFILE_ID, 'tb_percentage': str(charity_pct), 'tb_amount': charity_amount})
                                network_participants = _participants_110
                        else:
                            middle_uids = middle_nodes
                            _effective_bounty_39 = effective_bounty
                            _seen_41 = seen
                            network_result = list(middle_uids)
                            if len(network_result) < 2 and CHARITY_PROFILE_ID not in _seen_41:
                                network_result.append(CHARITY_PROFILE_ID)
                            if not network_result:
                                network_participants = []
                            else:
                                network_percentage = _BOUNTY_NETWORK_POOL / len(network_result)
                                participants = [{'tb_profile_id': _uid_40, **_bounty_pct_amount(_effective_bounty_39, network_percentage)} for _uid_40 in network_result]
                                _r__without_zero_charity_14 = [_p_38 for _p_38 in participants if _p_38.get('tb_profile_id') != CHARITY_PROFILE_ID or _charity_share_is_payable(_p_38.get('tb_amount'), _p_38.get('tb_percentage'))]
                                network_participants = _r__without_zero_charity_14
                        print("network_participants: ", network_participants)

                        # Process known participants (buyer, recommender, ec-wallet)
                        for participant in known_participants:
                            participant_id = participant.get("tb_profile_id")
                            if not participant_id:
                                continue

                            print(f"Processing known participant: {participant_id}")

                            try:
                                transaction_bounty_stored_procedure_response = db.call(
                                    procedure="new_transaction_bounty_uid"
                                )
                                if (
                                    not transaction_bounty_stored_procedure_response.get(
                                        "result"
                                    )
                                    or len(
                                        transaction_bounty_stored_procedure_response[
                                            "result"
                                        ]
                                    )
                                    == 0
                                ):
                                    print(
                                        f"Warning: Failed to generate bounty UID for participant: {participant_id}"
                                    )
                                    continue

                                new_transaction_bounty_uid = (
                                    transaction_bounty_stored_procedure_response[
                                        "result"
                                    ][0]["new_id"]
                                )
                                print(
                                    "new_transaction_bounty_uid: ",
                                    new_transaction_bounty_uid,
                                    type(new_transaction_bounty_uid),
                                )

                                # Create new dictionary for each bounty to avoid data leakage
                                tx_bounty = {
                                    "tb_uid": new_transaction_bounty_uid,
                                    "tb_ti_id": new_transaction_item_uid,
                                    "tb_profile_id": participant_id,
                                    "tb_percentage": participant["tb_percentage"],
                                    "tb_amount": participant["tb_amount"],
                                }
                                print("tx_bounty: ", tx_bounty)

                                bounty_response = db.insert(
                                    "every_circle.transactions_bounty", tx_bounty
                                )
                                print(
                                    "transaction_bounty post response: ",
                                    bounty_response,
                                )

                                if bounty_response.get("code") == 200:
                                    bounty_count += 1

                                    print("bounty_count: ", bounty_count)

                                    bounty_amount = tx_bounty["tb_amount"]
                                    wallet_result = credit_bounty_to_wallet(
                                        db,
                                        participant_id,
                                        bounty_amount,
                                        in_escrow=True,
                                    )
                                    print("wallet_result: ", wallet_result)
                                    if wallet_result.get("code") != 200:
                                        print(
                                            f"Warning: Failed to update wallet for "
                                            f"participant {participant_id}: {wallet_result}"
                                        )
                                    
                                else:
                                    print(
                                        f"Warning: Failed to insert bounty for participant {participant_id}: {bounty_response}"
                                    )
                            except Exception as e:
                                print(
                                    f"Error processing bounty for participant {participant_id}: {str(e)}"
                                )
                                continue

                        # Process network participants
                        for participant in network_participants:
                            participant_id = participant.get("tb_profile_id")
                            if not participant_id:
                                continue

                            print(f"Processing network participant: {participant_id}")

                            try:
                                transaction_bounty_stored_procedure_response = db.call(
                                    procedure="new_transaction_bounty_uid"
                                )
                                if (
                                    not transaction_bounty_stored_procedure_response.get(
                                        "result"
                                    )
                                    or len(
                                        transaction_bounty_stored_procedure_response[
                                            "result"
                                        ]
                                    )
                                    == 0
                                ):
                                    print(
                                        f"Warning: Failed to generate bounty UID for network participant: {participant_id}"
                                    )
                                    continue

                                new_transaction_bounty_uid = (
                                    transaction_bounty_stored_procedure_response[
                                        "result"
                                    ][0]["new_id"]
                                )
                                print(
                                    "new_transaction_bounty_uid: ",
                                    new_transaction_bounty_uid,
                                    type(new_transaction_bounty_uid),
                                )

                                tx_bounty = {
                                    "tb_uid": new_transaction_bounty_uid,
                                    "tb_ti_id": new_transaction_item_uid,
                                    "tb_profile_id": participant_id,
                                    "tb_percentage": participant["tb_percentage"],
                                    "tb_amount": participant["tb_amount"],
                                }
                                print("tx_bounty: ", tx_bounty)

                                bounty_response = db.insert(
                                    "every_circle.transactions_bounty", tx_bounty
                                )
                                print(
                                    "transaction_bounty post response: ",
                                    bounty_response,
                                )

                                if bounty_response.get("code") == 200:
                                    bounty_count += 1

                                    print("bounty_count: ", bounty_count)

                                    bounty_amount = tx_bounty["tb_amount"]
                                    wallet_result = credit_bounty_to_wallet(
                                        db,
                                        participant_id,
                                        bounty_amount,
                                        in_escrow=True,
                                    )
                                    print("wallet_result: ", wallet_result)
                                    if wallet_result.get("code") != 200:
                                        print(
                                            f"Warning: Failed to update wallet for "
                                            f"network participant {participant_id}: {wallet_result}"
                                        )

                                else:
                                    print(
                                        f"Warning: Failed to insert bounty for network participant {participant_id}: {bounty_response}"
                                    )
                            except Exception as e:
                                print(
                                    f"Error processing bounty for network participant {participant_id}: {str(e)}"
                                )
                                continue

                if payload.get("total_shipping") is not None:
                    parsed_total = _parse_line_shipping_amount(payload.get("total_shipping"))
                    order_shipping = 0.0 if parsed_total is None else parsed_total
                else:
                    order_shipping = checkout_plan.get("order_shipping")
                    if order_shipping is None:
                        order_shipping = round(order_shipping_total, 2)

                db.update(
                    "every_circle.transactions",
                    {"transaction_uid": new_transaction_uid},
                    {"transaction_shipping": order_shipping},
                )
                response["transaction_shipping"] = order_shipping
                response["shipping_actual_pending"] = bool(
                    checkout_plan.get("shipping_actual_pending")
                )

                response["transaction_items"] = items_count
                response["transaction_bounty_count"] = bounty_count

                try:
                    seller_credit = credit_seller_proceeds_at_checkout(
                        db, new_transaction_uid
                    )
                    response["seller_proceeds_credit"] = seller_credit
                    if seller_credit.get("code") != 200:
                        print(
                            "Warning: Failed to credit seller pending proceeds at "
                            f"checkout: {seller_credit}"
                        )
                except Exception as seller_credit_err:
                    print(
                        "Warning: Exception crediting seller pending proceeds at "
                        f"checkout: {seller_credit_err}"
                    )
                    response["seller_proceeds_credit"] = {
                        "code": 500,
                        "message": str(seller_credit_err),
                    }

                if inventory_updates:
                    response["inventory_updates"] = [
                        {
                            k: v
                            for k, v in entry.items()
                            if k
                            not in (
                                "table",
                                "uid_column",
                                "qty_column",
                                "limited",
                                "purchased_qty",
                            )
                        }
                        for entry in inventory_updates
                    ]
                response["message"] = "Transaction completed successfully"
                response["code"] = 200
                response["schema_version"] = 3
                purchase_row = None
                try:
                    from account_screen_v3 import build_buyer_purchase_row_v3

                    purchase_row = build_buyer_purchase_row_v3(
                        db,
                        payload.get("profile_id"),
                        new_transaction_uid,
                        tz_name=_request_timezone(),
                    )
                except Exception as purchase_row_err:
                    print(
                        "Warning: Failed to build v3 purchase_row after checkout: "
                        f"{purchase_row_err}"
                    )
                if purchase_row:
                    response["purchase_row"] = purchase_row
                return response, 200

        except Exception as e:
            print(f"Error in Transactions POST: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500


    def put(self):
        print("In Transactions PUT")
        response = {}

        try:
            payload = request.get_json()
            if not payload:
                response["message"] = "Request body is required"
                response["code"] = 400
                return response, 400

            if not payload.get("transaction_uid"):
                response["message"] = "transaction_uid is required"
                response["code"] = 400
                return response, 400

            transaction_uid = payload.get("transaction_uid")
            delivery_items = payload.get("delivery_verification_items")
            fulfillment_updates = payload.get("fulfillment_updates")

            if delivery_items is not None:
                return self._put_delivery_verification(
                    transaction_uid, payload, delivery_items
                )

            if fulfillment_updates is not None:
                return self._put_fulfillment_updates(
                    transaction_uid, payload, fulfillment_updates
                )

            update_fields = {}

            if "transaction_in_escrow" in payload:
                update_fields["transaction_in_escrow"] = (
                    1 if payload.get("transaction_in_escrow") else 0
                )

            if "transaction_return_requested" in payload:
                update_fields["transaction_return_requested"] = (
                    1 if payload.get("transaction_return_requested") else 0
                )

            # return note / status / seller_note live on transaction_return_requests

            if not update_fields:
                response["message"] = "No valid fields to update"
                response["code"] = 400
                return response, 400

            with connect() as db:
                update_response = db.update(
                    "every_circle.transactions",
                    {"transaction_uid": transaction_uid},
                    update_fields,
                )

                if update_response.get("code") != 200:
                    response["message"] = update_response.get(
                        "message", "Failed to update transaction"
                    )
                    response["code"] = update_response.get("code", 500)
                    return response, response["code"]

                response["message"] = "Transaction updated successfully"
                response["code"] = 200
                response["transaction_uid"] = transaction_uid
                response.update(update_fields)
                return response, 200

        except Exception as e:
            print(f"Error in Transactions PUT: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500

    def _put_fulfillment_updates(
        self, transaction_uid, payload, fulfillment_updates
    ):
        """Seller marks line items shipped / updates tracking (ti_fulfillment_*)."""
        response = {}

        if not isinstance(fulfillment_updates, list) or len(fulfillment_updates) == 0:
            response["message"] = "fulfillment_updates must be a non-empty list"
            response["code"] = 400
            return response, 400

        seller_id = (
            payload.get("seller_id")
            or payload.get("business_uid")
            or payload.get("business_id")
            or payload.get("profile_id")
            or _get_authenticated_profile_id()
        )
        if not seller_id:
            response["message"] = (
                "seller_id, business_uid, or authenticated seller identity is required"
            )
            response["code"] = 403
            return response, 403

        seller_id = str(seller_id)
        seen_ti = set()
        updated_lines = []
        shipped_at = utc_now_str()

        try:
            with connect() as db:
                tx_row_q = db.execute(
                    """
                    SELECT transaction_uid, transaction_business_id,
                           transaction_profile_id,
                           COALESCE(transaction_type, 'sale') AS transaction_type
                    FROM every_circle.transactions
                    WHERE transaction_uid = %s
                    """,
                    (transaction_uid,),
                )
                tx_rows = tx_row_q.get("result") or []
                if not tx_rows:
                    response["message"] = "Transaction not found"
                    response["code"] = 404
                    return response, 404

                tx_row = tx_rows[0]
                if (tx_row.get("transaction_type") or "sale") != "sale":
                    response["message"] = (
                        "Fulfillment can only be updated on a sale transaction"
                    )
                    response["code"] = 400
                    return response, 400

                if str(tx_row.get("transaction_business_id") or "") != seller_id:
                    response["message"] = (
                        "Caller is not the seller on this transaction"
                    )
                    response["code"] = 403
                    return response, 403

                for entry in fulfillment_updates:
                    if not isinstance(entry, dict):
                        response["message"] = (
                            "Each fulfillment_updates entry must be an object"
                        )
                        response["code"] = 400
                        return response, 400

                    item_uid = entry.get("transaction_item_uid")
                    status = (
                        str(entry.get("fulfillment_status") or "").strip().lower()
                    )

                    if not item_uid:
                        response["message"] = (
                            "Each entry requires transaction_item_uid"
                        )
                        response["code"] = 400
                        return response, 400
                    if item_uid in seen_ti:
                        response["message"] = (
                            f"Duplicate transaction_item_uid: {item_uid}"
                        )
                        response["code"] = 400
                        return response, 400
                    seen_ti.add(item_uid)

                    if status not in SELLER_FULFILLMENT_STATUSES:
                        response["message"] = (
                            f"Invalid fulfillment_status for {item_uid}. "
                            f"Allowed: {', '.join(sorted(SELLER_FULFILLMENT_STATUSES))}"
                        )
                        response["code"] = 400
                        return response, 400

                    ti_row = _resolve_transaction_item(
                        db, transaction_uid, item_uid
                    )
                    if not ti_row:
                        response["message"] = (
                            f"Transaction item not found on this sale: {item_uid}"
                        )
                        response["code"] = 404
                        return response, 404

                    ti_uid = ti_row.get("ti_uid")
                    order_qty = int(ti_row.get("ti_bs_qty") or 0)
                    current_shipped = int(ti_row.get("ti_shipped_qty") or 0)
                    remaining_to_ship = _remaining_to_ship_qty(
                        db,
                        transaction_uid,
                        ti_uid,
                        order_qty,
                        current_shipped,
                    )
                    current_status = (
                        ti_row.get("ti_fulfillment_status") or "not_required"
                    )
                    if (
                        current_status == "not_required"
                        or int(ti_row.get("ti_shipping_not_required") or 0) == 1
                    ):
                        response["message"] = (
                            f"Item {ti_uid} does not require shipping "
                            f"(fulfillment_status=not_required)"
                        )
                        response["code"] = 400
                        return response, 400
                    if current_status == FULFILLMENT_STATUS_DELIVERED:
                        response["message"] = (
                            f"Item {ti_uid} is already delivered and cannot be updated"
                        )
                        response["code"] = 400
                        return response, 400

                    if status == FULFILLMENT_STATUS_NOT_SHIPPED:
                        new_shipped_qty = 0
                    elif "shipped_quantity" in entry:
                        try:
                            ship_qty = int(entry.get("shipped_quantity"))
                        except (TypeError, ValueError):
                            ship_qty = -1
                        if ship_qty < 1:
                            response["message"] = (
                                f"Invalid shipped_quantity for item {item_uid}"
                            )
                            response["code"] = 400
                            return response, 400
                        if ship_qty > remaining_to_ship:
                            response["message"] = (
                                f"shipped_quantity exceeds remaining qty for {item_uid} "
                                f"(remaining: {remaining_to_ship})"
                            )
                            response["code"] = 400
                            return response, 400
                        new_shipped_qty = current_shipped + ship_qty
                    elif status == FULFILLMENT_STATUS_IN_TRANSIT:
                        # Default: ship all remaining units (after returns/cancels)
                        if remaining_to_ship < 1:
                            response["message"] = (
                                f"No remaining quantity to ship for {item_uid}"
                            )
                            response["code"] = 400
                            return response, 400
                        new_shipped_qty = current_shipped + remaining_to_ship
                    else:
                        new_shipped_qty = current_shipped

                    tracking_carrier = ti_row.get("ti_tracking_carrier")
                    if "tracking_carrier" in entry:
                        incoming = entry.get("tracking_carrier")
                        if incoming is not None and str(incoming).strip():
                            tracking_carrier = append_fulfillment_field(
                                tracking_carrier,
                                incoming,
                                separator=" | ",
                                max_len=TI_TRACKING_CARRIER_MAX_LEN,
                            )
                        elif incoming is not None and not str(incoming).strip():
                            # Explicit empty string clears history
                            tracking_carrier = None

                    tracking_number = ti_row.get("ti_tracking_number")
                    if "tracking_number" in entry:
                        incoming = entry.get("tracking_number")
                        if incoming is not None and str(incoming).strip():
                            tracking_number = append_fulfillment_field(
                                tracking_number,
                                incoming,
                                separator=" | ",
                                max_len=TI_TRACKING_NUMBER_MAX_LEN,
                            )
                        elif incoming is not None and not str(incoming).strip():
                            tracking_number = None

                    fulfillment_note = ti_row.get("ti_fulfillment_note")
                    if "fulfillment_note" in entry:
                        incoming = entry.get("fulfillment_note")
                        if incoming is not None and str(incoming).strip():
                            fulfillment_note = append_fulfillment_field(
                                fulfillment_note,
                                incoming,
                                separator="\n",
                            )
                        elif incoming is not None and not str(incoming).strip():
                            fulfillment_note = None

                    new_shipped_at = ti_row.get("ti_shipped_at")
                    if status == FULFILLMENT_STATUS_IN_TRANSIT and not new_shipped_at:
                        new_shipped_at = shipped_at
                    if status == FULFILLMENT_STATUS_NOT_SHIPPED:
                        new_shipped_at = None

                    ti_update = db.execute(
                        """
                        UPDATE every_circle.transactions_items
                        SET ti_fulfillment_status = %s,
                            ti_shipped_qty = %s,
                            ti_shipped_at = %s,
                            ti_tracking_carrier = %s,
                            ti_tracking_number = %s,
                            ti_fulfillment_note = %s
                        WHERE ti_uid = %s AND ti_transaction_id = %s
                        """,
                        (
                            status,
                            new_shipped_qty,
                            new_shipped_at,
                            tracking_carrier,
                            tracking_number,
                            fulfillment_note,
                            ti_uid,
                            transaction_uid,
                        ),
                        "post",
                    )
                    if ti_update.get("code") != 200:
                        response["message"] = ti_update.get(
                            "message", "Failed to update transaction item fulfillment"
                        )
                        response["code"] = ti_update.get("code", 500)
                        return response, response["code"]

                    updated_lines.append(
                        {
                            "transaction_item_uid": ti_uid,
                            "fulfillment_status": status,
                            "shipped_quantity": new_shipped_qty,
                            "ti_bs_qty": order_qty,
                            "shipped_at": new_shipped_at,
                            "tracking_carrier": tracking_carrier,
                            "tracking_number": tracking_number,
                            "fulfillment_note": fulfillment_note,
                        }
                    )

                response["message"] = "Fulfillment updated successfully"
                response["code"] = 200
                response["transaction_uid"] = transaction_uid
                response["fulfillment_updates"] = updated_lines

                buyer_profile_id = tx_row.get("transaction_profile_id")
                if buyer_profile_id:
                    from account_screen_v3 import build_buyer_purchase_row_v3

                    tz_name = request.args.get("timezone") or request.args.get("tz")
                    purchase_row = build_buyer_purchase_row_v3(
                        db,
                        buyer_profile_id,
                        transaction_uid,
                        tz_name=tz_name,
                    )
                    if purchase_row:
                        response["purchase_row"] = purchase_row
                return response, 200

        except Exception as e:
            print(f"Error in Transactions PUT (fulfillment): {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500

    def _put_delivery_verification(
        self, transaction_uid, payload, delivery_items
    ):
        response = {}

        if not isinstance(delivery_items, list) or len(delivery_items) == 0:
            response["message"] = (
                "delivery_verification_items must be a non-empty list"
            )
            response["code"] = 400
            return response, 400

        if "transaction_in_escrow" not in payload:
            response["message"] = "transaction_in_escrow is required"
            response["code"] = 400
            return response, 400

        buyer_profile_id = _get_authenticated_profile_id()
        if not buyer_profile_id:
            response["message"] = "Authenticated buyer profile is required"
            response["code"] = 403
            return response, 403

        received_at = utc_now_str()
        updated_lines = []
        seen_ti = set()

        try:
            with connect() as db:
                tx_row_q = db.execute(
                    """
                    SELECT transaction_uid, transaction_profile_id, transaction_in_escrow
                    FROM every_circle.transactions
                    WHERE transaction_uid = %s
                    """,
                    (transaction_uid,),
                )
                tx_rows = tx_row_q.get("result") or []
                if not tx_rows:
                    response["message"] = "Transaction not found"
                    response["code"] = 404
                    return response, 404

                tx_row = tx_rows[0]
                if tx_row.get("transaction_profile_id") != buyer_profile_id:
                    response["message"] = (
                        "Caller is not the buyer on this transaction"
                    )
                    response["code"] = 403
                    return response, 403

                for entry in delivery_items:
                    item_uid = entry.get("transaction_item_uid")
                    try:
                        received_qty = int(entry.get("received_quantity"))
                    except (TypeError, ValueError):
                        received_qty = -1

                    if not item_uid:
                        response["message"] = (
                            "Each entry requires transaction_item_uid"
                        )
                        response["code"] = 400
                        return response, 400
                    if received_qty < 1:
                        response["message"] = (
                            f"Invalid received_quantity for item {item_uid}"
                        )
                        response["code"] = 400
                        return response, 400
                    if item_uid in seen_ti:
                        response["message"] = (
                            f"Duplicate transaction_item_uid: {item_uid}"
                        )
                        response["code"] = 400
                        return response, 400
                    seen_ti.add(item_uid)

                    ti_row = _resolve_transaction_item(
                        db, transaction_uid, item_uid
                    )
                    if not ti_row:
                        response["message"] = (
                            f"Transaction item not found on this sale: {item_uid}"
                        )
                        response["code"] = 400
                        return response, 400

                    ti_uid = ti_row.get("ti_uid")
                    order_qty = int(ti_row.get("ti_bs_qty") or 0)
                    current_received = int(ti_row.get("ti_received_qty") or 0)
                    cancelled = _cancelled_qty(db, transaction_uid, ti_uid)
                    from order_quantity_context import receivable_units_from_totals

                    # Cap is purchased − pre-ship cancels only. Returns do not
                    # shrink receivable: ti_received_qty is gross and returns
                    # come from the verified pool (see units_ledger).
                    receivable = receivable_units_from_totals(order_qty, cancelled)
                    remaining = receivable - current_received

                    if order_qty <= 0 or receivable <= 0:
                        response["message"] = (
                            f"Item {item_uid} is not eligible for delivery verification"
                        )
                        response["code"] = 400
                        return response, 400
                    if received_qty > remaining:
                        response["message"] = (
                            f"received_quantity exceeds remaining receivable qty for "
                            f"{item_uid} (remaining: {remaining})"
                        )
                        response["code"] = 400
                        return response, 400

                    new_received = current_received + received_qty

                    from transaction_shipping import (
                        FULFILLMENT_STATUS_IN_TRANSIT,
                        line_is_shippable_row,
                    )

                    ship_sets = []
                    ship_params = []
                    if line_is_shippable_row(ti_row):
                        current_shipped = int(ti_row.get("ti_shipped_qty") or 0)
                        new_shipped = max(current_shipped, new_received)
                        if new_shipped > current_shipped:
                            ship_sets.extend(
                                [
                                    "ti_shipped_qty = %s",
                                    "ti_shipped_at = COALESCE(ti_shipped_at, %s)",
                                ]
                            )
                            ship_params.extend([new_shipped, received_at])
                            current_status = (
                                ti_row.get("ti_fulfillment_status") or "not_shipped"
                            )
                            if current_status == "not_shipped":
                                ship_sets.append("ti_fulfillment_status = %s")
                                ship_params.append(FULFILLMENT_STATUS_IN_TRANSIT)

                    set_clause = "ti_received_qty = %s, ti_received_at = %s"
                    set_params = [new_received, received_at]
                    if ship_sets:
                        set_clause = f"{set_clause}, " + ", ".join(ship_sets)
                        set_params.extend(ship_params)

                    ti_update = db.execute(
                        f"""
                        UPDATE every_circle.transactions_items
                        SET {set_clause}
                        WHERE ti_uid = %s AND ti_transaction_id = %s
                        """,
                        tuple(set_params + [ti_uid, transaction_uid]),
                        "post",
                    )
                    if ti_update.get("code") != 200:
                        response["message"] = ti_update.get(
                            "message", "Failed to update transaction item"
                        )
                        response["code"] = ti_update.get("code", 500)
                        return response, response["code"]

                    credit_result = credit_partial_delivery(
                        db,
                        transaction_uid,
                        ti_uid,
                        received_qty,
                        new_received,
                    )
                    if credit_result.get("code") != 200:
                        response["message"] = credit_result.get(
                            "message",
                            "Failed to credit seller for partial delivery",
                        )
                        response["code"] = credit_result.get("code", 500)
                        response["partial_delivery_credit"] = credit_result
                        return response, response["code"]

                    line_out = {
                        "transaction_item_uid": ti_uid,
                        "ti_received_qty": new_received,
                        "ti_bs_qty": order_qty,
                        "wt_uid": credit_result.get("wt_uid"),
                        "wt_amount": credit_result.get("wt_amount"),
                    }
                    updated_lines.append(line_out)

                from order_quantity_context import verification_complete

                # True when every receivable unit (purchased − pre-ship cancel)
                # is verified. Returns do not shrink receivable.
                line_q = db.execute(
                    """
                    SELECT ti_uid, ti_bs_qty, COALESCE(ti_received_qty, 0) AS ti_received_qty
                    FROM every_circle.transactions_items
                    WHERE ti_transaction_id = %s
                      AND ti_bs_qty > 0
                    """,
                    (transaction_uid,),
                )
                all_received = True
                for row in line_q.get("result") or []:
                    line_ti_uid = row.get("ti_uid")
                    purchased = int(row.get("ti_bs_qty") or 0)
                    verified = int(row.get("ti_received_qty") or 0)
                    cancelled = _cancelled_qty(db, transaction_uid, line_ti_uid)
                    if not verification_complete(verified, purchased, cancelled):
                        all_received = False
                        break
                update_fields = {}

                if all_received:
                    update_fields["transaction_in_escrow"] = 0
                elif int(tx_row.get("transaction_in_escrow") or 0) == 1:
                    update_fields["transaction_in_escrow"] = 1

                if "transaction_return_requested" in payload:
                    update_fields["transaction_return_requested"] = (
                        1 if payload.get("transaction_return_requested") else 0
                    )
                # return note / status live on transaction_return_requests

                if update_fields:
                    update_response = db.update(
                        "every_circle.transactions",
                        {"transaction_uid": transaction_uid},
                        update_fields,
                    )
                    if update_response.get("code") != 200:
                        response["message"] = update_response.get(
                            "message", "Failed to update transaction"
                        )
                        response["code"] = update_response.get("code", 500)
                        return response, response["code"]

                response["message"] = "Transaction updated successfully"
                response["code"] = 200
                response["transaction_uid"] = transaction_uid
                response.update(update_fields)
                response["delivery_verification_items"] = updated_lines
                response["all_items_received"] = all_received

                from account_screen_v3 import build_buyer_purchase_row_v3

                tz_name = request.args.get("timezone") or request.args.get("tz")
                purchase_row = build_buyer_purchase_row_v3(
                    db,
                    buyer_profile_id,
                    transaction_uid,
                    tz_name=tz_name,
                )
                if purchase_row:
                    response["purchase_row"] = purchase_row
                return response, 200

        except Exception as e:
            print(f"Error in Transactions PUT (delivery verification): {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500


def _batch_order_bounty_paid(db, transaction_uids):
    """Sum of all bounty rows on a sale (what the business paid out)."""
    uids = [u for u in (transaction_uids or []) if u]
    if not uids:
        return {}
    placeholders = ", ".join(["%s"] * len(uids))
    q = db.execute(
        f"""
        SELECT ti.ti_transaction_id AS transaction_uid,
               COALESCE(SUM(tb.tb_amount), 0) AS order_bounty_paid
        FROM every_circle.transactions_items ti
        LEFT JOIN every_circle.transactions_bounty tb ON tb.tb_ti_id = ti.ti_uid
        WHERE ti.ti_transaction_id IN ({placeholders})
        GROUP BY ti.ti_transaction_id
        """,
        tuple(uids),
    )
    out = {}
    for row in q.get("result") or []:
        out[row.get("transaction_uid")] = round(_to_float(row.get("order_bounty_paid")), 4)
    return out


def _batch_return_requests(db, transaction_uids):
    """
    Load return-request rows keyed by sale transaction_uid.
    Value is a list of hydrated requests (newest first).
    """
    uids = [u for u in (transaction_uids or []) if u]
    if not uids:
        return {}
    placeholders = ", ".join(["%s"] * len(uids))
    q = db.execute(
        f"""
        SELECT {_TRR_SELECT_COLS}
        FROM every_circle.transaction_return_requests
        WHERE trr_transaction_uid IN ({placeholders})
        ORDER BY trr_created_at DESC, trr_updated_at DESC
        """,
        tuple(uids),
    )
    out = {}
    for row in q.get("result") or []:
        hydrated = _hydrate_return_request_row(row)
        sale_uid = hydrated.get("trr_transaction_uid")
        out.setdefault(sale_uid, []).append(hydrated)
    return out


def _line_bounty_totals(db, ti_uids):
    uids = [u for u in (ti_uids or []) if u]
    if not uids:
        return {}
    placeholders = ", ".join(["%s"] * len(uids))
    q = db.execute(
        f"""
        SELECT tb_ti_id, COALESCE(SUM(tb_amount), 0) AS line_bounty
        FROM every_circle.transactions_bounty
        WHERE tb_ti_id IN ({placeholders})
        GROUP BY tb_ti_id
        """,
        tuple(uids),
    )
    return {
        row.get("tb_ti_id"): _to_float(row.get("line_bounty"))
        for row in (q.get("result") or [])
    }


def _order_bounty_paid(db, order_uid):
    """Total bounty paid on a sale (sum of transactions_bounty on all lines)."""
    if not order_uid:
        return 0.0
    return _batch_order_bounty_paid(db, [order_uid]).get(order_uid, 0.0)



def _fetch_ti_row_for_bounty(db, ti_uid, order_uid):
    """Load sale line with catalog bounty from business_services or profile_expertise."""
    if not ti_uid:
        return None
    params = [ti_uid]
    order_clause = ""
    if order_uid:
        order_clause = " AND ti.ti_transaction_id = %s"
        params.append(order_uid)
    q = db.execute(
        f"""
        SELECT
            ti.*,
            bs.bs_bounty,
            bs.bs_bounty_type,
            pe.profile_expertise_bounty,
            pe.profile_expertise_bounty_type
        FROM every_circle.transactions_items ti
        LEFT JOIN every_circle.business_services bs
            ON ti.ti_bs_id = bs.bs_uid
        LEFT JOIN every_circle.profile_expertise pe
            ON ti.ti_bs_id = pe.profile_expertise_uid
        WHERE ti.ti_uid = %s{order_clause}
        LIMIT 1
        """,
        tuple(params),
    )
    rows = q.get("result") or []
    return rows[0] if rows else None


def _catalog_bounty_unit_and_type(ti_row):
    if not ti_row:
        return 0.0, "per_item"
    unit = _to_float(
        ti_row.get("bs_bounty")
        or ti_row.get("ti_bs_bounty")
        or ti_row.get("profile_expertise_bounty")
    )
    bounty_type = str(
        ti_row.get("bs_bounty_type")
        or ti_row.get("ti_bs_bounty_type")
        or ti_row.get("profile_expertise_bounty_type")
        or "per_item"
    ).strip().lower()
    return unit, bounty_type


def _seller_bounty_pool_for_line_row(ti_row):
    """
    Seller bounty pool for one sale line (matches bounty_results.bounty_paid).
    per_item: unit bounty × purchased qty; total: flat line bounty.
    Supports business_services (250-*) and profile_expertise (150-*).
    """
    if not ti_row:
        return 0.0
    unit, bounty_type = _catalog_bounty_unit_and_type(ti_row)
    if unit <= 0:
        return 0.0
    original_qty = max(1, int(ti_row.get("ti_bs_qty") or 0))
    if bounty_type == "total":
        return unit
    return unit * original_qty


def _seller_bounty_to_reclaim_for_line(ti_row, return_qty, *, line_bounty_ledger=None):
    """Seller bounty reversed for a partial/full line return."""
    if not ti_row:
        return 0.0
    try:
        rq = int(return_qty)
    except (TypeError, ValueError):
        return 0.0
    if rq < 1:
        return 0.0
    original_qty = int(ti_row.get("ti_bs_qty") or 0)
    scale = _bounty_scale_for_line(rq, original_qty)
    if scale is None:
        return 0.0

    ledger_pool = round(_to_float(line_bounty_ledger), 4)
    if ledger_pool > 0:
        return round(ledger_pool * scale, 4)

    line_pool = _seller_bounty_pool_for_line_row(ti_row)
    if line_pool > 0:
        _unit, bounty_type = _catalog_bounty_unit_and_type(ti_row)
        if bounty_type == "total":
            return round(line_pool * scale, 4)
        unit, _ = _catalog_bounty_unit_and_type(ti_row)
        return round(unit * rq, 4)
    return 0.0


def _bounty_to_reclaim_for_line(
    db,
    order_uid,
    ti_uid,
    return_qty,
    *,
    ti_row=None,
    line_bounty_ledger=None,
):
    """
    Seller bounty to reclaim for one returned sale line.

    Priority:
      1. transactions_bounty ledger on the line × return ratio
      2. Catalog bounty (250-* services or 150-* expertise) × return qty/ratio
      3. Single-line order fallback: order_bounty_paid × return ratio
    """
    try:
        rq = int(return_qty or 0)
    except (TypeError, ValueError):
        return 0.0
    if rq < 1 or not ti_uid:
        return 0.0

    if ti_row is None:
        ti_row = _fetch_ti_row_for_bounty(db, ti_uid, order_uid)
    if not ti_row:
        return 0.0

    if line_bounty_ledger is None:
        line_bounty_ledger = _line_bounty_totals(db, [ti_uid]).get(ti_uid, 0.0)

    reclaim = _seller_bounty_to_reclaim_for_line(
        ti_row, rq, line_bounty_ledger=line_bounty_ledger
    )
    if reclaim > 0:
        return round(reclaim, 4)

    original_qty = int(ti_row.get("ti_bs_qty") or 0)
    scale = _bounty_scale_for_line(rq, original_qty)
    if scale is None:
        return 0.0

    _db_66 = db
    _order_uid_65 = order_uid
    if not _order_uid_65:
        _r__sale_line_count_64 = 0
    else:
        q = _db_66.execute('\n        SELECT COUNT(*) AS line_count\n        FROM every_circle.transactions_items\n        WHERE ti_transaction_id = %s\n        ', (_order_uid_65,))
        rows = q.get('result') or []
        if not rows:
            _r__sale_line_count_64 = 0
        else:
            try:
                _r__sale_line_count_64 = int(rows[0].get('line_count') or 0)
            except (TypeError, ValueError):
                _r__sale_line_count_64 = 0
    if _r__sale_line_count_64 == 1:
        order_bounty = _order_bounty_paid(db, order_uid)
        if order_bounty > 0:
            return round(order_bounty * scale, 4)

    return 0.0


def _bounty_to_reclaim_for_items(db, order_uid, items_payload):
    """Seller bounty to reclaim — scaled by return qty per line."""
    if not items_payload:
        return 0.0
    ti_uids = [
        e.get("transaction_item_uid")
        for e in items_payload
        if e.get("transaction_item_uid")
    ]
    line_bounties = _line_bounty_totals(db, ti_uids)
    total = 0.0
    for entry in items_payload:
        ti_uid = entry.get("transaction_item_uid")
        if not ti_uid:
            continue
        try:
            rq = int(entry.get("return_quantity"))
        except (TypeError, ValueError):
            continue
        if rq < 1:
            continue
        total += _bounty_to_reclaim_for_line(
            db,
            order_uid,
            ti_uid,
            rq,
            line_bounty_ledger=line_bounties.get(ti_uid, 0.0),
        )
    return round(total, 4)




def _pending_return_payload_for_sale(db, sale_row, pending, *, compact=True):
    """
    Build pending_return object for a seller sale / synthetic return row.

    compact=True (list views): drop aliased status fields, nulls, and fields that
    duplicate the items[] array.
    """
    if not pending:
        return None

    order_uid = (
        sale_row.get("trr_transaction_uid")
        or sale_row.get("transaction_original_uid")
        or sale_row.get("transaction_uid")
        or pending.get("trr_transaction_uid")
    )
    if not order_uid:
        print(
            "Error: _pending_return_payload_for_sale could not resolve parent sale "
            f"uid (trr_uid={pending.get('trr_uid')!r})"
        )
        return None
    items = pending.get("items") or []
    ti_uids = [
        e.get("transaction_item_uid") or e.get("ti_uid")
        for e in items
        if e.get("transaction_item_uid") or e.get("ti_uid")
    ]
    name_map = _sale_item_names_by_ti(db, order_uid, ti_uids)
    enriched_items = []
    for entry in items:
        item = dict(entry)
        ti_uid = item.get("transaction_item_uid") or item.get("ti_uid")
        looked = name_map.get(ti_uid) or {}
        if looked.get("item_name") and not item.get("item_name"):
            item["item_name"] = looked["item_name"]
        if looked.get("ti_bs_id") and not item.get("ti_bs_id"):
            item["ti_bs_id"] = looked["ti_bs_id"]
        if looked.get("ti_bs_cost") is not None and item.get("ti_bs_cost") is None:
            item["ti_bs_cost"] = looked["ti_bs_cost"]
        enriched_items.append(item)
    cancel_only = bool(
        pending.get("cancel_unshipped")
        or pending.get("pre_ship_cancel")
        or pending.get("is_cancel_before_ship")
    )
    from line_commerce_fields import expand_return_line_splits

    expanded_items = []
    for item in enriched_items:
        ti_uid = item.get("transaction_item_uid") or item.get("ti_uid")
        ti_row = _fetch_ti_row_for_bounty(db, ti_uid, order_uid) if ti_uid else None
        expanded_items.extend(
            expand_return_line_splits(
                db,
                order_uid,
                item,
                ti_row=ti_row,
                cancel_only=cancel_only,
            )
        )
    items = expanded_items

    estimated_refund = None
    stored_json = pending.get("trr_estimated_refund_json")
    if stored_json:
        try:
            estimated_refund = json.loads(stored_json)
        except (TypeError, ValueError, json.JSONDecodeError):
            estimated_refund = None

    if estimated_refund is None and enriched_items:
        ok, _err, ctx = _validate_and_price_return_items(
            db,
            order_uid,
            [
                {
                    **entry,
                    "_order_cancel": bool(
                        pending.get("cancel_unshipped")
                        or pending.get("pre_ship_cancel")
                    ),
                }
                for entry in enriched_items
            ],
            exclude_trr_uid=pending.get("trr_uid"),
            enforce_return_eligibility=False,
        )
        if ok:
            refund_meta = _refund_breakdown_from_context(sale_row, ctx)
            estimated_refund = _estimated_refund_api_payload(
                refund_meta, compact=compact
            )
        else:
            stored = _to_float(pending.get("trr_estimated_total"))
            if stored:
                estimated_refund = {"total": round(stored, 4)}

    _db_45 = db
    _order_uid_46 = order_uid
    _pending_42 = pending
    _items_43 = enriched_items
    stored_value = _pending_42.get('trr_bounty_to_reclaim')
    if stored_value is None:
        _stored_44 = None
    else:
        amount = round(_to_float(stored_value), 4)
        _stored_44 = amount if amount > 0 else None
    if _stored_44 is not None:
        bounty_to_reclaim = _stored_44
    else:
        bounty_to_reclaim = _bounty_to_reclaim_for_items(_db_45, _order_uid_46, _items_43)

    note = pending.get("trr_note") or pending.get("note")
    payload = {
        "trr_uid": pending.get("trr_uid"),
        "note": note,
        "trr_note": note,
        "items": items,
        "estimated_refund": estimated_refund,
        "bounty_to_reclaim": bounty_to_reclaim,
        "created_at": pending.get("trr_created_at"),
        "cancel_unshipped": bool(pending.get("cancel_unshipped")),
        "pre_ship_cancel": bool(pending.get("pre_ship_cancel") or pending.get("cancel_unshipped")),
        "is_cancel_before_ship": bool(
            pending.get("is_cancel_before_ship") or pending.get("cancel_unshipped")
        ),
    }
    seller_note = pending.get("trr_seller_note") or pending.get("seller_note")
    if seller_note:
        payload["seller_note"] = seller_note
    if estimated_refund and estimated_refund.get("total") is not None:
        payload["estimated_total"] = estimated_refund["total"]

    if compact:
        payload.update(_return_request_public_payload(pending))
        return _omit_empty(payload)

    payload["seller_note"] = seller_note
    payload["transaction_item_uid"] = pending.get("transaction_item_uid") or pending.get(
        "trr_ti_uid"
    )
    payload["return_quantity"] = (
        pending.get("return_quantity")
        if pending.get("return_quantity") is not None
        else pending.get("trr_return_quantity")
    )
    payload["return_transaction_uid"] = pending.get("trr_return_transaction_uid")
    payload["stripe_refund_id"] = pending.get("trr_stripe_refund_id")
    payload["updated_at"] = pending.get("trr_updated_at")
    payload.update(_return_request_public_payload(pending))
    return payload


def _sale_item_names_by_ti(db, order_uid, ti_uids):
    """Map sale transaction_item_uid → human item name for pending return rows."""
    uids = [u for u in (ti_uids or []) if u]
    if not order_uid or not uids:
        return {}
    placeholders = ", ".join(["%s"] * len(uids))
    q = db.execute(
        f"""
        SELECT
            ti.ti_uid,
            ti.ti_bs_id,
            ti.ti_bs_cost,
            ti.ti_bs_cost_currency,
            CASE
                WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_service_name
                WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_title
                WHEN ti.ti_bs_id LIKE '165-%%' THEN pw.profile_wish_title
                ELSE ti.ti_bs_id
            END AS item_name
        FROM every_circle.transactions_items ti
        LEFT JOIN every_circle.business_services bs
            ON ti.ti_bs_id = bs.bs_uid
        LEFT JOIN every_circle.profile_expertise pe
            ON ti.ti_bs_id = pe.profile_expertise_uid
        LEFT JOIN every_circle.wish_response wr
            ON ti.ti_bs_id = wr.wish_response_uid
        LEFT JOIN every_circle.profile_wish pw
            ON wr.wr_profile_wish_id = pw.profile_wish_uid
        WHERE ti.ti_transaction_id = %s
          AND ti.ti_uid IN ({placeholders})
        """,
        tuple([order_uid] + uids),
    )
    out = {}
    for row in q.get("result") or []:
        out[row.get("ti_uid")] = {
            "item_name": row.get("item_name"),
            "ti_bs_id": row.get("ti_bs_id"),
            "ti_bs_cost": row.get("ti_bs_cost"),
            "ti_bs_cost_currency": row.get("ti_bs_cost_currency"),
        }
    return out






def _enrich_list_transaction_rows(db, rows):
    """
    Attach return/refund status + pending_return summary for Account Screen lists
    (personal purchases and business seller_transactions).

    Ensures completed reverse return transactions appear as first-class Return rows
    with parent sale linkage (trr_transaction_uid / transaction_original_uid),
    negative total, item lines, and returned/refunded statuses.
    Also injects synthetic Return rows for open (not yet confirmed) requests.
    """
    if not rows:
        return rows

    sale_uids = []
    return_tx_uids = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        is_return = _is_return_list_row(row)
        if is_return:
            uid, err = _resolve_parent_sale_uid(row, context="list enrich scan")
            if err:
                # Skip linking; row still returned later with error marker.
                pass
            if row.get("transaction_uid"):
                return_tx_uids.append(row.get("transaction_uid"))
        else:
            uid = row.get("transaction_uid")
        if uid:
            sale_uids.append(uid)

    bounty_map = _batch_order_bounty_paid(db, sale_uids)
    return_req_map = _batch_return_requests(db, sale_uids)
    _db_119 = db
    _return_tx_uids_120 = return_tx_uids
    uids = [u for u in _return_tx_uids_120 or [] if u]
    if not uids:
        return_lines_map = {}
    else:
        placeholders = ', '.join(['%s'] * len(uids))
        q = _db_119.execute(f"\n        SELECT\n            ti.ti_transaction_id AS return_transaction_uid,\n            ti.ti_uid,\n            ti.ti_original_ti_uid,\n            ti.ti_bs_id,\n            ti.ti_bs_qty,\n            ti.ti_bs_cost,\n            CASE\n                WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_service_name\n                WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_title\n                WHEN ti.ti_bs_id LIKE '165-%%' THEN pw.profile_wish_title\n                ELSE ti.ti_bs_id\n            END AS item_name\n        FROM every_circle.transactions_items ti\n        LEFT JOIN every_circle.business_services bs\n            ON ti.ti_bs_id = bs.bs_uid\n        LEFT JOIN every_circle.profile_expertise pe\n            ON ti.ti_bs_id = pe.profile_expertise_uid\n        LEFT JOIN every_circle.wish_response wr\n            ON ti.ti_bs_id = wr.wish_response_uid\n        LEFT JOIN every_circle.profile_wish pw\n            ON wr.wr_profile_wish_id = pw.profile_wish_uid\n        WHERE ti.ti_transaction_id IN ({placeholders})\n        ORDER BY ti.ti_uid\n        ", tuple(uids))
        _out_117 = {}
        for _row_118 in q.get('result') or []:
            tx_uid = _row_118.get('return_transaction_uid')
            qty = int(_to_float(_row_118.get('ti_bs_qty')))
            return_shipped_qty, cancel_unshipped_qty = _return_ledger_line_split(_db_119, tx_uid, _row_118)
            _out_117.setdefault(tx_uid, []).append({'ti_uid': _row_118.get('ti_uid'), 'ti_original_ti_uid': _row_118.get('ti_original_ti_uid'), 'transaction_item_uid': _row_118.get('ti_original_ti_uid') or _row_118.get('ti_uid'), 'ti_bs_id': _row_118.get('ti_bs_id'), 'item_name': _row_118.get('item_name'), 'quantity': qty, 'return_quantity': abs(qty), 'return_shipped_qty': return_shipped_qty, 'cancel_unshipped_qty': cancel_unshipped_qty, 'unit_cost': _to_float(_row_118.get('ti_bs_cost'))})
        return_lines_map = _out_117

    ledger_to_reqs = {}
    for reqs in return_req_map.values():
        for req in reqs:
            ledger_uid = req.get("trr_return_transaction_uid")
            if ledger_uid:
                ledger_to_reqs.setdefault(ledger_uid, []).append(req)

    enriched = []
    sales_by_uid = {}
    for row in rows:
        if not isinstance(row, dict):
            enriched.append(row)
            continue

        out = dict(row)
        out.pop("order_uid", None)
        is_return = _is_return_list_row(out)
        if is_return:
            sale_uid, parent_err = _resolve_parent_sale_uid(
                out, context="list enrich return row"
            )
            if parent_err:
                out["parent_sale_resolve_error"] = parent_err
        else:
            sale_uid = out.get("transaction_uid")

        out["order_bounty_paid"] = bounty_map.get(sale_uid, 0.0) if sale_uid else 0.0
        reqs = return_req_map.get(sale_uid) or [] if sale_uid else []
        open_reqs = [
            r
            for r in reqs
            if _is_open_return(r.get("return_status"), r.get("refund_status"))
        ]

        if is_return:
            linked_reqs = ledger_to_reqs.get(out.get("transaction_uid")) or []
            linked = linked_reqs[0] if linked_reqs else None
            _out_121 = out
            linked_req = linked
            return_lines = return_lines_map.get(out.get('transaction_uid')) or []
            _db_123 = db
            _out_121['transaction_type'] = 'return'
            _out_121['is_return'] = True
            _out_121['is_pending_return'] = False
            _out_121.pop('order_uid', None)
            parent_sale, _parent_err_122 = _resolve_parent_sale_uid({**_out_121, 'trr_transaction_uid': (linked_req or {}).get('trr_transaction_uid') or _out_121.get('trr_transaction_uid'), 'is_return': True}, context='completed return list row')
            if parent_sale:
                _out_121['transaction_original_uid'] = parent_sale
                if linked_req and linked_req.get('trr_transaction_uid'):
                    _out_121['trr_transaction_uid'] = linked_req.get('trr_transaction_uid')
            elif _parent_err_122:
                _out_121['parent_sale_resolve_error'] = _parent_err_122
            if linked_req:
                rs, fs = _pair_for_sale(_out_121, linked_req)
                _out_121.update(_status_payload(rs, fs))
                _out_121['trr_uid'] = linked_req.get('trr_uid')
                cancel_flag = _is_cancel_unshipped_request(linked_req)
                _out_121['cancel_unshipped'] = cancel_flag
                _out_121['pre_ship_cancel'] = cancel_flag
                _out_121['is_cancel_before_ship'] = cancel_flag
            elif not _out_121.get('return_status') and (not _out_121.get('refund_status')):
                _out_121.update(_status_payload(RETURN_STATUS_RETURNED, REFUND_STATUS_REFUNDED))
            lines = return_lines or []
            if _db_123 and parent_sale and lines:
                from line_commerce_fields import collapse_return_lines_for_list_row
                lines = collapse_return_lines_for_list_row(lines)
            _out_121['return_lines'] = lines
            _out_121['lines'] = lines
            if lines and (not _out_121.get('purchased_item')):
                _out_121['purchased_item'] = ', '.join((str(l.get('item_name') or l.get('ti_bs_id') or '') for l in lines if l.get('item_name') or l.get('ti_bs_id')))
            qty_sum = sum((int(l.get('return_quantity') or 0) for l in lines))
            if qty_sum and _out_121.get('ti_bs_qty') is None:
                _out_121['ti_bs_qty'] = -qty_sum
            _out_121['return_quantity_total'] = qty_sum or abs(int(_to_float(_out_121.get('ti_bs_qty'))))
            _out_121['refund_amount'] = abs(_to_float(_out_121.get('transaction_total')))
            _out_121['pending_return'] = None
            out = _out_121
            if linked_reqs:
                out["trr_uids"] = [
                    r.get("trr_uid") for r in linked_reqs if r.get("trr_uid")
                ]
        else:
            out["is_return"] = False
            out["is_pending_return"] = False
            if sale_uid:
                sales_by_uid[sale_uid] = out
            if open_reqs:
                _clear_parent_sale_return_status(out)
                pending_returns_payload = [
                    _pending_return_payload_for_sale(db, out, req, compact=True)
                    for req in open_reqs
                ]
                pending_returns_payload = [
                    p for p in pending_returns_payload if p
                ]
                if pending_returns_payload:
                    out["pending_returns"] = pending_returns_payload
                    out["pending_return"] = pending_returns_payload[0]
                    note = pending_returns_payload[0].get("note") or pending_returns_payload[
                        0
                    ].get("trr_note")
                    if note:
                        out["transaction_return_note"] = note
                return_items = []
                for req in open_reqs:
                    return_items.extend(req.get("items") or [])
                if return_items:
                    ti_uids = [
                        e.get("transaction_item_uid") or e.get("ti_uid")
                        for e in return_items
                        if e.get("transaction_item_uid") or e.get("ti_uid")
                    ]
                    name_map = _sale_item_names_by_ti(db, sale_uid, ti_uids)
                    enriched_return_items = []
                    for entry in return_items:
                        item = dict(entry)
                        ti_uid = item.get("transaction_item_uid") or item.get("ti_uid")
                        looked = name_map.get(ti_uid) or {}
                        if looked.get("item_name") and not item.get("item_name"):
                            item["item_name"] = looked["item_name"]
                        if looked.get("ti_bs_cost") is not None and item.get(
                            "ti_bs_cost"
                        ) is None:
                            item["ti_bs_cost"] = looked["ti_bs_cost"]
                        enriched_return_items.append(item)
                    from line_commerce_fields import expand_return_lines_list

                    out["transaction_return_items"] = expand_return_lines_list(
                        db, sale_uid, enriched_return_items
                    )
            else:
                out.pop("pending_returns", None)
                out.pop("pending_return", None)

        enriched.append(out)

    synthetic = []
    for sale_uid, sale_row in sales_by_uid.items():
        _reqs_77 = return_req_map.get(sale_uid) or []
        batches = []
        index_by_key = {}
        for _req_78 in _reqs_77 or []:
            if not _is_open_return(_req_78.get('return_status'), _req_78.get('refund_status')):
                continue
            if _req_78.get('trr_return_transaction_uid'):
                continue
            key = (str(_req_78.get('trr_transaction_uid') or ''), str(_req_78.get('trr_created_at') or ''))
            if key not in index_by_key:
                index_by_key[key] = len(batches)
                batches.append([])
            batches[index_by_key[key]].append(_req_78)
        _r__group_open_return_batches_76 = batches
        for batch in _r__group_open_return_batches_76:
            for req in batch:
                _db_161 = db
                _sale_row_165 = sale_row
                pending_reqs = [req]
                if isinstance(pending_reqs, dict):
                    pending_reqs = [pending_reqs]
                pending_reqs = [_p_154 for _p_154 in pending_reqs or [] if _p_154]
                if not pending_reqs:
                    row = None
                else:
                    order_uid = _sale_row_165.get('transaction_uid')
                    if not order_uid:
                        print(f'Error: _synthetic_pending_return_row missing sale transaction_uid (trr_uids={[_p_154.get('trr_uid') for _p_154 in pending_reqs]!r})')
                        row = None
                    else:
                        primary = pending_reqs[0]
                        trr_uids = [_p_154.get('trr_uid') for _p_154 in pending_reqs if _p_154.get('trr_uid')]
                        pending_payloads = [_pending_return_payload_for_sale(_db_161, _sale_row_165, _p_154, compact=True) for _p_154 in pending_reqs]
                        credit = 0.0
                        bounty_total = 0.0
                        for payload, _req_158 in zip(pending_payloads, pending_reqs):
                            if payload and payload.get('estimated_refund'):
                                estimated_refund = payload['estimated_refund']
                                if not isinstance(estimated_refund, dict):
                                    _r__estimated_refund_total_5 = 0.0
                                else:
                                    _r__estimated_refund_total_5 = _to_float(estimated_refund.get('total'))
                                credit += _r__estimated_refund_total_5
                            elif _req_158.get('trr_estimated_total') is not None:
                                credit += _to_float(_req_158.get('trr_estimated_total'))
                            if payload:
                                bounty_total += _to_float(payload.get('bounty_to_reclaim'))
                        _req_34 = primary
                        _req_7 = _req_34
                        from order_display import build_return_request_display
                        result = build_return_request_display(_req_7)
                        api = result or {}
                        status_fields = {'return_status': api.get('return_status'), 'refund_status': api.get('refund_status'), 'display_status': api.get('display_status')}
                        _cancel_flag_162 = any((_is_cancel_unshipped_request(_p_154) for _p_154 in pending_reqs))
                        items = []
                        for _req_158 in pending_reqs:
                            items.extend(_req_158.get('items') or [])
                        _ti_uids_159 = [_e_160.get('transaction_item_uid') for _e_160 in items if _e_160.get('transaction_item_uid')]
                        _name_map_163 = _sale_item_names_by_ti(_db_161, order_uid, _ti_uids_159)
                        item_names = []
                        raw_lines = []
                        qty_total = 0
                        for _req_158 in pending_reqs:
                            req_cancel = _is_cancel_unshipped_request(_req_158)
                            for _entry_164 in _req_158.get('items') or []:
                                _ti_uid_155 = _entry_164.get('transaction_item_uid') or _entry_164.get('ti_uid')
                                try:
                                    rq = int(_entry_164.get('return_quantity') or 0)
                                except (TypeError, ValueError):
                                    rq = 0
                                qty_total += abs(rq)
                                looked_up = _name_map_163.get(_ti_uid_155) or {}
                                name = _entry_164.get('item_name') or _entry_164.get('bs_service_name') or looked_up.get('item_name')
                                if name:
                                    item_names.append(str(name))
                                line_entry = {'ti_uid': _ti_uid_155, 'transaction_item_uid': _ti_uid_155, 'ti_bs_id': looked_up.get('ti_bs_id') or _entry_164.get('ti_bs_id'), 'item_name': name, 'return_quantity': abs(rq), 'ti_bs_cost': looked_up.get('ti_bs_cost') or _entry_164.get('ti_bs_cost'), 'ti_bs_cost_currency': looked_up.get('ti_bs_cost_currency') or _entry_164.get('ti_bs_cost_currency')}
                                if _entry_164.get('return_shipped_qty') is not None:
                                    line_entry['return_shipped_qty'] = int(_entry_164.get('return_shipped_qty') or 0)
                                if _entry_164.get('cancel_unshipped_qty') is not None:
                                    line_entry['cancel_unshipped_qty'] = int(_entry_164.get('cancel_unshipped_qty') or 0)
                                if _req_158.get('trr_uid'):
                                    line_entry['trr_uid'] = _req_158.get('trr_uid')
                                _apply_return_item_split(line_entry, cancel_only=req_cancel)
                                raw_lines.append(line_entry)
                        from line_commerce_fields import collapse_return_lines_for_list_row
                        _return_lines_156 = collapse_return_lines_for_list_row(raw_lines)
                        api_status = _return_request_public_payload(primary, qty=qty_total)
                        subtotal = round(sum((_to_float((_p_154.get('estimated_refund') or {}).get('subtotal')) for _p_154 in pending_payloads if _p_154)), 4)
                        taxes = round(sum((_to_float((_p_154.get('estimated_refund') or {}).get('taxes')) for _p_154 in pending_payloads if _p_154)), 4)
                        shipping_refund = round(sum((_to_float((_p_154.get('estimated_refund') or {}).get('shipping_refund')) for _p_154 in pending_payloads if _p_154)), 4)
                        fees_allocated = round(sum((_to_float((_p_154.get('estimated_refund') or {}).get('fees_allocated')) for _p_154 in pending_payloads if _p_154)), 4)
                        credit = round(credit, 4)
                        batch_estimated_refund = {'subtotal': subtotal, 'taxes': taxes, 'shipping_refund': shipping_refund, 'fees_allocated': fees_allocated, 'total': credit, 'total_customer_credit': credit}
                        primary_ti_bs_id = None
                        if _return_lines_156:
                            primary_ti_bs_id = _return_lines_156[0].get('ti_bs_id')
                        _row_157 = {'trr_uids': trr_uids, 'trr_transaction_uid': order_uid, 'transaction_uid': trr_uids[0] if len(trr_uids) == 1 else None, 'order_uid': order_uid, 'original_transaction_uid': order_uid, 'transaction_type': 'return', 'is_return': 1, 'is_pending_return': True, 'transaction_datetime': primary.get('trr_created_at') or _sale_row_165.get('transaction_datetime'), 'transaction_total': f'{-abs(credit):.4f}', 'seller_id': _sale_row_165.get('seller_id') or _sale_row_165.get('transaction_business_id'), 'business_name': _sale_row_165.get('business_name'), 'transaction_profile_id': _sale_row_165.get('transaction_profile_id'), 'transaction_return_note': primary.get('trr_note'), 'purchased_item': ', '.join(item_names) if item_names else None, 'ti_bs_id': primary_ti_bs_id, 'ti_bs_qty': qty_total, 'return_lines': _return_lines_156, 'return_quantity_total': qty_total, 'refund_amount': round(abs(credit), 4), 'bounty_to_reclaim': round(bounty_total, 4), 'cancel_unshipped': _cancel_flag_162, 'pre_ship_cancel': _cancel_flag_162, 'is_cancel_before_ship': _cancel_flag_162, 'estimated_total': credit, 'estimated_refund': batch_estimated_refund, **status_fields}
                        if api_status.get('display'):
                            _row_157['display'] = api_status['display']
                        if len(trr_uids) == 1:
                            _row_157['trr_uid'] = trr_uids[0]
                            _row_157['transaction_uid'] = trr_uids[0]
                        row = _omit_empty(_row_157)
                if row:
                    synthetic.append(row)

    if synthetic:
        enriched.extend(synthetic)
        enriched.sort(
            key=lambda r: (
                str(r.get("transaction_datetime") or ""),
                str(
                    r.get("transaction_uid")
                    or r.get("trr_uid")
                    or (r.get("trr_uids") or [None])[0]
                    or ""
                ),
            ),
            reverse=True,
        )

    return enriched


# Back-compat alias used by seller path
def _enrich_seller_transaction_rows(db, rows):
    return _enrich_list_transaction_rows(db, rows)


class SellerTransactions(Resource):

    def get(self, profile_id=None):
        print(f"In SellerTransactions GET with profile_id: {profile_id}")
        response = {}

        try:
            if not profile_id:
                response["message"] = "profile_id is required"
                response["code"] = 400
                return response, 400

            with connect() as db:
                ensure_fulfillment_list_rollups(db)
                fulfillment_summary = fulfillment_list_summary_sql("ti")
                # Execute query to get transactions
                query = (
                    """
                    SELECT
                        t.transaction_uid,
                        t.transaction_original_uid,
                        COALESCE(t.transaction_type, 'sale') AS transaction_type,
                        (COALESCE(t.transaction_type, 'sale') = 'return') AS is_return,
                        t.transaction_datetime,
                        t.transaction_total,
                        t.transaction_amount,
                        t.transaction_taxes,
                        t.transaction_fees,
                        t.transaction_shipping,
                        t.transaction_business_id AS seller_id,
                        t.transaction_profile_id,
                        t.transaction_in_escrow,
                        t.transaction_return_requested,
                        t.transaction_return_note,
                        
                        -- ti.*,
                        CASE
                            WHEN ti.ti_bs_id LIKE '250-%%' THEN biz.business_name
                            WHEN ti.ti_bs_id LIKE '150-%%' THEN
                                CONCAT(expertise_pp.profile_personal_first_name, ' ', expertise_pp.profile_personal_last_name)
                            WHEN ti.ti_bs_id LIKE '165-%%' THEN
                                CONCAT(wish_pp.profile_personal_first_name, ' ', wish_pp.profile_personal_last_name)
                            ELSE NULL
                        END AS business_name,
                        CASE
                            WHEN ti.ti_bs_id LIKE '250-%%' THEN 'Business'
                            WHEN ti.ti_bs_id LIKE '150-%%' THEN 'Offering'
                            WHEN ti.ti_bs_id LIKE '165-%%' THEN 'Seeking'
                            ELSE 'Unknown'
                        END AS purchase_type,
                        GROUP_CONCAT(
                            CASE
                                WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_service_name
                                WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_title
                                WHEN ti.ti_bs_id LIKE '165-%%' THEN pw.profile_wish_title
                                ELSE 'See Receipt'
                            END
                            ORDER BY ti.ti_uid
                            SEPARATOR ', '
                        ) AS purchased_item,
                        MIN(ti.ti_bs_id) AS ti_bs_id,
                        SUM(ti.ti_bs_qty) AS ti_bs_qty,
                        MIN(ti.ti_uid) AS ti_uid,
                        MAX(ti.ti_fulfillment_method) AS ti_fulfillment_method,
                        MIN(ti.ti_bs_cost) AS unit_price,
                        __FULFILLMENT_SUMMARY__,
                        MIN(buyer_pp.profile_personal_first_name) AS buyer_first_name,
                        MIN(buyer_pp.profile_personal_last_name) AS buyer_last_name,
                        MIN(buyer_u.user_email_id) AS buyer_email,
                        MIN(buyer_pp.profile_personal_email_is_public) AS buyer_email_is_public,
                        MIN(buyer_pp.profile_personal_phone_number) AS buyer_phone,
                        MIN(buyer_pp.profile_personal_phone_number_is_public) AS buyer_phone_is_public,
                        MIN(buyer_pp.profile_personal_city) AS buyer_city,
                        MIN(buyer_pp.profile_personal_state) AS buyer_state,
                        MIN(buyer_pp.profile_personal_location_is_public) AS buyer_location_is_public
                    FROM every_circle.transactions t
                    LEFT JOIN every_circle.transactions_items ti
                    ON t.transaction_uid = ti.ti_transaction_id
                    LEFT JOIN every_circle.business_services bs
                    ON ti.ti_bs_id = bs.bs_uid
                    LEFT JOIN every_circle.business biz
                    ON bs.bs_business_id = biz.business_uid
                    LEFT JOIN every_circle.profile_personal buyer_pp
                    ON t.transaction_profile_id = buyer_pp.profile_personal_uid
                    LEFT JOIN every_circle.users buyer_u
                    ON buyer_pp.profile_personal_user_id = buyer_u.user_uid
                    LEFT JOIN every_circle.profile_personal seller_pp
                    ON t.transaction_business_id = seller_pp.profile_personal_user_id
                    LEFT JOIN every_circle.profile_expertise pe
                    ON ti.ti_bs_id = pe.profile_expertise_uid
                    LEFT JOIN every_circle.profile_personal expertise_pp
                    ON pe.profile_expertise_profile_personal_id = expertise_pp.profile_personal_uid
                    LEFT JOIN every_circle.wish_response wr
                    ON ti.ti_bs_id = wr.wish_response_uid
                    LEFT JOIN every_circle.profile_wish pw
                    ON wr.wr_profile_wish_id = pw.profile_wish_uid
                    LEFT JOIN every_circle.profile_personal wish_pp
                    ON pw.profile_wish_profile_personal_id = wish_pp.profile_personal_uid
                    WHERE t.transaction_business_id = %s
                    -- WHERE t.transaction_business_id = '110-000014'
                    GROUP BY
                    t.transaction_uid,
                    t.transaction_datetime,
                    t.transaction_total,
                    t.transaction_profile_id,
                    seller_id,
                    business_name,
                    purchase_type
                    ORDER BY t.transaction_datetime DESC, ti_uid ASC
               """
                ).replace("__FULFILLMENT_SUMMARY__", fulfillment_summary)

                print(f"Executing seller query for profile_id: {profile_id}")
                result = db.execute(query, (profile_id,))
                # print(f"Seller query result: {result}")

                if result.get("code") == 200:
                    rows = _enrich_transaction_rows(result.get("result", []))
                    rows = attach_shipping_to_transaction_rows(db, rows)
                    rows = apply_order_fulfillment_summary(rows)
                    rows = sync_list_rows_fulfillment_from_context(db, rows)
                    from order_quantity_context import apply_list_verification_status

                    rows = apply_list_verification_status(db, rows)
                    rows = _enrich_list_transaction_rows(db, rows)
                    response["message"] = "Seller transactions retrieved successfully"
                    response["code"] = 200
                    response["data"] = rows
                    response["count"] = len(rows)
                    if _request_timezone():
                        response["timezone"] = _request_timezone()
                    response["datetime_storage"] = "UTC"
                else:
                    response["message"] = result.get(
                        "message", "Query execution failed"
                    )
                    response["code"] = result.get("code", 500)
                    response["error"] = result.get("error", "Unknown error")
                    return response, response["code"]

                return response, 200

        except Exception as e:
            print(f"Error in SellerTransactions GET: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500
        

class DeclinedReturns(Resource):

    def get(self):
        print("In DeclinedReturns GET")
        response = {}

        try:
            with connect() as db:
                query = """
                    SELECT
                        t.transaction_uid,
                        t.transaction_profile_id,
                        t.transaction_business_id,
                        t.transaction_datetime,
                        r.trr_uid,
                        r.trr_transaction_uid,
                        r.trr_ti_uid,
                        r.trr_return_quantity,
                        COALESCE(r.trr_note, t.transaction_return_note) AS transaction_return_note,
                        r.trr_seller_note AS transaction_return_seller_note,
                        r.trr_return_status AS return_status,
                        COALESCE(r.trr_refund_status, r.trr_status, 'rejected') AS refund_status,
                        CONCAT(p.profile_personal_first_name, ' ', p.profile_personal_last_name) AS buyer_name,
                        b.business_name AS seller_name
                    FROM every_circle.transactions t
                    INNER JOIN every_circle.transaction_return_requests r
                        ON r.trr_transaction_uid = t.transaction_uid
                    LEFT JOIN every_circle.profile_personal p
                        ON p.profile_personal_uid = t.transaction_profile_id
                    LEFT JOIN every_circle.business b
                        ON b.business_uid = t.transaction_business_id
                    WHERE COALESCE(r.trr_refund_status, r.trr_status)
                          IN ('rejected', 'declined')
                    ORDER BY t.transaction_datetime DESC
                """
                result = db.execute(query)
                print("DeclinedReturns query result:", result)

                if result.get("code") == 200:
                    rows = result.get("result", []) or []
                    for row in rows:
                        rs, fs = _normalize_status_pair(
                            row.get("return_status"),
                            row.get("refund_status"),
                        )
                        row.update(_status_payload(rs, fs))
                    response["message"] = "Rejected returns retrieved successfully"
                    response["code"] = 200
                    response["data"] = rows
                else:
                    response["message"] = "Query execution failed"
                    response["code"] = result.get("code", 500)
                    return response, response["code"]

                return response, 200

        except Exception as e:
            print(f"Error in DeclinedReturns GET: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500
        
    def put(self):
        print("In DeclinedReturns PUT")
        response = {}

        try:
            data = request.get_json()
            transaction_uid = data.get("transaction_uid")
            trr_uids = _parse_trr_uids_from_payload(data or {})
            trr_uid = trr_uids[0] if trr_uids else None
            seller_note = data.get("transaction_return_seller_note", "")

            if not transaction_uid:
                response["message"] = "transaction_uid is required"
                response["code"] = 400
                return response, 400

            action = data.get("action", "decline")

            with connect() as db:
                if action == "resolve":
                    favor = data.get("resolved_in_favor_of", "seller")
                    if favor == "buyer":
                        body, status_code = _finalize_pending_return(
                            db,
                            transaction_uid,
                            seller_note=seller_note or None,
                            trr_uids=trr_uids or None,
                            trr_uid=trr_uid,
                        )
                        if status_code == 200:
                            body["message"] = (
                                "Return resolved in buyer's favor; refund finalized"
                            )
                        return body, status_code

                    if len(trr_uids) > 1:
                        requests, resolve_err = _load_return_request_wave(
                            db, transaction_uid, trr_uids
                        )
                        if resolve_err:
                            return resolve_err, resolve_err.get("code", 400)
                    else:
                        pending, resolve_err = _resolve_return_request(
                            db, transaction_uid, trr_uid
                        )
                        if resolve_err:
                            return resolve_err, resolve_err.get("code", 400)
                        requests = [pending]
                    batch_uids = [r.get("trr_uid") for r in requests]
                    trr_uid = batch_uids[0] if batch_uids else None
                    orig_tx = _load_sale_for_return(db, transaction_uid) or {}
                    # Use first request's logistics state for final return column.
                    cur_return, _cur_refund = _pair_for_sale(orig_tx, requests[0])
                    final_return = (
                        RETURN_STATUS_RETURNED
                        if cur_return == RETURN_STATUS_RETURNED
                        else RETURN_STATUS_RETURNING
                    )
                    _update_return_statuses(
                        db,
                        transaction_uid,
                        final_return,
                        REFUND_STATUS_REJECTED,
                        trr_uids=batch_uids,
                        return_requested=0,
                        seller_note=seller_note or None,
                    )
                    from wallet_return_reservations import (
                        clear_return_reservations,
                        release_pending_after_reservation_clear,
                    )

                    clear_return_reservations(db, batch_uids)
                    if not _sale_has_other_open_returns(
                        db, transaction_uid, exclude_trr_uid=batch_uids
                    ):
                        ti_uids = set()
                        for req in requests:
                            if req.get("trr_ti_uid"):
                                ti_uids.add(req.get("trr_ti_uid"))
                        for ti_uid in ti_uids:
                            release_pending_after_reservation_clear(
                                db, transaction_uid, ti_uid
                            )
                    response["message"] = (
                        f"Return resolved in seller's favor "
                        f"({_display_return_status(final_return, REFUND_STATUS_REJECTED)})"
                    )
                    response["code"] = 200
                    response["trr_uid"] = trr_uid
                    response["trr_uids"] = batch_uids
                    response.update(
                        _status_payload(final_return, REFUND_STATUS_REJECTED)
                    )
                    return response, 200

                if len(trr_uids) > 1:
                    requests, resolve_err = _load_return_request_wave(
                        db, transaction_uid, trr_uids
                    )
                    if resolve_err:
                        return resolve_err, resolve_err.get("code", 400)
                else:
                    pending, resolve_err = _resolve_return_request(
                        db, transaction_uid, trr_uid
                    )
                    if resolve_err:
                        return resolve_err, resolve_err.get("code", 400)
                    requests = [pending]
                batch_uids = [r.get("trr_uid") for r in requests]
                trr_uid = batch_uids[0] if batch_uids else None

                _update_return_statuses(
                    db,
                    transaction_uid,
                    RETURN_STATUS_RETURNING,
                    REFUND_STATUS_REJECTED,
                    trr_uids=batch_uids,
                    return_requested=1,
                    seller_note=seller_note or None,
                )
                from wallet_return_reservations import (
                    clear_return_reservations,
                    release_pending_after_reservation_clear,
                )

                clear_return_reservations(db, batch_uids)
                if not _sale_has_other_open_returns(
                    db, transaction_uid, exclude_trr_uid=batch_uids
                ):
                    ti_uids = set()
                    for req in requests:
                        if req.get("trr_ti_uid"):
                            ti_uids.add(req.get("trr_ti_uid"))
                    for ti_uid in ti_uids:
                        release_pending_after_reservation_clear(
                            db, transaction_uid, ti_uid
                        )
                response["message"] = "Return rejected (Returning - Rejected)"
                response["code"] = 200
                response["trr_uid"] = trr_uid
                response["trr_uids"] = batch_uids
                response.update(
                    _status_payload(RETURN_STATUS_RETURNING, REFUND_STATUS_REJECTED)
                )
                return response, 200

        except Exception as e:
            print(f"Error in DeclinedReturns PUT: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500
