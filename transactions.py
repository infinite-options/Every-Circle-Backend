from aiohttp import payload
from flask_restful import Resource
from datetime import datetime, timedelta, timezone
import os
import traceback
from flask import request, jsonify
import json
import requests as http_requests

from flask_jwt_extended import get_jwt_identity, verify_jwt_in_request

from data_ec import connect, processImage
from moderation import MODERATED_ACTIVE, is_owner_available_for_public_interaction
from user_path_connection import ConnectionsPath
from escrow_release import release_escrow_for_transaction, summarize_escrow_result
from wallet_ids import EC_WALLET_ID
from wallet_service import credit_bounty_to_wallet, debit_bounty_from_wallet
from datetime_utils import utc_now_str, enrich_datetime_fields, parse_stored_datetime
from transaction_shipping import (
    normalize_shipping_address,
    insert_transaction_shipping,
    attach_shipping_to_transaction_rows,
    apply_order_fulfillment_summary,
    fulfillment_list_summary_sql,
    append_fulfillment_field,
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

_RETURN_REQUESTS_TABLE_READY = False
_RETURN_ELIGIBILITY_COLUMNS_READY = False

RETURN_INELIGIBLE_NOT_RETURNABLE = "Not returnable"
RETURN_INELIGIBLE_OUTSIDE_WINDOW = "Outside return window"


def _ensure_return_eligibility_columns(db):
    """Add ti_bs_is_returnable on transactions_items when missing."""
    global _RETURN_ELIGIBILITY_COLUMNS_READY
    if _RETURN_ELIGIBILITY_COLUMNS_READY:
        return
    db.execute(
        "ALTER TABLE every_circle.transactions_items "
        "ADD COLUMN ti_bs_is_returnable TINYINT(1) NULL DEFAULT 1",
        cmd="post",
    )
    _RETURN_ELIGIBILITY_COLUMNS_READY = True


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
    if not req:
        return False
    if req.get("cancel_unshipped") or req.get("pre_ship_cancel") or req.get(
        "is_cancel_before_ship"
    ):
        return True
    if req.get("trr_cancel_unshipped") in (1, "1", True, "true"):
        return True
    rs = (req.get("return_status") or req.get("trr_return_status") or "").strip().lower()
    return rs == RETURN_STATUS_CANCELLED


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


def _to_float(value):
    if value is None:
        return 0.0
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


def _selected_bso_ids(item):
    """Collect selected option UIDs (255-…) for ti_bso_id."""
    options = _build_selected_options(item) or []
    uids = []
    seen = set()
    for opt in options:
        bso_uid = (opt.get("bso_uid") or "").strip()
        if not bso_uid or bso_uid in seen:
            continue
        seen.add(bso_uid)
        uids.append(bso_uid)
    return uids


def _apply_item_options_to_tx_item(tx_item, item, ti_bs_id):
    """Persist selected options, special instructions, and line price from checkout."""
    options = _build_selected_options(item)
    if options:
        tx_item["ti_selected_options"] = json.dumps(options)
        bso_ids = _selected_bso_ids(item)
        if bso_ids:
            # One option → single uid; multiple groups → comma-separated.
            tx_item["ti_bso_id"] = ",".join(bso_ids)

    special = (item.get("special_instructions") or "").strip()
    if special:
        tx_item["ti_special_instructions"] = special

    if item.get("choices_extra_cost") is not None:
        tx_item["ti_choices_extra_cost"] = _to_float(item.get("choices_extra_cost"))

    unit_price = item.get("unit_price")
    if unit_price is not None and ti_bs_id and str(ti_bs_id).startswith("250"):
        tx_item["ti_bs_cost"] = _strip_currency(unit_price)


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


def _fetch_connection_path(path_from, path_to):
    """Return combined_path string or None."""
    if not path_from or not path_to:
        return None
    try:
        connections_path = ConnectionsPath()
        network_response, network_status = connections_path.get(path_from, path_to)
        if network_status != 200 or not network_response.get("combined_path"):
            print(
                f"Warning: Could not find connection path "
                f"{path_from} -> {path_to}. Status: {network_status}, "
                f"Response: {network_response}"
            )
            return None
        combined_path = network_response["combined_path"]
        print("network combined_path: ", combined_path)
        return combined_path
    except Exception as e:
        print(f"Error getting connection path: {str(e)}")
        return None


def _middle_path_nodes(combined_path, seen):
    """Nodes strictly between path endpoints, excluding known bounty recipients."""
    if not combined_path:
        return []
    try:
        uids = combined_path.split(",")
        middle = uids[1:-1] if len(uids) > 2 else []
        return [uid for uid in middle if uid and uid not in seen]
    except Exception as e:
        print(f"Error processing network path: {str(e)}")
        return []


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


def _without_zero_charity(participants):
    return [
        p
        for p in participants
        if p.get("tb_profile_id") != CHARITY_PROFILE_ID
        or _charity_share_is_payable(p.get("tb_amount"), p.get("tb_percentage"))
    ]


def _network_participants_capped(middle_uids, effective_bounty):
    """
    Split the 40% network pool across intermediaries; cap each at 20%.
    Any undistributed share goes to charity.
    """
    pool = round(_BOUNTY_NETWORK_POOL * effective_bounty, 4)
    max_per = round(_BOUNTY_NETWORK_MAX_PERSON * effective_bounty, 4)
    if not middle_uids:
        if pool <= 0:
            return []
        return [
            {
                "tb_profile_id": CHARITY_PROFILE_ID,
                **_bounty_pct_amount(effective_bounty, _BOUNTY_NETWORK_POOL),
            }
        ]

    per_person = min(round(pool / len(middle_uids), 4), max_per)
    total_paid = round(per_person * len(middle_uids), 4)
    charity_amount = round(pool - total_paid, 4)
    person_pct = round(per_person / effective_bounty, 4) if effective_bounty else 0

    participants = [
        {
            "tb_profile_id": uid,
            "tb_percentage": str(person_pct),
            "tb_amount": per_person,
        }
        for uid in middle_uids
    ]
    if charity_amount > 0:
        charity_pct = (
            round(charity_amount / effective_bounty, 4) if effective_bounty else 0
        )
        if _charity_share_is_payable(charity_amount, charity_pct):
            participants.append(
                {
                    "tb_profile_id": CHARITY_PROFILE_ID,
                    "tb_percentage": str(charity_pct),
                    "tb_amount": charity_amount,
                }
            )
    return participants


def _network_participants_business(middle_uids, effective_bounty, seen):
    """Legacy business network split: equal shares of 40%, pad with charity if < 2."""
    network_result = list(middle_uids)
    if len(network_result) < 2 and CHARITY_PROFILE_ID not in seen:
        network_result.append(CHARITY_PROFILE_ID)
    if not network_result:
        return []
    network_percentage = _BOUNTY_NETWORK_POOL / len(network_result)
    return _without_zero_charity(
        [
            {
                "tb_profile_id": uid,
                **_bounty_pct_amount(effective_bounty, network_percentage),
            }
            for uid in network_result
        ]
    )


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
               ti_shipped_at, ti_tracking_carrier, ti_tracking_number,
               ti_fulfillment_note
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s AND ti_bs_id = %s
        """,
        (transaction_uid, transaction_item_uid),
    )
    ti_rows = ti_q.get("result") or []
    return ti_rows[0] if ti_rows else None


def _all_lines_fully_received(db, transaction_uid):
    incomplete_q = db.execute(
        """
        SELECT COUNT(*) AS incomplete_count
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s
          AND ti_bs_qty > 0
          AND COALESCE(ti_received_qty, 0) < ti_bs_qty
        """,
        (transaction_uid,),
    )
    rows = incomplete_q.get("result") or []
    return int(rows[0].get("incomplete_count") or 0) == 0 if rows else False


def _ensure_return_requests_table(db):
    """Create pending-return side table once per process if missing."""
    global _RETURN_REQUESTS_TABLE_READY
    if _RETURN_REQUESTS_TABLE_READY:
        return
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS every_circle.transaction_return_requests (
            trr_uid VARCHAR(64) NOT NULL,
            trr_transaction_uid VARCHAR(64) NOT NULL,
            trr_profile_id VARCHAR(64) NOT NULL,
            trr_ti_uid VARCHAR(64) NULL,
            trr_return_quantity INT NULL,
            trr_items_json MEDIUMTEXT NULL,
            trr_note TEXT NULL,
            trr_seller_note TEXT NULL,
            trr_status VARCHAR(32) NOT NULL DEFAULT 'pending',
            trr_return_status VARCHAR(32) NOT NULL DEFAULT 'returning',
            trr_refund_status VARCHAR(32) NOT NULL DEFAULT 'pending',
            trr_cancel_unshipped TINYINT(1) NOT NULL DEFAULT 0,
            trr_estimated_total DECIMAL(18,4) NULL,
            trr_return_transaction_uid VARCHAR(64) NULL,
            trr_stripe_refund_id VARCHAR(128) NULL,
            trr_created_at DATETIME NOT NULL,
            trr_updated_at DATETIME NOT NULL,
            PRIMARY KEY (trr_uid),
            KEY idx_trr_transaction_uid (trr_transaction_uid),
            KEY idx_trr_ti_uid (trr_ti_uid)
        )
        """,
        cmd="post",
    )
    # Older table installs may lack columns / still use sale-uid PK.
    for ddl in (
        "ALTER TABLE every_circle.transaction_return_requests ADD COLUMN trr_uid VARCHAR(64) NULL",
        "ALTER TABLE every_circle.transaction_return_requests ADD COLUMN trr_return_status VARCHAR(32) NULL",
        "ALTER TABLE every_circle.transaction_return_requests ADD COLUMN trr_refund_status VARCHAR(32) NULL",
        "ALTER TABLE every_circle.transaction_return_requests ADD COLUMN trr_ti_uid VARCHAR(64) NULL",
        "ALTER TABLE every_circle.transaction_return_requests ADD COLUMN trr_return_quantity INT NULL",
        "ALTER TABLE every_circle.transaction_return_requests ADD COLUMN trr_seller_note TEXT NULL",
        "ALTER TABLE every_circle.transaction_return_requests ADD COLUMN trr_cancel_unshipped TINYINT(1) NOT NULL DEFAULT 0",
        "ALTER TABLE every_circle.transaction_return_requests MODIFY COLUMN trr_items_json MEDIUMTEXT NULL",
    ):
        db.execute(ddl, cmd="post")
    _RETURN_REQUESTS_TABLE_READY = True


def _new_trr_uid(db):
    """Allocate trr_uid via transaction_return_requests_uid stored procedure."""
    uid_resp = db.call(procedure="every_circle.transaction_return_requests_uid")
    if not uid_resp.get("result") or len(uid_resp["result"]) == 0:
        return None
    return uid_resp["result"][0].get("new_id")


def _items_from_return_request_row(row):
    """
    Build the single-item (or legacy multi-item) list for a return-request row.
    Prefer columnar trr_ti_uid / trr_return_quantity; fall back to trr_items_json.
    """
    ti_uid = row.get("trr_ti_uid")
    if ti_uid:
        try:
            qty = int(row.get("trr_return_quantity") or 0)
        except (TypeError, ValueError):
            qty = 0
        return [
            {
                "transaction_item_uid": ti_uid,
                "return_quantity": qty,
            }
        ]
    try:
        items = json.loads(row.get("trr_items_json") or "[]")
    except (TypeError, ValueError, json.JSONDecodeError):
        return []
    return items if isinstance(items, list) else []


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
    trr_stripe_refund_id, trr_created_at, trr_updated_at
"""


def _already_returned_qty(db, order_uid, ti_uid):
    q = db.execute(
        """
        SELECT COALESCE(SUM(ABS(rti.ti_bs_qty)), 0) AS returned_qty
        FROM every_circle.transactions_items rti
        INNER JOIN every_circle.transactions rt
            ON rti.ti_transaction_id = rt.transaction_uid
        WHERE rt.transaction_original_uid = %s
          AND COALESCE(rt.transaction_type, 'return') = 'return'
          AND rti.ti_original_ti_uid = %s
        """,
        (order_uid, ti_uid),
    )
    rows = q.get("result") or []
    return int(_to_float(rows[0].get("returned_qty"))) if rows else 0


def _as_trr_uid_set(exclude_trr_uid=None):
    """Normalize a single uid, list, or set into a set of trr_uid strings."""
    if not exclude_trr_uid:
        return set()
    if isinstance(exclude_trr_uid, (list, tuple, set)):
        return {u for u in exclude_trr_uid if u}
    return {exclude_trr_uid}


def _reserved_return_qty(db, order_uid, ti_uid, exclude_trr_uid=None):
    """Qty already claimed by other open return requests on this sale."""
    exclude = _as_trr_uid_set(exclude_trr_uid)
    open_reqs = _load_open_return_requests(db, order_uid)
    reserved = 0
    for req in open_reqs:
        if req.get("trr_uid") in exclude:
            continue
        # One row == one item (new); legacy rows may still list multiple in items.
        if req.get("trr_ti_uid"):
            if req.get("trr_ti_uid") != ti_uid:
                continue
            try:
                reserved += int(req.get("trr_return_quantity") or 0)
            except (TypeError, ValueError):
                continue
            continue
        for entry in req.get("items") or []:
            if entry.get("transaction_item_uid") != ti_uid:
                continue
            try:
                reserved += int(entry.get("return_quantity") or 0)
            except (TypeError, ValueError):
                continue
    return reserved


def _load_sale_for_return(db, transaction_uid):
    tx_row_q = db.execute(
        """
        SELECT transaction_uid, transaction_profile_id, transaction_business_id,
               transaction_stripe_pi, transaction_total, transaction_amount,
               transaction_taxes, transaction_fees,
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
    _ensure_return_eligibility_columns(db)

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
    lines_processed = []
    seen_ti = set()

    for entry in items_payload:
        ti_uid = entry.get("transaction_item_uid")
        try:
            rq = int(entry.get("return_quantity"))
        except (TypeError, ValueError):
            rq = -1

        if not ti_uid:
            return False, {
                "message": "Each entry requires transaction_item_uid",
                "code": 400,
            }, None
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

        ti_q = db.execute(
            """
            SELECT ti_uid, ti_transaction_id, ti_bs_id, ti_bso_id, ti_bs_qty, ti_bs_cost,
                   ti_bs_cost_currency, ti_bs_sku, ti_bs_is_taxable, ti_bs_tax_rate,
                   ti_bs_refund_policy, ti_bs_return_window_days, ti_bs_is_returnable,
                   ti_received_at, ti_selected_options, ti_special_instructions,
                   ti_choices_extra_cost
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
        already_returned = _already_returned_qty(db, original_tx_uid, ti_uid)
        reserved = _reserved_return_qty(
            db,
            original_tx_uid,
            ti_uid,
            exclude_trr_uid=exclude_trr_uid,
        )
        remaining = original_qty - already_returned - reserved
        if rq > remaining:
            return False, {
                "message": (
                    f"return_quantity exceeds remaining returnable qty for {ti_uid} "
                    f"(requested {rq}, remaining {remaining})"
                ),
                "code": 400,
            }, None

        unit_cost = _to_float(ti_row.get("ti_bs_cost"))
        scale = _bounty_scale_for_line(rq, original_qty)
        if scale is None:
            return False, {
                "message": (
                    f"return_quantity must be between 1 and {original_qty} for {ti_uid}"
                ),
                "code": 400,
            }, None

        line_subtotal = round(unit_cost * rq, 4)
        line_tax = _tax_amount_for_line(
            line_subtotal,
            ti_row.get("ti_bs_is_taxable"),
            ti_row.get("ti_bs_tax_rate"),
        )
        refund_subtotal += line_subtotal
        refund_tax += line_tax

        lines_processed.append(
            {
                "original_ti_uid": ti_uid,
                "ti_bs_id": ti_row.get("ti_bs_id"),
                "return_quantity": rq,
                "original_quantity": original_qty,
                "already_returned": already_returned,
                "unit_cost": unit_cost,
                "line_subtotal": line_subtotal,
                "line_tax": line_tax,
                "snapshot": ti_row,
            }
        )

    return True, None, {
        "order_subtotal": order_subtotal,
        "refund_subtotal": refund_subtotal,
        "refund_tax": refund_tax,
        "lines_processed": lines_processed,
    }


def _refund_breakdown_from_context(orig_tx, ctx):
    order_subtotal = ctx["order_subtotal"]
    refund_subtotal = ctx["refund_subtotal"]
    refund_tax = ctx["refund_tax"]
    orig_fees = abs(_to_float(orig_tx.get("transaction_fees")))
    fee_ratio = refund_subtotal / order_subtotal if order_subtotal > 0 else 0.0
    refund_fees = round(orig_fees * fee_ratio, 4)
    refund_grand = round(refund_subtotal + refund_tax + refund_fees, 4)
    return {
        "subtotal": round(refund_subtotal, 4),
        "taxes": round(refund_tax, 4),
        "fees_allocated": round(refund_fees, 4),
        "total_customer_credit": round(refund_grand, 4),
        "fee_allocation_ratio": round(fee_ratio, 6),
        "original_order_subtotal": round(order_subtotal, 4),
        "refund_fees": refund_fees,
        "refund_grand": refund_grand,
        "fee_ratio": fee_ratio,
    }


def _stripe_secret_key():
    mode = (
        os.getenv("STRIPE_MODE")
        or os.getenv("stripe_mode")
        or os.getenv("RDS_DB")
        or "dev"
    ).lower()
    if mode in ("prod", "production", "live"):
        return os.getenv("stripe_secret_live_key")
    return os.getenv("stripe_secret_test_key") or os.getenv("stripe_secret_live_key")


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


def _issue_stripe_refund(payment_intent_id, amount_dollars, metadata=None):
    """
    Create a Stripe refund against a PaymentIntent.
    Returns dict with ok/skipped/error and refund_id when available.
    """
    payment_intent_id = _normalize_stripe_payment_intent_id(payment_intent_id)
    if not payment_intent_id:
        return {"ok": False, "skipped": True, "message": "No Stripe payment intent on sale"}

    secret = _stripe_secret_key()
    if not secret:
        return {
            "ok": False,
            "skipped": True,
            "message": "Stripe secret key not configured",
        }

    amount_cents = int(round(abs(_to_float(amount_dollars)) * 100))
    if amount_cents < 1:
        return {"ok": False, "skipped": True, "message": "Refund amount too small"}

    data = {
        "payment_intent": payment_intent_id,
        "amount": str(amount_cents),
    }
    if metadata:
        for i, (k, v) in enumerate(metadata.items()):
            if v is None:
                continue
            data[f"metadata[{k}]"] = str(v)

    try:
        resp = http_requests.post(
            "https://api.stripe.com/v1/refunds",
            data=data,
            auth=(secret, ""),
            timeout=30,
        )
    except Exception as e:
        return {"ok": False, "skipped": False, "message": f"Stripe request failed: {e}"}

    try:
        body = resp.json()
    except Exception:
        body = {"raw": resp.text}

    if resp.status_code >= 400:
        return {
            "ok": False,
            "skipped": False,
            "message": body.get("error", {}).get("message")
            if isinstance(body.get("error"), dict)
            else body.get("error") or f"Stripe HTTP {resp.status_code}",
            "stripe_status": resp.status_code,
            "stripe_response": body,
        }

    return {
        "ok": True,
        "skipped": False,
        "refund_id": body.get("id"),
        "stripe_status": resp.status_code,
        "stripe_response": body,
    }


def _create_return_ledger(db, orig_tx, ctx, refund_meta, return_note):
    """Insert negative sale + reverse bounties. Returns (ok, error_or_None, result_dict)."""
    original_tx_uid = orig_tx.get("transaction_uid")
    lines_processed = ctx["lines_processed"]
    refund_grand = refund_meta["refund_grand"]
    refund_subtotal = refund_meta["subtotal"]
    refund_tax = refund_meta["taxes"]
    refund_fees = refund_meta["refund_fees"]
    fee_ratio = refund_meta["fee_ratio"]
    order_subtotal = refund_meta["original_order_subtotal"]

    new_uid_resp = db.call(procedure="new_transaction_uid")
    if not new_uid_resp.get("result") or len(new_uid_resp["result"]) == 0:
        return False, {
            "message": "Failed to generate return transaction UID",
            "code": 500,
        }, None

    new_transaction_uid = new_uid_resp["result"][0]["new_id"]
    transactions_datetime = utc_now_str()

    new_transaction = {
        "transaction_uid": new_transaction_uid,
        "transaction_datetime": transactions_datetime,
        "transaction_profile_id": orig_tx.get("transaction_profile_id"),
        "transaction_business_id": orig_tx.get("transaction_business_id"),
        "transaction_stripe_pi": orig_tx.get("transaction_stripe_pi"),
        "transaction_total": f"{-refund_grand:.4f}",
        "transaction_amount": f"{-refund_subtotal:.4f}",
        "transaction_taxes": f"{-refund_tax:.4f}",
        "transaction_fees": f"{-refund_fees:.4f}",
        "transaction_in_escrow": 0,
        "transaction_return_note": return_note,
        "transaction_type": "return",
        "transaction_original_uid": original_tx_uid,
    }

    tx_insert = db.insert("every_circle.transactions", new_transaction)
    if tx_insert.get("code") != 200:
        return False, {
            "message": tx_insert.get("message", "Failed to insert return transaction"),
            "code": tx_insert.get("code", 500),
        }, None

    bounty_insert_count = 0
    item_insert_count = 0
    response_lines = []

    for line in lines_processed:
        ti_row = line["snapshot"]
        rq = line["return_quantity"]
        original_qty = line["original_quantity"]
        ti_bs_id = ti_row.get("ti_bs_id")

        ti_uid_resp = db.call(procedure="new_transaction_item_uid")
        if not ti_uid_resp.get("result") or len(ti_uid_resp["result"]) == 0:
            return False, {
                "message": "Failed to generate return line item UID",
                "code": 500,
            }, None

        new_ti_uid = ti_uid_resp["result"][0]["new_id"]
        neg_qty = -int(rq)

        tx_item = {
            "ti_uid": new_ti_uid,
            "ti_transaction_id": new_transaction_uid,
            "ti_original_ti_uid": line["original_ti_uid"],
            "ti_bs_id": ti_bs_id,
            "ti_bs_qty": neg_qty,
            "ti_bs_cost": ti_row.get("ti_bs_cost"),
            "ti_bs_cost_currency": ti_row.get("ti_bs_cost_currency"),
            "ti_bs_sku": ti_row.get("ti_bs_sku"),
            "ti_bs_is_taxable": ti_row.get("ti_bs_is_taxable"),
            "ti_bs_tax_rate": ti_row.get("ti_bs_tax_rate"),
            "ti_bs_refund_policy": ti_row.get("ti_bs_refund_policy"),
            "ti_bs_return_window_days": ti_row.get("ti_bs_return_window_days"),
            "ti_bs_is_returnable": _normalize_is_returnable(
                ti_row.get("ti_bs_is_returnable")
            ),
        }
        if ti_row.get("ti_bso_id"):
            tx_item["ti_bso_id"] = ti_row.get("ti_bso_id")
        if ti_row.get("ti_selected_options") is not None:
            tx_item["ti_selected_options"] = ti_row.get("ti_selected_options")
        if ti_row.get("ti_special_instructions"):
            tx_item["ti_special_instructions"] = ti_row.get(
                "ti_special_instructions"
            )
        if ti_row.get("ti_choices_extra_cost") is not None:
            tx_item["ti_choices_extra_cost"] = ti_row.get("ti_choices_extra_cost")

        ti_insert = db.insert("every_circle.transactions_items", tx_item)
        if ti_insert.get("code") != 200:
            return False, {
                "message": ti_insert.get(
                    "message", "Failed to insert return transaction item"
                ),
                "code": ti_insert.get("code", 500),
            }, None
        item_insert_count += 1

        scale = _bounty_scale_for_line(rq, original_qty) or 0.0
        bounty_q = db.execute(
            """
            SELECT tb_uid, tb_profile_id, tb_percentage, tb_amount
            FROM every_circle.transactions_bounty
            WHERE tb_ti_id = %s
            """,
            (line["original_ti_uid"],),
        )
        bounty_rows = bounty_q.get("result") or []

        for br in bounty_rows:
            raw_amt = _to_float(br.get("tb_amount"))
            reversal = round(-scale * raw_amt, 4)
            if reversal == 0:
                continue

            bounty_uid_resp = db.call(procedure="new_transaction_bounty_uid")
            if not bounty_uid_resp.get("result") or len(bounty_uid_resp["result"]) == 0:
                print("Warning: Failed to generate bounty UID for reversal")
                continue

            new_tb_uid = bounty_uid_resp["result"][0]["new_id"]
            tx_bounty = {
                "tb_uid": new_tb_uid,
                "tb_ti_id": new_ti_uid,
                "tb_profile_id": br.get("tb_profile_id"),
                "tb_percentage": br.get("tb_percentage"),
                "tb_amount": reversal,
            }
            bins = db.insert("every_circle.transactions_bounty", tx_bounty)
            if bins.get("code") == 200:
                bounty_insert_count += 1
                reversal_abs = abs(reversal)
                if reversal_abs > 0:
                    wallet_result = debit_bounty_from_wallet(
                        db,
                        br.get("tb_profile_id"),
                        reversal_abs,
                    )
                    if wallet_result.get("code") != 200:
                        print(
                            "Warning: Failed to debit wallet on return for "
                            f"{br.get('tb_profile_id')}: {wallet_result}"
                        )

        response_lines.append(
            {
                "original_transaction_item_uid": line["original_ti_uid"],
                "new_transaction_item_uid": new_ti_uid,
                "return_quantity": rq,
                "line_subtotal": line["line_subtotal"],
                "line_tax": line["line_tax"],
            }
        )

    return True, None, {
        "return_transaction_uid": new_transaction_uid,
        "original_transaction_uid": original_tx_uid,
        "trr_transaction_uid": original_tx_uid,
        "refund_breakdown": {
            "subtotal": round(refund_subtotal, 4),
            "taxes": round(refund_tax, 4),
            "fees_allocated": round(refund_fees, 4),
            "total_customer_credit": round(refund_grand, 4),
            "fee_allocation_ratio": round(fee_ratio, 6),
            "original_order_subtotal": round(order_subtotal, 4),
        },
        "ledger_amounts_negative": {
            "transaction_total": new_transaction["transaction_total"],
            "transaction_amount": new_transaction["transaction_amount"],
            "transaction_taxes": new_transaction["transaction_taxes"],
            "transaction_fees": new_transaction["transaction_fees"],
        },
        "transaction_items_created": item_insert_count,
        "bounty_reversal_rows_created": bounty_insert_count,
        "lines": response_lines,
    }


def _load_return_request_by_uid(db, trr_uid):
    if not trr_uid:
        return None
    _ensure_return_requests_table(db)
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
    _ensure_return_requests_table(db)
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
    db, order_uid, ti_uid, order_qty, shipped_qty, exclude_trr_uid=None
):
    """
    Units still shippable after accounting for already-shipped, ledger-returned,
    and open return/cancel reservations.
    remaining_to_ship = max(purchased - shipped - returned - reserved, 0)
    """
    returned = _already_returned_qty(db, order_uid, ti_uid)
    reserved = _reserved_return_qty(
        db, order_uid, ti_uid, exclude_trr_uid=exclude_trr_uid
    )
    return max(int(order_qty or 0) - int(shipped_qty or 0) - returned - reserved, 0)


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
    """Estimated customer credit for a single return line (incl. fee share)."""
    line_ctx = {
        "order_subtotal": ctx["order_subtotal"],
        "refund_subtotal": line["line_subtotal"],
        "refund_tax": line["line_tax"],
    }
    return _refund_breakdown_from_context(orig_tx, line_ctx)["total_customer_credit"]


def _insert_return_request(
    db,
    transaction_uid,
    profile_id,
    ti_uid,
    return_quantity,
    note,
    return_status,
    refund_status,
    estimated_total,
    seller_note=None,
    created_at=None,
    cancel_unshipped=False,
):
    """
    Insert one return-request row for a single sale line item.
    Multiple open requests per sale are allowed.

    Field mapping (sale → request row):
      transaction_original_uid / sale uid → trr_transaction_uid
      transaction_return_note → trr_note
      transaction_return_status → trr_return_status
      transaction_return_seller_note → trr_seller_note
    """
    _ensure_return_requests_table(db)
    trr_uid = _new_trr_uid(db)
    if not trr_uid:
        return {
            "code": 500,
            "message": "Failed to generate return request UID",
        }, None

    try:
        qty = int(return_quantity)
    except (TypeError, ValueError):
        qty = 0

    item_payload = [
        {
            "transaction_item_uid": ti_uid,
            "return_quantity": qty,
        }
    ]
    now = created_at or utc_now_str()
    fields = {
        "trr_uid": trr_uid,
        "trr_transaction_uid": transaction_uid,
        "trr_profile_id": profile_id,
        "trr_ti_uid": ti_uid,
        "trr_return_quantity": qty,
        # Kept for older readers; source of truth is trr_ti_uid / trr_return_quantity.
        "trr_items_json": json.dumps(item_payload),
        "trr_note": note,
        "trr_seller_note": seller_note,
        "trr_status": refund_status,
        "trr_return_status": return_status,
        "trr_refund_status": refund_status,
        "trr_cancel_unshipped": 1 if cancel_unshipped else 0,
        "trr_estimated_total": estimated_total,
        "trr_return_transaction_uid": None,
        "trr_stripe_refund_id": None,
        "trr_created_at": now,
        "trr_updated_at": now,
    }
    result = db.insert("every_circle.transaction_return_requests", fields)
    return result, trr_uid


def _insert_return_requests_for_items(
    db,
    transaction_uid,
    profile_id,
    items_payload,
    note,
    return_status,
    refund_status,
    orig_tx,
    ctx,
    cancel_unshipped=False,
):
    """
    Insert one return-request row per item in the request.
    All rows in the same POST share trr_created_at so list views can batch them.
    Returns (result_or_error, trr_uids_list).
    """
    lines_by_ti = {
        line.get("original_ti_uid"): line for line in (ctx.get("lines_processed") or [])
    }
    batch_created_at = utc_now_str()
    trr_uids = []
    for entry in items_payload:
        ti_uid = entry.get("transaction_item_uid")
        try:
            qty = int(entry.get("return_quantity") or 0)
        except (TypeError, ValueError):
            qty = 0
        line = lines_by_ti.get(ti_uid) or {
            "line_subtotal": 0.0,
            "line_tax": 0.0,
        }
        estimated_total = _line_estimated_total(orig_tx, ctx, line)
        insert_result, trr_uid = _insert_return_request(
            db,
            transaction_uid,
            profile_id,
            ti_uid,
            qty,
            note,
            return_status,
            refund_status,
            estimated_total,
            created_at=batch_created_at,
            cancel_unshipped=cancel_unshipped,
        )
        if not trr_uid or insert_result.get("code") != 200:
            return (
                insert_result
                if insert_result
                else {
                    "code": 500,
                    "message": "Failed to save return request",
                },
                trr_uids,
            )
        trr_uids.append(trr_uid)
    return {"code": 200, "message": "ok"}, trr_uids


def _sale_has_other_open_returns(db, transaction_uid, exclude_trr_uid=None):
    exclude = _as_trr_uid_set(exclude_trr_uid)
    for req in _load_open_return_requests(db, transaction_uid):
        if req.get("trr_uid") in exclude:
            continue
        return True
    return False


def _update_return_request_row(
    db,
    trr_uid,
    return_status,
    refund_status,
    *,
    return_note=None,
    seller_note=None,
    return_transaction_uid=None,
    stripe_refund_id=None,
):
    if not trr_uid:
        return
    _ensure_return_requests_table(db)
    req_fields = {
        "trr_status": refund_status,
        "trr_return_status": return_status,
        "trr_refund_status": refund_status,
        "trr_updated_at": utc_now_str(),
    }
    if return_note is not None:
        req_fields["trr_note"] = return_note
    if seller_note is not None:
        req_fields["trr_seller_note"] = seller_note
    if return_transaction_uid is not None:
        req_fields["trr_return_transaction_uid"] = return_transaction_uid
    if stripe_refund_id is not None:
        req_fields["trr_stripe_refund_id"] = stripe_refund_id
    db.update(
        "every_circle.transaction_return_requests",
        {"trr_uid": trr_uid},
        req_fields,
    )


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
        _update_return_request_row(
            db,
            uid,
            return_status,
            refund_status,
            return_note=return_note,
            seller_note=seller_note,
            return_transaction_uid=return_transaction_uid,
            stripe_refund_id=stripe_refund_id,
        )


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
        items_payload,
        exclude_trr_uid=batch_uids,
        enforce_return_eligibility=False,
    )
    if not ok:
        return err, err.get("code", 400)

    refund_meta = _refund_breakdown_from_context(orig_tx, ctx)
    ledger_ok, ledger_err, ledger_result = _create_return_ledger(
        db, orig_tx, ctx, refund_meta, return_note
    )
    if not ledger_ok:
        return ledger_err, ledger_err.get("code", 500)

    if isinstance(stripe_refund_from_client, dict) and (
        "ok" in stripe_refund_from_client
        or stripe_refund_from_client.get("refund_id")
    ):
        stripe_result = {
            "ok": bool(stripe_refund_from_client.get("ok")),
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
        stripe_result = _issue_stripe_refund(
            orig_tx.get("transaction_stripe_pi"),
            refund_meta["refund_grand"],
            metadata=stripe_meta,
        )

    final_refund_status = (
        REFUND_STATUS_REFUNDED if stripe_result.get("ok") else REFUND_STATUS_REJECTED
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

    response = {
        "message": ok_msg if stripe_result.get("ok") else fail_msg,
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
        },
        "seller_note": seller_note,
        "refund_business_code_hint": _refund_business_code_from_note(seller_note),
    }
    response.update(ledger_result)
    return response, 200


def _refund_business_code_from_note(seller_note):
    """
    Map seller confirm note → IO-Payments business_code.
    ECTEST / PMTEST → test Stripe account; else EC (live). Explicit EC/PM also accepted.
    """
    n = (seller_note or "").strip().upper()
    if n in ("ECTEST", "PMTEST", "EC", "PM"):
        return n
    return "EC"


class ReturnTransaction(Resource):
    """
    POST: buyer requests a return → creates one trr_uid row per item.
    Physical returns start as Returning - Pending.
    Unshipped / cancel_unshipped requests start as Cancelled - Pending.

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

                ok, err, ctx = _validate_and_price_return_items(
                    db, original_tx_uid, items_payload
                )
                if not ok:
                    return err, err.get("code", 400)

                all_unshipped = _items_all_unshipped(
                    db, original_tx_uid, items_payload
                )
                if cancel_flag and not all_unshipped:
                    response["message"] = (
                        "cancel_unshipped / pre_ship_cancel requires every "
                        "requested item to have ti_shipped_qty == 0"
                    )
                    response["code"] = 400
                    return response, 400

                is_cancel = cancel_flag or all_unshipped
                return_status = (
                    RETURN_STATUS_CANCELLED
                    if is_cancel
                    else RETURN_STATUS_RETURNING
                )

                refund_meta = _refund_breakdown_from_context(orig_tx, ctx)
                insert_result, trr_uids = _insert_return_requests_for_items(
                    db,
                    original_tx_uid,
                    profile_id,
                    items_payload,
                    return_note,
                    return_status,
                    REFUND_STATUS_PENDING,
                    orig_tx,
                    ctx,
                    cancel_unshipped=is_cancel,
                )
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
                response["trr_transaction_uid"] = original_tx_uid
                response["transaction_uid"] = original_tx_uid
                response["transaction_return_requested"] = 1
                response["cancel_unshipped"] = is_cancel
                response["pre_ship_cancel"] = is_cancel
                response["is_cancel_before_ship"] = is_cancel
                response.update(_status_payload(return_status, REFUND_STATUS_PENDING))
                response["estimated_refund_breakdown"] = {
                    "subtotal": refund_meta["subtotal"],
                    "taxes": refund_meta["taxes"],
                    "fees_allocated": refund_meta["fees_allocated"],
                    "total_customer_credit": refund_meta["total_customer_credit"],
                    "fee_allocation_ratio": refund_meta["fee_allocation_ratio"],
                    "original_order_subtotal": refund_meta["original_order_subtotal"],
                }
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
                fulfillment_summary = fulfillment_list_summary_sql("ti")
                # Execute query with parameterized profile_id for security
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
                    t.transaction_profile_id,
                    t.transaction_in_escrow,
                    t.transaction_return_requested,
                    t.transaction_return_note,
                    t.transaction_business_id AS seller_id,
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
                    SUM(ti.ti_bs_qty) AS ti_bs_qty,
                    MIN(ti.ti_uid) AS ti_uid,
                    __FULFILLMENT_SUMMARY__
                    FROM every_circle.transactions t
                    LEFT JOIN every_circle.transactions_items ti
                    ON t.transaction_uid = ti.ti_transaction_id
                    LEFT JOIN every_circle.business_services bs
                    ON ti.ti_bs_id = bs.bs_uid
                    LEFT JOIN every_circle.business biz
                    ON bs.bs_business_id = biz.business_uid
                    
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
                    WHERE t.transaction_profile_id = %s
                    -- WHERE t.transaction_profile_id = '110-000014'
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

                print(f"Executing query for profile_id: {profile_id}")
                result = db.execute(query, (profile_id,))
                # print(f"Query result: {result}")

                if result.get("code") == 200:
                    rows = _enrich_transaction_rows(result.get("result", []))
                    rows = attach_shipping_to_transaction_rows(db, rows)
                    rows = apply_order_fulfillment_summary(rows)
                    rows = _enrich_list_transaction_rows(db, rows)
                    response["message"] = "Purchase Transactions retrieved successfully"
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
                "stripe_payment_intent",
                "total_amount_paid",
                "total_costs",
                "items",
            ]
            missing_fields = [
                field for field in required_fields if not payload.get(field)
            ]

            if missing_fields:
                response["message"] = (
                    f"Missing required fields: {', '.join(missing_fields)}"
                )
                response["code"] = 400
                return response, 400
            print("No Missing Fields")

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
                "transaction_stripe_pi": _normalize_stripe_payment_intent_id(
                    payload.get("stripe_payment_intent")
                ),
                "transaction_total": payload.get("total_amount_paid"),
                "transaction_amount": payload.get("total_costs"),
                "transaction_taxes": payload.get("total_taxes"),
                "transaction_fees": payload.get("total_fees"),
                "transaction_in_escrow": (
                    1 if payload.get("transaction_in_escrow") else 0
                ),
                "transaction_type": "sale",
            }

            with connect() as db:
                # Generate new transaction UID
                transaction_stored_procedure_response = db.call(
                    procedure="new_transaction_uid"
                )
                if (
                    not transaction_stored_procedure_response.get("result")
                    or len(transaction_stored_procedure_response["result"]) == 0
                ):
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
                    response["message"] = transaction_response.get(
                        "message", "Failed to insert transaction"
                    )
                    response["code"] = transaction_response.get("code", 500)
                    return response, response["code"]

                response["transaction"] = transaction_response
                response["transaction_uid"] = new_transaction_uid

                if shipping_fields:
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
                _ensure_return_eligibility_columns(db)

                for item in payload.get("items", []):
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
                        tx_item["ti_bs_cost"] = _strip_currency(bs_data.get("bs_cost"))
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
                        item_bounty_type = (
                            bs_data.get("bs_bounty_type", "per_item") or "per_item"
                        )
                        print("tx_item: ", tx_item)

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

                        tx_item["ti_bs_cost"] = _strip_currency(
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
                        item_bounty_type = (
                            bs_data.get("profile_expertise_bounty_type", "per_item") or "per_item"
                        )
                        print("tx_item: ", tx_item)

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

                        tx_item["ti_bs_cost"] = _strip_currency(
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
                        item_bounty_type = (
                            bs_data.get("profile_wish_bounty_type", "per_item") or "per_item"
                        )
                        print("tx_item: ", tx_item)

                    else:
                        print("ti_bs_id is not a valid ID")
                        continue

                    _apply_item_options_to_tx_item(tx_item, item, ti_bs_id)

                    if shipping_fields:
                        tx_item["ti_fulfillment_status"] = FULFILLMENT_STATUS_NOT_SHIPPED

                    # # Get other item details from business services table using parameterized query
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

                    # Insert transaction item
                    transaction_item_response = db.insert(
                        "every_circle.transactions_items", tx_item
                    )
                    print("transaction_item post response: ", transaction_item_response)

                    if transaction_item_response.get("code") == 200:
                        items_count += 1
                    else:
                        print(
                            f"Warning: Failed to insert transaction item: {transaction_item_response}"
                        )
                        continue

                    # Decrement expertise quantity in DB when an offering with limited stock is sold
                    if ti_bs_id and str(ti_bs_id).startswith("150"):
                        purchased_qty = int(item.get("quantity") or 1)
                        db.execute(
                            """
                            UPDATE every_circle.profile_expertise
                            SET profile_expertise_quantity = GREATEST(0, profile_expertise_quantity - %s)
                            WHERE profile_expertise_uid = %s
                              AND profile_expertise_quantity IS NOT NULL
                              AND profile_expertise_quantity > 0
                            """,
                            (purchased_qty, ti_bs_id),
                            cmd="post",
                        )
                        print(f"Decremented expertise quantity for {ti_bs_id} by {purchased_qty}")

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

                        combined_path = _fetch_connection_path(path_from, path_to)

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

                        middle_nodes = _middle_path_nodes(combined_path, seen)
                        if is_expertise_item or is_wish_item:
                            network_participants = _network_participants_capped(
                                middle_nodes, effective_bounty
                            )
                        else:
                            network_participants = _network_participants_business(
                                middle_nodes, effective_bounty, seen
                            )
                        print("network_participants: ", network_participants)

                        in_escrow = bool(transaction.get("transaction_in_escrow"))

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
                                        in_escrow=in_escrow,
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
                                        in_escrow=in_escrow,
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

                response["transaction_items"] = items_count
                response["transaction_bounty_count"] = bounty_count
                response["message"] = "Transaction completed successfully"
                response["code"] = 200
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
                    if current_status == "not_required":
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
                    remaining = order_qty - current_received

                    if order_qty <= 0:
                        response["message"] = (
                            f"Item {item_uid} is not eligible for delivery verification"
                        )
                        response["code"] = 400
                        return response, 400
                    if received_qty > remaining:
                        response["message"] = (
                            f"received_quantity exceeds remaining qty for {item_uid} "
                            f"(remaining: {remaining})"
                        )
                        response["code"] = 400
                        return response, 400

                    new_received = current_received + received_qty
                    current_status = (
                        ti_row.get("ti_fulfillment_status") or "not_required"
                    )
                    set_delivered = new_received >= order_qty and current_status in (
                        FULFILLMENT_STATUS_NOT_SHIPPED,
                        FULFILLMENT_STATUS_IN_TRANSIT,
                    )

                    if set_delivered:
                        ti_update = db.execute(
                            """
                            UPDATE every_circle.transactions_items
                            SET ti_received_qty = %s,
                                ti_received_at = %s,
                                ti_fulfillment_status = %s
                            WHERE ti_uid = %s AND ti_transaction_id = %s
                            """,
                            (
                                new_received,
                                received_at,
                                FULFILLMENT_STATUS_DELIVERED,
                                ti_uid,
                                transaction_uid,
                            ),
                            "post",
                        )
                    else:
                        ti_update = db.execute(
                            """
                            UPDATE every_circle.transactions_items
                            SET ti_received_qty = %s,
                                ti_received_at = %s
                            WHERE ti_uid = %s AND ti_transaction_id = %s
                            """,
                            (new_received, received_at, ti_uid, transaction_uid),
                            "post",
                        )
                    if ti_update.get("code") != 200:
                        response["message"] = ti_update.get(
                            "message", "Failed to update transaction item"
                        )
                        response["code"] = ti_update.get("code", 500)
                        return response, response["code"]

                    line_out = {
                        "transaction_item_uid": ti_uid,
                        "ti_received_qty": new_received,
                        "ti_bs_qty": order_qty,
                    }
                    if set_delivered:
                        line_out["fulfillment_status"] = FULFILLMENT_STATUS_DELIVERED
                    updated_lines.append(line_out)

                all_received = _all_lines_fully_received(db, transaction_uid)
                escrow_release_result = None
                update_fields = {}

                if all_received and int(tx_row.get("transaction_in_escrow") or 0) == 1:
                    escrow_release_result = release_escrow_for_transaction(
                        db, transaction_uid, reason="buyer_confirmed"
                    )
                    if escrow_release_result.get("code") != 200:
                        response["message"] = escrow_release_result.get(
                            "message", "Failed to release escrow"
                        )
                        response["code"] = escrow_release_result.get("code", 500)
                        return response, response["code"]
                    update_fields["transaction_in_escrow"] = 0
                else:
                    update_fields["transaction_in_escrow"] = 0 if all_received else 1

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
                if escrow_release_result:
                    response["escrow_release"] = summarize_escrow_result(
                        escrow_release_result
                    )
                response["delivery_verification_items"] = updated_lines
                response["all_items_received"] = all_received
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
    _ensure_return_requests_table(db)
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


def _bounty_to_reclaim_for_items(db, order_uid, items_payload):
    """Scale original line bounty by return_quantity / original qty."""
    if not items_payload:
        return 0.0
    ti_uids = [e.get("transaction_item_uid") for e in items_payload if e.get("transaction_item_uid")]
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
        ti_q = db.execute(
            """
            SELECT ti_bs_qty
            FROM every_circle.transactions_items
            WHERE ti_uid = %s AND ti_transaction_id = %s
            """,
            (ti_uid, order_uid),
        )
        ti_rows = ti_q.get("result") or []
        if not ti_rows:
            continue
        original_qty = int(ti_rows[0].get("ti_bs_qty") or 0)
        scale = _bounty_scale_for_line(rq, original_qty)
        if scale is None:
            continue
        total += line_bounties.get(ti_uid, 0.0) * scale
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
    rs, fs = _pair_for_sale(sale_row, pending)
    items = pending.get("items") or []

    estimated_refund = None
    if items:
        ok, _err, ctx = _validate_and_price_return_items(
            db,
            order_uid,
            items,
            exclude_trr_uid=pending.get("trr_uid"),
            enforce_return_eligibility=False,
        )
        if ok:
            refund_meta = _refund_breakdown_from_context(sale_row, ctx)
            if compact:
                estimated_refund = {
                    "subtotal": refund_meta["subtotal"],
                    "taxes": refund_meta["taxes"],
                    "total_customer_credit": refund_meta["total_customer_credit"],
                }
                if refund_meta["fees_allocated"]:
                    estimated_refund["fees_allocated"] = refund_meta["fees_allocated"]
            else:
                estimated_refund = {
                    "subtotal": refund_meta["subtotal"],
                    "taxes": refund_meta["taxes"],
                    "fees_allocated": refund_meta["fees_allocated"],
                    "total_customer_credit": refund_meta["total_customer_credit"],
                    "fee_allocation_ratio": refund_meta["fee_allocation_ratio"],
                    "original_order_subtotal": refund_meta["original_order_subtotal"],
                }
        else:
            stored = _to_float(pending.get("trr_estimated_total"))
            if stored:
                estimated_refund = {
                    "total_customer_credit": round(stored, 4),
                }

    bounty_to_reclaim = _bounty_to_reclaim_for_items(db, order_uid, items)

    payload = {
        "trr_uid": pending.get("trr_uid"),
        "note": pending.get("trr_note") or pending.get("note"),
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

    if compact:
        payload.update(_list_status_payload(rs, fs))
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
    payload.update(_status_payload(rs, fs))
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
        }
    return out


def _synthetic_pending_return_row(db, sale_row, pending_reqs):
    """
    Account-list line for one open return wave (no ledger yet).

    pending_reqs: one or more TRR rows sharing the same
    trr_transaction_uid + trr_created_at (one row per returned item).
    Aggregates into a single list entry.
    """
    if isinstance(pending_reqs, dict):
        pending_reqs = [pending_reqs]
    pending_reqs = [p for p in (pending_reqs or []) if p]
    if not pending_reqs:
        return None

    order_uid = sale_row.get("transaction_uid")
    if not order_uid:
        print(
            "Error: _synthetic_pending_return_row missing sale transaction_uid "
            f"(trr_uids={[p.get('trr_uid') for p in pending_reqs]!r})"
        )
        return None
    primary = pending_reqs[0]
    trr_uids = [p.get("trr_uid") for p in pending_reqs if p.get("trr_uid")]

    pending_payloads = [
        _pending_return_payload_for_sale(db, sale_row, p, compact=True)
        for p in pending_reqs
    ]
    credit = 0.0
    bounty_total = 0.0
    for payload, req in zip(pending_payloads, pending_reqs):
        if payload and payload.get("estimated_refund"):
            credit += _to_float(
                payload["estimated_refund"].get("total_customer_credit")
            )
        elif req.get("trr_estimated_total") is not None:
            credit += _to_float(req.get("trr_estimated_total"))
        if payload:
            bounty_total += _to_float(payload.get("bounty_to_reclaim"))

    rs, fs = _pair_for_sale(sale_row, primary)
    status_fields = _list_status_payload(rs, fs)
    cancel_flag = any(_is_cancel_unshipped_request(p) for p in pending_reqs)

    items = []
    for req in pending_reqs:
        items.extend(req.get("items") or [])
    ti_uids = [e.get("transaction_item_uid") for e in items if e.get("transaction_item_uid")]
    name_map = _sale_item_names_by_ti(db, order_uid, ti_uids)

    item_names = []
    return_lines = []
    qty_total = 0
    for entry in items:
        ti_uid = entry.get("transaction_item_uid")
        try:
            rq = int(entry.get("return_quantity") or 0)
        except (TypeError, ValueError):
            rq = 0
        qty_total += abs(rq)
        looked_up = name_map.get(ti_uid) or {}
        name = (
            entry.get("item_name")
            or entry.get("bs_service_name")
            or looked_up.get("item_name")
        )
        if name:
            item_names.append(str(name))
        return_lines.append(
            {
                "ti_uid": ti_uid,
                "item_name": name,
                "return_quantity": abs(rq),
            }
        )

    subtotal = round(
        sum(
            _to_float((p.get("estimated_refund") or {}).get("subtotal"))
            for p in pending_payloads
            if p
        ),
        4,
    )
    taxes = round(
        sum(
            _to_float((p.get("estimated_refund") or {}).get("taxes"))
            for p in pending_payloads
            if p
        ),
        4,
    )

    # Pending returns are not transactions yet — identity is trr_uid(s),
    # parent sale is trr_transaction_uid only (no transaction_uid / transaction_original_uid aliases).
    row = {
        "trr_uids": trr_uids,
        "trr_transaction_uid": order_uid,
        "transaction_type": "return",
        "is_return": True,
        "is_pending_return": True,
        "transaction_datetime": primary.get("trr_created_at")
        or sale_row.get("transaction_datetime"),
        "transaction_total": f"{-abs(credit):.4f}",
        "seller_id": sale_row.get("seller_id") or sale_row.get("transaction_business_id"),
        "business_name": sale_row.get("business_name"),
        "transaction_profile_id": sale_row.get("transaction_profile_id"),
        "transaction_return_note": primary.get("trr_note"),
        "purchased_item": ", ".join(item_names) if item_names else None,
        "ti_bs_qty": qty_total,
        "return_lines": return_lines,
        "return_quantity_total": qty_total,
        "refund_amount": round(abs(credit), 4),
        "bounty_to_reclaim": round(bounty_total, 4),
        "cancel_unshipped": cancel_flag,
        "pre_ship_cancel": cancel_flag,
        "is_cancel_before_ship": cancel_flag,
        "estimated_refund": {
            "subtotal": subtotal,
            "taxes": taxes,
            "total_customer_credit": round(credit, 4),
        },
        # Keep a slim pending_return for FE helpers that still read pending_return.*.
        "pending_return": _omit_empty(
            {
                "trr_uids": trr_uids,
                "trr_transaction_uid": order_uid,
                "note": primary.get("trr_note"),
                "items": items,
                "estimated_refund": {
                    "subtotal": subtotal,
                    "taxes": taxes,
                    "total_customer_credit": round(credit, 4),
                },
                "bounty_to_reclaim": round(bounty_total, 4),
                "created_at": primary.get("trr_created_at"),
                "cancel_unshipped": cancel_flag,
                "pre_ship_cancel": cancel_flag,
                "is_cancel_before_ship": cancel_flag,
                **status_fields,
            }
        ),
        **status_fields,
    }
    if len(trr_uids) == 1:
        row["trr_uid"] = trr_uids[0]
    return _omit_empty(row)


def _group_open_return_batches(reqs):
    """
    Group open, not-yet-ledgered return-request rows by sale + created_at.
    Preserves newest-first order of first-seen batches.
    """
    batches = []
    index_by_key = {}
    for req in reqs or []:
        if not _is_open_return(req.get("return_status"), req.get("refund_status")):
            continue
        if req.get("trr_return_transaction_uid"):
            continue
        key = (
            str(req.get("trr_transaction_uid") or ""),
            str(req.get("trr_created_at") or ""),
        )
        if key not in index_by_key:
            index_by_key[key] = len(batches)
            batches.append([])
        batches[index_by_key[key]].append(req)
    return batches


def _batch_return_lines(db, return_tx_uids):
    """
    Load return ledger line items keyed by return transaction_uid.
    Each line: item name, qty (signed negative), abs qty, cost, original sale ti uid.
    """
    uids = [u for u in (return_tx_uids or []) if u]
    if not uids:
        return {}
    placeholders = ", ".join(["%s"] * len(uids))
    q = db.execute(
        f"""
        SELECT
            ti.ti_transaction_id AS return_transaction_uid,
            ti.ti_uid,
            ti.ti_original_ti_uid,
            ti.ti_bs_id,
            ti.ti_bs_qty,
            ti.ti_bs_cost,
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
        WHERE ti.ti_transaction_id IN ({placeholders})
        ORDER BY ti.ti_uid
        """,
        tuple(uids),
    )
    out = {}
    for row in q.get("result") or []:
        tx_uid = row.get("return_transaction_uid")
        qty = int(_to_float(row.get("ti_bs_qty")))
        out.setdefault(tx_uid, []).append(
            {
                "ti_uid": row.get("ti_uid"),
                "ti_original_ti_uid": row.get("ti_original_ti_uid"),
                "ti_bs_id": row.get("ti_bs_id"),
                "item_name": row.get("item_name"),
                "quantity": qty,
                "return_quantity": abs(qty),
                "unit_cost": _to_float(row.get("ti_bs_cost")),
            }
        )
    return out


def _normalize_completed_return_row(out, linked_req=None, return_lines=None):
    """Ensure completed reverse-txn rows have FE-stable return fields."""
    out["transaction_type"] = "return"
    out["is_return"] = True
    out["is_pending_return"] = False
    out.pop("order_uid", None)

    parent_sale, parent_err = _resolve_parent_sale_uid(
        {
            **out,
            "trr_transaction_uid": (linked_req or {}).get("trr_transaction_uid")
            or out.get("trr_transaction_uid"),
            "is_return": True,
        },
        context="completed return list row",
    )
    if parent_sale:
        out["transaction_original_uid"] = parent_sale
        if linked_req and linked_req.get("trr_transaction_uid"):
            out["trr_transaction_uid"] = linked_req.get("trr_transaction_uid")
    elif parent_err:
        # Keep row but surface the resolution failure for clients.
        out["parent_sale_resolve_error"] = parent_err

    if linked_req:
        rs, fs = _pair_for_sale(out, linked_req)
        out.update(_status_payload(rs, fs))
        out["trr_uid"] = linked_req.get("trr_uid")
        cancel_flag = _is_cancel_unshipped_request(linked_req)
        out["cancel_unshipped"] = cancel_flag
        out["pre_ship_cancel"] = cancel_flag
        out["is_cancel_before_ship"] = cancel_flag
    else:
        # Historical ledger with no linked request row
        if not out.get("return_status") and not out.get("refund_status"):
            out.update(
                _status_payload(RETURN_STATUS_RETURNED, REFUND_STATUS_REFUNDED)
            )

    lines = return_lines or []
    out["return_lines"] = lines
    out["lines"] = lines
    if lines and not out.get("purchased_item"):
        out["purchased_item"] = ", ".join(
            str(l.get("item_name") or l.get("ti_bs_id") or "")
            for l in lines
            if l.get("item_name") or l.get("ti_bs_id")
        )
    qty_sum = sum(int(l.get("return_quantity") or 0) for l in lines)
    if qty_sum and out.get("ti_bs_qty") is None:
        out["ti_bs_qty"] = -qty_sum
    out["return_quantity_total"] = qty_sum or abs(int(_to_float(out.get("ti_bs_qty"))))
    out["refund_amount"] = abs(_to_float(out.get("transaction_total")))
    out["pending_return"] = None
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
    return_lines_map = _batch_return_lines(db, return_tx_uids)

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
            out = _normalize_completed_return_row(
                out,
                linked_req=linked,
                return_lines=return_lines_map.get(out.get("transaction_uid")) or [],
            )
            if linked_reqs:
                out["trr_uids"] = [
                    r.get("trr_uid") for r in linked_reqs if r.get("trr_uid")
                ]
        else:
            out["is_return"] = False
            out["is_pending_return"] = False
            if sale_uid:
                sales_by_uid[sale_uid] = out
            # Return detail lives on synthetic return list rows only.
            # Sale keeps status flags so the purchase can show Returning/Pending.
            out.pop("pending_returns", None)
            out.pop("pending_return", None)
            if open_reqs:
                rs, fs = _pair_for_sale(out, open_reqs[0])
                out.update(_list_status_payload(rs, fs))

        enriched.append(out)

    synthetic = []
    for sale_uid, sale_row in sales_by_uid.items():
        for batch in _group_open_return_batches(return_req_map.get(sale_uid) or []):
            row = _synthetic_pending_return_row(db, sale_row, batch)
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
                _ensure_return_requests_table(db)
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
