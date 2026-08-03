"""
Seller wallet_transactions ledger: table ensure, seller-eligible totals,
and partial-delivery credit (idempotent insert + wallet credit).

Statuses: posted (spendable) | held (return-window lock until wt_available_at).
Types: partial_delivery_credit | return_clawback.
"""

import re
from datetime import datetime, timedelta, timezone

from datetime_utils import parse_stored_datetime, utc_now_str
from wallet_service import (
    _round_money,
    _to_float,
    credit_seller_proceeds_to_wallet,
    debit_seller_proceeds_from_wallet,
    release_bounty_for_line,
    release_bounty_for_line_net,
    release_seller_hold_to_useable,
    sync_bounty_release_after_line_credit,
)

WT_TYPE_PARTIAL_DELIVERY_CREDIT = "partial_delivery_credit"
WT_TYPE_RETURN_CLAWBACK = "return_clawback"
WT_TYPE_RETURN_REFUND_RESERVATION = "return_refund_reservation"
WT_TYPE_BOUNTY_RECLAIM_RESERVATION = "bounty_reclaim_reservation"
WT_STATUS_POSTED = "posted"
WT_STATUS_HELD = "held"
WT_STATUS_RESERVED = "reserved"
WT_STATUS_CLEARED = "cleared"

_DATETIME_FMT = "%Y-%m-%d %H:%M:%S"

_WALLET_TRANSACTIONS_TABLE_READY = False

# First numeric token from display costs stored on transaction items.
# Real values in DB include: "89.99", "100/each", "1000/hr", "200 total", "$1,000.50".
_UNIT_COST_RE = re.compile(r"[+-]?\d+(?:,\d{3})*(?:\.\d+)?")


def _parse_unit_cost(value):
    """
    Parse ti_bs_cost into a numeric unit price.

    Offering costs are often display strings with a unit suffix. Checkout still
    treats the leading number as per-unit (``qty * number == line total``), e.g.:
      - ``89.99``
      - ``100/each`` / ``1000/hr`` / ``100/day``
      - ``200 total``
      - ``$1,000.50``
    Plain ``float()`` / ``_to_float`` return 0 for those and zero out credits.
    """
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    s = str(value).replace("$", "").strip()
    if not s:
        return 0.0
    try:
        return float(s.replace(",", ""))
    except (TypeError, ValueError):
        pass
    match = _UNIT_COST_RE.search(s)
    if not match:
        return 0.0
    try:
        return float(match.group(0).replace(",", ""))
    except (TypeError, ValueError):
        return 0.0


def _ensure_wallet_transactions_table(db):
    """Create wallet_transactions once per process if missing."""
    global _WALLET_TRANSACTIONS_TABLE_READY
    if _WALLET_TRANSACTIONS_TABLE_READY:
        return
    db.execute(
        """
        CREATE TABLE IF NOT EXISTS every_circle.wallet_transactions (
            wt_uid VARCHAR(64) NOT NULL,
            wt_profile_id VARCHAR(64) NOT NULL,
            wt_buyer_id VARCHAR(64) NOT NULL,
            wt_seller_id VARCHAR(64) NOT NULL,
            wt_transaction_id VARCHAR(64) NOT NULL,
            wt_ti_id VARCHAR(64) NOT NULL,
            wt_type VARCHAR(32) NOT NULL,
            wt_status VARCHAR(32) NOT NULL DEFAULT 'posted',
            wt_qty INT NOT NULL,
            wt_received_qty_after INT NOT NULL,
            wt_unit_cost DECIMAL(18,4) NOT NULL,
            wt_amount DECIMAL(18,4) NOT NULL,
            wt_currency VARCHAR(8) NOT NULL,
            wt_idempotency_key VARCHAR(128) NOT NULL,
            wt_note VARCHAR(512) NULL,
            wt_available_at DATETIME NULL,
            wt_created_at DATETIME NOT NULL,
            wt_updated_at DATETIME NOT NULL,
            PRIMARY KEY (wt_uid),
            UNIQUE KEY uq_wt_idempotency (wt_idempotency_key),
            KEY idx_wt_transaction_id (wt_transaction_id),
            KEY idx_wt_ti_id (wt_ti_id),
            KEY idx_wt_profile_id (wt_profile_id),
            KEY idx_wt_held_available (wt_status, wt_available_at)
        )
        """,
        cmd="post",
    )
    # Older installs may lack columns, unique key, or lookup indexes.
    for ddl in (
        "ALTER TABLE every_circle.wallet_transactions "
        "ADD COLUMN wt_available_at DATETIME NULL",
        "ALTER TABLE every_circle.wallet_transactions "
        "ADD UNIQUE KEY uq_wt_idempotency (wt_idempotency_key)",
        "ALTER TABLE every_circle.wallet_transactions "
        "ADD KEY idx_wt_transaction_id (wt_transaction_id)",
        "ALTER TABLE every_circle.wallet_transactions "
        "ADD KEY idx_wt_ti_id (wt_ti_id)",
        "ALTER TABLE every_circle.wallet_transactions "
        "ADD KEY idx_wt_profile_id (wt_profile_id)",
        "ALTER TABLE every_circle.wallet_transactions "
        "ADD KEY idx_wt_held_available (wt_status, wt_available_at)",
    ):
        db.execute(ddl, cmd="post")
    _WALLET_TRANSACTIONS_TABLE_READY = True


def _new_wallet_transaction_uid(db):
    """Allocate wt_uid via every_circle.new_wallet_transaction_uid (same style as logs_uid)."""
    uid_result = db.execute("CALL every_circle.new_wallet_transaction_uid()")
    if not uid_result or "result" not in uid_result or not uid_result["result"]:
        return None
    return uid_result["result"][0].get("new_id")


def _order_bounty_paid(db, transaction_uid):
    """Sum of all bounty rows on a sale (same rollup as transactions._batch_order_bounty_paid)."""
    if not transaction_uid:
        return 0.0
    q = db.execute(
        """
        SELECT COALESCE(SUM(tb.tb_amount), 0) AS order_bounty_paid
        FROM every_circle.transactions_items ti
        LEFT JOIN every_circle.transactions_bounty tb ON tb.tb_ti_id = ti.ti_uid
        WHERE ti.ti_transaction_id = %s
        """,
        (transaction_uid,),
    )
    rows = q.get("result") or []
    if not rows:
        return 0.0
    return _round_money(rows[0].get("order_bounty_paid"))


def compute_seller_eligible_total(db, transaction_uid):
    """
    Order-level seller pool:
      transaction_amount + COALESCE(transaction_shipping, 0) - SUM(bounty on sale)
    """
    if not transaction_uid:
        return 0.0
    tx_q = db.execute(
        """
        SELECT transaction_amount, transaction_shipping
        FROM every_circle.transactions
        WHERE transaction_uid = %s
        """,
        (transaction_uid,),
    )
    tx_rows = tx_q.get("result") or []
    if not tx_rows:
        return 0.0
    tx = tx_rows[0]
    amount = _to_float(tx.get("transaction_amount"))
    shipping = _to_float(tx.get("transaction_shipping"))
    bounty = _order_bounty_paid(db, transaction_uid)
    return _round_money(amount + shipping - bounty)


def _business_owner_profile_uid(db, business_uid):
    """First profile_personal_uid linked to a business via business_user."""
    business_uid = str(business_uid or "").strip()
    if not business_uid:
        return None
    q = db.execute(
        """
        SELECT pp.profile_personal_uid
        FROM every_circle.business_user bu
        JOIN every_circle.users u ON u.user_uid = bu.bu_user_id
        JOIN every_circle.profile_personal pp ON pp.profile_personal_user_id = u.user_uid
        WHERE bu.bu_business_id = %s
        LIMIT 1
        """,
        (business_uid,),
    )
    rows = q.get("result") or []
    if rows and rows[0].get("profile_personal_uid"):
        return rows[0]["profile_personal_uid"]
    return None


def resolve_seller_wallet_profile_id(db, transaction_business_id):
    """
    Map transaction_business_id to the profile that owns the seller wallet.

    Personal sellers: business id is profile_personal_uid (or user id).
    Business sellers: first owner profile via business_user.
    """
    seller_id = str(transaction_business_id or "").strip()
    if not seller_id:
        return None

    by_uid = db.execute(
        """
        SELECT profile_personal_uid
        FROM every_circle.profile_personal
        WHERE profile_personal_uid = %s
        LIMIT 1
        """,
        (seller_id,),
    )
    rows = by_uid.get("result") or []
    if rows and rows[0].get("profile_personal_uid"):
        return rows[0]["profile_personal_uid"]

    by_user = db.execute(
        """
        SELECT profile_personal_uid
        FROM every_circle.profile_personal
        WHERE profile_personal_user_id = %s
        LIMIT 1
        """,
        (seller_id,),
    )
    rows = by_user.get("result") or []
    if rows and rows[0].get("profile_personal_uid"):
        return rows[0]["profile_personal_uid"]

    return _business_owner_profile_uid(db, seller_id)


def _as_returnable_flag(value, default=True):
    """Interpret ti_bs_is_returnable (0/1, bool, or common string forms)."""
    if value is None:
        return bool(default)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return int(value) != 0
    s = str(value).strip().lower()
    if s in ("0", "false", "no", "off"):
        return False
    if s in ("1", "true", "yes", "on"):
        return True
    return bool(default)


def _parse_positive_window_days(value):
    """Return a positive int return window, or None if null/empty/invalid."""
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    try:
        days = int(value)
    except (TypeError, ValueError):
        try:
            days = int(float(s))
        except (TypeError, ValueError):
            return None
    return days if days > 0 else None


def _seller_proceeds_hold_decision(ti_row):
    """
    Hold returnable sale lines that have a positive return_window_days.

    Returns (hold, wt_available_at, window_days).
    Non-returnable or null/empty/invalid window → (False, None, None).
    available_at = ti_received_at + window_days (UTC naive string).
    """
    if not _as_returnable_flag(ti_row.get("ti_bs_is_returnable"), default=True):
        return False, None, None

    window_days = _parse_positive_window_days(ti_row.get("ti_bs_return_window_days"))
    if window_days is None:
        return False, None, None

    received_at = parse_stored_datetime(ti_row.get("ti_received_at"))
    if received_at is None:
        received_at = datetime.now(timezone.utc)

    available_at = received_at + timedelta(days=window_days)
    if available_at.tzinfo is not None:
        available_at = available_at.astimezone(timezone.utc).replace(tzinfo=None)
    return True, available_at.strftime(_DATETIME_FMT), window_days


def _posted_credits_total(db, transaction_uid):
    """
    Net held + posted seller proceeds for an order (cap basis).

    partial_delivery_credit amounts minus return_clawback (negative) rows.
    """
    q = db.execute(
        """
        SELECT COALESCE(SUM(wt_amount), 0) AS credited
        FROM every_circle.wallet_transactions
        WHERE wt_transaction_id = %s
          AND wt_type IN (%s, %s)
          AND wt_status IN (%s, %s)
        """,
        (
            transaction_uid,
            WT_TYPE_PARTIAL_DELIVERY_CREDIT,
            WT_TYPE_RETURN_CLAWBACK,
            WT_STATUS_POSTED,
            WT_STATUS_HELD,
        ),
    )
    rows = q.get("result") or []
    if not rows:
        return 0.0
    return _round_money(rows[0].get("credited"))


def _total_order_qty(db, transaction_uid):
    q = db.execute(
        """
        SELECT COALESCE(SUM(ti_bs_qty), 0) AS total_qty
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s
        """,
        (transaction_uid,),
    )
    rows = q.get("result") or []
    if not rows:
        return 0
    try:
        return int(rows[0].get("total_qty") or 0)
    except (TypeError, ValueError):
        return 0


def compute_partial_delivery_credit_amount(
    seller_eligible_total,
    transaction_amount,
    qty,
    unit_cost,
    total_order_qty,
    already_credited,
):
    """
    Proportional share of seller_eligible_total for a receive delta.

    Normal: seller_eligible_total * (qty * unit_cost / transaction_amount)
    If transaction_amount is 0: equal split by qty / total_order_qty.
    Capped so cumulative held+posted credits never exceed seller_eligible_total.
    """
    eligible = _round_money(seller_eligible_total)
    if eligible <= 0 or qty <= 0:
        return 0.0

    tx_amount = _to_float(transaction_amount)
    if tx_amount > 0:
        delta_value = _to_float(qty) * _to_float(unit_cost)
        raw = eligible * (delta_value / tx_amount)
    else:
        order_qty = int(total_order_qty or 0)
        if order_qty <= 0:
            return 0.0
        raw = eligible * (_to_float(qty) / order_qty)

    remaining = _round_money(eligible - _to_float(already_credited))
    if remaining <= 0:
        return 0.0
    return _round_money(min(raw, remaining))


def _fetch_wt_by_idempotency_key(db, idempotency_key):
    q = db.execute(
        """
        SELECT wt_uid, wt_amount, wt_profile_id, wt_qty, wt_received_qty_after,
               wt_transaction_id, wt_ti_id, wt_currency, wt_status, wt_type,
               wt_available_at
        FROM every_circle.wallet_transactions
        WHERE wt_idempotency_key = %s
        LIMIT 1
        """,
        (idempotency_key,),
    )
    rows = q.get("result") or []
    return rows[0] if rows else None


def _wt_success_payload(row, *, idempotent_replay=False, wallet_result=None):
    payload = {
        "code": 200,
        "wt_uid": row.get("wt_uid"),
        "wt_amount": _round_money(row.get("wt_amount")),
        "wt_profile_id": row.get("wt_profile_id"),
        "wt_qty": row.get("wt_qty"),
        "wt_received_qty_after": row.get("wt_received_qty_after"),
        "wt_transaction_id": row.get("wt_transaction_id"),
        "wt_ti_id": row.get("wt_ti_id"),
        "idempotent_replay": bool(idempotent_replay),
        "credited": not idempotent_replay
        and _round_money(row.get("wt_amount")) > 0,
    }
    if wallet_result is not None:
        payload["wallet"] = wallet_result
    return payload


def _attach_bounty_release_to_credit_result(
    db, result, *, ti_uid, ti, received_qty_after, hold
):
    """Augment a credit_partial_delivery payload with coupled bounty release."""
    if not isinstance(result, dict) or result.get("code") != 200:
        return result
    order_qty = int(ti.get("ti_bs_qty") or 0)
    bounty_release = sync_bounty_release_after_line_credit(
        db,
        ti_uid,
        received_qty_after=received_qty_after,
        order_qty=order_qty,
        hold=hold,
    )
    result["bounty_release"] = bounty_release
    if bounty_release.get("code") != 200:
        result["code"] = bounty_release.get("code", 500)
        result["message"] = bounty_release.get(
            "message", "Failed to release bounty for line"
        )
    return result


def credit_partial_delivery(db, transaction_uid, ti_uid, qty, received_qty_after):
    """
    Insert a partial_delivery_credit ledger row and credit the seller wallet.

    Idempotency key: ``{ti_uid}:{received_qty_after}``. Duplicate key is a
    success/no-op (existing row returned, wallet not re-credited).

    Returnable lines with a positive ``ti_bs_return_window_days`` credit
    ``wallet_pending`` with status ``held`` until ``wt_available_at``;
    otherwise credit useable with status ``posted``.
    """
    try:
        qty = int(qty)
        received_qty_after = int(received_qty_after)
    except (TypeError, ValueError):
        return {
            "code": 400,
            "message": "qty and received_qty_after must be integers",
        }
    if not transaction_uid or not ti_uid:
        return {
            "code": 400,
            "message": "transaction_uid and ti_uid are required",
        }
    if qty < 1 or received_qty_after < 1:
        return {
            "code": 400,
            "message": "qty and received_qty_after must be >= 1",
        }

    idempotency_key = f"{ti_uid}:{received_qty_after}"
    existing = _fetch_wt_by_idempotency_key(db, idempotency_key)

    tx_q = db.execute(
        """
        SELECT transaction_uid, transaction_profile_id, transaction_business_id,
               transaction_amount, transaction_shipping
        FROM every_circle.transactions
        WHERE transaction_uid = %s
        """,
        (transaction_uid,),
    )
    tx_rows = tx_q.get("result") or []
    if not tx_rows:
        return {"code": 404, "message": f"Transaction not found: {transaction_uid}"}
    tx = tx_rows[0]

    ti_q = db.execute(
        """
        SELECT ti_uid, ti_transaction_id, ti_bs_cost, ti_bs_cost_currency, ti_bs_qty,
               ti_bs_is_returnable, ti_bs_return_window_days, ti_received_at
        FROM every_circle.transactions_items
        WHERE ti_uid = %s AND ti_transaction_id = %s
        """,
        (ti_uid, transaction_uid),
    )
    ti_rows = ti_q.get("result") or []
    if not ti_rows:
        return {
            "code": 404,
            "message": f"Transaction item not found on sale: {ti_uid}",
        }
    ti = ti_rows[0]

    hold, available_at, _window_days = _seller_proceeds_hold_decision(ti)
    wt_status = WT_STATUS_HELD if hold else WT_STATUS_POSTED

    seller_id = tx.get("transaction_business_id")
    seller_profile_id = resolve_seller_wallet_profile_id(db, seller_id)
    if not seller_profile_id:
        return {
            "code": 500,
            "message": (
                "Unable to resolve seller wallet profile for "
                f"transaction_business_id={seller_id!r}"
            ),
            "transaction_uid": transaction_uid,
            "ti_uid": ti_uid,
        }

    seller_eligible = compute_seller_eligible_total(db, transaction_uid)
    unit_cost = _parse_unit_cost(ti.get("ti_bs_cost"))
    transaction_amount = _to_float(tx.get("transaction_amount"))
    total_order_qty = _total_order_qty(db, transaction_uid)
    already_credited = _posted_credits_total(db, transaction_uid)

    credit_amount = compute_partial_delivery_credit_amount(
        seller_eligible_total=seller_eligible,
        transaction_amount=transaction_amount,
        qty=qty,
        unit_cost=unit_cost,
        total_order_qty=total_order_qty,
        already_credited=already_credited,
    )

    if (
        existing
        and _to_float(existing.get("wt_amount")) > 0
    ):
        return _attach_bounty_release_to_credit_result(
            db,
            _wt_success_payload(existing, idempotent_replay=True),
            ti_uid=ti_uid,
            ti=ti,
            received_qty_after=received_qty_after,
            hold=hold,
        )

    # Repair prior zero-amount ledger rows (e.g. unparseable "100/each" unit cost).
    if existing and _to_float(existing.get("wt_amount")) == 0 and credit_amount > 0:
        now = utc_now_str()
        upd = db.update(
            "every_circle.wallet_transactions",
            {"wt_uid": existing.get("wt_uid")},
            {
                "wt_unit_cost": _round_money(unit_cost),
                "wt_amount": credit_amount,
                "wt_status": wt_status,
                "wt_available_at": available_at,
                "wt_updated_at": now,
            },
        )
        if upd.get("code") != 200:
            return {
                "code": upd.get("code", 500),
                "message": upd.get(
                    "message", "Failed to repair wallet_transactions row"
                ),
                "wt_uid": existing.get("wt_uid"),
            }
        wallet_result = credit_seller_proceeds_to_wallet(
            db, seller_profile_id, credit_amount, hold=hold
        )
        if wallet_result.get("code") != 200:
            return {
                "code": wallet_result.get("code", 500),
                "message": wallet_result.get(
                    "message", "Failed to credit seller wallet"
                ),
                "wt_uid": existing.get("wt_uid"),
                "wt_amount": credit_amount,
                "wallet": wallet_result,
            }
        repaired = dict(existing)
        repaired["wt_unit_cost"] = _round_money(unit_cost)
        repaired["wt_amount"] = credit_amount
        repaired["wt_profile_id"] = seller_profile_id
        repaired["wt_status"] = wt_status
        repaired["wt_available_at"] = available_at
        return _attach_bounty_release_to_credit_result(
            db,
            _wt_success_payload(
                repaired, idempotent_replay=False, wallet_result=wallet_result
            ),
            ti_uid=ti_uid,
            ti=ti,
            received_qty_after=received_qty_after,
            hold=hold,
        )

    if existing:
        return _attach_bounty_release_to_credit_result(
            db,
            _wt_success_payload(existing, idempotent_replay=True),
            ti_uid=ti_uid,
            ti=ti,
            received_qty_after=received_qty_after,
            hold=hold,
        )

    currency = (ti.get("ti_bs_cost_currency") or "USD").strip() or "USD"
    now = utc_now_str()
    wt_uid = _new_wallet_transaction_uid(db)
    if not wt_uid:
        return {
            "code": 500,
            "message": "Failed to generate wt_uid via new_wallet_transaction_uid",
            "transaction_uid": transaction_uid,
            "ti_uid": ti_uid,
        }

    insert_row = {
        "wt_uid": wt_uid,
        "wt_profile_id": seller_profile_id,
        "wt_buyer_id": tx.get("transaction_profile_id"),
        "wt_seller_id": seller_id,
        "wt_transaction_id": transaction_uid,
        "wt_ti_id": ti_uid,
        "wt_type": WT_TYPE_PARTIAL_DELIVERY_CREDIT,
        "wt_status": wt_status,
        "wt_qty": qty,
        "wt_received_qty_after": received_qty_after,
        "wt_unit_cost": _round_money(unit_cost),
        "wt_amount": credit_amount,
        "wt_currency": currency[:8],
        "wt_idempotency_key": idempotency_key,
        "wt_note": None,
        "wt_available_at": available_at,
        "wt_created_at": now,
        "wt_updated_at": now,
    }

    insert_result = db.insert("every_circle.wallet_transactions", insert_row)
    if insert_result.get("code") != 200:
        insert_msg = (insert_result.get("message") or "").lower()
        if "duplicate entry" in insert_msg:
            existing = _fetch_wt_by_idempotency_key(db, idempotency_key)
            if existing:
                return _attach_bounty_release_to_credit_result(
                    db,
                    _wt_success_payload(existing, idempotent_replay=True),
                    ti_uid=ti_uid,
                    ti=ti,
                    received_qty_after=received_qty_after,
                    hold=hold,
                )
        return {
            "code": insert_result.get("code", 500),
            "message": insert_result.get(
                "message", "Failed to insert wallet_transactions row"
            ),
            "transaction_uid": transaction_uid,
            "ti_uid": ti_uid,
        }

    wallet_result = None
    if credit_amount > 0:
        wallet_result = credit_seller_proceeds_to_wallet(
            db, seller_profile_id, credit_amount, hold=hold
        )
        if wallet_result.get("code") != 200:
            return {
                "code": wallet_result.get("code", 500),
                "message": wallet_result.get(
                    "message", "Failed to credit seller wallet"
                ),
                "wt_uid": wt_uid,
                "wt_amount": credit_amount,
                "wallet": wallet_result,
            }

    return _attach_bounty_release_to_credit_result(
        db,
        _wt_success_payload(
            insert_row, idempotent_replay=False, wallet_result=wallet_result
        ),
        ti_uid=ti_uid,
        ti=ti,
        received_qty_after=received_qty_after,
        hold=hold,
    )


def _line_seller_proceeds_net(db, ti_uid):
    """
    Net credited seller proceeds on a sale line (credits minus prior clawbacks).

    Returns dict with net_amount, net_qty, held_amount, posted_amount, and
    identity fields from the latest credit row (if any).
    """
    empty = {
        "net_amount": 0.0,
        "net_qty": 0,
        "held_amount": 0.0,
        "posted_amount": 0.0,
        "wt_profile_id": None,
        "wt_buyer_id": None,
        "wt_seller_id": None,
        "wt_transaction_id": None,
        "wt_currency": "USD",
        "wt_unit_cost": 0.0,
    }
    if not ti_uid:
        return empty

    q = db.execute(
        """
        SELECT
            COALESCE(SUM(wt_amount), 0) AS net_amount,
            COALESCE(SUM(
                CASE
                    WHEN wt_type = %s THEN wt_qty
                    WHEN wt_type = %s THEN -ABS(wt_qty)
                    ELSE 0
                END
            ), 0) AS net_qty,
            COALESCE(SUM(
                CASE WHEN wt_status = %s THEN wt_amount ELSE 0 END
            ), 0) AS held_amount,
            COALESCE(SUM(
                CASE WHEN wt_status = %s THEN wt_amount ELSE 0 END
            ), 0) AS posted_amount
        FROM every_circle.wallet_transactions
        WHERE wt_ti_id = %s
          AND wt_type IN (%s, %s)
          AND wt_status IN (%s, %s)
        """,
        (
            WT_TYPE_PARTIAL_DELIVERY_CREDIT,
            WT_TYPE_RETURN_CLAWBACK,
            WT_STATUS_HELD,
            WT_STATUS_POSTED,
            ti_uid,
            WT_TYPE_PARTIAL_DELIVERY_CREDIT,
            WT_TYPE_RETURN_CLAWBACK,
            WT_STATUS_HELD,
            WT_STATUS_POSTED,
        ),
    )
    rows = q.get("result") or []
    if not rows:
        return empty

    row = rows[0]
    try:
        net_qty = int(row.get("net_qty") or 0)
    except (TypeError, ValueError):
        net_qty = 0

    meta_q = db.execute(
        """
        SELECT wt_profile_id, wt_buyer_id, wt_seller_id, wt_transaction_id,
               wt_currency, wt_unit_cost
        FROM every_circle.wallet_transactions
        WHERE wt_ti_id = %s
          AND wt_type = %s
        ORDER BY wt_created_at DESC, wt_uid DESC
        LIMIT 1
        """,
        (ti_uid, WT_TYPE_PARTIAL_DELIVERY_CREDIT),
    )
    meta_rows = meta_q.get("result") or []
    meta = meta_rows[0] if meta_rows else {}

    return {
        "net_amount": _round_money(row.get("net_amount")),
        "net_qty": net_qty,
        "held_amount": _round_money(row.get("held_amount")),
        "posted_amount": _round_money(row.get("posted_amount")),
        "wt_profile_id": meta.get("wt_profile_id"),
        "wt_buyer_id": meta.get("wt_buyer_id"),
        "wt_seller_id": meta.get("wt_seller_id"),
        "wt_transaction_id": meta.get("wt_transaction_id"),
        "wt_currency": (meta.get("wt_currency") or "USD"),
        "wt_unit_cost": _round_money(meta.get("wt_unit_cost")),
    }


def compute_return_clawback_amount(net_amount, net_qty, return_qty):
    """
    Proportional clawback of net credited proceeds for a returned qty.

    clawback = net_amount * (min(return_qty, net_qty) / net_qty)
    """
    net_amount = _round_money(net_amount)
    try:
        net_qty = int(net_qty or 0)
        return_qty = int(return_qty or 0)
    except (TypeError, ValueError):
        return 0.0
    if net_amount <= 0 or net_qty <= 0 or return_qty <= 0:
        return 0.0
    qty = min(return_qty, net_qty)
    return _round_money(net_amount * (qty / float(net_qty)))


def _insert_return_clawback_row(
    db,
    *,
    idempotency_key,
    profile_id,
    buyer_id,
    seller_id,
    transaction_id,
    ti_id,
    qty,
    unit_cost,
    amount,
    currency,
    status,
    note=None,
):
    """Insert one return_clawback ledger row. amount should be negative."""
    existing = _fetch_wt_by_idempotency_key(db, idempotency_key)
    if existing:
        return {
            "code": 200,
            "idempotent_replay": True,
            "wt_uid": existing.get("wt_uid"),
            "wt_amount": _round_money(existing.get("wt_amount")),
            "wt_status": existing.get("wt_status"),
            "row": existing,
        }

    wt_uid = _new_wallet_transaction_uid(db)
    if not wt_uid:
        return {
            "code": 500,
            "message": "Failed to generate wt_uid via new_wallet_transaction_uid",
        }

    now = utc_now_str()
    insert_row = {
        "wt_uid": wt_uid,
        "wt_profile_id": profile_id,
        "wt_buyer_id": buyer_id or "",
        "wt_seller_id": seller_id or "",
        "wt_transaction_id": transaction_id,
        "wt_ti_id": ti_id,
        "wt_type": WT_TYPE_RETURN_CLAWBACK,
        "wt_status": status,
        "wt_qty": int(qty or 0),
        "wt_received_qty_after": 0,
        "wt_unit_cost": _round_money(unit_cost),
        "wt_amount": _round_money(amount),
        "wt_currency": (currency or "USD")[:8],
        "wt_idempotency_key": idempotency_key,
        "wt_note": note,
        "wt_available_at": None,
        "wt_created_at": now,
        "wt_updated_at": now,
    }
    insert_result = db.insert("every_circle.wallet_transactions", insert_row)
    if insert_result.get("code") != 200:
        insert_msg = (insert_result.get("message") or "").lower()
        if "duplicate entry" in insert_msg:
            existing = _fetch_wt_by_idempotency_key(db, idempotency_key)
            if existing:
                return {
                    "code": 200,
                    "idempotent_replay": True,
                    "wt_uid": existing.get("wt_uid"),
                    "wt_amount": _round_money(existing.get("wt_amount")),
                    "wt_status": existing.get("wt_status"),
                    "row": existing,
                }
        return {
            "code": insert_result.get("code", 500),
            "message": insert_result.get(
                "message", "Failed to insert return_clawback row"
            ),
        }
    return {
        "code": 200,
        "idempotent_replay": False,
        "wt_uid": wt_uid,
        "wt_amount": _round_money(amount),
        "wt_status": status,
        "row": insert_row,
    }


def _find_pending_clawback_holds(db, original_ti_uid):
    """Held return_clawback rows created when buyer opened the return request."""
    if not original_ti_uid:
        return []
    q = db.execute(
        """
        SELECT wt_uid, wt_profile_id, wt_amount, wt_status, wt_idempotency_key,
               wt_buyer_id, wt_seller_id, wt_transaction_id, wt_ti_id, wt_currency,
               wt_unit_cost, wt_qty, wt_note
        FROM every_circle.wallet_transactions
        WHERE wt_ti_id = %s
          AND wt_type = %s
          AND wt_status = %s
          AND wt_idempotency_key LIKE 'return_clawback_hold:%%'
        ORDER BY wt_created_at ASC, wt_uid ASC
        """,
        (original_ti_uid, WT_TYPE_RETURN_CLAWBACK, WT_STATUS_HELD),
    )
    return q.get("result") or []


def _finalize_pending_clawback_holds(
    db,
    *,
    original_ti_uid,
    return_ti_uid=None,
    trr_uid=None,
):
    """
    Post held return_clawback rows from return request and debit actual/lifetime.

    Called on return confirm when clawback holds were created at request time.
    """
    holds = _find_pending_clawback_holds(db, original_ti_uid)
    if trr_uid:
        holds = [h for h in holds if (h.get("wt_note") or "") == trr_uid]
    if not holds:
        return None

    total = _round_money(sum(abs(_to_float(h.get("wt_amount"))) for h in holds))
    if total <= 0:
        return {
            "code": 200,
            "skipped": True,
            "clawed": 0,
            "original_ti_uid": original_ti_uid,
            "return_ti_uid": return_ti_uid,
        }

    profile_id = holds[0].get("wt_profile_id")
    now = utc_now_str()
    wt_uids = []
    for row in holds:
        wt_uid = row.get("wt_uid")
        status = row.get("wt_status")
        if status == WT_STATUS_POSTED:
            wt_uids.append(wt_uid)
            continue
        note = row.get("wt_note") or ""
        if return_ti_uid and note and not note.startswith("return_clawback for"):
            note = f"return_clawback for {return_ti_uid}"
        upd = db.update(
            "every_circle.wallet_transactions",
            {"wt_uid": wt_uid},
            {
                "wt_status": WT_STATUS_POSTED,
                "wt_note": note or row.get("wt_note"),
                "wt_updated_at": now,
            },
        )
        if upd.get("code") != 200:
            return {
                "code": upd.get("code", 500),
                "message": upd.get("message", "Failed to finalize clawback hold"),
                "wt_uid": wt_uid,
            }
        wt_uids.append(wt_uid)

    wallet_result = debit_seller_proceeds_from_wallet(db, profile_id, total)
    if wallet_result.get("code") != 200:
        return {
            "code": wallet_result.get("code", 500),
            "message": wallet_result.get(
                "message", "Failed to debit seller proceeds on return finalize"
            ),
            "clawed": total,
            "original_ti_uid": original_ti_uid,
            "return_ti_uid": return_ti_uid,
            "wt_uids": wt_uids,
            "wallet": wallet_result,
        }

    return {
        "code": 200,
        "clawed": total,
        "held_part": total,
        "posted_part": 0.0,
        "original_ti_uid": original_ti_uid,
        "return_ti_uid": return_ti_uid,
        "wt_profile_id": profile_id,
        "wt_uids": wt_uids,
        "idempotent_replay": False,
        "finalized_request_hold": True,
        "wallet": wallet_result,
    }


def clawback_seller_proceeds_on_return(
    db,
    *,
    original_ti_uid,
    return_ti_uid,
    return_qty,
    transaction_uid=None,
    trr_uid=None,
):
    """
    Reverse seller proceeds for returned (previously credited) quantity.

    Computes clawback proportional to return_qty vs net credited qty/amount on
    the original sale line. Inserts negative ``return_clawback`` row(s) and
    debits the seller wallet (pending first, then useable).

    Idempotency key: ``clawback:{return_ti_uid}`` (and ``:held`` / ``:posted``
    suffixes when the clawback splits across buckets).

    Clawback ledger status mirrors the funds being reversed (held vs posted)
    so wallet reconcile buckets stay consistent.
    """
    _ensure_wallet_transactions_table(db)

    if not original_ti_uid or not return_ti_uid:
        return {
            "code": 400,
            "message": "original_ti_uid and return_ti_uid are required",
        }

    try:
        return_qty = int(return_qty or 0)
    except (TypeError, ValueError):
        return {"code": 400, "message": "return_qty must be an integer"}

    if return_qty <= 0:
        return {
            "code": 200,
            "skipped": True,
            "message": "No shipped return qty to claw back",
            "clawed": 0,
            "original_ti_uid": original_ti_uid,
            "return_ti_uid": return_ti_uid,
        }

    finalized = _finalize_pending_clawback_holds(
        db,
        original_ti_uid=original_ti_uid,
        return_ti_uid=return_ti_uid,
        trr_uid=trr_uid,
    )
    if finalized is not None:
        return finalized

    # Full clawback already recorded for this return line.
    primary_key = f"clawback:{return_ti_uid}"
    existing_primary = _fetch_wt_by_idempotency_key(db, primary_key)
    existing_held = _fetch_wt_by_idempotency_key(db, f"{primary_key}:held")
    existing_posted = _fetch_wt_by_idempotency_key(db, f"{primary_key}:posted")
    if existing_primary or existing_held or existing_posted:
        replay_amount = _round_money(
            abs(_to_float((existing_primary or {}).get("wt_amount")))
            + abs(_to_float((existing_held or {}).get("wt_amount")))
            + abs(_to_float((existing_posted or {}).get("wt_amount")))
        )
        return {
            "code": 200,
            "idempotent_replay": True,
            "clawed": replay_amount,
            "original_ti_uid": original_ti_uid,
            "return_ti_uid": return_ti_uid,
            "wt_uids": [
                r.get("wt_uid")
                for r in (existing_primary, existing_held, existing_posted)
                if r
            ],
        }

    line_net = _line_seller_proceeds_net(db, original_ti_uid)
    clawback_amount = compute_return_clawback_amount(
        line_net["net_amount"], line_net["net_qty"], return_qty
    )
    if clawback_amount <= 0:
        return {
            "code": 200,
            "skipped": True,
            "message": "No net credited seller proceeds to claw back",
            "clawed": 0,
            "original_ti_uid": original_ti_uid,
            "return_ti_uid": return_ti_uid,
        }

    profile_id = line_net.get("wt_profile_id")
    transaction_id = transaction_uid or line_net.get("wt_transaction_id")
    if not profile_id:
        # Resolve seller from the sale if credit rows lack profile (edge case).
        if transaction_id:
            tx_q = db.execute(
                """
                SELECT transaction_business_id, transaction_profile_id
                FROM every_circle.transactions
                WHERE transaction_uid = %s
                LIMIT 1
                """,
                (transaction_id,),
            )
            tx_rows = tx_q.get("result") or []
            if tx_rows:
                profile_id = resolve_seller_wallet_profile_id(
                    db, tx_rows[0].get("transaction_business_id")
                )
                if not line_net.get("wt_buyer_id"):
                    line_net["wt_buyer_id"] = tx_rows[0].get("transaction_profile_id")
                if not line_net.get("wt_seller_id"):
                    line_net["wt_seller_id"] = tx_rows[0].get(
                        "transaction_business_id"
                    )
        if not profile_id:
            return {
                "code": 500,
                "message": (
                    "Unable to resolve seller wallet profile for clawback "
                    f"on ti_uid={original_ti_uid!r}"
                ),
                "original_ti_uid": original_ti_uid,
                "return_ti_uid": return_ti_uid,
            }

    if not transaction_id:
        return {
            "code": 500,
            "message": "Unable to resolve transaction_id for clawback",
            "original_ti_uid": original_ti_uid,
            "return_ti_uid": return_ti_uid,
        }

    held_net = max(_to_float(line_net.get("held_amount")), 0.0)
    held_part = _round_money(min(clawback_amount, held_net))
    posted_part = _round_money(clawback_amount - held_part)

    claw_qty = min(return_qty, max(int(line_net.get("net_qty") or 0), 0))
    if clawback_amount > 0 and held_part > 0 and posted_part > 0:
        held_qty = int(round(claw_qty * (held_part / clawback_amount)))
        held_qty = min(max(held_qty, 0), claw_qty)
        posted_qty = claw_qty - held_qty
    elif held_part > 0:
        held_qty = claw_qty
        posted_qty = 0
    else:
        held_qty = 0
        posted_qty = claw_qty

    note = f"return_clawback for {return_ti_uid}"
    inserted = []
    common = {
        "profile_id": profile_id,
        "buyer_id": line_net.get("wt_buyer_id"),
        "seller_id": line_net.get("wt_seller_id"),
        "transaction_id": transaction_id,
        "ti_id": original_ti_uid,
        "unit_cost": line_net.get("wt_unit_cost"),
        "currency": line_net.get("wt_currency"),
        "note": note,
    }

    if held_part > 0 and posted_part > 0:
        held_ins = _insert_return_clawback_row(
            db,
            idempotency_key=f"{primary_key}:held",
            qty=held_qty,
            amount=-held_part,
            status=WT_STATUS_HELD,
            **common,
        )
        if held_ins.get("code") != 200:
            return {
                **held_ins,
                "original_ti_uid": original_ti_uid,
                "return_ti_uid": return_ti_uid,
            }
        inserted.append(held_ins)

        posted_ins = _insert_return_clawback_row(
            db,
            idempotency_key=f"{primary_key}:posted",
            qty=posted_qty,
            amount=-posted_part,
            status=WT_STATUS_POSTED,
            **common,
        )
        if posted_ins.get("code") != 200:
            return {
                **posted_ins,
                "original_ti_uid": original_ti_uid,
                "return_ti_uid": return_ti_uid,
            }
        inserted.append(posted_ins)
    else:
        status = WT_STATUS_HELD if held_part > 0 else WT_STATUS_POSTED
        part = held_part if held_part > 0 else posted_part
        part_qty = held_qty if held_part > 0 else posted_qty
        ins = _insert_return_clawback_row(
            db,
            idempotency_key=primary_key,
            qty=part_qty,
            amount=-part,
            status=status,
            **common,
        )
        if ins.get("code") != 200:
            return {
                **ins,
                "original_ti_uid": original_ti_uid,
                "return_ti_uid": return_ti_uid,
            }
        inserted.append(ins)

    any_new = any(not r.get("idempotent_replay") for r in inserted)
    wallet_result = None
    if any_new and clawback_amount > 0:
        wallet_result = debit_seller_proceeds_from_wallet(
            db, profile_id, clawback_amount
        )
        if wallet_result.get("code") != 200:
            return {
                "code": wallet_result.get("code", 500),
                "message": wallet_result.get(
                    "message", "Failed to debit seller proceeds on return"
                ),
                "clawed": clawback_amount,
                "original_ti_uid": original_ti_uid,
                "return_ti_uid": return_ti_uid,
                "wt_uids": [r.get("wt_uid") for r in inserted],
                "wallet": wallet_result,
            }

    return {
        "code": 200,
        "clawed": clawback_amount,
        "held_part": held_part,
        "posted_part": posted_part,
        "original_ti_uid": original_ti_uid,
        "return_ti_uid": return_ti_uid,
        "wt_profile_id": profile_id,
        "wt_uids": [r.get("wt_uid") for r in inserted],
        "idempotent_replay": not any_new,
        "wallet": wallet_result,
    }


def release_held_wallet_transaction(db, wt_row):
    """
    Move one held ledger credit from wallet_pending to useable and mark posted.

    Idempotent: already-posted rows are skipped. Caller must ensure the hold
    is eligible (``wt_available_at`` passed, no open return on the line).
    """
    if not wt_row:
        return {"code": 400, "message": "wt_row is required"}

    wt_uid = wt_row.get("wt_uid")
    profile_id = wt_row.get("wt_profile_id")
    amount = _round_money(wt_row.get("wt_amount"))
    status = (wt_row.get("wt_status") or "").strip().lower()

    if not wt_uid:
        return {"code": 400, "message": "wt_uid is required"}

    if status == WT_STATUS_POSTED:
        return {
            "code": 200,
            "skipped": True,
            "message": "Already posted",
            "wt_uid": wt_uid,
            "wt_ti_id": wt_row.get("wt_ti_id"),
            "wt_transaction_id": wt_row.get("wt_transaction_id"),
            "moved_to_useable": 0,
        }

    if status != WT_STATUS_HELD:
        return {
            "code": 409,
            "message": f"Unexpected wt_status={wt_row.get('wt_status')!r}",
            "wt_uid": wt_uid,
            "wt_ti_id": wt_row.get("wt_ti_id"),
            "wt_transaction_id": wt_row.get("wt_transaction_id"),
            "skipped": True,
        }

    wallet_result = None
    if amount > 0:
        if not profile_id:
            return {
                "code": 400,
                "message": "wt_profile_id is required to release hold",
                "wt_uid": wt_uid,
            }
        wallet_result = release_seller_hold_to_useable(db, profile_id, amount)
        if wallet_result.get("code") != 200:
            return {
                "code": wallet_result.get("code", 500),
                "message": wallet_result.get(
                    "message", "Failed to release seller hold to useable"
                ),
                "wt_uid": wt_uid,
                "wt_ti_id": wt_row.get("wt_ti_id"),
                "wt_transaction_id": wt_row.get("wt_transaction_id"),
                "wallet": wallet_result,
            }

    now = utc_now_str()
    upd = db.update(
        "every_circle.wallet_transactions",
        {"wt_uid": wt_uid},
        {
            "wt_status": WT_STATUS_POSTED,
            "wt_updated_at": now,
        },
    )
    if upd.get("code") != 200:
        return {
            "code": upd.get("code", 500),
            "message": upd.get(
                "message", "Failed to mark wallet_transactions row posted"
            ),
            "wt_uid": wt_uid,
            "wt_ti_id": wt_row.get("wt_ti_id"),
            "wt_transaction_id": wt_row.get("wt_transaction_id"),
            "wallet": wallet_result,
        }

    # Held return_clawback rows on this line must flip with the credit so
    # reconcile pending/useable buckets stay aligned after release.
    ti_id = wt_row.get("wt_ti_id")
    if ti_id:
        db.execute(
            """
            UPDATE every_circle.wallet_transactions
            SET wt_status = %s, wt_updated_at = %s
            WHERE wt_ti_id = %s
              AND wt_type = %s
              AND wt_status = %s
            """,
            (
                WT_STATUS_POSTED,
                now,
                ti_id,
                WT_TYPE_RETURN_CLAWBACK,
                WT_STATUS_HELD,
            ),
            cmd="post",
        )

    bounty_release = release_bounty_for_line_net(db, ti_id) if ti_id else None

    return {
        "code": 200,
        "message": "Seller hold released",
        "wt_uid": wt_uid,
        "wt_ti_id": wt_row.get("wt_ti_id"),
        "wt_transaction_id": wt_row.get("wt_transaction_id"),
        "wt_profile_id": profile_id,
        "moved_to_useable": (
            wallet_result.get("moved_to_useable", amount) if wallet_result else 0
        ),
        "wallet": wallet_result,
        "bounty_release": bounty_release,
    }


def _line_held_proceeds_total(db, ti_uid):
    """Sum held partial_delivery_credit amounts for a sale line."""
    if not ti_uid:
        return 0.0
    q = db.execute(
        """
        SELECT COALESCE(SUM(wt_amount), 0) AS held_total
        FROM every_circle.wallet_transactions
        WHERE wt_ti_id = %s
          AND wt_type = %s
          AND wt_status = %s
        """,
        (ti_uid, WT_TYPE_PARTIAL_DELIVERY_CREDIT, WT_STATUS_HELD),
    )
    rows = q.get("result") or []
    if not rows:
        return 0.0
    return _round_money(rows[0].get("held_total"))


def release_partial_held_wallet_transaction(db, wt_row, release_amount):
    """
    Release part of a held partial_delivery_credit row to useable.

    Splits the row when release_amount < wt_amount: the released portion is
    marked posted; the remainder stays held with a new wt_uid.
    """
    if not wt_row:
        return {"code": 400, "message": "wt_row is required"}

    wt_uid = wt_row.get("wt_uid")
    profile_id = wt_row.get("wt_profile_id")
    full_amount = _round_money(wt_row.get("wt_amount"))
    release_amount = _round_money(release_amount)

    if release_amount <= 0:
        return {
            "code": 200,
            "skipped": True,
            "message": "Nothing to release",
            "wt_uid": wt_uid,
            "moved_to_useable": 0,
        }

    if release_amount >= full_amount:
        return release_held_wallet_transaction(db, wt_row)

    wallet_result = release_seller_hold_to_useable(db, profile_id, release_amount)
    if wallet_result.get("code") != 200:
        return {
            "code": wallet_result.get("code", 500),
            "message": wallet_result.get(
                "message", "Failed partial release to useable"
            ),
            "wt_uid": wt_uid,
            "wallet": wallet_result,
        }

    remainder = _round_money(full_amount - release_amount)
    now = utc_now_str()
    new_uid = _new_wallet_transaction_uid(db)
    if not new_uid:
        return {"code": 500, "message": "Failed to generate wt_uid for split hold"}

    released_row = dict(wt_row)
    released_row.update(
        {
            "wt_uid": wt_uid,
            "wt_amount": release_amount,
            "wt_status": WT_STATUS_POSTED,
            "wt_updated_at": now,
        }
    )
    upd = db.update(
        "every_circle.wallet_transactions",
        {"wt_uid": wt_uid},
        {
            "wt_amount": release_amount,
            "wt_status": WT_STATUS_POSTED,
            "wt_updated_at": now,
        },
    )
    if upd.get("code") != 200:
        return {
            "code": upd.get("code", 500),
            "message": upd.get("message", "Failed to update released portion"),
            "wt_uid": wt_uid,
        }

    held_row = dict(wt_row)
    held_row.update(
        {
            "wt_uid": new_uid,
            "wt_amount": remainder,
            "wt_status": WT_STATUS_HELD,
            "wt_idempotency_key": f"{wt_row.get('wt_idempotency_key')}:held_remainder",
            "wt_created_at": now,
            "wt_updated_at": now,
        }
    )
    ins = db.insert("every_circle.wallet_transactions", held_row)
    if ins.get("code") != 200:
        return {
            "code": ins.get("code", 500),
            "message": ins.get("message", "Failed to insert held remainder row"),
            "wt_uid": wt_uid,
        }

    ti_id = wt_row.get("wt_ti_id")
    bounty_release = release_bounty_for_line_net(db, ti_id) if ti_id else None

    return {
        "code": 200,
        "message": "Partial seller hold released",
        "wt_uid": wt_uid,
        "wt_ti_id": ti_id,
        "wt_transaction_id": wt_row.get("wt_transaction_id"),
        "wt_profile_id": profile_id,
        "moved_to_useable": release_amount,
        "partial": True,
        "held_remainder_wt_uid": new_uid,
        "wallet": wallet_result,
        "bounty_release": bounty_release,
    }
