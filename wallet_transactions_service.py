"""
Seller wallet_transactions ledger: table ensure, seller-eligible totals,
and partial-delivery credit (idempotent insert + wallet credit).
"""

import re

from datetime_utils import utc_now_str
from wallet_service import _round_money, _to_float, credit_seller_proceeds_to_wallet

WT_TYPE_PARTIAL_DELIVERY_CREDIT = "partial_delivery_credit"
WT_STATUS_POSTED = "posted"

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
            wt_created_at DATETIME NOT NULL,
            wt_updated_at DATETIME NOT NULL,
            PRIMARY KEY (wt_uid),
            UNIQUE KEY uq_wt_idempotency (wt_idempotency_key),
            KEY idx_wt_transaction_id (wt_transaction_id),
            KEY idx_wt_ti_id (wt_ti_id),
            KEY idx_wt_profile_id (wt_profile_id)
        )
        """,
        cmd="post",
    )
    # Older installs may lack the unique key or lookup indexes.
    for ddl in (
        "ALTER TABLE every_circle.wallet_transactions "
        "ADD UNIQUE KEY uq_wt_idempotency (wt_idempotency_key)",
        "ALTER TABLE every_circle.wallet_transactions "
        "ADD KEY idx_wt_transaction_id (wt_transaction_id)",
        "ALTER TABLE every_circle.wallet_transactions "
        "ADD KEY idx_wt_ti_id (wt_ti_id)",
        "ALTER TABLE every_circle.wallet_transactions "
        "ADD KEY idx_wt_profile_id (wt_profile_id)",
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


def _posted_credits_total(db, transaction_uid):
    q = db.execute(
        """
        SELECT COALESCE(SUM(wt_amount), 0) AS credited
        FROM every_circle.wallet_transactions
        WHERE wt_transaction_id = %s
          AND wt_type = %s
          AND wt_status = %s
        """,
        (transaction_uid, WT_TYPE_PARTIAL_DELIVERY_CREDIT, WT_STATUS_POSTED),
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
    Capped so cumulative posted credits never exceed seller_eligible_total.
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
               wt_transaction_id, wt_ti_id, wt_currency, wt_status, wt_type
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


def credit_partial_delivery(db, transaction_uid, ti_uid, qty, received_qty_after):
    """
    Insert a partial_delivery_credit ledger row and credit the seller wallet.

    Idempotency key: ``{ti_uid}:{received_qty_after}``. Duplicate key is a
    success/no-op (existing row returned, wallet not re-credited).
    """
    _ensure_wallet_transactions_table(db)

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
        SELECT ti_uid, ti_transaction_id, ti_bs_cost, ti_bs_cost_currency, ti_bs_qty
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
        return _wt_success_payload(existing, idempotent_replay=True)

    # Repair prior zero-amount ledger rows (e.g. unparseable "100/each" unit cost).
    if existing and _to_float(existing.get("wt_amount")) == 0 and credit_amount > 0:
        now = utc_now_str()
        upd = db.update(
            "every_circle.wallet_transactions",
            {"wt_uid": existing.get("wt_uid")},
            {
                "wt_unit_cost": _round_money(unit_cost),
                "wt_amount": credit_amount,
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
            db, seller_profile_id, credit_amount
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
        return _wt_success_payload(
            repaired, idempotent_replay=False, wallet_result=wallet_result
        )

    if existing:
        return _wt_success_payload(existing, idempotent_replay=True)

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
        "wt_status": WT_STATUS_POSTED,
        "wt_qty": qty,
        "wt_received_qty_after": received_qty_after,
        "wt_unit_cost": _round_money(unit_cost),
        "wt_amount": credit_amount,
        "wt_currency": currency[:8],
        "wt_idempotency_key": idempotency_key,
        "wt_note": None,
        "wt_created_at": now,
        "wt_updated_at": now,
    }

    insert_result = db.insert("every_circle.wallet_transactions", insert_row)
    if insert_result.get("code") != 200:
        insert_msg = (insert_result.get("message") or "").lower()
        if "duplicate entry" in insert_msg:
            existing = _fetch_wt_by_idempotency_key(db, idempotency_key)
            if existing:
                return _wt_success_payload(existing, idempotent_replay=True)
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
            db, seller_profile_id, credit_amount
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

    return _wt_success_payload(
        insert_row, idempotent_replay=False, wallet_result=wallet_result
    )
