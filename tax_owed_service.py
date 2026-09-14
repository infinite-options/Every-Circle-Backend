"""
Tax owed liability ledger writers.

Credits sales tax collected at checkout into tax_transactions / tax_owed, and
reverses prorated tax on return confirm. Does not change wallet proceeds math.
"""

from datetime_utils import utc_now_str
from tax_owed_schema import ensure_tax_owed_tables, new_tax_transaction_uid
from wallet_service import _round_money, _to_float
from wallet_transactions_service import resolve_seller_wallet_profile_id

TT_TYPE_TAX_COLLECTED = "tax_collected"
TT_TYPE_TAX_REVERSED = "tax_reversed"
TT_TYPE_TAX_REMITTED = "tax_remitted"
TT_STATUS_POSTED = "posted"

CHECKOUT_TAX_IDEMPOTENCY_SUFFIX = "tax:checkout"
REMIT_SENTINEL = "remit"


def _checkout_tax_idempotency_key(ti_uid):
    return f"{ti_uid}:{CHECKOUT_TAX_IDEMPOTENCY_SUFFIX}"


def _reverse_tax_idempotency_key(return_ti_uid):
    return f"tax_reverse:{return_ti_uid}"


def _fetch_tt_by_idempotency_key(db, idempotency_key):
    if not idempotency_key:
        return None
    q = db.execute(
        """
        SELECT *
        FROM every_circle.tax_transactions
        WHERE tt_idempotency_key = %s
        LIMIT 1
        """,
        (idempotency_key,),
    )
    rows = q.get("result") or []
    return rows[0] if rows else None


def get_tax_owed_row(db, profile_id):
    """Return tax_owed snapshot for profile_id, or None."""
    if not profile_id:
        return None
    ensure_tax_owed_tables(db)
    q = db.execute(
        """
        SELECT
            tax_owed_profile_id,
            tax_owed_balance,
            tax_owed_lifetime_collected,
            tax_owed_lifetime_reversed,
            tax_owed_lifetime_remitted,
            tax_owed_updated_at
        FROM every_circle.tax_owed
        WHERE tax_owed_profile_id = %s
        LIMIT 1
        """,
        (profile_id,),
    )
    rows = q.get("result") or []
    return rows[0] if rows else None


def build_tax_owed_summary(db, profile_id):
    """API-shaped tax_owed block (zeros when no row yet)."""
    row = get_tax_owed_row(db, profile_id)
    if not row:
        return {
            "tax_owed_profile_id": profile_id,
            "tax_owed_balance": 0.0,
            "tax_owed_lifetime_collected": 0.0,
            "tax_owed_lifetime_reversed": 0.0,
            "tax_owed_lifetime_remitted": 0.0,
            "currency": "USD",
        }
    return {
        "tax_owed_profile_id": row.get("tax_owed_profile_id") or profile_id,
        "tax_owed_balance": _round_money(row.get("tax_owed_balance")),
        "tax_owed_lifetime_collected": _round_money(
            row.get("tax_owed_lifetime_collected")
        ),
        "tax_owed_lifetime_reversed": _round_money(
            row.get("tax_owed_lifetime_reversed")
        ),
        "tax_owed_lifetime_remitted": _round_money(
            row.get("tax_owed_lifetime_remitted")
        ),
        "currency": "USD",
    }


def _apply_tax_owed_delta(
    db,
    profile_id,
    *,
    collected_delta=0.0,
    reversed_delta=0.0,
    remitted_delta=0.0,
):
    """
    Upsert tax_owed balances.

    collected_delta: positive tax collected (balance up)
    reversed_delta: positive absolute tax reversed (balance down)
    remitted_delta: positive absolute tax remitted/paid (balance down)
    """
    collected_delta = _round_money(collected_delta)
    reversed_delta = _round_money(reversed_delta)
    remitted_delta = _round_money(remitted_delta)
    if collected_delta == 0 and reversed_delta == 0 and remitted_delta == 0:
        return {"code": 200, "skipped": True}

    now = utc_now_str()
    existing = get_tax_owed_row(db, profile_id)
    if not existing:
        balance = _round_money(collected_delta - reversed_delta - remitted_delta)
        insert_result = db.insert(
            "every_circle.tax_owed",
            {
                "tax_owed_profile_id": profile_id,
                "tax_owed_balance": balance,
                "tax_owed_lifetime_collected": collected_delta,
                "tax_owed_lifetime_reversed": reversed_delta,
                "tax_owed_lifetime_remitted": remitted_delta,
                "tax_owed_updated_at": now,
            },
        )
        if insert_result.get("code") != 200:
            return {
                "code": insert_result.get("code", 500),
                "message": insert_result.get(
                    "message", "Failed to insert tax_owed row"
                ),
            }
        return {"code": 200, "tax_owed_balance": balance, "created": True}

    balance = _round_money(
        _to_float(existing.get("tax_owed_balance"))
        + collected_delta
        - reversed_delta
        - remitted_delta
    )
    lifetime_collected = _round_money(
        _to_float(existing.get("tax_owed_lifetime_collected")) + collected_delta
    )
    lifetime_reversed = _round_money(
        _to_float(existing.get("tax_owed_lifetime_reversed")) + reversed_delta
    )
    lifetime_remitted = _round_money(
        _to_float(existing.get("tax_owed_lifetime_remitted")) + remitted_delta
    )
    update_result = db.update(
        "every_circle.tax_owed",
        {"tax_owed_profile_id": profile_id},
        {
            "tax_owed_balance": balance,
            "tax_owed_lifetime_collected": lifetime_collected,
            "tax_owed_lifetime_reversed": lifetime_reversed,
            "tax_owed_lifetime_remitted": lifetime_remitted,
            "tax_owed_updated_at": now,
        },
    )
    if update_result.get("code") != 200:
        return {
            "code": update_result.get("code", 500),
            "message": update_result.get(
                "message", "Failed to update tax_owed row"
            ),
        }
    return {"code": 200, "tax_owed_balance": balance, "created": False}


def _insert_tax_transaction(db, row):
    result = db.insert("every_circle.tax_transactions", row)
    if result.get("code") == 200:
        return result
    msg = (result.get("message") or "").lower()
    if "duplicate entry" in msg:
        existing = _fetch_tt_by_idempotency_key(db, row.get("tt_idempotency_key"))
        if existing:
            return {
                "code": 200,
                "idempotent_replay": True,
                "existing": existing,
            }
    return result


def credit_tax_collected_at_checkout(db, transaction_uid):
    """
    Credit seller tax_owed for each taxable sale line at order placement.

    One posted ``tax_collected`` row per line with tax > 0.
    Idempotent via ``{ti_uid}:tax:checkout``.
    """
    if not transaction_uid:
        return {"code": 400, "message": "transaction_uid is required"}

    ensure_tax_owed_tables(db)

    tx_q = db.execute(
        """
        SELECT transaction_uid, transaction_profile_id, transaction_business_id
        FROM every_circle.transactions
        WHERE transaction_uid = %s
        """,
        (transaction_uid,),
    )
    tx_rows = tx_q.get("result") or []
    if not tx_rows:
        return {"code": 404, "message": f"Transaction not found: {transaction_uid}"}
    tx = tx_rows[0]

    seller_id = tx.get("transaction_business_id")
    seller_profile_id = resolve_seller_wallet_profile_id(db, seller_id)
    if not seller_profile_id:
        return {
            "code": 500,
            "message": (
                "Unable to resolve seller tax profile for "
                f"transaction_business_id={seller_id!r}"
            ),
            "transaction_uid": transaction_uid,
        }

    from line_commerce_fields import _line_tax_snapshot, load_commerce_sale_line

    lines_q = db.execute(
        """
        SELECT ti_uid, ti_bs_qty, ti_bs_cost_currency, ti_bs_tax_rate
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s
        ORDER BY ti_uid ASC
        """,
        (transaction_uid,),
    )
    line_rows = lines_q.get("result") or []
    credited = []
    skipped = []
    total_credited = 0.0

    for ti in line_rows:
        ti_uid = ti.get("ti_uid")
        if not ti_uid:
            continue
        order_qty = int(ti.get("ti_bs_qty") or 0)
        if order_qty < 1:
            skipped.append({"ti_uid": ti_uid, "reason": "qty < 1"})
            continue

        idempotency_key = _checkout_tax_idempotency_key(ti_uid)
        existing = _fetch_tt_by_idempotency_key(db, idempotency_key)
        if existing and _to_float(existing.get("tt_amount")) > 0:
            skipped.append(
                {
                    "ti_uid": ti_uid,
                    "reason": "already_credited",
                    "tt_uid": existing.get("tt_uid"),
                    "tt_amount": existing.get("tt_amount"),
                }
            )
            continue

        line_row = load_commerce_sale_line(db, transaction_uid, ti_uid)
        if not line_row:
            skipped.append({"ti_uid": ti_uid, "reason": "sale_line_not_found"})
            continue

        tax_amount = _line_tax_snapshot(line_row)
        if tax_amount is None:
            tax_amount = 0.0
        tax_amount = _round_money(tax_amount)
        if tax_amount <= 0:
            skipped.append({"ti_uid": ti_uid, "reason": "zero_tax"})
            continue

        currency = (ti.get("ti_bs_cost_currency") or "USD").strip() or "USD"
        tax_rate = ti.get("ti_bs_tax_rate")
        if tax_rate is None and line_row:
            tax_rate = line_row.get("ti_bs_tax_rate")
        now = utc_now_str()
        tt_uid = new_tax_transaction_uid(db)
        if not tt_uid:
            return {
                "code": 500,
                "message": "Failed to generate tt_uid via new_tax_transaction_uid",
                "transaction_uid": transaction_uid,
                "ti_uid": ti_uid,
            }

        insert_row = {
            "tt_uid": tt_uid,
            "tt_profile_id": seller_profile_id,
            "tt_buyer_id": tx.get("transaction_profile_id"),
            "tt_seller_id": seller_id,
            "tt_transaction_id": transaction_uid,
            "tt_ti_id": ti_uid,
            "tt_type": TT_TYPE_TAX_COLLECTED,
            "tt_status": TT_STATUS_POSTED,
            "tt_qty": order_qty,
            "tt_amount": tax_amount,
            "tt_currency": currency[:8],
            "tt_tax_rate": tax_rate,
            "tt_idempotency_key": idempotency_key,
            "tt_note": "sales tax collected at checkout",
            "tt_created_at": now,
            "tt_updated_at": now,
        }
        insert_result = _insert_tax_transaction(db, insert_row)
        if insert_result.get("idempotent_replay"):
            existing = insert_result.get("existing") or {}
            skipped.append(
                {
                    "ti_uid": ti_uid,
                    "reason": "already_credited",
                    "tt_uid": existing.get("tt_uid"),
                }
            )
            continue
        if insert_result.get("code") != 200:
            return {
                "code": insert_result.get("code", 500),
                "message": insert_result.get(
                    "message", "Failed to insert tax_transactions row"
                ),
                "transaction_uid": transaction_uid,
                "ti_uid": ti_uid,
            }

        balance_result = _apply_tax_owed_delta(
            db, seller_profile_id, collected_delta=tax_amount
        )
        if balance_result.get("code") != 200:
            return {
                "code": balance_result.get("code", 500),
                "message": balance_result.get(
                    "message", "Failed to update tax_owed at checkout"
                ),
                "transaction_uid": transaction_uid,
                "ti_uid": ti_uid,
                "tt_uid": tt_uid,
            }

        total_credited = _round_money(total_credited + tax_amount)
        credited.append(
            {
                "ti_uid": ti_uid,
                "tt_uid": tt_uid,
                "tt_amount": tax_amount,
                "tt_qty": order_qty,
            }
        )

    return {
        "code": 200,
        "transaction_uid": transaction_uid,
        "seller_profile_id": seller_profile_id,
        "credited_total": total_credited,
        "lines": credited,
        "skipped": skipped,
    }


def reverse_tax_on_return(
    db,
    *,
    original_ti_uid,
    return_ti_uid,
    return_qty,
    transaction_uid=None,
):
    """
    Reverse prorated sales tax for returned/cancelled quantity.

    Inserts negative ``tax_reversed`` row; idempotent via
    ``tax_reverse:{return_ti_uid}``.
    """
    ensure_tax_owed_tables(db)

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
            "message": "No return qty to reverse tax",
            "reversed": 0,
            "original_ti_uid": original_ti_uid,
            "return_ti_uid": return_ti_uid,
        }

    idempotency_key = _reverse_tax_idempotency_key(return_ti_uid)
    existing = _fetch_tt_by_idempotency_key(db, idempotency_key)
    if existing:
        return {
            "code": 200,
            "idempotent_replay": True,
            "reversed": abs(_to_float(existing.get("tt_amount"))),
            "original_ti_uid": original_ti_uid,
            "return_ti_uid": return_ti_uid,
            "tt_uid": existing.get("tt_uid"),
        }

    from line_commerce_fields import _line_tax_snapshot

    ti_q = db.execute(
        """
        SELECT
            ti.ti_uid,
            ti.ti_transaction_id,
            ti.ti_bs_qty,
            ti.ti_bs_cost_currency,
            ti.ti_bs_is_taxable,
            ti.ti_bs_tax_rate,
            ti.ti_line_tax_amount,
            ti.ti_tax_amount,
            t.transaction_profile_id,
            t.transaction_business_id
        FROM every_circle.transactions_items ti
        INNER JOIN every_circle.transactions t
            ON t.transaction_uid = ti.ti_transaction_id
        WHERE ti.ti_uid = %s
        LIMIT 1
        """,
        (original_ti_uid,),
    )
    ti_rows = ti_q.get("result") or []
    if not ti_rows:
        return {
            "code": 404,
            "message": f"Original sale line not found: {original_ti_uid}",
            "reversed": 0,
        }
    ti_row = ti_rows[0]
    sale_txn_uid = transaction_uid or ti_row.get("ti_transaction_id")
    orig_qty = int(ti_row.get("ti_bs_qty") or 0)
    if orig_qty <= 0:
        return {
            "code": 200,
            "skipped": True,
            "message": "Original line qty is zero",
            "reversed": 0,
        }

    stored_line_tax = _line_tax_snapshot(ti_row)
    if stored_line_tax is None:
        stored_line_tax = 0.0
    tax_refund = _round_money(_to_float(stored_line_tax) * return_qty / orig_qty)
    if tax_refund <= 0:
        return {
            "code": 200,
            "skipped": True,
            "message": "No tax to reverse",
            "reversed": 0,
            "original_ti_uid": original_ti_uid,
            "return_ti_uid": return_ti_uid,
        }

    seller_id = ti_row.get("transaction_business_id")
    seller_profile_id = resolve_seller_wallet_profile_id(db, seller_id)
    if not seller_profile_id:
        return {
            "code": 500,
            "message": (
                "Unable to resolve seller tax profile for "
                f"transaction_business_id={seller_id!r}"
            ),
            "reversed": 0,
        }

    signed_amount = _round_money(-tax_refund)
    currency = (ti_row.get("ti_bs_cost_currency") or "USD").strip() or "USD"
    now = utc_now_str()
    tt_uid = new_tax_transaction_uid(db)
    if not tt_uid:
        return {
            "code": 500,
            "message": "Failed to generate tt_uid via new_tax_transaction_uid",
            "reversed": 0,
        }

    insert_row = {
        "tt_uid": tt_uid,
        "tt_profile_id": seller_profile_id,
        "tt_buyer_id": ti_row.get("transaction_profile_id"),
        "tt_seller_id": seller_id,
        "tt_transaction_id": sale_txn_uid,
        "tt_ti_id": return_ti_uid,
        "tt_type": TT_TYPE_TAX_REVERSED,
        "tt_status": TT_STATUS_POSTED,
        "tt_qty": return_qty,
        "tt_amount": signed_amount,
        "tt_currency": currency[:8],
        "tt_tax_rate": ti_row.get("ti_bs_tax_rate"),
        "tt_idempotency_key": idempotency_key,
        "tt_note": f"sales tax reversed on return ({return_qty} of {orig_qty})",
        "tt_created_at": now,
        "tt_updated_at": now,
    }
    insert_result = _insert_tax_transaction(db, insert_row)
    if insert_result.get("idempotent_replay"):
        existing = insert_result.get("existing") or {}
        return {
            "code": 200,
            "idempotent_replay": True,
            "reversed": abs(_to_float(existing.get("tt_amount"))),
            "tt_uid": existing.get("tt_uid"),
            "original_ti_uid": original_ti_uid,
            "return_ti_uid": return_ti_uid,
        }
    if insert_result.get("code") != 200:
        return {
            "code": insert_result.get("code", 500),
            "message": insert_result.get(
                "message", "Failed to insert tax reverse row"
            ),
            "reversed": 0,
        }

    balance_result = _apply_tax_owed_delta(
        db, seller_profile_id, reversed_delta=tax_refund
    )
    if balance_result.get("code") != 200:
        return {
            "code": balance_result.get("code", 500),
            "message": balance_result.get(
                "message", "Failed to update tax_owed on reverse"
            ),
            "reversed": 0,
            "tt_uid": tt_uid,
        }

    return {
        "code": 200,
        "reversed": tax_refund,
        "tt_uid": tt_uid,
        "tt_amount": signed_amount,
        "seller_profile_id": seller_profile_id,
        "original_ti_uid": original_ti_uid,
        "return_ti_uid": return_ti_uid,
    }


def remit_tax_owed(db, profile_id, amount, *, note=None, idempotency_key=None):
    """
    Record that the seller remitted sales tax outside the app.

    Inserts a negative ``tax_remitted`` row, increases lifetime_remitted,
    and decreases tax_owed_balance. Does not touch wallet.
    """
    ensure_tax_owed_tables(db)
    profile_id = str(profile_id or "").strip()
    if not profile_id:
        return {"code": 400, "message": "profile_id is required"}

    try:
        amount = _round_money(amount)
    except Exception:
        return {"code": 400, "message": "amount must be a number"}

    if amount <= 0:
        return {"code": 400, "message": "amount must be greater than zero"}

    summary = build_tax_owed_summary(db, profile_id)
    owed_balance = _round_money(summary.get("tax_owed_balance"))
    if owed_balance <= 0:
        return {
            "code": 400,
            "message": "No tax owed to remit",
            "tax_owed": summary,
        }

    if amount > owed_balance:
        amount = owed_balance

    key = (idempotency_key or "").strip()
    if not key:
        # Unique per submit when client does not send a key.
        import time

        key = f"tax_remit:{profile_id}:{int(time.time() * 1000)}"
    else:
        key = key[:128]

    existing = _fetch_tt_by_idempotency_key(db, key)
    if existing:
        return {
            "code": 200,
            "idempotent_replay": True,
            "remitted": abs(_to_float(existing.get("tt_amount"))),
            "ledger_entry_uid": existing.get("tt_uid"),
            "tax_owed": build_tax_owed_summary(db, profile_id),
        }

    signed_amount = _round_money(-amount)
    now = utc_now_str()
    tt_uid = new_tax_transaction_uid(db)
    if not tt_uid:
        return {
            "code": 500,
            "message": "Failed to generate tt_uid via new_tax_transaction_uid",
        }

    note_text = (note or "").strip() or "sales tax remitted by seller"
    insert_row = {
        "tt_uid": tt_uid,
        "tt_profile_id": profile_id,
        "tt_buyer_id": profile_id,
        "tt_seller_id": profile_id,
        "tt_transaction_id": REMIT_SENTINEL,
        "tt_ti_id": REMIT_SENTINEL,
        "tt_type": TT_TYPE_TAX_REMITTED,
        "tt_status": TT_STATUS_POSTED,
        "tt_qty": 0,
        "tt_amount": signed_amount,
        "tt_currency": "USD",
        "tt_tax_rate": None,
        "tt_idempotency_key": key,
        "tt_note": note_text[:512],
        "tt_created_at": now,
        "tt_updated_at": now,
    }
    insert_result = _insert_tax_transaction(db, insert_row)
    if insert_result.get("idempotent_replay"):
        existing = insert_result.get("existing") or {}
        return {
            "code": 200,
            "idempotent_replay": True,
            "remitted": abs(_to_float(existing.get("tt_amount"))),
            "ledger_entry_uid": existing.get("tt_uid"),
            "tax_owed": build_tax_owed_summary(db, profile_id),
        }
    if insert_result.get("code") != 200:
        return {
            "code": insert_result.get("code", 500),
            "message": insert_result.get(
                "message", "Failed to insert tax remittance row"
            ),
        }

    balance_result = _apply_tax_owed_delta(
        db, profile_id, remitted_delta=amount
    )
    if balance_result.get("code") != 200:
        return {
            "code": balance_result.get("code", 500),
            "message": balance_result.get(
                "message", "Failed to update tax_owed on remit"
            ),
            "ledger_entry_uid": tt_uid,
        }

    return {
        "code": 200,
        "message": "Tax remittance recorded successfully",
        "remitted": amount,
        "ledger_entry_uid": tt_uid,
        "tax_owed": build_tax_owed_summary(db, profile_id),
    }
