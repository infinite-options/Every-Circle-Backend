"""
Wallet balance updates kept in sync with transactions_bounty + escrow state.
"""

from wallet_ids import resolve_wallet_profile_id


def _to_float(value):
    if value is None:
        return 0.0
    if isinstance(value, str):
        value = value.replace("$", "").replace(",", "").strip()
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _round_money(value):
    return round(_to_float(value), 4)


def get_wallet_row(db, bounty_profile_id):
    wallet_id = resolve_wallet_profile_id(bounty_profile_id)
    wallet_q = db.execute(
        """
        SELECT
            wallet_profile_id,
            wallet_pending,
            wallet_useable_balance,
            wallet_actual_balance,
            wallet_lifetime_earning,
            wallet_reserve,
            wallet_lifetime_spent
        FROM every_circle.wallet
        WHERE wallet_profile_id = %s
        """,
        (wallet_id,),
    )
    wallets = wallet_q.get("result") or []
    return wallets[0] if wallets else None


def credit_bounty_to_wallet(db, bounty_profile_id, amount, in_escrow=False):
    """
    Add bounty at purchase time.
    Always increases actual + lifetime; splits pending vs useable by escrow flag.
    """
    amount = _round_money(amount)
    if not bounty_profile_id or amount <= 0:
        return {"code": 200, "skipped": True, "wallet_profile_id": bounty_profile_id}

    wallet_id = resolve_wallet_profile_id(bounty_profile_id)
    wallet = get_wallet_row(db, bounty_profile_id)

    if wallet:
        actual = _to_float(wallet.get("wallet_actual_balance"))
        useable = _to_float(wallet.get("wallet_useable_balance"))
        pending = _to_float(wallet.get("wallet_pending"))
        lifetime = _to_float(wallet.get("wallet_lifetime_earning"))

        updates = {
            "wallet_actual_balance": _round_money(actual + amount),
            "wallet_lifetime_earning": _round_money(lifetime + amount),
        }
        if in_escrow:
            updates["wallet_pending"] = _round_money(pending + amount)
        else:
            updates["wallet_useable_balance"] = _round_money(useable + amount)

        result = db.update(
            "every_circle.wallet",
            {"wallet_profile_id": wallet_id},
            updates,
        )
        if result.get("code") != 200:
            return {
                "code": result.get("code", 500),
                "message": result.get("message", "Failed to update wallet"),
                "wallet_profile_id": bounty_profile_id,
            }
        return {
            "code": 200,
            "wallet_profile_id": bounty_profile_id,
            "wallet_pk": wallet_id,
            "credited": amount,
            "in_escrow": bool(in_escrow),
            "wallet_created": False,
        }

    insert_result = db.insert(
        "every_circle.wallet",
        {
            "wallet_profile_id": wallet_id,
            "wallet_actual_balance": amount,
            "wallet_pending": amount if in_escrow else 0,
            "wallet_useable_balance": 0 if in_escrow else amount,
            "wallet_reserve": 0,
            "wallet_lifetime_earning": amount,
            "wallet_lifetime_spent": 0,
        },
    )
    if insert_result.get("code") != 200:
        return {
            "code": insert_result.get("code", 500),
            "message": insert_result.get("message", "Failed to create wallet"),
            "wallet_profile_id": bounty_profile_id,
        }
    return {
        "code": 200,
        "wallet_profile_id": bounty_profile_id,
        "wallet_pk": wallet_id,
        "credited": amount,
        "in_escrow": bool(in_escrow),
        "wallet_created": True,
    }


def _release_existing_wallet(db, wallet, bounty_profile_id, amount):
    stored_id = wallet.get("wallet_profile_id")
    pending = _to_float(wallet.get("wallet_pending"))
    useable = _to_float(wallet.get("wallet_useable_balance"))
    actual = _to_float(wallet.get("wallet_actual_balance"))
    lifetime = _to_float(wallet.get("wallet_lifetime_earning"))

    from_pending = min(amount, pending)
    remainder = amount - from_pending

    updates = {
        "wallet_pending": _round_money(pending - from_pending),
        "wallet_useable_balance": _round_money(useable + amount),
    }

    # Legacy rows: bounty ledger exists but wallet was never credited at purchase.
    if remainder > 0 and lifetime < amount:
        gap = _round_money(amount - lifetime)
        if gap > 0:
            updates["wallet_actual_balance"] = _round_money(actual + gap)
            updates["wallet_lifetime_earning"] = _round_money(lifetime + gap)

    result = db.update(
        "every_circle.wallet",
        {"wallet_profile_id": stored_id},
        updates,
    )
    if result.get("code") != 200:
        return {
            "code": result.get("code", 500),
            "message": result.get("message", "Failed to release wallet bounty"),
            "wallet_profile_id": bounty_profile_id,
        }

    return {
        "code": 200,
        "wallet_profile_id": bounty_profile_id,
        "wallet_pk": stored_id,
        "moved_to_useable": amount,
        "from_pending": from_pending,
        "wallet_created": False,
    }


def release_bounty_to_useable(db, bounty_profile_id, amount):
    """
    Move escrowed bounty to useable when transaction_in_escrow clears.
    Does not change lifetime/actual when purchase credited correctly.
    """
    amount = _round_money(amount)
    if not bounty_profile_id or amount <= 0:
        return {
            "code": 200,
            "wallet_profile_id": bounty_profile_id,
            "moved_to_useable": 0,
            "wallet_created": False,
        }

    wallet_id = resolve_wallet_profile_id(bounty_profile_id)
    wallet = get_wallet_row(db, bounty_profile_id)

    if not wallet:
        insert_result = db.insert(
            "every_circle.wallet",
            {
                "wallet_profile_id": wallet_id,
                "wallet_actual_balance": amount,
                "wallet_pending": 0,
                "wallet_useable_balance": amount,
                "wallet_reserve": 0,
                "wallet_lifetime_earning": amount,
                "wallet_lifetime_spent": 0,
            },
        )
        if insert_result.get("code") != 200:
            insert_msg = insert_result.get("message", "")
            if "duplicate entry" in insert_msg.lower():
                wallet = get_wallet_row(db, bounty_profile_id)
                if wallet:
                    return _release_existing_wallet(
                        db, wallet, bounty_profile_id, amount
                    )
            return {
                "code": insert_result.get("code", 500),
                "message": insert_result.get("message", "Failed to create wallet"),
                "wallet_profile_id": bounty_profile_id,
            }
        return {
            "code": 200,
            "wallet_profile_id": bounty_profile_id,
            "wallet_pk": wallet_id,
            "moved_to_useable": amount,
            "wallet_created": True,
        }

    return _release_existing_wallet(db, wallet, bounty_profile_id, amount)


def debit_bounty_from_wallet(db, bounty_profile_id, amount):
    """
    Remove bounty on return (negative transactions_bounty row).
    Removes from useable first, then pending, then actual/lifetime.
    """
    amount = _round_money(abs(amount))
    if not bounty_profile_id or amount <= 0:
        return {"code": 200, "skipped": True, "wallet_profile_id": bounty_profile_id}

    wallet_id = resolve_wallet_profile_id(bounty_profile_id)
    wallet = get_wallet_row(db, bounty_profile_id)
    if not wallet:
        return {
            "code": 404,
            "message": f"Wallet not found for {bounty_profile_id}",
            "wallet_profile_id": bounty_profile_id,
        }

    useable = _to_float(wallet.get("wallet_useable_balance"))
    pending = _to_float(wallet.get("wallet_pending"))
    actual = _to_float(wallet.get("wallet_actual_balance"))
    lifetime = _to_float(wallet.get("wallet_lifetime_earning"))

    from_useable = min(amount, useable)
    remaining = amount - from_useable
    from_pending = min(remaining, pending)

    updates = {
        "wallet_useable_balance": _round_money(useable - from_useable),
        "wallet_pending": _round_money(pending - from_pending),
        "wallet_actual_balance": _round_money(actual - amount),
        "wallet_lifetime_earning": _round_money(lifetime - amount),
    }

    result = db.update(
        "every_circle.wallet",
        {"wallet_profile_id": wallet_id},
        updates,
    )
    if result.get("code") != 200:
        return {
            "code": result.get("code", 500),
            "message": result.get("message", "Failed to debit wallet"),
            "wallet_profile_id": bounty_profile_id,
        }
    return {
        "code": 200,
        "wallet_profile_id": bounty_profile_id,
        "debited": amount,
    }


def compute_wallet_from_bounty_ledger(db, profile_id):
    """
    Recompute wallet balances from transactions_bounty + current transaction_in_escrow.
    """
    ledger_q = db.execute(
        """
        SELECT
            COALESCE(SUM(tb.tb_amount), 0) AS total_earned,
            COALESCE(SUM(
                CASE WHEN COALESCE(t.transaction_in_escrow, 0) = 1
                THEN tb.tb_amount ELSE 0 END
            ), 0) AS pending_amount,
            COALESCE(SUM(
                CASE WHEN COALESCE(t.transaction_in_escrow, 0) = 0
                THEN tb.tb_amount ELSE 0 END
            ), 0) AS useable_amount
        FROM every_circle.transactions_bounty tb
        INNER JOIN every_circle.transactions_items ti ON tb.tb_ti_id = ti.ti_uid
        INNER JOIN every_circle.transactions t ON ti.ti_transaction_id = t.transaction_uid
        WHERE tb.tb_profile_id = %s
        """,
        (profile_id,),
    )
    rows = ledger_q.get("result") or []
    if not rows:
        return {
            "wallet_actual_balance": 0,
            "wallet_pending": 0,
            "wallet_useable_balance": 0,
            "wallet_lifetime_earning": 0,
        }

    row = rows[0]
    total = _round_money(row.get("total_earned"))
    pending = _round_money(row.get("pending_amount"))
    useable = _round_money(row.get("useable_amount"))

    return {
        "wallet_actual_balance": total,
        "wallet_pending": pending,
        "wallet_useable_balance": useable,
        "wallet_lifetime_earning": total,
    }


def reconcile_profile_wallet(db, profile_id):
    """Overwrite wallet row to match bounty ledger + escrow flags."""
    wallet_id = resolve_wallet_profile_id(profile_id)
    computed = compute_wallet_from_bounty_ledger(db, profile_id)
    wallet = get_wallet_row(db, profile_id)

    fields = {
        "wallet_actual_balance": computed["wallet_actual_balance"],
        "wallet_pending": computed["wallet_pending"],
        "wallet_useable_balance": computed["wallet_useable_balance"],
        "wallet_lifetime_earning": computed["wallet_lifetime_earning"],
    }

    if wallet:
        result = db.update(
            "every_circle.wallet",
            {"wallet_profile_id": wallet_id},
            fields,
        )
        action = "updated"
    else:
        fields.update(
            {
                "wallet_profile_id": wallet_id,
                "wallet_reserve": 0,
                "wallet_lifetime_spent": 0,
            }
        )
        result = db.insert("every_circle.wallet", fields)
        action = "created"

    if result.get("code") != 200:
        return {
            "code": result.get("code", 500),
            "message": result.get("message", "Failed to reconcile wallet"),
            "profile_id": profile_id,
        }

    return {
        "code": 200,
        "profile_id": profile_id,
        "wallet_profile_id": wallet_id,
        "action": action,
        "wallet": fields,
    }


def reconcile_all_profile_wallets(db):
    """Reconcile every profile that appears in transactions_bounty."""
    profiles_q = db.execute(
        """
        SELECT DISTINCT tb_profile_id AS profile_id
        FROM every_circle.transactions_bounty
        WHERE tb_profile_id IS NOT NULL AND tb_profile_id != ''
        ORDER BY tb_profile_id
        """
    )
    profiles = profiles_q.get("result") or []
    results = []
    failed = []

    for row in profiles:
        profile_id = row.get("profile_id")
        if not profile_id:
            continue
        outcome = reconcile_profile_wallet(db, profile_id)
        if outcome.get("code") == 200:
            results.append(outcome)
        else:
            failed.append(outcome)

    return {
        "code": 200 if not failed else 500,
        "reconciled_count": len(results),
        "failed_count": len(failed),
        "reconciled": results,
        "failed": failed,
    }
