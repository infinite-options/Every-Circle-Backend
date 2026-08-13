"""
Wallet balance updates kept in sync with transactions_bounty + seller proceeds.

Bounty is always credited to wallet_pending at purchase. It moves to useable when
the sale line is fully verified and either (a) there is no return-window hold, or
(b) the seller hold cron releases the line after wt_available_at.
"""

from datetime_utils import parse_stored_datetime, utc_now_str
from wallet_ids import resolve_wallet_profile_id

_BOUNTY_RELEASE_COLUMN_READY = False
_BOUNTY_RELEASE_BACKFILL_DONE = False
_BOUNTY_RELEASE_REPAIR_DONE = False


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


def adjust_wallet_reserve(db, profile_id, delta):
    """Increment/decrement wallet_reserve (blocks spendable balance)."""
    delta = _round_money(delta)
    if not profile_id or delta == 0:
        return {"code": 200, "skipped": True, "delta": 0}

    wallet_id = resolve_wallet_profile_id(profile_id)
    wallet = get_wallet_row(db, profile_id)
    if not wallet:
        if delta < 0:
            return {"code": 200, "skipped": True, "delta": 0}
        insert_result = db.insert(
            "every_circle.wallet",
            {
                "wallet_profile_id": wallet_id,
                "wallet_actual_balance": 0,
                "wallet_pending": 0,
                "wallet_useable_balance": 0,
                "wallet_reserve": _round_money(delta),
                "wallet_lifetime_earning": 0,
                "wallet_lifetime_spent": 0,
            },
        )
        if insert_result.get("code") != 200:
            return {
                "code": insert_result.get("code", 500),
                "message": insert_result.get("message", "Failed to create wallet"),
            }
        return {
            "code": 200,
            "wallet_profile_id": profile_id,
            "wallet_reserve": _round_money(delta),
            "delta": delta,
        }

    current = _to_float(wallet.get("wallet_reserve"))
    useable = _to_float(wallet.get("wallet_useable_balance"))
    new_reserve = _round_money(max(0.0, current + delta))
    new_useable = _round_money(max(0.0, useable - delta))
    result = db.update(
        "every_circle.wallet",
        {"wallet_profile_id": wallet_id},
        {
            "wallet_reserve": new_reserve,
            "wallet_useable_balance": new_useable,
        },
    )
    if result.get("code") != 200:
        return {
            "code": result.get("code", 500),
            "message": result.get("message", "Failed to update wallet_reserve"),
        }
    return {
        "code": 200,
        "wallet_profile_id": profile_id,
        "wallet_reserve": new_reserve,
        "wallet_useable_balance": new_useable,
        "delta": delta,
    }


def apply_pending_clawback_hold(db, profile_id, amount):
    """
    Reserve pending seller proceeds when a return is opened (before confirm).

    Reduces wallet_pending only; actual/lifetime are debited on confirm.
    """
    amount = _round_money(abs(amount))
    if not profile_id or amount <= 0:
        return {"code": 200, "skipped": True, "wallet_profile_id": profile_id}

    wallet_id = resolve_wallet_profile_id(profile_id)
    wallet = get_wallet_row(db, profile_id)
    if not wallet:
        return {
            "code": 404,
            "message": f"Wallet not found for {profile_id}",
            "wallet_profile_id": profile_id,
        }

    pending = _to_float(wallet.get("wallet_pending"))
    from_pending = min(amount, pending)
    result = db.update(
        "every_circle.wallet",
        {"wallet_profile_id": wallet_id},
        {"wallet_pending": _round_money(pending - from_pending)},
    )
    if result.get("code") != 200:
        return {
            "code": result.get("code", 500),
            "message": result.get("message", "Failed to apply pending clawback hold"),
            "wallet_profile_id": profile_id,
        }
    return {
        "code": 200,
        "wallet_profile_id": profile_id,
        "held_from_pending": from_pending,
    }


def release_pending_clawback_hold(db, profile_id, amount):
    """Reverse apply_pending_clawback_hold when a return request is declined."""
    amount = _round_money(abs(amount))
    if not profile_id or amount <= 0:
        return {"code": 200, "skipped": True, "wallet_profile_id": profile_id}

    wallet_id = resolve_wallet_profile_id(profile_id)
    wallet = get_wallet_row(db, profile_id)
    if not wallet:
        return {
            "code": 404,
            "message": f"Wallet not found for {profile_id}",
            "wallet_profile_id": profile_id,
        }

    pending = _to_float(wallet.get("wallet_pending"))
    result = db.update(
        "every_circle.wallet",
        {"wallet_profile_id": wallet_id},
        {"wallet_pending": _round_money(pending + amount)},
    )
    if result.get("code") != 200:
        return {
            "code": result.get("code", 500),
            "message": result.get("message", "Failed to release pending clawback hold"),
            "wallet_profile_id": profile_id,
        }
    return {
        "code": 200,
        "wallet_profile_id": profile_id,
        "released_to_pending": amount,
    }


def build_wallet_summary(db, profile_id):
    """Authoritative wallet block for wallet_ledger and account-screen APIs."""
    wallet = get_wallet_row(db, profile_id)
    if not wallet:
        return None
    reserve = _round_money(wallet.get("wallet_reserve"))
    useable = _round_money(wallet.get("wallet_useable_balance"))
    return {
        "wallet_profile_id": wallet.get("wallet_profile_id"),
        "wallet_useable_balance": useable,
        "wallet_pending": _round_money(wallet.get("wallet_pending")),
        "wallet_reserve": reserve,
        "wallet_useable_after_reserve": _round_money(max(0.0, useable)),
        "wallet_actual_balance": _round_money(wallet.get("wallet_actual_balance")),
        "wallet_lifetime_earning": _round_money(wallet.get("wallet_lifetime_earning")),
        "wallet_lifetime_spent": _round_money(wallet.get("wallet_lifetime_spent")),
    }


def ensure_bounty_release_column(db):
    """Add ti_bounty_released_at once per process (tracks pending → useable release)."""
    global _BOUNTY_RELEASE_COLUMN_READY, _BOUNTY_RELEASE_BACKFILL_DONE
    if not _BOUNTY_RELEASE_COLUMN_READY:
        db.execute(
            "ALTER TABLE every_circle.transactions_items "
            "ADD COLUMN ti_bounty_released_at DATETIME NULL",
            cmd="post",
        )
        _BOUNTY_RELEASE_COLUMN_READY = True

        if not _BOUNTY_RELEASE_BACKFILL_DONE:
            # Legacy rows: escrow cleared, line fully verified, no active seller hold.
            db.execute(
                """
                UPDATE every_circle.transactions_items ti
                INNER JOIN every_circle.transactions t
                    ON ti.ti_transaction_id = t.transaction_uid
                SET ti.ti_bounty_released_at = COALESCE(ti.ti_received_at, t.transaction_datetime)
                WHERE ti.ti_bounty_released_at IS NULL
                  AND COALESCE(t.transaction_in_escrow, 0) = 0
                  AND COALESCE(ti.ti_received_qty, 0) >= CAST(ti.ti_bs_qty AS SIGNED)
                  AND CAST(ti.ti_bs_qty AS UNSIGNED) > 0
                  AND EXISTS (
                    SELECT 1 FROM every_circle.transactions_bounty tb
                    WHERE tb.tb_ti_id = ti.ti_uid AND tb.tb_amount > 0.0001
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM every_circle.wallet_transactions wt
                    WHERE wt.wt_ti_id = ti.ti_uid
                      AND wt.wt_type = 'partial_delivery_credit'
                      AND wt.wt_status = 'held'
                  )
                """,
                cmd="post",
            )
            _BOUNTY_RELEASE_BACKFILL_DONE = True

    global _BOUNTY_RELEASE_REPAIR_DONE
    if not _BOUNTY_RELEASE_REPAIR_DONE:
        _repair_partial_bounty_release_flags(db)
        _BOUNTY_RELEASE_REPAIR_DONE = True


def _repair_partial_bounty_release_flags(db):
    """Clear bounty-released markers on lines that are not fully verified."""
    db.execute(
        """
        UPDATE every_circle.transactions_items
        SET ti_bounty_released_at = NULL
        WHERE ti_bounty_released_at IS NOT NULL
          AND CAST(ti_bs_qty AS UNSIGNED) > 0
          AND COALESCE(ti_received_qty, 0) < CAST(ti_bs_qty AS SIGNED)
        """,
        cmd="post",
    )


def line_is_fully_verified(received_qty, order_qty):
    order_qty = int(order_qty or 0)
    if order_qty <= 0:
        return False
    return int(received_qty or 0) >= order_qty


def bounty_was_released_to_useable_at(db, original_ti_uid, event_dt=None):
    """
    True when sale-line bounty was in the useable pool at event_dt.

    Pending bounty (return window / not yet released) has NULL ti_bounty_released_at
    or a release timestamp after the event.
    """
    original_ti_uid = str(original_ti_uid or "").strip()
    if not original_ti_uid:
        return False

    ti_q = db.execute(
        """
        SELECT ti_bounty_released_at
        FROM every_circle.transactions_items
        WHERE ti_uid = %s
        LIMIT 1
        """,
        (original_ti_uid,),
    )
    rows = ti_q.get("result") or []
    if not rows:
        return False

    released_at = rows[0].get("ti_bounty_released_at")
    if not released_at:
        return False
    if not event_dt:
        return True

    released = parse_stored_datetime(released_at)
    event = parse_stored_datetime(event_dt)
    if released is None or event is None:
        return True
    return released <= event


def bounty_reversal_ledger_availability(db, row, amount):
    """
    Map bounty reversal to ledger availability (pending vs useable).

    Reversals claw from the same pool the original bounty_earned occupied at
    reversal time (ti_bounty_released_at on the source sale line).
    """
    amount = _round_money(amount)
    released_at = row.get("source_bounty_released_at")
    reversal_dt = row.get("transaction_datetime")
    if released_at is not None or row.get("source_ti_uid"):
        if not released_at:
            return "pending", 0.0
        if reversal_dt:
            released = parse_stored_datetime(released_at)
            event = parse_stored_datetime(reversal_dt)
            if released is not None and event is not None and released > event:
                return "pending", 0.0
        return "useable", amount

    original_ti_uid = row.get("source_ti_uid") or row.get("ti_uid")
    if (row.get("transaction_type") or "").lower() == "return":
        q = db.execute(
            """
            SELECT ti_original_ti_uid
            FROM every_circle.transactions_items
            WHERE ti_uid = %s
            LIMIT 1
            """,
            (row.get("ti_uid"),),
        )
        ti_rows = q.get("result") or []
        if ti_rows and ti_rows[0].get("ti_original_ti_uid"):
            original_ti_uid = ti_rows[0].get("ti_original_ti_uid")

    if bounty_was_released_to_useable_at(
        db, original_ti_uid, reversal_dt
    ):
        return "useable", amount
    return "pending", 0.0


def release_bounty_for_line(db, ti_uid):
    """
    Move all bounty for a sale line from pending to useable for every recipient.

    Idempotent: no-op when ti_bounty_released_at is already set.
    """
    ensure_bounty_release_column(db)
    ti_uid = str(ti_uid or "").strip()
    if not ti_uid:
        return {"code": 400, "message": "ti_uid is required", "ti_uid": ti_uid}

    ti_q = db.execute(
        """
        SELECT ti_uid, ti_bs_qty, COALESCE(ti_received_qty, 0) AS ti_received_qty,
               ti_bounty_released_at
        FROM every_circle.transactions_items
        WHERE ti_uid = %s
        """,
        (ti_uid,),
    )
    ti_rows = ti_q.get("result") or []
    if not ti_rows:
        return {"code": 404, "message": f"Transaction item not found: {ti_uid}", "ti_uid": ti_uid}

    ti = ti_rows[0]
    order_qty = int(ti.get("ti_bs_qty") or 0)
    received_qty = int(ti.get("ti_received_qty") or 0)

    if ti.get("ti_bounty_released_at"):
        if line_is_fully_verified(received_qty, order_qty):
            return {
                "code": 200,
                "skipped": True,
                "reason": "already_released",
                "ti_uid": ti_uid,
            }
        db.update(
            "every_circle.transactions_items",
            {"ti_uid": ti_uid},
            {"ti_bounty_released_at": None},
        )

    if not line_is_fully_verified(received_qty, order_qty):
        return {
            "code": 200,
            "skipped": True,
            "reason": "line_not_fully_verified",
            "ti_uid": ti_uid,
        }

    bounty_q = db.execute(
        """
        SELECT tb_profile_id, COALESCE(SUM(tb_amount), 0) AS bounty_total
        FROM every_circle.transactions_bounty
        WHERE tb_ti_id = %s
        GROUP BY tb_profile_id
        HAVING bounty_total > 0.0001
        """,
        (ti_uid,),
    )
    recipients = bounty_q.get("result") or []
    if not recipients:
        now = utc_now_str()
        db.update(
            "every_circle.transactions_items",
            {"ti_uid": ti_uid},
            {"ti_bounty_released_at": now},
        )
        return {
            "code": 200,
            "ti_uid": ti_uid,
            "released": True,
            "wallet_updates": [],
            "total_moved": 0.0,
        }

    wallet_updates = []
    for row in recipients:
        profile_id = row.get("tb_profile_id")
        amount = _round_money(row.get("bounty_total"))
        if not profile_id or amount <= 0:
            continue
        outcome = release_bounty_to_useable(db, profile_id, amount)
        if outcome.get("code") != 200:
            return {
                "code": outcome.get("code", 500),
                "message": outcome.get(
                    "message", f"Failed to release bounty for {profile_id}"
                ),
                "ti_uid": ti_uid,
                "wallet_updates": wallet_updates,
            }
        wallet_updates.append(outcome)

    now = utc_now_str()
    mark = db.update(
        "every_circle.transactions_items",
        {"ti_uid": ti_uid},
        {"ti_bounty_released_at": now},
    )
    if mark.get("code") != 200:
        return {
            "code": mark.get("code", 500),
            "message": mark.get("message", "Failed to mark bounty released"),
            "ti_uid": ti_uid,
            "wallet_updates": wallet_updates,
        }

    total_moved = _round_money(
        sum(_to_float(w.get("moved_to_useable")) for w in wallet_updates)
    )
    return {
        "code": 200,
        "ti_uid": ti_uid,
        "released": True,
        "wallet_updates": wallet_updates,
        "total_moved": total_moved,
    }


def release_bounty_for_line_net(db, ti_uid):
    """
    Release bounty for a line minus active bounty reclaim reservations.

    Full release when no reservations; partial per-recipient release when
    reservations cover part of pending bounty.
    """
    ensure_bounty_release_column(db)
    ti_uid = str(ti_uid or "").strip()
    if not ti_uid:
        return {"code": 400, "message": "ti_uid is required", "ti_uid": ti_uid}

    from wallet_return_reservations import sum_active_bounty_reservation

    ti_q = db.execute(
        """
        SELECT ti_uid, ti_bs_qty, COALESCE(ti_received_qty, 0) AS ti_received_qty,
               ti_bounty_released_at
        FROM every_circle.transactions_items
        WHERE ti_uid = %s
        """,
        (ti_uid,),
    )
    ti_rows = ti_q.get("result") or []
    if not ti_rows:
        return {"code": 404, "message": f"Transaction item not found: {ti_uid}", "ti_uid": ti_uid}

    ti = ti_rows[0]
    order_qty = int(ti.get("ti_bs_qty") or 0)
    received_qty = int(ti.get("ti_received_qty") or 0)

    if ti.get("ti_bounty_released_at") and line_is_fully_verified(received_qty, order_qty):
        return {
            "code": 200,
            "skipped": True,
            "reason": "already_released",
            "ti_uid": ti_uid,
        }

    if not line_is_fully_verified(received_qty, order_qty):
        return {
            "code": 200,
            "skipped": True,
            "reason": "line_not_fully_verified",
            "ti_uid": ti_uid,
        }

    reserved_total = sum_active_bounty_reservation(db, ti_uid=ti_uid)
    if reserved_total <= 0:
        return release_bounty_for_line(db, ti_uid)

    bounty_q = db.execute(
        """
        SELECT tb_profile_id, COALESCE(SUM(tb_amount), 0) AS bounty_total
        FROM every_circle.transactions_bounty
        WHERE tb_ti_id = %s
        GROUP BY tb_profile_id
        HAVING bounty_total > 0.0001
        """,
        (ti_uid,),
    )
    recipients = bounty_q.get("result") or []
    if not recipients:
        now = utc_now_str()
        db.update(
            "every_circle.transactions_items",
            {"ti_uid": ti_uid},
            {"ti_bounty_released_at": now},
        )
        return {
            "code": 200,
            "ti_uid": ti_uid,
            "released": True,
            "wallet_updates": [],
            "total_moved": 0.0,
        }

    pending_total = _round_money(
        sum(_to_float(r.get("bounty_total")) for r in recipients)
    )
    releasable_total = _round_money(max(0.0, pending_total - reserved_total))
    if releasable_total <= 0:
        return {
            "code": 200,
            "skipped": True,
            "reason": "fully_reserved",
            "ti_uid": ti_uid,
            "reserved_total": reserved_total,
        }

    wallet_updates = []
    for row in recipients:
        profile_id = row.get("tb_profile_id")
        pending_amt = _round_money(row.get("bounty_total"))
        reserved_amt = sum_active_bounty_reservation(
            db, ti_uid=ti_uid, profile_id=profile_id
        )
        release_amt = _round_money(max(0.0, pending_amt - reserved_amt))
        if not profile_id or release_amt <= 0:
            continue
        outcome = release_bounty_to_useable(db, profile_id, release_amt)
        if outcome.get("code") != 200:
            return {
                "code": outcome.get("code", 500),
                "message": outcome.get(
                    "message", f"Failed partial bounty release for {profile_id}"
                ),
                "ti_uid": ti_uid,
                "wallet_updates": wallet_updates,
            }
        wallet_updates.append(outcome)

    total_moved = _round_money(
        sum(_to_float(w.get("moved_to_useable")) for w in wallet_updates)
    )

    return {
        "code": 200,
        "ti_uid": ti_uid,
        "released": True,
        "partial": True,
        "wallet_updates": wallet_updates,
        "total_moved": total_moved,
        "reserved_total": reserved_total,
    }


def sync_bounty_release_after_line_credit(
    db, ti_uid, *, received_qty_after, order_qty, hold
):
    """
    Release bounty when a line is fully verified and seller proceeds are not held.

    When hold=True (return window), bounty stays pending until seller hold cron
    calls release_bounty_for_line after wt_available_at.
    """
    if hold:
        return {
            "code": 200,
            "skipped": True,
            "reason": "return_window_hold",
            "ti_uid": ti_uid,
        }
    if not line_is_fully_verified(received_qty_after, order_qty):
        return {
            "code": 200,
            "skipped": True,
            "reason": "line_not_fully_verified",
            "ti_uid": ti_uid,
        }
    return release_bounty_for_line(db, ti_uid)


def credit_bounty_to_wallet(db, bounty_profile_id, amount, in_escrow=False):
    """
    Add bounty at purchase time.

    Always credits wallet_pending (and actual + lifetime). The in_escrow argument
    is kept for call-site compatibility; new purchases always hold bounty pending
    until line verification (+ return window when applicable).
    """
    in_escrow = True
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


def credit_seller_proceeds_to_wallet(db, profile_id, amount, hold=False):
    """
    Credit seller sale proceeds to the wallet.

    Always increases wallet_actual_balance and wallet_lifetime_earning.
    If hold=True (return-window lock), credits wallet_pending; otherwise
    credits wallet_useable_balance.
    """
    amount = _round_money(amount)
    hold = bool(hold)
    if not profile_id or amount <= 0:
        return {
            "code": 200,
            "skipped": True,
            "wallet_profile_id": profile_id,
            "hold": hold,
        }

    wallet_id = resolve_wallet_profile_id(profile_id)
    wallet = get_wallet_row(db, profile_id)

    if wallet:
        actual = _to_float(wallet.get("wallet_actual_balance"))
        useable = _to_float(wallet.get("wallet_useable_balance"))
        pending = _to_float(wallet.get("wallet_pending"))
        lifetime = _to_float(wallet.get("wallet_lifetime_earning"))

        updates = {
            "wallet_actual_balance": _round_money(actual + amount),
            "wallet_lifetime_earning": _round_money(lifetime + amount),
        }
        if hold:
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
                "wallet_profile_id": profile_id,
            }
        return {
            "code": 200,
            "wallet_profile_id": profile_id,
            "wallet_pk": wallet_id,
            "credited": amount,
            "hold": hold,
            "wallet_created": False,
        }

    insert_result = db.insert(
        "every_circle.wallet",
        {
            "wallet_profile_id": wallet_id,
            "wallet_actual_balance": amount,
            "wallet_pending": amount if hold else 0,
            "wallet_useable_balance": 0 if hold else amount,
            "wallet_reserve": 0,
            "wallet_lifetime_earning": amount,
            "wallet_lifetime_spent": 0,
        },
    )
    if insert_result.get("code") != 200:
        insert_msg = insert_result.get("message", "")
        if "duplicate entry" in insert_msg.lower():
            wallet = get_wallet_row(db, profile_id)
            if wallet:
                return credit_seller_proceeds_to_wallet(
                    db, profile_id, amount, hold=hold
                )
        return {
            "code": insert_result.get("code", 500),
            "message": insert_result.get("message", "Failed to create wallet"),
            "wallet_profile_id": profile_id,
        }
    return {
        "code": 200,
        "wallet_profile_id": profile_id,
        "wallet_pk": wallet_id,
        "credited": amount,
        "hold": hold,
        "wallet_created": True,
    }


def release_seller_hold_to_useable(db, profile_id, amount):
    """
    Move return-window held seller proceeds from pending to useable.

    Does not change wallet_actual_balance or wallet_lifetime_earning.
    """
    amount = _round_money(amount)
    if not profile_id or amount <= 0:
        return {
            "code": 200,
            "skipped": True,
            "wallet_profile_id": profile_id,
            "moved_to_useable": 0,
        }

    wallet_id = resolve_wallet_profile_id(profile_id)
    wallet = get_wallet_row(db, profile_id)
    if not wallet:
        return {
            "code": 404,
            "message": f"Wallet not found for {profile_id}",
            "wallet_profile_id": profile_id,
        }

    pending = _to_float(wallet.get("wallet_pending"))
    useable = _to_float(wallet.get("wallet_useable_balance"))
    from_pending = min(amount, pending)

    result = db.update(
        "every_circle.wallet",
        {"wallet_profile_id": wallet_id},
        {
            "wallet_pending": _round_money(pending - from_pending),
            "wallet_useable_balance": _round_money(useable + from_pending),
        },
    )
    if result.get("code") != 200:
        return {
            "code": result.get("code", 500),
            "message": result.get("message", "Failed to release seller hold"),
            "wallet_profile_id": profile_id,
        }
    return {
        "code": 200,
        "wallet_profile_id": profile_id,
        "wallet_pk": wallet_id,
        "moved_to_useable": from_pending,
        "from_pending": from_pending,
    }


def debit_bounty_from_wallet(db, bounty_profile_id, amount, *, prefer_pending=False):
    """
    Remove bounty on return (negative transactions_bounty row).

    When prefer_pending=True (original bounty still in return window / not released),
    debits wallet_pending first. Otherwise debits useable first, then pending.
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

    if prefer_pending:
        from_pending = min(amount, pending)
        remaining = amount - from_pending
        from_useable = min(remaining, useable)
    else:
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
        "from_useable": from_useable,
        "from_pending": from_pending,
    }


def debit_useable_for_purchase(db, profile_id, amount):
    """
    Spend useable wallet balance at buyer checkout.

    Debits wallet_useable_balance and wallet_actual_balance only (never pending).
    Increments wallet_lifetime_spent. Leaves wallet_lifetime_earning unchanged.
    """
    amount = _round_money(abs(amount))
    if not profile_id or amount <= 0:
        return {"code": 200, "skipped": True, "wallet_profile_id": profile_id, "debited": 0.0}

    wallet_id = resolve_wallet_profile_id(profile_id)
    wallet = get_wallet_row(db, profile_id)
    if not wallet:
        return {
            "code": 400,
            "message": f"No wallet balance available for {profile_id}",
            "wallet_profile_id": profile_id,
        }

    useable = _to_float(wallet.get("wallet_useable_balance"))
    actual = _to_float(wallet.get("wallet_actual_balance"))
    lifetime_spent = _to_float(wallet.get("wallet_lifetime_spent"))

    if amount > useable + 1e-9:
        return {
            "code": 400,
            "message": (
                f"Insufficient useable wallet balance "
                f"(requested {amount:.2f}, available {useable:.2f})"
            ),
            "wallet_profile_id": profile_id,
            "available": useable,
            "requested": amount,
        }

    updates = {
        "wallet_useable_balance": _round_money(useable - amount),
        "wallet_actual_balance": _round_money(actual - amount),
        "wallet_lifetime_spent": _round_money(lifetime_spent + amount),
    }

    result = db.update(
        "every_circle.wallet",
        {"wallet_profile_id": wallet_id},
        updates,
    )
    if result.get("code") != 200:
        return {
            "code": result.get("code", 500),
            "message": result.get("message", "Failed to debit wallet for purchase"),
            "wallet_profile_id": profile_id,
        }
    return {
        "code": 200,
        "wallet_profile_id": profile_id,
        "wallet_pk": wallet_id,
        "debited": amount,
        "useable_remaining": updates["wallet_useable_balance"],
    }


def credit_useable_from_refund(db, profile_id, amount):
    """
    Restore useable wallet balance when a purchase paid (partly) with wallet is refunded.
    """
    amount = _round_money(abs(amount))
    if not profile_id or amount <= 0:
        return {"code": 200, "skipped": True, "wallet_profile_id": profile_id, "credited": 0.0}

    wallet_id = resolve_wallet_profile_id(profile_id)
    wallet = get_wallet_row(db, profile_id)

    if wallet:
        useable = _to_float(wallet.get("wallet_useable_balance"))
        actual = _to_float(wallet.get("wallet_actual_balance"))
        lifetime_spent = _to_float(wallet.get("wallet_lifetime_spent"))
        updates = {
            "wallet_useable_balance": _round_money(useable + amount),
            "wallet_actual_balance": _round_money(actual + amount),
            "wallet_lifetime_spent": _round_money(max(0.0, lifetime_spent - amount)),
        }
        result = db.update(
            "every_circle.wallet",
            {"wallet_profile_id": wallet_id},
            updates,
        )
        if result.get("code") != 200:
            return {
                "code": result.get("code", 500),
                "message": result.get("message", "Failed to credit wallet refund"),
                "wallet_profile_id": profile_id,
            }
        return {
            "code": 200,
            "wallet_profile_id": profile_id,
            "wallet_pk": wallet_id,
            "credited": amount,
            "wallet_created": False,
        }

    insert_result = db.insert(
        "every_circle.wallet",
        {
            "wallet_profile_id": wallet_id,
            "wallet_actual_balance": amount,
            "wallet_pending": 0,
            "wallet_useable_balance": amount,
            "wallet_reserve": 0,
            "wallet_lifetime_earning": 0,
            "wallet_lifetime_spent": 0,
        },
    )
    if insert_result.get("code") != 200:
        return {
            "code": insert_result.get("code", 500),
            "message": insert_result.get("message", "Failed to create wallet for refund"),
            "wallet_profile_id": profile_id,
        }
    return {
        "code": 200,
        "wallet_profile_id": profile_id,
        "wallet_pk": wallet_id,
        "credited": amount,
        "wallet_created": True,
    }


def transfer_wallet_refund_to_buyer(
    db,
    *,
    buyer_profile_id,
    seller_profile_id,
    amount,
    seller_clawed_amount=0.0,
):
    """
    Return wallet-paid purchase funds to the buyer.

    When the seller already had delivery proceeds clawed (``seller_clawed_amount``),
    that debit already removed money from the seller wallet — credit the buyer so
    those funds effectively move seller → buyer (up to ``amount``).

    When nothing was clawed (e.g. pre-ship cancel), credit the buyer from platform
    books only (do not debit unrelated seller earnings).

    When clawed amount is less than the wallet refund (partial delivery credit vs
    full wallet tender), debit the seller for the shortfall if they have balance,
    then credit the buyer for the full wallet refund.
    """
    amount = _round_money(abs(amount))
    seller_clawed_amount = _round_money(abs(seller_clawed_amount))
    if not buyer_profile_id or amount <= 0:
        return {
            "code": 200,
            "skipped": True,
            "credited": 0.0,
            "seller_debited": 0.0,
            "funded_by": "none",
        }

    extra_from_seller = _round_money(max(0.0, amount - seller_clawed_amount))
    seller_debit_result = None
    seller_debited = 0.0
    funded_by = "platform"

    if seller_clawed_amount > 0 and extra_from_seller <= 0:
        funded_by = "seller_clawback"
    elif seller_profile_id and extra_from_seller > 0:
        seller_wallet = get_wallet_row(db, seller_profile_id)
        if seller_wallet:
            available = _round_money(
                _to_float(seller_wallet.get("wallet_pending"))
                + _to_float(seller_wallet.get("wallet_useable_balance"))
            )
            to_debit = _round_money(min(extra_from_seller, available))
            if to_debit > 0:
                seller_debit_result = debit_seller_proceeds_from_wallet(
                    db, seller_profile_id, to_debit
                )
                if seller_debit_result.get("code") == 200:
                    seller_debited = _round_money(
                        seller_debit_result.get("debited") or to_debit
                    )
        if seller_clawed_amount > 0 and seller_debited > 0:
            funded_by = "seller_clawback_and_balance"
        elif seller_debited > 0:
            funded_by = "seller_balance"
        elif seller_clawed_amount > 0:
            funded_by = "seller_clawback"
    elif seller_clawed_amount > 0:
        funded_by = "seller_clawback"

    buyer_credit = credit_useable_from_refund(db, buyer_profile_id, amount)
    if buyer_credit.get("code") != 200:
        return {
            "code": buyer_credit.get("code", 500),
            "message": buyer_credit.get(
                "message", "Failed to credit buyer wallet on refund"
            ),
            "buyer_credit": buyer_credit,
            "seller_debit": seller_debit_result,
            "seller_debited": seller_debited,
            "credited": 0.0,
            "funded_by": funded_by,
        }

    return {
        "code": 200,
        "buyer_profile_id": buyer_profile_id,
        "seller_profile_id": seller_profile_id,
        "credited": amount,
        "seller_debited": seller_debited,
        "seller_clawed_amount": seller_clawed_amount,
        "funded_by": funded_by,
        "buyer_credit": buyer_credit,
        "seller_debit": seller_debit_result,
    }


def debit_seller_proceeds_pending_only(db, profile_id, amount):
    """
    Reverse seller proceeds from the pending bucket only (return-window holds).

    Use when clawing back units whose proceeds were never posted to useable.
    """
    amount = _round_money(abs(amount))
    if not profile_id or amount <= 0:
        return {"code": 200, "skipped": True, "wallet_profile_id": profile_id}

    wallet_id = resolve_wallet_profile_id(profile_id)
    wallet = get_wallet_row(db, profile_id)
    if not wallet:
        return {
            "code": 404,
            "message": f"Wallet not found for {profile_id}",
            "wallet_profile_id": profile_id,
        }

    pending = _to_float(wallet.get("wallet_pending"))
    actual = _to_float(wallet.get("wallet_actual_balance"))
    lifetime = _to_float(wallet.get("wallet_lifetime_earning"))
    from_pending = min(amount, pending)

    updates = {
        "wallet_pending": _round_money(pending - from_pending),
        "wallet_actual_balance": _round_money(actual - from_pending),
        "wallet_lifetime_earning": _round_money(lifetime - from_pending),
    }

    result = db.update(
        "every_circle.wallet",
        {"wallet_profile_id": wallet_id},
        updates,
    )
    if result.get("code") != 200:
        return {
            "code": result.get("code", 500),
            "message": result.get("message", "Failed to debit seller pending proceeds"),
            "wallet_profile_id": profile_id,
        }
    return {
        "code": 200,
        "wallet_profile_id": profile_id,
        "wallet_pk": wallet_id,
        "debited": from_pending,
        "from_pending": from_pending,
        "from_useable": 0.0,
    }


def debit_seller_proceeds_from_wallet(db, profile_id, amount):
    """
    Reverse seller sale proceeds on confirmed return.

    Debits pending first (held return-window funds), then useable.
    Always reduces wallet_actual_balance and wallet_lifetime_earning.
    """
    amount = _round_money(abs(amount))
    if not profile_id or amount <= 0:
        return {"code": 200, "skipped": True, "wallet_profile_id": profile_id}

    wallet_id = resolve_wallet_profile_id(profile_id)
    wallet = get_wallet_row(db, profile_id)
    if not wallet:
        return {
            "code": 404,
            "message": f"Wallet not found for {profile_id}",
            "wallet_profile_id": profile_id,
        }

    pending = _to_float(wallet.get("wallet_pending"))
    useable = _to_float(wallet.get("wallet_useable_balance"))
    actual = _to_float(wallet.get("wallet_actual_balance"))
    lifetime = _to_float(wallet.get("wallet_lifetime_earning"))

    from_pending = min(amount, pending)
    remaining = amount - from_pending
    from_useable = min(remaining, useable)

    updates = {
        "wallet_pending": _round_money(pending - from_pending),
        "wallet_useable_balance": _round_money(useable - from_useable),
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
            "message": result.get("message", "Failed to debit seller proceeds"),
            "wallet_profile_id": profile_id,
        }
    return {
        "code": 200,
        "wallet_profile_id": profile_id,
        "wallet_pk": wallet_id,
        "debited": amount,
        "from_pending": from_pending,
        "from_useable": from_useable,
    }


def _sum_wallet_transactions_by_status(db, profile_id, statuses):
    """
    Sum wallet_transactions.wt_amount for a profile filtered by wt_status.

    Includes seller proceed types so clawbacks (negative amounts) net correctly.
    Excludes return/bounty reservation rows (those adjust bounty pools separately).
    """
    if not profile_id or not statuses:
        return 0.0
    # Match either the passed id or the resolved wallet PK (legacy platform ids).
    wallet_id = resolve_wallet_profile_id(profile_id)
    placeholders = ", ".join(["%s"] * len(statuses))
    q = db.execute(
        f"""
        SELECT COALESCE(SUM(wt_amount), 0) AS seller_proceeds
        FROM every_circle.wallet_transactions
        WHERE wt_status IN ({placeholders})
          AND wt_profile_id IN (%s, %s)
          AND wt_type NOT IN (
              'return_refund_reservation',
              'bounty_reclaim_reservation'
          )
        """,
        tuple(statuses) + (profile_id, wallet_id),
    )
    rows = q.get("result") or []
    if not rows:
        return 0.0
    return _round_money(rows[0].get("seller_proceeds"))


def _sum_posted_wallet_transactions(db, profile_id):
    """Sum posted wallet_transactions.wt_amount (spendable seller proceeds)."""
    return _sum_wallet_transactions_by_status(db, profile_id, ("posted",))


def _sum_held_wallet_transactions(db, profile_id):
    """Sum held wallet_transactions.wt_amount (return-window locked proceeds)."""
    return _sum_wallet_transactions_by_status(db, profile_id, ("held",))


def _sum_buyer_wallet_purchase_spent(db, profile_id):
    """
    Net wallet balance spent by this buyer on purchases.

    Sales store positive transaction_wallet_amount; return ledgers store the
    negative restored amount. Net = spent - restored.
    """
    try:
        spent_q = db.execute(
            """
            SELECT COALESCE(SUM(COALESCE(transaction_wallet_amount, 0)), 0) AS net_spent
            FROM every_circle.transactions
            WHERE transaction_profile_id = %s
              AND COALESCE(transaction_wallet_amount, 0) != 0
            """,
            (profile_id,),
        )
    except Exception as e:
        print(f"wallet purchase spent sum (ok if column missing): {e}")
        return 0.0
    rows = spent_q.get("result") or []
    if not rows:
        return 0.0
    return _round_money(rows[0].get("net_spent"))


def _apply_bounty_reservation_to_pools(bounty_pending, bounty_useable, reserved):
    """
    Mirror wallet_ledger bounty_reclaim_reserved: prefer pending, then useable.

    Reserved amounts are removed from earned pools (not parked in wallet_reserve).
    """
    reserved = _round_money(max(0.0, _to_float(reserved)))
    pending = _round_money(bounty_pending)
    useable = _round_money(bounty_useable)
    from_pending = min(reserved, pending)
    remaining = _round_money(reserved - from_pending)
    from_useable = min(remaining, useable)
    return (
        _round_money(pending - from_pending),
        _round_money(useable - from_useable),
        reserved,
    )


def compute_wallet_from_bounty_ledger(db, profile_id):
    """
    Recompute wallet balances from transactions_bounty + line release state, plus
    wallet_transactions seller proceeds (posted + held), minus buyer
    wallet purchase spends (transaction_wallet_amount).

    Bounty useable/pending follows ti_bounty_released_at (not transaction_in_escrow),
    including negative reversal rows so pending nets to the same figure as
    wallet (actual − useable). Then subtracts active bounty_reclaim_reservation
    amounts (ledger SoT).

    useable = bounty_useable + SUM(posted wt_amount) - net_purchase_spent
    pending = bounty_pending + SUM(held wt_amount)
    lifetime_earning = bounty_total + SUM(held + posted wt_amount)
    actual = lifetime_earning - net_purchase_spent
    """
    ensure_bounty_release_column(db)
    ledger_q = db.execute(
        """
        SELECT
            COALESCE(SUM(tb.tb_amount), 0) AS total_earned,
            COALESCE(SUM(
                CASE
                    WHEN ti.ti_bounty_released_at IS NULL
                    THEN tb.tb_amount ELSE 0
                END
            ), 0) AS pending_amount,
            COALESCE(SUM(
                CASE
                    WHEN ti.ti_bounty_released_at IS NOT NULL
                    THEN tb.tb_amount ELSE 0
                END
            ), 0) AS useable_amount
        FROM every_circle.transactions_bounty tb
        INNER JOIN every_circle.transactions_items ti ON tb.tb_ti_id = ti.ti_uid
        INNER JOIN every_circle.transactions t ON ti.ti_transaction_id = t.transaction_uid
        WHERE tb.tb_profile_id = %s
        """,
        (profile_id,),
    )
    rows = ledger_q.get("result") or []
    if rows:
        row = rows[0]
        bounty_total = _round_money(row.get("total_earned"))
        bounty_pending = _round_money(row.get("pending_amount"))
        bounty_useable = _round_money(row.get("useable_amount"))
    else:
        bounty_total = 0.0
        bounty_pending = 0.0
        bounty_useable = 0.0

    from wallet_return_reservations import sum_active_bounty_reservation

    bounty_reserved = sum_active_bounty_reservation(db, profile_id=profile_id)
    bounty_pending, bounty_useable, bounty_reserved = _apply_bounty_reservation_to_pools(
        bounty_pending, bounty_useable, bounty_reserved
    )
    bounty_total = _round_money(max(0.0, bounty_total - bounty_reserved))

    seller_posted = _sum_posted_wallet_transactions(db, profile_id)
    seller_held = _sum_held_wallet_transactions(db, profile_id)
    seller_proceeds = _round_money(seller_posted + seller_held)
    purchase_spent = _sum_buyer_wallet_purchase_spent(db, profile_id)
    lifetime = _round_money(bounty_total + seller_proceeds)
    useable = _round_money(bounty_useable + seller_posted - purchase_spent)
    pending = _round_money(bounty_pending + seller_held)
    actual = _round_money(lifetime - purchase_spent)

    return {
        "wallet_actual_balance": actual,
        "wallet_pending": pending,
        "wallet_useable_balance": useable,
        "wallet_lifetime_earning": lifetime,
        "wallet_lifetime_spent": purchase_spent,
        "bounty_total": bounty_total,
        "bounty_useable": bounty_useable,
        "bounty_pending": bounty_pending,
        "bounty_reserved": bounty_reserved,
        "seller_proceeds": seller_proceeds,
        "seller_posted": seller_posted,
        "seller_held": seller_held,
        "purchase_spent": purchase_spent,
    }


def reconcile_profile_wallet(db, profile_id):
    """Overwrite wallet row to match bounty ledger + line release + seller proceeds."""
    wallet_id = resolve_wallet_profile_id(profile_id)
    computed = compute_wallet_from_bounty_ledger(db, profile_id)
    wallet = get_wallet_row(db, profile_id)

    fields = {
        "wallet_actual_balance": computed["wallet_actual_balance"],
        "wallet_pending": computed["wallet_pending"],
        "wallet_useable_balance": computed["wallet_useable_balance"],
        "wallet_lifetime_earning": computed["wallet_lifetime_earning"],
        "wallet_lifetime_spent": computed.get("wallet_lifetime_spent", 0),
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
        "seller_proceeds": computed.get("seller_proceeds", 0),
    }


def reconcile_all_profile_wallets(db):
    """Reconcile every profile in transactions_bounty or wallet_transactions."""
    profiles_q = db.execute(
        """
        SELECT DISTINCT profile_id FROM (
            SELECT tb_profile_id AS profile_id
            FROM every_circle.transactions_bounty
            WHERE tb_profile_id IS NOT NULL AND tb_profile_id != ''
            UNION
            SELECT wt_profile_id AS profile_id
            FROM every_circle.wallet_transactions
            WHERE wt_profile_id IS NOT NULL AND wt_profile_id != ''
        ) AS profiles
        ORDER BY profile_id
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
