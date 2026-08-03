#!/usr/bin/env python3
"""
Backfill held return_clawback wallet rows for open TRRs created before the
sale_proceeds_clawback-on-request deploy (500-000692-style orders).

Open TRRs may only have legacy return_refund_reservation rows (wallet_reserve)
or no wallet hold at all. This script:

  1. Clears legacy return_refund_reservation rows (restores wallet_reserve)
  2. Creates held return_clawback rows (return_clawback_hold:{trr_uid})
  3. Applies pending clawback hold on wallet_pending
  4. Ensures bounty_reclaim_reservation rows exist (idempotent)

Dry-run is the default. Pass --apply to write changes.

Examples:
  python backfill_return_clawback_holds.py
  python backfill_return_clawback_holds.py --order 500-000692
  python backfill_return_clawback_holds.py --trr TRR-000123 --apply
  python backfill_return_clawback_holds.py --apply
"""

from __future__ import annotations

import argparse
import json
import sys

from data_ec import connect
from datetime_utils import utc_now_str
from transactions import (
    REFUND_STATUS_PENDING,
    RETURN_STATUS_CANCELLED,
    RETURN_STATUS_RETURNED,
    RETURN_STATUS_RETURNING,
    _hydrate_return_request_row,
    _is_open_return,
    _load_sale_for_return,
)
from wallet_return_reservations import (
    create_reservations_for_return_request,
    ensure_trr_reservation_columns,
    persist_trr_refund_metadata,
)
from wallet_service import _round_money, _to_float, adjust_wallet_reserve
from wallet_transactions_service import (
    WT_STATUS_CLEARED,
    WT_STATUS_RESERVED,
    WT_TYPE_RETURN_REFUND_RESERVATION,
    _ensure_wallet_transactions_table,
)

_OPEN_RETURN_STATUSES = (
    (RETURN_STATUS_RETURNING, REFUND_STATUS_PENDING),
    (RETURN_STATUS_RETURNED, REFUND_STATUS_PENDING),
    (RETURN_STATUS_CANCELLED, REFUND_STATUS_PENDING),
)


def _parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Backfill held return_clawback rows for open return requests (TRRs)."
        )
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Write changes (default is dry-run only)",
    )
    parser.add_argument(
        "--order",
        dest="order_uid",
        help="Limit to one sale order uid (e.g. 500-000692)",
    )
    parser.add_argument(
        "--trr",
        dest="trr_uid",
        help="Limit to one return-request uid",
    )
    parser.add_argument(
        "--profile",
        dest="profile_id",
        help="Limit to TRRs on sales where seller profile/business matches",
    )
    return parser.parse_args()


def _fetch_open_trrs(db, *, order_uid=None, trr_uid=None, profile_id=None):
    clauses = [
        "trr.trr_return_transaction_uid IS NULL",
        "COALESCE(t.transaction_type, 'sale') = 'sale'",
    ]
    params = []

    if order_uid:
        clauses.append("trr.trr_transaction_uid = %s")
        params.append(order_uid)
    if trr_uid:
        clauses.append("trr.trr_uid = %s")
        params.append(trr_uid)
    if profile_id:
        clauses.append("t.transaction_business_id = %s")
        params.append(profile_id)

    q = db.execute(
        f"""
        SELECT
            trr.trr_uid,
            trr.trr_transaction_uid,
            trr.trr_profile_id,
            trr.trr_ti_uid,
            trr.trr_return_quantity,
            trr.trr_items_json,
            trr.trr_note,
            trr.trr_seller_note,
            trr.trr_status,
            trr.trr_return_status,
            trr.trr_refund_status,
            trr.trr_cancel_unshipped,
            trr.trr_estimated_total,
            trr.trr_return_transaction_uid,
            trr.trr_stripe_refund_id,
            trr.trr_created_at,
            trr.trr_updated_at,
            trr.trr_bounty_to_reclaim,
            trr.trr_estimated_refund_json,
            t.transaction_profile_id,
            t.transaction_business_id
        FROM every_circle.transaction_return_requests trr
        INNER JOIN every_circle.transactions t
            ON trr.trr_transaction_uid = t.transaction_uid
        WHERE {" AND ".join(clauses)}
        ORDER BY trr.trr_created_at ASC, trr.trr_uid ASC
        """,
        tuple(params),
    )
    rows = []
    for raw in q.get("result") or []:
        row = _hydrate_return_request_row(dict(raw))
        if not row:
            continue
        rs = row.get("return_status")
        fs = row.get("refund_status")
        if not _is_open_return(rs, fs):
            continue
        row["transaction_profile_id"] = raw.get("transaction_profile_id")
        row["transaction_business_id"] = raw.get("transaction_business_id")
        rows.append(row)
    return rows


def _wallet_row_by_key(db, idempotency_key):
    q = db.execute(
        """
        SELECT wt_uid, wt_profile_id, wt_amount, wt_status, wt_type, wt_note
        FROM every_circle.wallet_transactions
        WHERE wt_idempotency_key = %s
        LIMIT 1
        """,
        (idempotency_key,),
    )
    rows = q.get("result") or []
    return rows[0] if rows else None


def _clawback_hold_key(trr_uid):
    return f"return_clawback_hold:{trr_uid}"


def _legacy_proceeds_key(trr_uid):
    return f"return_reservation:{trr_uid}:proceeds"


def _clear_legacy_proceeds_reservation(db, trr_uid, *, apply=False):
    """
    Clear legacy return_refund_reservation and decrement wallet_reserve.

    Returns a result dict describing what happened.
    """
    key = _legacy_proceeds_key(trr_uid)
    row = _wallet_row_by_key(db, key)
    if not row:
        return {"cleared": False, "reason": "no_legacy_row", "idempotency_key": key}
    if (row.get("wt_type") or "") != WT_TYPE_RETURN_REFUND_RESERVATION:
        return {
            "cleared": False,
            "reason": "not_proceeds_reservation",
            "wt_type": row.get("wt_type"),
        }
    if (row.get("wt_status") or "") != WT_STATUS_RESERVED:
        return {
            "cleared": False,
            "reason": "not_active_reserved",
            "wt_status": row.get("wt_status"),
        }

    amount = _round_money(row.get("wt_amount"))
    profile_id = row.get("wt_profile_id")
    result = {
        "cleared": True,
        "wt_uid": row.get("wt_uid"),
        "amount": amount,
        "profile_id": profile_id,
        "idempotency_key": key,
    }
    if not apply:
        result["dry_run"] = True
        return result

    now = utc_now_str()
    upd = db.update(
        "every_circle.wallet_transactions",
        {"wt_uid": row.get("wt_uid")},
        {"wt_status": WT_STATUS_CLEARED, "wt_updated_at": now},
    )
    if upd.get("code") != 200:
        return {
            "cleared": False,
            "reason": "update_failed",
            "message": upd.get("message"),
            "wt_uid": row.get("wt_uid"),
        }

    reserve_result = adjust_wallet_reserve(db, profile_id, -amount)
    if reserve_result.get("code") != 200:
        return {
            "cleared": False,
            "reason": "reserve_adjust_failed",
            "message": reserve_result.get("message"),
            "wt_uid": row.get("wt_uid"),
        }

    result["wallet_reserve"] = reserve_result
    return result


def _ti_currency(db, ti_uid):
    if not ti_uid:
        return "USD"
    q = db.execute(
        """
        SELECT ti_bs_cost_currency
        FROM every_circle.transactions_items
        WHERE ti_uid = %s
        LIMIT 1
        """,
        (ti_uid,),
    )
    rows = q.get("result") or []
    if not rows:
        return "USD"
    return (rows[0].get("ti_bs_cost_currency") or "USD")[:8]


def _backfill_trr(db, trr_row, *, apply=False):
    trr_uid = trr_row.get("trr_uid")
    order_uid = trr_row.get("trr_transaction_uid")
    ti_uid = trr_row.get("trr_ti_uid")
    if not ti_uid:
        items = trr_row.get("items") or []
        if items:
            ti_uid = items[0].get("transaction_item_uid")

    try:
        return_qty = int(trr_row.get("trr_return_quantity") or 0)
    except (TypeError, ValueError):
        return_qty = 0
    if return_qty <= 0 and ti_uid:
        items = trr_row.get("items") or []
        if items:
            try:
                return_qty = int(items[0].get("return_quantity") or 0)
            except (TypeError, ValueError):
                return_qty = 0

    outcome = {
        "trr_uid": trr_uid,
        "order_uid": order_uid,
        "ti_uid": ti_uid,
        "return_qty": return_qty,
        "return_status": trr_row.get("return_status"),
        "refund_status": trr_row.get("refund_status"),
        "actions": [],
    }

    if not trr_uid or not order_uid or not ti_uid:
        outcome["skipped"] = True
        outcome["reason"] = "missing_trr_order_or_ti"
        return outcome

    clawback_key = _clawback_hold_key(trr_uid)
    existing_clawback = _wallet_row_by_key(db, clawback_key)
    legacy = _wallet_row_by_key(db, _legacy_proceeds_key(trr_uid))

    outcome["had_clawback_hold"] = bool(existing_clawback)
    outcome["had_legacy_proceeds_reservation"] = bool(
        legacy
        and legacy.get("wt_type") == WT_TYPE_RETURN_REFUND_RESERVATION
        and legacy.get("wt_status") == WT_STATUS_RESERVED
    )

    needs_clawback = not existing_clawback
    needs_legacy_clear = outcome["had_legacy_proceeds_reservation"]
    needs_metadata = (
        not trr_row.get("trr_estimated_refund_json")
        or trr_row.get("trr_bounty_to_reclaim") is None
        or _to_float(trr_row.get("trr_bounty_to_reclaim")) <= 0
    )
    needs_work = needs_clawback or needs_legacy_clear or needs_metadata

    if not needs_work and existing_clawback:
        outcome["skipped"] = True
        outcome["reason"] = "already_complete"
        return outcome

    if needs_metadata:
        if apply:
            sale = _load_sale_for_return(db, order_uid)
            meta_result = persist_trr_refund_metadata(
                db,
                trr_uid=trr_uid,
                transaction_uid=order_uid,
                ti_uid=ti_uid,
                return_qty=return_qty,
                trr_estimated_total=trr_row.get("trr_estimated_total"),
                orig_tx=sale,
            )
            outcome["actions"].append({"persist_trr_metadata": meta_result})
            if meta_result.get("code") != 200:
                outcome["error"] = meta_result
                return outcome
            trr_row["trr_bounty_to_reclaim"] = meta_result.get("bounty_to_reclaim")
        else:
            outcome["actions"].append(
                {"persist_trr_metadata": {"dry_run": True, "trr_uid": trr_uid}}
            )

    if needs_legacy_clear:
        legacy_result = _clear_legacy_proceeds_reservation(
            db, trr_uid, apply=apply
        )
        outcome["actions"].append({"clear_legacy_proceeds": legacy_result})
        if legacy_result.get("cleared") is False and legacy_result.get("reason") not in (
            "no_legacy_row",
            "not_active_reserved",
        ):
            outcome["error"] = legacy_result
            return outcome

    if needs_clawback:
        if not apply:
            outcome["actions"].append(
                {
                    "create_clawback_hold": {
                        "dry_run": True,
                        "idempotency_key": clawback_key,
                        "ti_uid": ti_uid,
                        "return_qty": return_qty,
                    }
                }
            )
        else:
            sale = _load_sale_for_return(db, order_uid)
            if not sale:
                outcome["error"] = {
                    "reason": "sale_not_found",
                    "order_uid": order_uid,
                }
                return outcome

            reservation_result = create_reservations_for_return_request(
                db,
                trr_uid=trr_uid,
                transaction_uid=order_uid,
                ti_uid=ti_uid,
                refund_amount=_round_money(trr_row.get("trr_estimated_total")),
                bounty_to_reclaim=_round_money(
                    trr_row.get("trr_bounty_to_reclaim")
                ),
                buyer_id=sale.get("transaction_profile_id"),
                seller_id=sale.get("transaction_business_id"),
                return_qty=return_qty,
                currency=_ti_currency(db, ti_uid),
            )
            outcome["actions"].append(
                {"create_reservations": reservation_result}
            )
            if reservation_result.get("code") != 200:
                outcome["error"] = reservation_result
                return outcome
    elif apply:
        sale = _load_sale_for_return(db, order_uid)
        if sale:
            reservation_result = create_reservations_for_return_request(
                db,
                trr_uid=trr_uid,
                transaction_uid=order_uid,
                ti_uid=ti_uid,
                refund_amount=_round_money(trr_row.get("trr_estimated_total")),
                bounty_to_reclaim=_round_money(
                    trr_row.get("trr_bounty_to_reclaim")
                ),
                buyer_id=sale.get("transaction_profile_id"),
                seller_id=sale.get("transaction_business_id"),
                return_qty=return_qty,
                currency=_ti_currency(db, ti_uid),
            )
            outcome["actions"].append(
                {"ensure_bounty_reservations": reservation_result}
            )

    outcome["ok"] = True
    return outcome


def main():
    args = _parse_args()
    apply = bool(args.apply)
    mode = "APPLY" if apply else "DRY-RUN"

    print(f"backfill_return_clawback_holds [{mode}]")
    if not apply:
        print("No writes will be made. Re-run with --apply to persist changes.")

    with connect() as db:
        _ensure_wallet_transactions_table(db)
        ensure_trr_reservation_columns(db)

        trrs = _fetch_open_trrs(
            db,
            order_uid=args.order_uid,
            trr_uid=args.trr_uid,
            profile_id=args.profile_id,
        )

        if not trrs:
            print("No open TRRs matched filters.")
            return 0

        print(f"Found {len(trrs)} open TRR(s) to inspect.")
        results = []
        errors = 0
        skipped = 0
        updated = 0

        for trr_row in trrs:
            outcome = _backfill_trr(db, trr_row, apply=apply)
            results.append(outcome)
            label = (
                f"  {outcome.get('trr_uid')} "
                f"order={outcome.get('order_uid')} "
                f"ti={outcome.get('ti_uid')} "
                f"qty={outcome.get('return_qty')}"
            )
            if outcome.get("error"):
                errors += 1
                print(f"{label} ERROR: {json.dumps(outcome['error'], default=str)}")
            elif outcome.get("skipped"):
                skipped += 1
                print(f"{label} skipped ({outcome.get('reason')})")
            else:
                updated += 1
                print(f"{label} {'would update' if not apply else 'updated'}")
                for action in outcome.get("actions") or []:
                    print(f"    {json.dumps(action, default=str)}")

        summary = {
            "mode": mode,
            "matched": len(trrs),
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
        }
        print(json.dumps(summary, indent=2))

        if errors:
            return 1
        return 0


if __name__ == "__main__":
    sys.exit(main())
