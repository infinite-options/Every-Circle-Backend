#!/usr/bin/env python3
"""
Backfill seller partial_delivery_credit wallet rows to include sales tax in proceeds.

Before the tax-inclusive seller proceeds change, partial_delivery_credit amounts
were proportional to (merchandise + shipping − bounty). Clawbacks already included
tax via compute_seller_proceeds_reversal_for_line; credits did not.

This script scales historical partial_delivery_credit wt_amount rows by:

  new_eligible / old_eligible

where old_eligible excludes transaction_taxes and new_eligible includes it.
Sale-proceeds ledger narrative (sale_proceeds_original, etc.) is recomputed at
read time and does not require DB backfill.

Dry-run is the default. Pass --apply to update wallet_transactions and wallets.

Examples:
  python backfill_sale_proceeds_tax.py
  python backfill_sale_proceeds_tax.py --order 500-000718
  python backfill_sale_proceeds_tax.py --apply
"""

from __future__ import annotations

import argparse
import json
import sys

from data_ec import connect
from datetime_utils import utc_now_str
from wallet_service import _round_money, _to_float, credit_seller_proceeds_to_wallet
from wallet_transactions_service import (
    WT_STATUS_HELD,
    WT_STATUS_POSTED,
    WT_TYPE_PARTIAL_DELIVERY_CREDIT,
    _ensure_wallet_transactions_table,
    _order_bounty_paid,
    compute_seller_eligible_total,
)


def _parse_args():
    parser = argparse.ArgumentParser(
        description=(
            "Backfill partial_delivery_credit amounts to include sales tax in proceeds."
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
        help="Limit to one sale order uid (e.g. 500-000718)",
    )
    return parser.parse_args()


def _legacy_seller_eligible_total(db, transaction_uid):
    """Pre-tax seller pool: merchandise + shipping − bounty."""
    if not transaction_uid:
        return 0.0
    q = db.execute(
        """
        SELECT transaction_amount, transaction_shipping
        FROM every_circle.transactions
        WHERE transaction_uid = %s
        """,
        (transaction_uid,),
    )
    rows = q.get("result") or []
    if not rows:
        return 0.0
    tx = rows[0]
    amount = _to_float(tx.get("transaction_amount"))
    shipping = _to_float(tx.get("transaction_shipping"))
    bounty = _order_bounty_paid(db, transaction_uid)
    return _round_money(amount + shipping - bounty)


def _fetch_credit_rows(db, *, order_uid=None):
    clauses = [
        "wt.wt_type = %s",
        "wt.wt_status IN (%s, %s)",
        "COALESCE(t.transaction_type, 'sale') = 'sale'",
    ]
    params = [WT_TYPE_PARTIAL_DELIVERY_CREDIT, WT_STATUS_POSTED, WT_STATUS_HELD]
    if order_uid:
        clauses.append("wt.wt_transaction_id = %s")
        params.append(order_uid)
    q = db.execute(
        f"""
        SELECT
            wt.wt_uid,
            wt.wt_profile_id,
            wt.wt_transaction_id,
            wt.wt_amount,
            wt.wt_status,
            wt.wt_available_at,
            t.transaction_taxes
        FROM every_circle.wallet_transactions wt
        INNER JOIN every_circle.transactions t
            ON wt.wt_transaction_id = t.transaction_uid
        WHERE {" AND ".join(clauses)}
        ORDER BY wt.wt_transaction_id ASC, wt.wt_created_at ASC, wt.wt_uid ASC
        """,
        tuple(params),
    )
    return q.get("result") or []


def _scale_credit_row(db, row, *, apply=False):
    order_uid = row.get("wt_transaction_id")
    old_amount = _round_money(row.get("wt_amount"))
    taxes = _to_float(row.get("transaction_taxes"))
    if old_amount <= 0 or taxes <= 0:
        return {
            "skipped": True,
            "reason": "no_tax_or_zero_amount",
            "order_uid": order_uid,
            "wt_uid": row.get("wt_uid"),
            "old_amount": old_amount,
        }

    old_eligible = _legacy_seller_eligible_total(db, order_uid)
    new_eligible = compute_seller_eligible_total(db, order_uid)
    if old_eligible <= 0 or new_eligible <= old_eligible:
        return {
            "skipped": True,
            "reason": "no_eligible_increase",
            "order_uid": order_uid,
            "wt_uid": row.get("wt_uid"),
            "old_amount": old_amount,
            "old_eligible": old_eligible,
            "new_eligible": new_eligible,
        }

    new_amount = _round_money(old_amount * (new_eligible / old_eligible))
    delta = _round_money(new_amount - old_amount)
    if abs(delta) <= 0.0001:
        return {
            "skipped": True,
            "reason": "no_delta",
            "order_uid": order_uid,
            "wt_uid": row.get("wt_uid"),
            "old_amount": old_amount,
            "new_amount": new_amount,
        }

    outcome = {
        "order_uid": order_uid,
        "wt_uid": row.get("wt_uid"),
        "old_amount": old_amount,
        "new_amount": new_amount,
        "delta": delta,
        "old_eligible": old_eligible,
        "new_eligible": new_eligible,
        "transaction_taxes": taxes,
        "wt_status": row.get("wt_status"),
    }

    if not apply:
        outcome["dry_run"] = True
        return outcome

    upd = db.update(
        "every_circle.wallet_transactions",
        {"wt_uid": row.get("wt_uid")},
        {"wt_amount": new_amount, "wt_updated_at": utc_now_str()},
    )
    if upd.get("code") != 200:
        outcome["error"] = upd
        return outcome

    profile_id = row.get("wt_profile_id")
    hold = (row.get("wt_status") or "") == WT_STATUS_HELD
    wallet_result = credit_seller_proceeds_to_wallet(db, profile_id, delta, hold=hold)
    if wallet_result.get("code") != 200:
        outcome["error"] = wallet_result
        return outcome

    outcome["wallet"] = wallet_result
    outcome["applied"] = True
    return outcome


def main():
    args = _parse_args()
    apply = bool(args.apply)
    mode = "APPLY" if apply else "DRY-RUN"

    print(f"backfill_sale_proceeds_tax [{mode}]")
    if not apply:
        print("No writes will be made. Re-run with --apply to persist changes.")

    with connect() as db:
        _ensure_wallet_transactions_table(db)
        rows = _fetch_credit_rows(db, order_uid=args.order_uid)

        if not rows:
            print("No partial_delivery_credit rows matched filters.")
            return 0

        print(f"Found {len(rows)} credit row(s) to inspect.")
        report = []
        errors = 0
        skipped = 0
        updated = 0

        for row in rows:
            outcome = _scale_credit_row(db, row, apply=apply)
            report.append(outcome)
            label = (
                f"  order={outcome.get('order_uid')} "
                f"wt={outcome.get('wt_uid')} "
                f"old={outcome.get('old_amount')} "
                f"new={outcome.get('new_amount')} "
                f"delta={outcome.get('delta')}"
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

        summary = {
            "mode": mode,
            "matched": len(rows),
            "updated": updated,
            "skipped": skipped,
            "errors": errors,
            "rows": report,
        }
        print(json.dumps({k: v for k, v in summary.items() if k != "rows"}, indent=2))

        if errors:
            return 1
        return 0


if __name__ == "__main__":
    sys.exit(main())
