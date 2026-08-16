"""Bounty return reservation must keep wallet / earnings / bounty_results aligned with ledger."""

import unittest
from unittest.mock import MagicMock, patch

from account_screen_v3 import (
    build_bounty_results_v3,
    build_earnings_v3,
    build_wallet_v3,
)
from wallet_service import (
    _apply_bounty_reservation_to_pools,
    compute_wallet_from_bounty_ledger,
)


class BountyReservationPoolTests(unittest.TestCase):
    def test_prefer_pending_then_useable(self):
        pending, useable, reserved = _apply_bounty_reservation_to_pools(19.2, 48.0, 9.6)
        self.assertEqual(pending, 9.6)
        self.assertEqual(useable, 48.0)
        self.assertEqual(reserved, 9.6)

    def test_overflow_into_useable(self):
        pending, useable, reserved = _apply_bounty_reservation_to_pools(5.0, 48.0, 9.6)
        self.assertEqual(pending, 0.0)
        self.assertEqual(useable, 43.4)
        self.assertEqual(reserved, 9.6)


class ComputeWalletWithReservationTests(unittest.TestCase):
    def test_compute_subtracts_active_bounty_reservation(self):
        db = MagicMock()

        def _exec(sql, params=None, *args, **kwargs):
            q = (sql or "").lower()
            if "sum(tb.tb_amount)" in q.replace(" ", "") or "total_earned" in q:
                return {
                    "result": [
                        {
                            "total_earned": 67.2,
                            "pending_amount": 19.2,
                            "useable_amount": 48.0,
                        }
                    ]
                }
            if "seller_proceeds" in q or "from every_circle.wallet_transactions" in q:
                return {"result": [{"seller_proceeds": 0}]}
            if "net_spent" in q or "transaction_wallet_amount" in q:
                return {"result": [{"net_spent": 0}]}
            if "alter table" in q:
                return {"code": 200, "result": []}
            return {"result": []}

        executed = []

        def _exec_capture(sql, params=None, *args, **kwargs):
            executed.append(sql or "")
            return _exec(sql, params, *args, **kwargs)

        db.execute.side_effect = _exec_capture

        with patch(
            "wallet_return_reservations.sum_active_bounty_reservation",
            return_value=9.6,
        ), patch(
            "wallet_service.ensure_bounty_release_column",
            return_value=None,
        ):
            computed = compute_wallet_from_bounty_ledger(db, "110-000108")

        ledger_sql = next(s for s in executed if "pending_amount" in s.lower())
        self.assertNotIn("tb.tb_amount > 0", " ".join(ledger_sql.split()))
        self.assertEqual(computed["bounty_useable"], 48.0)
        self.assertEqual(computed["bounty_pending"], 9.6)
        self.assertEqual(computed["bounty_total"], 57.6)
        self.assertEqual(computed["bounty_reserved"], 9.6)
        self.assertEqual(computed["wallet_actual_balance"], 57.6)
        self.assertEqual(computed["wallet_useable_balance"], 48.0)
        self.assertEqual(computed["wallet_pending"], 9.6)


class AccountScreenProjectionTests(unittest.TestCase):
    def test_wallet_and_earnings_match_computed_reservation(self):
        db = MagicMock()
        computed = {
            "wallet_actual_balance": 57.6,
            "wallet_pending": 9.6,
            "wallet_useable_balance": 48.0,
            "wallet_lifetime_earning": 57.6,
            "wallet_lifetime_spent": 0.0,
            "bounty_total": 57.6,
            "bounty_useable": 48.0,
            "bounty_pending": 9.6,
            "bounty_reserved": 9.6,
            "seller_proceeds": 0.0,
        }
        with patch(
            "account_screen_v3.compute_wallet_from_bounty_ledger",
            return_value=computed,
        ), patch(
            "account_screen_v3.get_wallet_row",
            return_value={"wallet_profile_id": "110-000108"},
        ), patch(
            "account_screen_v3._bounty_chart_series",
            return_value={"granularity": "day", "series": []},
        ):
            wallet = build_wallet_v3(db, "110-000108")
            earnings = build_earnings_v3(db, "110-000108")

        self.assertEqual(wallet["actual_balance"], 57.6)
        self.assertEqual(wallet["useable_balance"], 48.0)
        self.assertEqual(wallet["pending_balance"], 9.6)
        self.assertEqual(earnings["bounty_total_earned"], 57.6)
        self.assertEqual(earnings["bounty_useable"], 48.0)
        self.assertEqual(earnings["bounty_pending"], 9.6)
        self.assertEqual(earnings["bounty_reserved"], 9.6)

    def test_earnings_pending_matches_wallet_when_sql_pending_includes_ghost_reversal(self):
        """+$19.20 still unreleased after -$19.20 reversals must not inflate Earnings Pending."""
        db = MagicMock()
        computed = {
            "wallet_actual_balance": 128.0,
            "wallet_pending": 87.2,
            "wallet_useable_balance": 60.0,
            "wallet_lifetime_earning": 128.0,
            "wallet_lifetime_spent": 0.0,
            "bounty_total": 128.0,
            "bounty_useable": 60.0,
            "bounty_pending": 87.2,
            "bounty_reserved": 0.0,
            "seller_proceeds": 0.0,
        }
        with patch(
            "account_screen_v3.compute_wallet_from_bounty_ledger",
            return_value=computed,
        ), patch(
            "account_screen_v3.get_wallet_row",
            return_value={"wallet_profile_id": "110-000108"},
        ), patch(
            "account_screen_v3._bounty_chart_series",
            return_value={"granularity": "day", "series": []},
        ):
            wallet = build_wallet_v3(db, "110-000108")
            earnings = build_earnings_v3(db, "110-000108")

        self.assertEqual(wallet["actual_balance"], 128.0)
        self.assertEqual(wallet["useable_balance"], 60.0)
        self.assertEqual(wallet["pending_balance"], 68.0)
        self.assertEqual(earnings["bounty_total_earned"], 128.0)
        self.assertEqual(earnings["bounty_useable"], 60.0)
        self.assertEqual(earnings["bounty_pending"], 68.0)

    def test_bounty_chart_subtracts_active_reservations(self):
        from account_screen_v3 import _bounty_chart_series

        db = MagicMock()

        def _exec(sql, params=None, *args, **kwargs):
            q = (sql or "").lower()
            if "transactions_bounty" in q:
                return {
                    "result": [
                        {
                            "transaction_datetime": "2026-08-14 20:00:00",
                            "tb_amount": 1.6,
                        },
                        {
                            "transaction_datetime": "2026-08-15 14:00:00",
                            "tb_amount": 2.4,
                        },
                    ]
                }
            if "wallet_transactions" in q:
                return {
                    "result": [
                        {
                            "wt_created_at": "2026-08-15 00:12:42",
                            "wt_amount": 0.32,
                        }
                    ]
                }
            return {"result": []}

        db.execute.side_effect = _exec
        with patch(
            "wallet_ids.resolve_wallet_profile_id",
            return_value="110-000108",
        ):
            chart = _bounty_chart_series(db, "110-000108", "America/Los_Angeles")

        by_date = {p["date"]: p for p in chart["series"]}
        # Reservation at 00:12Z is Aug 14 local (America/Los_Angeles).
        self.assertEqual(by_date["2026-08-14"]["daily"], 1.28)
        self.assertEqual(by_date["2026-08-15"]["daily"], 2.4)
        self.assertEqual(by_date["2026-08-15"]["cumulative"], 3.68)

    def test_bounty_results_reduces_earned_and_marks_reserved(self):
        db = MagicMock()
        rows = [
            {
                "ti_uid": "510-000002",
                "tb_profile_id": "110-000108",
                "bounty_earned": 19.2,
                "tb_amount": 19.2,
                "tb_percentage": 0.4,
                "transaction_uid": "500-000002",
                "transaction_datetime": "2026-08-12 19:40:00",
                "ti_bounty_released_at": None,
                "purchaser_name": "Buyer",
                "display_name": "Toronto Tools",
            }
        ]
        with patch(
            "account_screen_v3.enrich_bounty_result_rows",
            side_effect=lambda _db, r: r,
        ), patch(
            "account_screen_v3.attach_line_snapshots_to_rows",
            side_effect=lambda _db, r: r,
        ), patch(
            "account_screen_v3._attach_bounty_row_catalog_fields",
            side_effect=lambda _db, r: r,
        ), patch(
            "account_screen_v3.batch_line_checkout_snapshots",
            return_value={},
        ), patch(
            "wallet_return_reservations.sum_active_bounty_reservation",
            return_value=9.6,
        ):
            out = build_bounty_results_v3(db, rows)

        row = out["rows"][0]
        self.assertEqual(row["bounty_earned"], 9.6)
        self.assertEqual(row["bounty_reserved"], 9.6)
        self.assertEqual(row["proceeds_status"], "reserved")
        self.assertEqual(row["display"]["status_label"], "Reserved")
        self.assertEqual(row["display"]["earned_label"], "$9.60")

    def test_bounty_results_preserves_negative_reversal_amount(self):
        """Return clawbacks must keep signed earned (not clamped to $0)."""
        db = MagicMock()
        rows = [
            {
                "ti_uid": "510-000029",
                "tb_profile_id": "110-000053",
                "bounty_earned": -1.6,
                "tb_amount": -1.6,
                "tb_percentage": 0.4,
                "bs_bounty": 2,
                "bs_bounty_type": "per_item",
                "ti_bs_qty": -2,
                "transaction_uid": "500-000025",
                "transaction_original_uid": "500-000024",
                "transaction_type": "return",
                "transaction_datetime": "2026-08-15 14:32:38",
                "ti_bounty_released_at": None,
                "purchaser_name": "PM Test19",
                "display_name": "Chinese25",
                "is_return": True,
            }
        ]
        with patch(
            "account_screen_v3.enrich_bounty_result_rows",
            side_effect=lambda _db, r: r,
        ), patch(
            "account_screen_v3.attach_line_snapshots_to_rows",
            side_effect=lambda _db, r: r,
        ), patch(
            "account_screen_v3._attach_bounty_row_catalog_fields",
            side_effect=lambda _db, r: r,
        ), patch(
            "account_screen_v3.batch_line_checkout_snapshots",
            return_value={},
        ), patch(
            "wallet_return_reservations.sum_active_bounty_reservation",
            return_value=0.0,
        ):
            out = build_bounty_results_v3(db, rows)

        row = out["rows"][0]
        self.assertEqual(row["bounty_earned"], -1.6)
        self.assertEqual(row["bounty_earned_gross"], -1.6)
        self.assertEqual(row["bounty_reserved"], 0.0)
        self.assertEqual(row["display"]["earned_label"], "-$1.60")
        self.assertEqual(row["proceeds_status"], "reversed")
        self.assertEqual(row["display"]["status_label"], "Reversed")
        self.assertEqual(row["transaction_uid"], "500-000025")
        self.assertEqual(row["order_uid"], "500-000024")
        self.assertEqual(row["display"]["pool_label"], "-$4.00")

    def test_bounty_results_pool_label_is_line_pool(self):
        db = MagicMock()
        rows = [
            {
                "ti_uid": "510-000028",
                "tb_profile_id": "110-000053",
                "bounty_earned": 3.2,
                "tb_amount": 3.2,
                "tb_percentage": 0.4,
                "bs_bounty": 2,
                "bs_bounty_type": "per_item",
                "ti_bs_qty": 4,
                "transaction_uid": "500-000024",
                "transaction_datetime": "2026-08-15 14:30:50",
                "ti_bounty_released_at": None,
                "purchaser_name": "PM Test19",
                "display_name": "Chinese25",
            }
        ]
        with patch(
            "account_screen_v3.enrich_bounty_result_rows",
            side_effect=lambda _db, r: r,
        ), patch(
            "account_screen_v3.attach_line_snapshots_to_rows",
            side_effect=lambda _db, r: r,
        ), patch(
            "account_screen_v3._attach_bounty_row_catalog_fields",
            side_effect=lambda _db, r: r,
        ), patch(
            "account_screen_v3.batch_line_checkout_snapshots",
            return_value={},
        ), patch(
            "wallet_return_reservations.sum_active_bounty_reservation",
            return_value=0.0,
        ):
            out = build_bounty_results_v3(db, rows)

        row = out["rows"][0]
        self.assertEqual(row["bs_bounty"], 2.0)
        self.assertEqual(row["display"]["pool_label"], "$8.00")
        self.assertEqual(row["display"]["earned_label"], "$3.20")
        self.assertEqual(row["order_uid"], "500-000024")


if __name__ == "__main__":
    unittest.main()
