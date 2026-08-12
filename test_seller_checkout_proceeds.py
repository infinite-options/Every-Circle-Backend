"""Seller pending credit at checkout + verify allocation (no double wallet credit)."""

import unittest
from unittest.mock import MagicMock, patch

from line_commerce_fields import compute_line_event_proceeds_breakdown
from wallet_transactions_service import (
    CHECKOUT_IDEMPOTENCY_SUFFIX,
    WT_STATUS_HELD,
    WT_STATUS_POSTED,
    WT_TYPE_PARTIAL_DELIVERY_CREDIT,
    _allocate_verified_from_checkout_credit,
    _checkout_idempotency_key,
)


class SellerCheckoutProceedsTests(unittest.TestCase):
    def test_checkout_idempotency_key(self):
        self.assertEqual(_checkout_idempotency_key("510-1"), f"510-1:{CHECKOUT_IDEMPOTENCY_SUFFIX}")

    def test_full_line_checkout_breakdown_matches_formula(self):
        line = {
            "ti_uid": "510-1",
            "ti_bs_qty": 2,
            "ti_bs_cost": 100.0,
            "ti_bs_is_taxable": 1,
            "ti_bs_tax_rate": 10.0,
            "ti_listing_shipping": "buyer fixed",
            "ti_shipping_amount": 5.0,
            "ti_line_shipping_amount": 10.0,
            "bs_bounty": 8.0,
            "bs_bounty_type": "fixed",
        }
        # merch 200 + tax 20 + ship 10 − bounty 8 = 222
        breakdown = compute_line_event_proceeds_breakdown(
            None, "500-1", line, verified_qty=2, line_bounty_ledger=8.0
        )
        self.assertIsNotNone(breakdown)
        self.assertEqual(breakdown["merchandise_amount"], 200.0)
        self.assertEqual(breakdown["sales_tax_amount"], 20.0)
        self.assertEqual(breakdown["shipping_amount"], 10.0)
        self.assertEqual(breakdown["bounty_amount"], -8.0)
        self.assertEqual(breakdown["amount"], 222.0)

    @patch("wallet_transactions_service._attach_bounty_release_to_credit_result", side_effect=lambda db, result, **kw: result)
    @patch("wallet_transactions_service.release_seller_hold_to_useable")
    @patch("wallet_transactions_service._new_wallet_transaction_uid", return_value="wt-verify-1")
    @patch("wallet_transactions_service._fetch_wt_by_idempotency_key")
    @patch("line_commerce_fields.load_commerce_sale_line")
    @patch("line_commerce_fields.compute_line_event_proceeds_breakdown")
    def test_allocate_no_window_moves_pending_to_useable(
        self,
        mock_breakdown,
        mock_load_line,
        mock_fetch,
        mock_new_uid,
        mock_release,
        mock_bounty,
    ):
        db = MagicMock()
        checkout = {
            "wt_uid": "wt-checkout",
            "wt_amount": 222.0,
            "wt_qty": 2,
            "wt_status": WT_STATUS_HELD,
            "wt_currency": "USD",
        }
        mock_fetch.side_effect = [None]  # no prior verify row
        with patch(
            "wallet_transactions_service._find_checkout_held_credit",
            return_value=checkout,
        ):
            mock_load_line.return_value = {"ti_uid": "510-1", "ti_bs_qty": 2}
            mock_breakdown.return_value = {"amount": 111.0}
            mock_release.return_value = {
                "code": 200,
                "moved_to_useable": 111.0,
            }
            db.update.return_value = {"code": 200}
            db.insert.return_value = {"code": 200}

            ti = {
                "ti_uid": "510-1",
                "ti_bs_qty": 2,
                "ti_bs_cost": 100,
                "ti_bs_is_returnable": 0,
                "ti_bs_return_window_days": None,
                "ti_received_at": "2026-08-12 12:00:00",
            }
            tx = {
                "transaction_profile_id": "110-buyer",
                "transaction_business_id": "110-seller",
            }
            result = _allocate_verified_from_checkout_credit(
                db,
                "500-1",
                "510-1",
                1,
                1,
                ti=ti,
                tx=tx,
                seller_profile_id="110-seller",
            )

        self.assertEqual(result.get("code"), 200)
        mock_release.assert_called_once()
        self.assertEqual(mock_release.call_args[0][2], 111.0)
        insert_row = db.insert.call_args[0][1]
        self.assertEqual(insert_row["wt_status"], WT_STATUS_POSTED)
        self.assertEqual(insert_row["wt_amount"], 111.0)
        # Checkout residual reduced, not a second lifetime credit
        checkout_upd = db.update.call_args[0][2]
        self.assertEqual(checkout_upd["wt_qty"], 1)
        self.assertEqual(checkout_upd["wt_amount"], 111.0)

    @patch("wallet_transactions_service._attach_bounty_release_to_credit_result", side_effect=lambda db, result, **kw: result)
    @patch("wallet_transactions_service.release_seller_hold_to_useable")
    @patch("wallet_transactions_service._new_wallet_transaction_uid", return_value="wt-verify-1")
    @patch("wallet_transactions_service._fetch_wt_by_idempotency_key")
    @patch("line_commerce_fields.load_commerce_sale_line")
    @patch("line_commerce_fields.compute_line_event_proceeds_breakdown")
    def test_allocate_with_window_keeps_pending_sets_available_at(
        self,
        mock_breakdown,
        mock_load_line,
        mock_fetch,
        mock_new_uid,
        mock_release,
        mock_bounty,
    ):
        db = MagicMock()
        checkout = {
            "wt_uid": "wt-checkout",
            "wt_amount": 111.0,
            "wt_qty": 1,
            "wt_status": WT_STATUS_HELD,
            "wt_currency": "USD",
        }
        mock_fetch.side_effect = [None]
        with patch(
            "wallet_transactions_service._find_checkout_held_credit",
            return_value=checkout,
        ):
            mock_load_line.return_value = {"ti_uid": "510-1", "ti_bs_qty": 1}
            mock_breakdown.return_value = {"amount": 111.0}
            db.update.return_value = {"code": 200}
            db.insert.return_value = {"code": 200}

            ti = {
                "ti_uid": "510-1",
                "ti_bs_qty": 1,
                "ti_bs_cost": 100,
                "ti_bs_is_returnable": 1,
                "ti_bs_return_window_days": 14,
                "ti_received_at": "2026-08-12 12:00:00",
            }
            result = _allocate_verified_from_checkout_credit(
                db,
                "500-1",
                "510-1",
                1,
                1,
                ti=ti,
                tx={
                    "transaction_profile_id": "110-buyer",
                    "transaction_business_id": "110-seller",
                },
                seller_profile_id="110-seller",
            )

        self.assertEqual(result.get("code"), 200)
        mock_release.assert_not_called()
        insert_row = db.insert.call_args[0][1]
        self.assertEqual(insert_row["wt_status"], WT_STATUS_HELD)
        self.assertIsNotNone(insert_row["wt_available_at"])
        self.assertEqual(insert_row["wt_type"], WT_TYPE_PARTIAL_DELIVERY_CREDIT)


if __name__ == "__main__":
    unittest.main()
