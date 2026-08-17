"""Seeking seller proceeds: do not subtract buyer-funded bounty."""

import unittest
from unittest.mock import MagicMock, patch

from line_commerce_fields import (
    compute_line_event_proceeds_breakdown,
    compute_line_proceeds_breakdown,
)
from order_detail import _attach_sale_commerce_fields
from transactions import is_seeking_sale_line, _seller_bounty_to_reclaim_for_line


class SeekingSellerProceedsTests(unittest.TestCase):
    def test_is_seeking_sale_line(self):
        self.assertTrue(is_seeking_sale_line("165-000001"))
        self.assertTrue(is_seeking_sale_line({"ti_bs_id": "165-9"}))
        self.assertTrue(is_seeking_sale_line({"purchase_type": "Seeking"}))
        self.assertFalse(is_seeking_sale_line("150-000001"))
        self.assertFalse(is_seeking_sale_line({"ti_bs_id": "250-1"}))

    def test_seeking_reclaim_is_zero_even_with_ledger(self):
        ti = {"ti_bs_id": "165-1", "ti_bs_qty": 3}
        self.assertEqual(
            _seller_bounty_to_reclaim_for_line(ti, 3, line_bounty_ledger=50.0),
            0.0,
        )

    def test_offering_reclaim_still_uses_ledger(self):
        ti = {"ti_bs_id": "150-1", "ti_bs_qty": 2}
        self.assertEqual(
            _seller_bounty_to_reclaim_for_line(ti, 2, line_bounty_ledger=20.0),
            20.0,
        )

    def test_seeking_line_event_proceeds_no_bounty_claw(self):
        line = {
            "ti_uid": "510-wish",
            "ti_bs_id": "165-000001",
            "ti_bs_qty": 3,
            "ti_bs_cost": 30.0,
            "ti_bs_is_taxable": 0,
            "ti_bs_tax_rate": 0,
            "ti_listing_shipping": None,
            "ti_shipping_amount": 0,
            "ti_line_shipping_amount": 0,
        }
        breakdown = compute_line_event_proceeds_breakdown(
            None, "500-1", line, verified_qty=3, line_bounty_ledger=50.0
        )
        self.assertIsNotNone(breakdown)
        self.assertEqual(breakdown["merchandise_amount"], 90.0)
        self.assertEqual(breakdown["bounty_amount"], 0.0)
        self.assertEqual(breakdown["amount"], 90.0)

    def test_offering_line_event_still_subtracts_bounty(self):
        line = {
            "ti_uid": "510-1",
            "ti_bs_id": "150-000001",
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
        breakdown = compute_line_event_proceeds_breakdown(
            None, "500-1", line, verified_qty=2, line_bounty_ledger=8.0
        )
        self.assertEqual(breakdown["bounty_amount"], -8.0)
        self.assertEqual(breakdown["amount"], 222.0)

    def test_seeking_line_proceeds_breakdown_no_claw(self):
        db = MagicMock()
        line = {
            "ti_uid": "510-wish",
            "ti_bs_id": "165-1",
            "ti_bs_qty": 3,
            "ti_bs_cost": 30.0,
            "ti_bs_is_taxable": 0,
            "ti_line_tax_amount": 0,
            "ti_line_shipping_amount": 0,
        }
        with patch(
            "line_commerce_fields._line_bounty_totals",
            return_value={"510-wish": 50.0},
        ):
            entry = compute_line_proceeds_breakdown(db, "500-1", line)
        self.assertEqual(entry["merchandise_amount"], 90.0)
        self.assertEqual(entry["bounty_amount"], 0.0)
        self.assertEqual(entry["net_amount"], 90.0)

    @patch("order_detail.attach_sale_lines_commerce", side_effect=lambda db, lines, **kw: lines)
    @patch("order_detail._order_bounty_paid", return_value=50.0)
    @patch("transactions._order_seller_funded_bounty_paid", return_value=0.0)
    def test_order_detail_seeking_omits_negative_bounty_amount(
        self, _funded, _paid, _attach
    ):
        sale = {
            "lines": [{"ti_bs_id": "165-1", "line_bounty_paid": 50}],
            "bounty_amount": -50,  # stale
        }
        out = _attach_sale_commerce_fields(MagicMock(), sale, "500-1")
        self.assertEqual(out["order_bounty_paid"], 50.0)
        self.assertNotIn("bounty_amount", out)

    @patch("order_detail.attach_sale_lines_commerce", side_effect=lambda db, lines, **kw: lines)
    @patch("order_detail._order_bounty_paid", return_value=8.0)
    @patch("transactions._order_seller_funded_bounty_paid", return_value=8.0)
    def test_order_detail_offering_keeps_negative_bounty_amount(
        self, _funded, _paid, _attach
    ):
        sale = {"lines": [{"ti_bs_id": "150-1", "line_bounty_paid": 8}]}
        out = _attach_sale_commerce_fields(MagicMock(), sale, "500-1")
        self.assertEqual(out["order_bounty_paid"], 8.0)
        self.assertEqual(out["bounty_amount"], -8.0)


class SellerFundedBountyOrderTests(unittest.TestCase):
    def test_eligible_total_skips_seeking_bounty(self):
        from wallet_transactions_service import compute_seller_eligible_total

        db = MagicMock()
        db.execute.return_value = {
            "result": [
                {
                    "transaction_amount": 90,
                    "transaction_taxes": 0,
                    "transaction_shipping": 0,
                }
            ]
        }
        with patch(
            "transactions._order_seller_funded_bounty_paid", return_value=0.0
        ):
            total = compute_seller_eligible_total(db, "500-seek")
        self.assertEqual(total, 90.0)

    def test_eligible_total_still_subtracts_product_bounty(self):
        from wallet_transactions_service import compute_seller_eligible_total

        db = MagicMock()
        db.execute.return_value = {
            "result": [
                {
                    "transaction_amount": 200,
                    "transaction_taxes": 20,
                    "transaction_shipping": 10,
                }
            ]
        }
        with patch(
            "transactions._order_seller_funded_bounty_paid", return_value=8.0
        ):
            total = compute_seller_eligible_total(db, "500-prod")
        self.assertEqual(total, 222.0)


if __name__ == "__main__":
    unittest.main()
