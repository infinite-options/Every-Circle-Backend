"""Line-exact partial_delivery_credit amounts (no order-level merchandise proration)."""

import unittest

from line_commerce_fields import compute_line_event_proceeds_breakdown


class PartialDeliveryCreditTests(unittest.TestCase):
    def test_verify_one_unit_line_exact_components(self):
        """500-000735 Belfort pattern: merch + tax + per-unit shipping − bounty."""
        line = {
            "ti_uid": "510-000912",
            "ti_bs_id": "150-000135",
            "ti_bs_qty": 2,
            "ti_bs_cost": 20.0,
            "ti_bs_is_taxable": 1,
            "ti_bs_tax_rate": 10.0,
            "ti_listing_shipping": "buyer fixed",
            "ti_shipping_amount": 3.0,
            "ti_line_shipping_amount": 6.0,
            "ti_shipping_refundable": 1,
            "bs_bounty": 2.0,
            "bs_bounty_type": "fixed",
        }
        breakdown = compute_line_event_proceeds_breakdown(
            None,
            "500-000735",
            line,
            verified_qty=1,
            line_bounty_ledger=2.0,
        )
        self.assertIsNotNone(breakdown)
        self.assertEqual(breakdown["merchandise_amount"], 20.0)
        self.assertEqual(breakdown["sales_tax_amount"], 2.0)
        self.assertEqual(breakdown["shipping_amount"], 3.0)
        self.assertEqual(breakdown["bounty_amount"], -1.0)
        self.assertEqual(breakdown["amount"], 24.0)


if __name__ == "__main__":
    unittest.main()
