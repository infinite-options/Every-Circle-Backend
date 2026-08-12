"""Transaction receipt per-line bounty pool serialization."""

import unittest
from unittest.mock import patch

from transaction_receipt import _attach_receipt_bounty_totals


class _FakeDb:
    pass


class TransactionReceiptBountyTests(unittest.TestCase):
    def test_bounty_totals_on_response(self):
        rows = [
            {
                "transaction_amount": 280,
                "transaction_taxes": 28,
                "transaction_shipping": 24,
                "transaction_fees": 9.96,
                "line_bounty_paid": 8,
            },
            {"line_bounty_paid": 12},
        ]
        payload = {}
        _attach_receipt_bounty_totals(rows, payload)
        self.assertEqual(payload["bounty_paid"], 20.0)
        self.assertEqual(payload["total_bounty_paid"], 20.0)
        self.assertEqual(payload["total_amount_paid"], 341.96)

    def test_enrich_business_service_line_rate_display(self):
        from transaction_receipt import _enrich_receipt_line

        row = _enrich_receipt_line(
            {
                "ti_bs_id": "250-000001",
                "ti_bs_cost": 15.0,
                "ti_bs_qty": 2,
                "ti_line_tax_amount": 2.40,
                "ti_line_shipping_amount": 5.0,
                "ti_shipping_amount": 2.5,
            }
        )
        self.assertEqual(row["offering_rate_display"], "$15/each")
        self.assertEqual(row["purchase_type"], "service")
        self.assertTrue(row["money"]["known"])

    @patch("line_commerce_fields.attach_sale_lines_commerce")
    @patch("transactions._seller_bounty_pool_for_line_row")
    def test_attach_receipt_bounty_fields_per_item(self, mock_pool, mock_attach):
        from transaction_receipt import _attach_receipt_bounty_fields

        def _side_effect(db, out, buyer_profile_id=None):
            out[0].update(
                {
                    "line_bounty_paid": 8,
                    "ti_bs_bounty": 2,
                    "ti_bs_bounty_type": "per_item",
                    "bounty_earned": 3.2,
                    "tb_percentage": 0.4,
                }
            )

        mock_attach.side_effect = _side_effect
        mock_pool.return_value = 0

        lines = [{"ti_uid": "510-000933", "ti_bs_qty": 4}]
        result = _attach_receipt_bounty_fields(_FakeDb(), lines, "110-000122")
        self.assertEqual(result[0]["line_bounty_paid"], 8)
        self.assertEqual(result[0]["ti_bs_bounty"], 2)
        self.assertEqual(result[0]["tb_percentage"], 40)
        self.assertEqual(result[0]["bounty_earned"], 3.2)

    @patch("line_commerce_fields.attach_sale_lines_commerce")
    @patch("transactions._seller_bounty_pool_for_line_row")
    def test_attach_receipt_bounty_omits_zero_pool(self, mock_pool, mock_attach):
        from transaction_receipt import _attach_receipt_bounty_fields

        lines = [{"ti_uid": "510-000999", "ti_bs_qty": 1}]

        def _side_effect(db, out, buyer_profile_id=None):
            out[0]["line_bounty_paid"] = 0

        mock_attach.side_effect = _side_effect
        mock_pool.return_value = 0
        result = _attach_receipt_bounty_fields(_FakeDb(), lines, "110-000122")
        self.assertNotIn("line_bounty_paid", result[0])


if __name__ == "__main__":
    unittest.main()
