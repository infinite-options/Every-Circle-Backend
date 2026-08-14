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
                "ti_bs_is_taxable": 1,
                "ti_bs_tax_rate": "8.00",
            }
        )
        self.assertEqual(row["offering_rate_display"], "$15/each")
        self.assertEqual(row["purchase_type"], "service")
        self.assertTrue(row["money"]["known"])
        self.assertEqual(row["money"]["merchandise"], 30.0)
        self.assertEqual(row["money"]["shipping"], 5.0)
        self.assertEqual(row["line_merchandise_total"], 30.0)
        self.assertEqual(row["ti_line_shipping_amount"], 5.0)

    def test_enrich_receipt_line_without_tax_snapshot_is_unknown(self):
        from transaction_receipt import _enrich_receipt_line

        row = _enrich_receipt_line(
            {
                "ti_bs_id": "250-000001",
                "ti_bs_cost": 170.0,
                "ti_bs_qty": 1,
                "ti_line_shipping_amount": 12.0,
                "ti_shipping_amount": 12.0,
            }
        )
        self.assertFalse(row.get("money", {}).get("known", False))

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
        self.assertEqual(result[0]["percent_label"], "40%")
        self.assertEqual(result[0]["bounty_earned"], 3.2)

    @patch("line_commerce_fields.attach_sale_lines_commerce")
    @patch("transactions._seller_bounty_pool_for_line_row")
    def test_attach_receipt_bounty_restores_total_and_shipping(self, mock_pool, mock_attach):
        from transaction_receipt import _attach_receipt_bounty_fields

        def _side_effect(db, out, buyer_profile_id=None):
            out[0].update(
                {
                    "line_bounty_paid": 8,
                    "tb_percentage": 0.4,
                    "bounty_earned": 8,
                    "ti_listing_shipping": "Buyer Fixed",
                    "ti_shipping_amount_per_unit": 12.0,
                }
            )
            out[0].pop("ti_line_shipping_amount", None)
            out[0].pop("ti_shipping_amount", None)

        mock_attach.side_effect = _side_effect
        mock_pool.return_value = 0

        lines = [
            {
                "ti_uid": "510-000009",
                "ti_bs_id": "250-000129",
                "ti_bs_qty": 1,
                "ti_bs_cost": 170,
                "ti_bs_is_taxable": 1,
                "ti_bs_tax_rate": "10.00",
                "ti_line_tax_amount": 17,
                "ti_line_shipping_amount": 12,
                "ti_shipping_amount": 12,
                "ti_listing_shipping": "Buyer Fixed",
            }
        ]
        result = _attach_receipt_bounty_fields(_FakeDb(), lines, "110-000108")
        self.assertTrue(result[0]["money"]["known"])
        self.assertEqual(result[0]["money"]["merchandise"], 170.0)
        self.assertEqual(result[0]["money"]["shipping"], 12.0)
        self.assertEqual(result[0]["line_merchandise_total"], 170.0)
        self.assertEqual(result[0]["ti_line_shipping_amount"], 12.0)
        self.assertEqual(result[0]["tb_percentage"], 40)
        self.assertEqual(result[0]["percent_label"], "40%")

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
