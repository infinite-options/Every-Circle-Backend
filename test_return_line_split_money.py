"""Per-split return line money for hybrid return/cancel rows."""

import unittest

from line_commerce_fields import (
    _return_line_split_specs,
    _split_row_refund_money,
    collapse_return_lines_for_list_row,
    expand_return_line_splits,
)


class _FakeDb:
    def execute(self, *args, **kwargs):
        return {"result": []}


class ReturnLineSplitMoneyTests(unittest.TestCase):
    def _acquire_ti_row(self):
        return {
            "ti_uid": "ti-123",
            "ti_bs_id": "150-000001",
            "ti_bs_qty": 2,
            "ti_bs_cost": 30.0,
            "ti_bs_is_taxable": 0,
            "ti_bs_tax_rate": 0,
            "ti_line_tax_amount": 0.0,
            "ti_shipping_amount": 2.0,
            "ti_listing_shipping": "buyer fixed",
            "ti_shipping_refundable": 0,
            "bs_bounty": 2.0,
            "bs_bounty_type": "per_item",
        }

    def test_hybrid_split_specs(self):
        specs = _return_line_split_specs(return_shipped_qty=1, cancel_unshipped_qty=1)
        self.assertEqual(len(specs), 2)
        self.assertEqual(specs[0]["return_kind"], "return")
        self.assertEqual(specs[0]["return_shipped_qty"], 1)
        self.assertEqual(specs[0]["cancel_unshipped_qty"], 0)
        self.assertEqual(specs[1]["return_kind"], "cancel")
        self.assertEqual(specs[1]["cancel_unshipped_qty"], 1)

    def test_acquire_hybrid_per_split_money(self):
        ti_row = self._acquire_ti_row()
        db = _FakeDb()

        return_row = _split_row_refund_money(
            db,
            "order-1",
            ti_row,
            return_qty=1,
            return_shipped_qty=1,
            cancel_unshipped_qty=0,
        )
        self.assertEqual(return_row["line_merchandise_refund"], 30.0)
        self.assertEqual(return_row["line_tax_refund"], 0.0)
        self.assertEqual(return_row["line_shipping_refund"], 0.0)
        self.assertEqual(return_row["line_bounty_reclaim"], 2.0)

        cancel_row = _split_row_refund_money(
            db,
            "order-1",
            ti_row,
            return_qty=1,
            return_shipped_qty=0,
            cancel_unshipped_qty=1,
        )
        self.assertEqual(cancel_row["line_merchandise_refund"], 30.0)
        self.assertEqual(cancel_row["line_tax_refund"], 0.0)
        self.assertEqual(cancel_row["line_shipping_refund"], 2.0)
        self.assertEqual(cancel_row["line_bounty_reclaim"], 2.0)

    def test_taxable_partial_return_tax_prorates_from_line_snapshot(self):
        ti_row = {
            "ti_uid": "ti-tax",
            "ti_bs_id": "250-1",
            "ti_bs_qty": 4,
            "ti_bs_cost": 150.0,
            "ti_bs_is_taxable": 1,
            "ti_bs_tax_rate": 10.0,
            "ti_line_tax_amount": 60.0,
            "ti_shipping_amount": 20.0,
            "ti_listing_shipping": "buyer fixed",
            "ti_shipping_refundable": 1,
            "bs_bounty": 10.0,
            "bs_bounty_type": "per_item",
        }
        db = _FakeDb()
        # Return 2 of 4 → half of stored line tax
        money = _split_row_refund_money(
            db,
            "order-tax",
            ti_row,
            return_qty=2,
            return_shipped_qty=2,
            cancel_unshipped_qty=0,
        )
        self.assertEqual(money["line_tax_refund"], 30.0)
        self.assertEqual(money["line_merchandise_refund"], 300.0)

        rows = expand_return_line_splits(
            db,
            "order-tax",
            {
                "transaction_item_uid": "ti-tax",
                "return_quantity": 2,
                "return_shipped_qty": 1,
                "cancel_unshipped_qty": 1,
            },
            ti_row=ti_row,
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["line_tax_refund"], 15.0)
        self.assertEqual(rows[1]["line_tax_refund"], 15.0)
        self.assertEqual(rows[0]["money"]["tax"], 15.0)
        self.assertTrue(rows[0]["money"]["known"])
        self.assertEqual(sum(r["line_tax_refund"] for r in rows), 30.0)

    def test_expand_hybrid_item_into_two_rows(self):
        db = _FakeDb()
        item = {
            "transaction_item_uid": "ti-123",
            "item_name": "Acquire Board Game",
            "return_quantity": 2,
            "return_shipped_qty": 1,
            "cancel_unshipped_qty": 1,
        }
        rows = expand_return_line_splits(
            db, "order-1", item, ti_row=self._acquire_ti_row()
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["return_kind"], "return")
        self.assertEqual(rows[1]["return_kind"], "cancel")
        self.assertNotIn("line_bounty_reclaim", item)
        self.assertEqual(rows[0]["line_bounty_reclaim"], 2.0)
        self.assertEqual(rows[1]["line_bounty_reclaim"], 2.0)
        self.assertEqual(rows[0]["line_shipping_refund"], 0.0)
        self.assertEqual(rows[1]["line_shipping_refund"], 2.0)
        self.assertEqual(
            sum(r["line_merchandise_refund"] for r in rows),
            60.0,
        )
        self.assertEqual(
            sum(r["line_bounty_reclaim"] for r in rows),
            4.0,
        )
        self.assertEqual(
            sum(r["line_shipping_refund"] for r in rows),
            2.0,
        )
        self.assertEqual(rows[0]["money"]["bounty"], 2.0)
        self.assertEqual(rows[1]["money"]["shipping"], 2.0)

    def test_expand_return_line_includes_bounty_percent(self):
        class _BountyDb(_FakeDb):
            def execute(self, query, params=None, **kwargs):
                sql = query if isinstance(query, str) else ""
                if "transaction_profile_id" in sql:
                    return {"result": [{"transaction_profile_id": "110-1"}]}
                if "transactions_bounty" in sql:
                    return {
                        "result": [
                            {
                                "tb_ti_id": "ti-123",
                                "tb_percentage": 0.4,
                                "tb_amount": 8.0,
                            }
                        ]
                    }
                return {"result": []}

        rows = expand_return_line_splits(
            _BountyDb(),
            "order-1",
            {
                "transaction_item_uid": "ti-123",
                "return_quantity": 1,
                "return_shipped_qty": 1,
                "cancel_unshipped_qty": 0,
            },
            ti_row=self._acquire_ti_row(),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["tb_percentage"], 40)
        self.assertEqual(rows[0]["percent_label"], "40%")

    def test_physical_return_only_single_row(self):
        db = _FakeDb()
        ti_row = self._acquire_ti_row()
        ti_row["ti_shipping_refundable"] = 1
        rows = expand_return_line_splits(
            db,
            "order-1",
            {
                "transaction_item_uid": "ti-123",
                "return_quantity": 1,
                "return_shipped_qty": 1,
                "cancel_unshipped_qty": 0,
            },
            ti_row=ti_row,
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["return_kind"], "return")
        self.assertEqual(rows[0]["line_shipping_refund"], 2.0)

    def test_cancel_only_single_row(self):
        db = _FakeDb()
        rows = expand_return_line_splits(
            db,
            "order-1",
            {
                "transaction_item_uid": "ti-123",
                "return_quantity": 2,
                "return_shipped_qty": 0,
                "cancel_unshipped_qty": 2,
            },
            ti_row=self._acquire_ti_row(),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["return_kind"], "cancel")
        self.assertEqual(rows[0]["line_shipping_refund"], 4.0)
        self.assertEqual(rows[0]["line_bounty_reclaim"], 4.0)

    def test_collapse_hybrid_splits_for_list_row(self):
        hybrid_splits = [
            {
                "transaction_item_uid": "ti-123",
                "ti_bs_id": "150-000135",
                "return_kind": "return",
                "return_quantity": 1,
                "return_shipped_qty": 1,
                "cancel_unshipped_qty": 0,
                "line_merchandise_refund": 30.0,
                "line_tax_refund": 3.0,
                "line_shipping_refund": 0.0,
                "line_bounty_reclaim": 2.0,
            },
            {
                "transaction_item_uid": "ti-123",
                "ti_bs_id": "150-000135",
                "return_kind": "cancel",
                "return_quantity": 1,
                "return_shipped_qty": 0,
                "cancel_unshipped_qty": 1,
                "line_merchandise_refund": 30.0,
                "line_tax_refund": 3.0,
                "line_shipping_refund": 2.0,
                "line_bounty_reclaim": 2.0,
            },
        ]
        merged = collapse_return_lines_for_list_row(hybrid_splits)
        self.assertEqual(len(merged), 1)
        self.assertNotIn("return_kind", merged[0])
        self.assertEqual(merged[0]["return_shipped_qty"], 1)
        self.assertEqual(merged[0]["cancel_unshipped_qty"], 1)
        self.assertEqual(merged[0]["return_quantity"], 2)
        self.assertEqual(merged[0]["line_merchandise_refund"], 60.0)
        self.assertEqual(merged[0]["line_bounty_reclaim"], 4.0)
        self.assertEqual(merged[0]["line_shipping_refund"], 2.0)


if __name__ == "__main__":
    unittest.main()
