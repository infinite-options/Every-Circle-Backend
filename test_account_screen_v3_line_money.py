"""Contract fixture: per-line tax/shipping snapshots → account-screen money blocks."""

import unittest

from line_commerce_fields import (
    aggregate_order_customer_money,
    build_purchase_line_v3_entry,
    format_offering_rate_display,
    format_profile_expertise_cost_label,
    order_money_from_line_snapshots,
    return_money_from_line_snapshots,
)


class _FakeDb:
    def __init__(self, ti_uids, snap_map):
        self.ti_uids = ti_uids
        self.snap_map = snap_map

    def execute(self, query, params=None, **kwargs):
        if "ALTER TABLE" in query or "UPDATE every_circle.transactions_items" in query:
            return {"code": 200}
        if "ti_transaction_id" in query:
            return {"result": [{"ti_uid": uid} for uid in self.ti_uids]}
        if "ti_uid IN" in query:
            return {
                "result": [
                    self.snap_map[uid]
                    for uid in self.ti_uids
                    if uid in self.snap_map
                ]
            }
        return {"result": []}


class AccountScreenV3LineMoneyTests(unittest.TestCase):
    def test_acquire_line_money(self):
        row = {
            "row_kind": "sale_line",
            "ti_bs_id": "150-000136",
            "ti_bs_cost": 30.0,
            "ti_bs_qty": 3,
            "ti_line_tax_amount": 9.0,
            "ti_line_shipping_amount": 12.0,
            "ti_shipping_amount": 4.0,
            "ti_bs_tax_rate": 10.0,
        }
        money = order_money_from_line_snapshots(row)
        self.assertTrue(money["known"])
        self.assertEqual(money["merchandise"], 90.0)
        self.assertEqual(money["tax"], 9.0)
        self.assertEqual(money["shipping"], 12.0)
        self.assertEqual(money["customer_total"], 111.0)

    def test_legacy_rate_fallback_for_pre_fix_orders(self):
        row = {
            "ti_bs_cost": 30.0,
            "ti_bs_qty": 3,
            "ti_bs_tax_rate": 10.0,
            "ti_bs_is_taxable": 1,
            "ti_line_shipping_amount": 12.0,
            "ti_shipping_amount": 4.0,
        }
        money = order_money_from_line_snapshots(row)
        self.assertTrue(money["known"])
        self.assertEqual(money["tax"], 9.0)
        self.assertEqual(money["customer_total"], 111.0)

    def test_seven_wonders_line_money(self):
        row = {
            "ti_bs_cost": 40.0,
            "ti_bs_qty": 5,
            "ti_line_tax_amount": 20.0,
            "ti_line_shipping_amount": 10.0,
        }
        money = order_money_from_line_snapshots(row)
        self.assertTrue(money["known"])
        self.assertEqual(money["customer_total"], 230.0)

    def test_fixture_totals_foot(self):
        rows = [
            {
                "ti_bs_cost": 30.0,
                "ti_bs_qty": 3,
                "ti_line_tax_amount": 9.0,
                "ti_line_shipping_amount": 12.0,
            },
            {
                "ti_bs_cost": 40.0,
                "ti_bs_qty": 5,
                "ti_line_tax_amount": 20.0,
                "ti_line_shipping_amount": 10.0,
            },
        ]
        totals = {"merchandise": 0.0, "tax": 0.0, "shipping": 0.0, "customer_total": 0.0}
        for row in rows:
            money = order_money_from_line_snapshots(row)
            self.assertTrue(money["known"])
            totals["merchandise"] += money["merchandise"]
            totals["tax"] += money["tax"]
            totals["shipping"] += money["shipping"]
            totals["customer_total"] += money["customer_total"]
        self.assertEqual(totals["merchandise"], 290.0)
        self.assertEqual(totals["tax"], 29.0)
        self.assertEqual(totals["shipping"], 22.0)
        self.assertEqual(totals["customer_total"], 341.0)

    def test_return_money_from_sale_snapshots(self):
        sale_line = {
            "ti_bs_cost": 30.0,
            "ti_bs_qty": 3,
            "ti_line_tax_amount": 9.0,
            "ti_line_shipping_amount": 12.0,
            "ti_shipping_amount": 4.0,
            "ti_shipping_refundable": 0,
            "ti_listing_shipping": "buyer fixed",
        }
        return_row = {
            "row_kind": "return",
            "units": {
                "return_shipped_qty": 1,
                "cancel_unshipped_qty": 1,
            },
        }
        money = return_money_from_line_snapshots(return_row, sale_line=sale_line)
        self.assertTrue(money["known"])
        self.assertEqual(money["merchandise"], -60.0)
        self.assertEqual(money["tax"], -6.0)
        self.assertEqual(money["customer_credit"], -70.0)

    def test_multi_item_order_aggregation(self):
        snap_map = {
            "510-000933": {
                "ti_uid": "510-000933",
                "ti_bs_cost": 30.0,
                "ti_bs_qty": 4,
                "ti_line_tax_amount": 12.0,
                "ti_line_shipping_amount": 16.0,
            },
            "510-000934": {
                "ti_uid": "510-000934",
                "ti_bs_cost": 40.0,
                "ti_bs_qty": 4,
                "ti_line_tax_amount": 16.0,
                "ti_line_shipping_amount": 8.0,
            },
        }
        db = _FakeDb(["510-000933", "510-000934"], snap_map)
        money = aggregate_order_customer_money(
            db,
            "500-000754",
            order_row={"transaction_fees": 9.96},
        )
        self.assertTrue(money["known"])
        self.assertEqual(money["merchandise"], 280.0)
        self.assertEqual(money["tax"], 28.0)
        self.assertEqual(money["shipping"], 24.0)
        self.assertEqual(money["customer_total"], 332.0)
        self.assertEqual(money["fees"], 9.96)
        self.assertEqual(money["customer_total_with_fees"], 341.96)

    def test_missing_snapshots_unknown(self):
        row = {"ti_bs_cost": 30.0, "ti_bs_qty": 3, "ti_line_shipping_amount": 12.0}
        money = order_money_from_line_snapshots(row)
        self.assertFalse(money["known"])

    def test_offering_rate_display_always_includes_each(self):
        self.assertEqual(format_offering_rate_display(30), "$30/each")
        self.assertEqual(format_offering_rate_display(40.0), "$40/each")
        self.assertEqual(format_offering_rate_display("30/each"), "$30/each")
        self.assertEqual(format_profile_expertise_cost_label(30), "30/each")

    def test_purchase_line_v3_entry_acquire_fixture(self):
        line = build_purchase_line_v3_entry(
            {
                "ti_uid": "510-000933",
                "ti_bs_id": "150-000136",
                "item_name": "Acquire Board Game",
                "ti_bs_qty": 4,
                "ti_bs_cost": 30,
                "ti_line_tax_amount": 12,
                "ti_line_shipping_amount": 16,
            }
        )
        self.assertEqual(line["offering_rate_display"], "$30/each")
        self.assertEqual(line["profile_expertise_cost"], "30/each")
        self.assertEqual(line["ti_bs_cost"], "30")
        self.assertEqual(line["money"]["customer_total"], 148.0)

    def test_purchase_line_v3_entry_seven_wonders_fixture(self):
        line = build_purchase_line_v3_entry(
            {
                "ti_uid": "510-000934",
                "ti_bs_id": "150-000137",
                "item_name": "7 Wonders Board Game",
                "ti_bs_qty": 4,
                "ti_bs_cost": 40,
                "ti_line_tax_amount": 16,
                "ti_line_shipping_amount": 8,
            }
        )
        self.assertEqual(line["offering_rate_display"], "$40/each")
        self.assertEqual(line["money"]["customer_total"], 184.0)

    def test_multi_item_line_totals_reconcile(self):
        lines = [
            build_purchase_line_v3_entry(
                {
                    "ti_uid": "510-000933",
                    "ti_bs_cost": 30,
                    "ti_bs_qty": 4,
                    "ti_line_tax_amount": 12,
                    "ti_line_shipping_amount": 16,
                }
            ),
            build_purchase_line_v3_entry(
                {
                    "ti_uid": "510-000934",
                    "ti_bs_cost": 40,
                    "ti_bs_qty": 4,
                    "ti_line_tax_amount": 16,
                    "ti_line_shipping_amount": 8,
                }
            ),
        ]
        order_total = sum(line["money"]["customer_total"] for line in lines)
        self.assertEqual(order_total, 332.0)

    def test_tb_percentage_fraction_to_whole_percent(self):
        from account_screen_v3_contract import (
            format_tb_percent_label,
            normalize_tb_percentage_display,
        )

        self.assertEqual(normalize_tb_percentage_display("0.4"), 40)
        self.assertEqual(normalize_tb_percentage_display(0.4), 40)
        self.assertEqual(normalize_tb_percentage_display(40), 40)
        self.assertEqual(format_tb_percent_label("0.4"), "40%")

    def test_return_bounty_line_scoped_not_order_total(self):
        from account_screen_v3 import _seller_bounty_block

        block = _seller_bounty_block(
            {
                "row_kind": "return",
                "ti_uid": "510-000926",
                "line_bounty_paid": 15,
                "order_bounty_paid": 21,
                "bounty_to_reclaim": 6,
            }
        )
        self.assertEqual(block["order_bounty_paid"], 15)
        self.assertEqual(block["bounty_to_reclaim"], 6)

    def test_normalize_purchase_type_lowercase(self):
        from account_screen_v3 import _normalize_purchase_type

        self.assertEqual(_normalize_purchase_type({"purchase_type": "Offering"}), "offering")
        self.assertEqual(_normalize_purchase_type({"ti_bs_id": "250-000001"}), "service")

    def test_round_ledger_entry_to_cents(self):
        from account_screen_v3 import _round_ledger_entry

        rounded = _round_ledger_entry(
            {
                "amount": 312.0002,
                "bounty_amount": -19.9998,
                "lines": [
                    {"net_amount": 140.0001, "bounty_amount": -8.0002},
                    {"net_amount": 172.0001, "bounty_amount": -11.9998},
                ],
            }
        )
        self.assertEqual(rounded["amount"], 312.0)
        self.assertEqual(rounded["bounty_amount"], -20.0)
        self.assertEqual(rounded["lines"][0]["net_amount"], 140.0)
        self.assertEqual(rounded["lines"][0]["bounty_amount"], -8.0)


if __name__ == "__main__":
    unittest.main()
