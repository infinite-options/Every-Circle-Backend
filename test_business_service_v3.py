"""Business product/service v3 parity with offering checkout snapshots."""

import unittest

from line_commerce_fields import (
    line_merchandise_total_from_row,
    line_snapshot_api_fields,
    order_money_from_line_snapshots,
)
from transactions import (
    _parse_limited_quantity,
    _validate_business_service_available,
    _validate_purchase_quantity,
)


class _FakeDb:
    def __init__(self, business=None):
        self.business = business or {}

    def execute(self, query, params=None, **kwargs):
        return {"result": []}


class BusinessServiceV3Tests(unittest.TestCase):
    def test_line_merchandise_includes_choice_extras(self):
        row = {
            "ti_bs_cost": 15.0,
            "ti_bs_qty": 2,
            "ti_choices_extra_cost": 3.0,
            "ti_line_tax_amount": 2.88,
            "ti_line_shipping_amount": 5.0,
            "ti_shipping_amount": 2.5,
        }
        self.assertEqual(line_merchandise_total_from_row(row), 33.0)
        money = order_money_from_line_snapshots(row)
        self.assertTrue(money["known"])
        self.assertEqual(money["merchandise"], 33.0)
        self.assertEqual(money["customer_total"], 40.88)
        snap = line_snapshot_api_fields(row)
        self.assertEqual(snap["line_merchandise_total"], 33.0)
        self.assertEqual(snap["ti_choices_extra_cost"], 3.0)

    def test_unlimited_quantity_parsing(self):
        self.assertIsNone(_parse_limited_quantity("unlimited"))
        self.assertIsNone(_parse_limited_quantity(None))
        self.assertEqual(_parse_limited_quantity("5"), 5)

    def test_validate_purchase_quantity_limited_stock(self):
        err = _validate_purchase_quantity(2, 3)
        self.assertIsNotNone(err)
        self.assertEqual(err[0]["code"], 409)
        self.assertIsNone(_validate_purchase_quantity(None, 99))

    def test_business_service_availability(self):
        ok_row = {
            "bs_is_visible": 1,
            "bs_status": "active",
        }
        db = _FakeDb()
        self.assertIsNone(_validate_business_service_available(db, ok_row))
        hidden = dict(ok_row, bs_is_visible=0)
        err = _validate_business_service_available(db, hidden)
        self.assertEqual(err["code"], 403)
        sold_out = dict(ok_row, bs_status="out_of_stock")
        err = _validate_business_service_available(db, sold_out)
        self.assertEqual(err["code"], 403)


if __name__ == "__main__":
    unittest.main()
