"""Order detail buyer/seller display names."""

import unittest
from unittest.mock import MagicMock

from order_detail import (
    _attach_party_display_names,
    _clean_display_name,
    _load_sale_header,
)


class PartyDisplayNameTests(unittest.TestCase):
    def test_clean_display_name(self):
        self.assertEqual(_clean_display_name("  Jane   Doe "), "Jane Doe")
        self.assertIsNone(_clean_display_name(""))
        self.assertIsNone(_clean_display_name("   "))
        self.assertIsNone(_clean_display_name(None))

    def test_attach_aliases(self):
        sale = _attach_party_display_names(
            {
                "purchaser_name": "  Pat Buyer ",
                "seller_name": "Helper Seller",
            }
        )
        self.assertEqual(sale["purchaser_name"], "Pat Buyer")
        self.assertEqual(sale["buyer_name"], "Pat Buyer")
        self.assertEqual(sale["seller_name"], "Helper Seller")
        self.assertEqual(sale["business_name"], "Helper Seller")
        self.assertEqual(sale["transaction_business_name"], "Helper Seller")

    def test_load_sale_header_personal_seller_110(self):
        db = MagicMock()
        db.execute.return_value = {
            "result": [
                {
                    "transaction_uid": "500-1",
                    "transaction_type": "sale",
                    "transaction_profile_id": "110-buyer",
                    "transaction_business_id": "110-seller",
                    "purchaser_name": "Jane Buyer",
                    "seller_name": "Sam Helper",
                }
            ]
        }
        sale = _load_sale_header(db, "500-1")
        self.assertEqual(sale["purchaser_name"], "Jane Buyer")
        self.assertEqual(sale["buyer_name"], "Jane Buyer")
        self.assertEqual(sale["seller_name"], "Sam Helper")
        self.assertEqual(sale["business_name"], "Sam Helper")
        sql = db.execute.call_args[0][0]
        self.assertIn("buyer_pp.profile_personal_uid", sql)
        self.assertIn("seller_pp.profile_personal_uid", sql)
        self.assertIn("200-%%", sql)
        self.assertIn("110-%%", sql)

    def test_load_sale_header_business_seller_200(self):
        db = MagicMock()
        db.execute.return_value = {
            "result": [
                {
                    "transaction_uid": "500-2",
                    "transaction_type": "sale",
                    "transaction_profile_id": "110-buyer",
                    "transaction_business_id": "200-biz",
                    "purchaser_name": "Jane Buyer",
                    "seller_name": "Acme Services",
                }
            ]
        }
        sale = _load_sale_header(db, "500-2")
        self.assertEqual(sale["seller_name"], "Acme Services")
        self.assertEqual(sale["transaction_business_name"], "Acme Services")


if __name__ == "__main__":
    unittest.main()
