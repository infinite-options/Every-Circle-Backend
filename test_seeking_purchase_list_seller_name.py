"""Seeking purchase list Seller/business_name uses helper, not wish poster."""

import unittest

from transactions import _buyer_purchase_list_query


class SeekingPurchaseListSellerNameTests(unittest.TestCase):
    def test_buyer_list_seeking_uses_seller_pp_not_wish_pp(self):
        sql = _buyer_purchase_list_query()
        self.assertIn(
            "WHEN ti.ti_bs_id LIKE '165-%%' THEN\n"
            "                            CONCAT(seller_pp.profile_personal_first_name, ' ', seller_pp.profile_personal_last_name)",
            sql,
        )
        self.assertNotIn("wish_pp.profile_personal_first_name", sql)
        self.assertIn(
            "ON t.transaction_business_id = seller_pp.profile_personal_uid",
            sql,
        )
        self.assertNotIn(
            "ON t.transaction_business_id = seller_pp.profile_personal_user_id",
            sql,
        )
        # Unchanged product / offering branches
        self.assertIn("WHEN ti.ti_bs_id LIKE '250-%%' THEN biz.business_name", sql)
        self.assertIn("expertise_pp.profile_personal_first_name", sql)


if __name__ == "__main__":
    unittest.main()
