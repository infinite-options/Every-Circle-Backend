"""Seller wallet resolution: business sales → business wallet; offerings → personal."""

import unittest
from unittest.mock import MagicMock

from wallet_transactions_service import resolve_seller_wallet_profile_id


class ResolveSellerWalletTests(unittest.TestCase):
    def test_business_sale_credits_business_uid(self):
        db = MagicMock()
        db.execute.return_value = {"result": [{"business_uid": "200-000001"}]}
        self.assertEqual(
            resolve_seller_wallet_profile_id(db, "200-000001"),
            "200-000001",
        )
        self.assertIn("business", db.execute.call_args[0][0].lower())

    def test_offering_sale_credits_personal_profile(self):
        db = MagicMock()

        def _exec(sql, params=None, *args, **kwargs):
            q = (sql or "").lower()
            if "from every_circle.business" in q:
                return {"result": []}
            if "from every_circle.profile_personal" in q and "profile_personal_uid" in q:
                return {"result": [{"profile_personal_uid": "110-000108"}]}
            return {"result": []}

        db.execute.side_effect = _exec
        self.assertEqual(
            resolve_seller_wallet_profile_id(db, "110-000108"),
            "110-000108",
        )

    def test_empty_seller_id(self):
        db = MagicMock()
        self.assertIsNone(resolve_seller_wallet_profile_id(db, None))
        self.assertIsNone(resolve_seller_wallet_profile_id(db, ""))


if __name__ == "__main__":
    unittest.main()
