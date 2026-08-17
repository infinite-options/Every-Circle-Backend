"""Seeking profile_wish shipping normalize + snapshot + checkout helpers."""

import unittest

from moderation import build_wish_snapshot
from transactions import (
    _compute_expected_line_shipping,
    _listing_shipping_type,
    _shipping_amount_from_product,
    _shipping_refundable_from_product,
)
from user_profile_info import _derive_wish_shipping_fields, _wish_dict_from_payload


class WishShippingNormalizeTests(unittest.TestCase):
    def test_buyer_fixed_keeps_amount_default_zero(self):
        data = {
            "profile_wish_shipping": "Buyer Fixed",
            "profile_wish_shipping_refundable": "1",
        }
        _derive_wish_shipping_fields(data)
        self.assertEqual(data["profile_wish_shipping"], "Buyer Fixed")
        self.assertEqual(data["profile_wish_shipping_amount"], 0.0)
        self.assertEqual(data["profile_wish_shipping_refundable"], 1)

    def test_buyer_fixed_with_amount(self):
        data = {
            "profile_wish_shipping": "Buyer Fixed",
            "profile_wish_shipping_amount": "12.50",
            "profile_wish_shipping_refundable": 0,
        }
        _derive_wish_shipping_fields(data)
        self.assertEqual(data["profile_wish_shipping_amount"], 12.5)
        self.assertEqual(data["profile_wish_shipping_refundable"], 0)

    def test_free_and_buyer_actual_clear_amount(self):
        free = {"profile_wish_shipping": "Free", "profile_wish_shipping_amount": 9}
        _derive_wish_shipping_fields(free)
        self.assertEqual(free["profile_wish_shipping"], "Free")
        self.assertIsNone(free["profile_wish_shipping_amount"])

        actual = {
            "profile_wish_shipping": "Buyer Actual",
            "profile_wish_shipping_amount": 9,
        }
        _derive_wish_shipping_fields(actual)
        self.assertEqual(actual["profile_wish_shipping"], "Buyer Actual")
        self.assertIsNone(actual["profile_wish_shipping_amount"])

    def test_invalid_or_null_clears_shipping(self):
        data = {
            "profile_wish_shipping": None,
            "profile_wish_shipping_amount": 5,
            "profile_wish_free_shipping": 1,
            "profile_wish_shipping_cost_type": "fixed",
        }
        out = _wish_dict_from_payload(data)
        self.assertIsNone(out.get("profile_wish_shipping"))
        self.assertNotIn("profile_wish_shipping_amount", out)
        self.assertNotIn("profile_wish_free_shipping", out)
        self.assertNotIn("profile_wish_shipping_cost_type", out)

    def test_camel_case_aliases(self):
        out = _wish_dict_from_payload(
            {
                "shipping": "Buyer Fixed",
                "shippingAmount": 8,
                "shippingRefundable": True,
            }
        )
        self.assertEqual(out["profile_wish_shipping"], "Buyer Fixed")
        self.assertEqual(out["profile_wish_shipping_amount"], 8.0)
        self.assertEqual(out["profile_wish_shipping_refundable"], 1)


class WishSnapshotShippingTests(unittest.TestCase):
    def test_snapshot_includes_shipping_fields(self):
        snap = build_wish_snapshot(
            {
                "profile_wish_uid": "160-1",
                "profile_wish_title": "Need help",
                "profile_wish_shipping": "Buyer Fixed",
                "profile_wish_shipping_amount": 7.5,
                "profile_wish_shipping_refundable": 1,
            }
        )
        self.assertEqual(snap["shipping"], "Buyer Fixed")
        self.assertEqual(snap["shippingAmount"], 7.5)
        self.assertEqual(snap["shippingRefundable"], 1)


class WishCheckoutShippingHelpersTests(unittest.TestCase):
    def test_listing_helpers_read_profile_wish_shipping(self):
        wish = {
            "profile_wish_shipping": "Buyer Fixed",
            "profile_wish_shipping_amount": 5.0,
            "profile_wish_shipping_refundable": 1,
        }
        self.assertEqual(_listing_shipping_type(wish), "Buyer Fixed")
        self.assertEqual(_shipping_amount_from_product(wish), 5.0)
        self.assertEqual(_shipping_refundable_from_product(wish), 1)
        self.assertEqual(
            _compute_expected_line_shipping("ship", wish, 3),
            15.0,
        )

    def test_offering_still_preferred_when_both_present(self):
        mixed = {
            "profile_expertise_shipping": "Free",
            "profile_wish_shipping": "Buyer Fixed",
            "profile_wish_shipping_amount": 9,
        }
        self.assertEqual(_listing_shipping_type(mixed), "Free")
        self.assertEqual(_shipping_amount_from_product(mixed), 0.0)


if __name__ == "__main__":
    unittest.main()
