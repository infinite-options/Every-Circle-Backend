"""Seeking checkout: unit cost stays listing rate; buyer bounty in paid total only."""

import unittest
from unittest.mock import patch

from transactions import (
    _apply_item_options_to_tx_item,
    _line_buyer_bounty_amount,
    _plan_checkout,
)


def _wish_listing(cost=30.0, bounty_type="total"):
    return {
        "wish_response_uid": "165-000001",
        "profile_wish_uid": "160-000001",
        "profile_wish_cost": cost,
        "profile_wish_is_taxable": 0,
        "profile_wish_tax_rate": 0,
        "profile_wish_mode": "In-Person",
        "profile_wish_bounty_type": bounty_type,
    }


def _expertise_listing(cost=30.0):
    return {
        "profile_expertise_uid": "150-000001",
        "profile_expertise_cost": cost,
        "profile_expertise_is_taxable": 0,
        "profile_expertise_tax_rate": 0,
        "profile_expertise_mode": "In-Person",
        "profile_expertise_bounty_type": "total",
    }


def _product_listing(cost=30.0):
    return {
        "bs_uid": "250-000001",
        "bs_cost": cost,
        "bs_is_taxable": 0,
        "bs_tax_rate": 0,
        "bs_mode": "In-Person",
        "bs_bounty_type": "total",
        "bs_moderated": 1,
    }


class LineBuyerBountyAmountTests(unittest.TestCase):
    def test_wish_total_bounty(self):
        item = {"bounty": 50, "bounty_type": "total"}
        self.assertEqual(
            _line_buyer_bounty_amount(item, _wish_listing(), 4, True), 50.0
        )

    def test_wish_per_item_bounty(self):
        item = {"bounty": 10, "bounty_type": "per_item"}
        self.assertEqual(
            _line_buyer_bounty_amount(item, _wish_listing(), 4, True), 40.0
        )

    def test_wish_uses_listing_bounty_type_when_omitted(self):
        item = {"bounty": 12}
        listing = _wish_listing(bounty_type="per_item")
        self.assertEqual(_line_buyer_bounty_amount(item, listing, 3, True), 36.0)

    def test_non_wish_ignores_bounty(self):
        item = {"bounty": 50, "bounty_type": "total"}
        self.assertEqual(
            _line_buyer_bounty_amount(item, _expertise_listing(), 4, False), 0.0
        )

    def test_missing_or_zero_bounty(self):
        self.assertEqual(
            _line_buyer_bounty_amount({}, _wish_listing(), 4, True), 0.0
        )
        self.assertEqual(
            _line_buyer_bounty_amount({"bounty": 0}, _wish_listing(), 4, True), 0.0
        )


class SeekingCheckoutPlanTests(unittest.TestCase):
    @patch("transactions._fetch_listing_for_checkout_item")
    def test_seeking_accepts_listing_unit_plus_buyer_bounty(self, mock_fetch):
        mock_fetch.return_value = (
            _wish_listing(),
            "165-000001",
            "pickup",
            True,
            None,
        )
        items = [
            {
                "wish_response_uid": "165-000001",
                "quantity": 4,
                "unit_price": 30,
                "bounty": 50,
                "bounty_type": "total",
                "line_tax_amount": 0,
                "fulfillment_method": "pickup",
                "shipping_not_required": 1,
                "line_shipping_amount": 0,
            }
        ]
        payload = {
            "total_costs": 120,
            "total_taxes": 0,
            "total_shipping": 0,
            "total_fees": 5.10,
            "total_amount_paid": 175.10,
            "total_bounty": 50,
        }
        ok, err, plan = _plan_checkout(None, items, payload, shipping_fields=None)
        self.assertTrue(ok, err)
        self.assertIsNone(err)
        self.assertEqual(plan["order_merchandise"], 120.0)
        self.assertEqual(plan["order_buyer_bounty"], 50.0)
        self.assertTrue(plan["any_wish"])
        self.assertEqual(plan["lines"][0]["unit_price"], 30.0)
        self.assertEqual(plan["lines"][0]["line_merchandise"], 120.0)
        self.assertEqual(plan["lines"][0]["line_buyer_bounty"], 50.0)

    @patch("transactions._fetch_listing_for_checkout_item")
    def test_seeking_rejects_amortized_unit_price_legacy_paid(self, mock_fetch):
        """FE amortized bounty into unit_price and paid without double-counting bounty."""
        mock_fetch.return_value = (
            _wish_listing(),
            "165-000001",
            "pickup",
            True,
            None,
        )
        items = [
            {
                "wish_response_uid": "165-000001",
                "quantity": 4,
                "unit_price": 42.50,
                "bounty": 50,
                "bounty_type": "total",
                "line_tax_amount": 0,
                "fulfillment_method": "pickup",
                "shipping_not_required": 1,
                "line_shipping_amount": 0,
            }
        ]
        # Amortized merchandise 170 + fees 5.10 = 175.10 (old FE paid total)
        payload = {
            "total_costs": 170,
            "total_taxes": 0,
            "total_shipping": 0,
            "total_fees": 5.10,
            "total_amount_paid": 175.10,
        }
        ok, err, plan = _plan_checkout(None, items, payload, shipping_fields=None)
        self.assertFalse(ok)
        self.assertIsNone(plan)
        self.assertEqual(err["code"], 400)
        self.assertIn("total_amount_paid mismatch", err["message"])
        # Expected paid once bounty is added: 170 + 5.10 + 50 = 225.10
        self.assertIn("225.10", err["message"])

    @patch("transactions._fetch_listing_for_checkout_item")
    def test_seeking_rejects_costs_that_include_bounty_vs_listing_unit(self, mock_fetch):
        mock_fetch.return_value = (
            _wish_listing(),
            "165-000001",
            "pickup",
            True,
            None,
        )
        items = [
            {
                "wish_response_uid": "165-000001",
                "quantity": 4,
                "unit_price": 30,
                "bounty": 50,
                "bounty_type": "total",
                "line_tax_amount": 0,
                "fulfillment_method": "pickup",
                "shipping_not_required": 1,
                "line_shipping_amount": 0,
            }
        ]
        payload = {
            "total_costs": 170,
            "total_taxes": 0,
            "total_shipping": 0,
            "total_fees": 5.10,
            "total_amount_paid": 175.10,
        }
        ok, err, plan = _plan_checkout(None, items, payload, shipping_fields=None)
        self.assertFalse(ok)
        self.assertIsNone(plan)
        self.assertIn("total_costs mismatch", err["message"])
        self.assertIn("120.00", err["message"])

    @patch("transactions._fetch_listing_for_checkout_item")
    def test_seeking_optional_total_bounty_mismatch(self, mock_fetch):
        mock_fetch.return_value = (
            _wish_listing(),
            "165-000001",
            "pickup",
            True,
            None,
        )
        items = [
            {
                "wish_response_uid": "165-000001",
                "quantity": 4,
                "unit_price": 30,
                "bounty": 50,
                "bounty_type": "total",
                "line_tax_amount": 0,
                "fulfillment_method": "pickup",
                "shipping_not_required": 1,
                "line_shipping_amount": 0,
            }
        ]
        payload = {
            "total_costs": 120,
            "total_taxes": 0,
            "total_shipping": 0,
            "total_fees": 5.10,
            "total_amount_paid": 175.10,
            "total_bounty": 40,
        }
        ok, err, plan = _plan_checkout(None, items, payload, shipping_fields=None)
        self.assertFalse(ok)
        self.assertIn("total_bounty mismatch", err["message"])

    @patch("transactions._fetch_listing_for_checkout_item")
    def test_offering_bounty_not_in_paid_total(self, mock_fetch):
        mock_fetch.return_value = (
            _expertise_listing(),
            "150-000001",
            "pickup",
            False,
            None,
        )
        items = [
            {
                "expertise_uid": "150-000001",
                "quantity": 4,
                "unit_price": 30,
                "bounty": 50,
                "bounty_type": "total",
                "line_tax_amount": 0,
                "fulfillment_method": "pickup",
                "shipping_not_required": 1,
                "line_shipping_amount": 0,
            }
        ]
        payload = {
            "total_costs": 120,
            "total_taxes": 0,
            "total_shipping": 0,
            "total_fees": 5.10,
            "total_amount_paid": 125.10,
        }
        ok, err, plan = _plan_checkout(None, items, payload, shipping_fields=None)
        self.assertTrue(ok, err)
        self.assertEqual(plan["order_buyer_bounty"], 0.0)
        self.assertFalse(plan["any_wish"])
        self.assertEqual(plan["order_merchandise"], 120.0)

    @patch("transactions._fetch_listing_for_checkout_item")
    def test_product_bounty_not_in_paid_total(self, mock_fetch):
        mock_fetch.return_value = (
            _product_listing(),
            "250-000001",
            "pickup",
            False,
            None,
        )
        items = [
            {
                "bs_uid": "250-000001",
                "quantity": 2,
                "unit_price": 30,
                "bounty": 20,
                "bounty_type": "total",
                "line_tax_amount": 0,
                "fulfillment_method": "pickup",
                "shipping_not_required": 1,
                "line_shipping_amount": 0,
            }
        ]
        payload = {
            "total_costs": 60,
            "total_taxes": 0,
            "total_shipping": 0,
            "total_fees": 3.00,
            "total_amount_paid": 63.00,
        }
        ok, err, plan = _plan_checkout(None, items, payload, shipping_fields=None)
        self.assertTrue(ok, err)
        self.assertEqual(plan["order_buyer_bounty"], 0.0)
        self.assertFalse(plan["any_wish"])


class SeekingUnitCostPersistTests(unittest.TestCase):
    def test_wish_keeps_listing_cost_despite_unit_price(self):
        tx_item = {"ti_bs_cost": 30.0}
        _apply_item_options_to_tx_item(
            tx_item,
            {"unit_price": 42.50, "quantity": 4},
            "165-000001",
        )
        self.assertEqual(tx_item["ti_bs_cost"], 30.0)

    def test_wish_omitted_unit_price_keeps_listing_cost(self):
        tx_item = {"ti_bs_cost": 30.0}
        _apply_item_options_to_tx_item(tx_item, {"quantity": 4}, "165-000001")
        self.assertEqual(tx_item["ti_bs_cost"], 30.0)

    def test_offering_still_overwrites_from_unit_price(self):
        tx_item = {"ti_bs_cost": 30.0}
        _apply_item_options_to_tx_item(
            tx_item,
            {"unit_price": 35.0},
            "150-000001",
        )
        self.assertEqual(float(tx_item["ti_bs_cost"]), 35.0)

    def test_product_still_overwrites_from_unit_price(self):
        tx_item = {"ti_bs_cost": 30.0}
        _apply_item_options_to_tx_item(
            tx_item,
            {"unit_price": 28.5},
            "250-000001",
        )
        self.assertEqual(float(tx_item["ti_bs_cost"]), 28.5)


if __name__ == "__main__":
    unittest.main()
