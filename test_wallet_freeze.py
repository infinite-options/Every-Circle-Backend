"""Unit tests for frozen-wallet guards on account deletion."""

import unittest
from unittest.mock import MagicMock, patch

from wallet_service import (
    adjust_wallet_reserve,
    credit_bounty_to_wallet,
    credit_seller_proceeds_to_wallet,
    credit_useable_from_refund,
    debit_useable_for_purchase,
    freeze_wallet,
    release_bounty_to_useable,
    release_seller_hold_to_useable,
    wallet_row_is_frozen,
)


class WalletRowIsFrozenTests(unittest.TestCase):
    def test_frozen_flag(self):
        self.assertTrue(wallet_row_is_frozen({"wallet_is_frozen": 1}))
        self.assertFalse(wallet_row_is_frozen({"wallet_is_frozen": 0}))
        self.assertFalse(wallet_row_is_frozen(None))


class FreezeWalletTests(unittest.TestCase):
    def test_freeze_wallet_updates_row(self):
        db = MagicMock()
        with patch(
            "wallet_service.get_wallet_row",
            return_value={"wallet_profile_id": "110-1", "wallet_is_frozen": 0},
        ):
            db.execute.return_value = {"code": 200}
            self.assertTrue(freeze_wallet(db, "110-1"))
        db.execute.assert_called_once()

    def test_freeze_wallet_no_row(self):
        db = MagicMock()
        with patch("wallet_service.get_wallet_row", return_value=None):
            self.assertFalse(freeze_wallet(db, "110-missing"))
        db.execute.assert_not_called()


class FrozenWalletGuardTests(unittest.TestCase):
    def setUp(self):
        self.db = MagicMock()
        self.frozen_wallet = {
            "wallet_profile_id": "110-1",
            "wallet_is_frozen": 1,
            "wallet_useable_balance": 25.0,
            "wallet_pending": 0.0,
            "wallet_actual_balance": 25.0,
            "wallet_lifetime_earning": 25.0,
            "wallet_lifetime_spent": 0.0,
            "wallet_reserve": 0.0,
        }

    def _patch_wallet(self):
        return patch("wallet_service.get_wallet_row", return_value=self.frozen_wallet)

    def test_blocks_spend(self):
        with self._patch_wallet():
            result = debit_useable_for_purchase(self.db, "110-1", 5.0)
        self.assertEqual(result["code"], 403)
        self.assertEqual(result["message"], "Wallet is frozen")

    def test_blocks_bounty_credit(self):
        with self._patch_wallet():
            result = credit_bounty_to_wallet(self.db, "110-1", 3.0)
        self.assertEqual(result["code"], 403)

    def test_blocks_seller_credit(self):
        with self._patch_wallet():
            result = credit_seller_proceeds_to_wallet(self.db, "110-1", 3.0)
        self.assertEqual(result["code"], 403)

    def test_blocks_bounty_release(self):
        with self._patch_wallet():
            result = release_bounty_to_useable(self.db, "110-1", 3.0)
        self.assertEqual(result["code"], 403)

    def test_blocks_seller_hold_release(self):
        with self._patch_wallet():
            result = release_seller_hold_to_useable(self.db, "110-1", 3.0)
        self.assertEqual(result["code"], 403)

    def test_blocks_refund_credit(self):
        with self._patch_wallet():
            result = credit_useable_from_refund(self.db, "110-1", 3.0)
        self.assertEqual(result["code"], 403)

    def test_blocks_reserve_release_to_useable(self):
        with self._patch_wallet():
            result = adjust_wallet_reserve(self.db, "110-1", -2.0)
        self.assertEqual(result["code"], 403)


if __name__ == "__main__":
    unittest.main()
