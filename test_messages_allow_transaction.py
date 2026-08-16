"""Unit tests for transaction-messaging privacy helpers."""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from chat import (
    _allow_transaction_flag,
    _messages_privacy_violation,
    _sale_lines_allow_transaction_messaging,
    _TRANSACTION_MESSAGING_GRACE_DAYS,
)
from user_profile_info import (
    _coerce_messages_allow_transaction,
    _normalize_messages_allow_transaction,
)


class AllowTransactionFlagTests(unittest.TestCase):
    def test_default_on_when_missing(self):
        self.assertTrue(_allow_transaction_flag(None))
        self.assertTrue(_allow_transaction_flag(""))
        self.assertEqual(_coerce_messages_allow_transaction(None), 1)
        self.assertEqual(
            _normalize_messages_allow_transaction({})[
                "profile_personal_messages_allow_transaction"
            ],
            1,
        )

    def test_coerce_zero_and_one(self):
        self.assertEqual(_coerce_messages_allow_transaction("0"), 0)
        self.assertEqual(_coerce_messages_allow_transaction("1"), 1)
        self.assertFalse(_allow_transaction_flag(0))
        self.assertTrue(_allow_transaction_flag(1))


class SaleLineMessagingWindowTests(unittest.TestCase):
    def test_open_order_when_unreceived(self):
        now = datetime(2026, 8, 16, tzinfo=timezone.utc)
        lines = [{"ti_received_at": None, "ti_bs_return_window_days": 14, "ti_bs_is_returnable": 1}]
        self.assertTrue(_sale_lines_allow_transaction_messaging(lines, now=now))

    def test_within_grace_after_return_window(self):
        received = datetime(2026, 7, 1, tzinfo=timezone.utc)
        # window 14 → closes Jul 15; grace → Jul 25
        now = received + timedelta(days=14 + _TRANSACTION_MESSAGING_GRACE_DAYS)
        lines = [
            {
                "ti_received_at": received.strftime("%Y-%m-%d %H:%M:%S"),
                "ti_bs_return_window_days": 14,
                "ti_bs_is_returnable": 1,
            }
        ]
        self.assertTrue(_sale_lines_allow_transaction_messaging(lines, now=now))

    def test_past_grace_after_return_window(self):
        received = datetime(2026, 7, 1, tzinfo=timezone.utc)
        now = received + timedelta(days=14 + _TRANSACTION_MESSAGING_GRACE_DAYS + 1)
        lines = [
            {
                "ti_received_at": received.strftime("%Y-%m-%d %H:%M:%S"),
                "ti_bs_return_window_days": 14,
                "ti_bs_is_returnable": 1,
            }
        ]
        self.assertFalse(_sale_lines_allow_transaction_messaging(lines, now=now))

    def test_non_returnable_uses_received_plus_grace(self):
        received = datetime(2026, 8, 1, tzinfo=timezone.utc)
        now = received + timedelta(days=_TRANSACTION_MESSAGING_GRACE_DAYS)
        lines = [
            {
                "ti_received_at": received.strftime("%Y-%m-%d %H:%M:%S"),
                "ti_bs_return_window_days": 30,
                "ti_bs_is_returnable": 0,
            }
        ]
        self.assertTrue(_sale_lines_allow_transaction_messaging(lines, now=now))
        self.assertFalse(
            _sale_lines_allow_transaction_messaging(
                lines, now=now + timedelta(days=1)
            )
        )


class MessagesPrivacyViolationTests(unittest.TestCase):
    @patch("chat._transaction_exception_allows", return_value=True)
    def test_transaction_exception_bypasses_messages_off(self, _exc):
        self.assertIsNone(
            _messages_privacy_violation("110-buyer", "110-seller")
        )

    @patch("chat._transaction_exception_allows", return_value=False)
    @patch("chat.connect")
    def test_messages_off_blocks_without_exception(self, mock_connect, _exc):
        db = MagicMock()
        mock_connect.return_value.__enter__.return_value = db
        db.execute.return_value = {
            "result": [
                {
                    "profile_personal_messages_off": 1,
                    "profile_personal_messages_receive_from": "everyone",
                    "profile_personal_messages_receive_types": None,
                }
            ]
        }
        msg = _messages_privacy_violation("110-a", "110-b")
        self.assertIn("messages turned off", msg)

    @patch("chat._transaction_exception_allows", return_value=False)
    @patch("chat._audience_allows", return_value=True)
    @patch("chat.connect")
    def test_messages_on_and_audience_ok(self, mock_connect, _aud, _exc):
        db = MagicMock()
        mock_connect.return_value.__enter__.return_value = db
        db.execute.return_value = {
            "result": [
                {
                    "profile_personal_messages_off": 0,
                    "profile_personal_messages_receive_from": "everyone",
                    "profile_personal_messages_receive_types": None,
                }
            ]
        }
        self.assertIsNone(_messages_privacy_violation("110-a", "110-b"))


if __name__ == "__main__":
    unittest.main()
