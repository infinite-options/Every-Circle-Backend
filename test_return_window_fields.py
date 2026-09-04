import unittest
from datetime import datetime, timedelta, timezone

from transactions import line_return_window_api_fields


class ReturnWindowFieldsTests(unittest.TestCase):
    def test_non_returnable_line(self):
        fields = line_return_window_api_fields(
            {"ti_bs_is_returnable": 0, "ti_bs_return_window_days": 30}
        )
        self.assertFalse(fields["returnable"])
        self.assertEqual(fields["return_window_closes_label"], "Not returnable")
        self.assertIsNone(fields["return_window_expires_at"])

    def test_open_window_label_and_expiry(self):
        received_at = datetime(2025, 8, 14, 12, 0, 0, tzinfo=timezone.utc)
        now = received_at + timedelta(days=10)
        fields = line_return_window_api_fields(
            {
                "ti_bs_is_returnable": 1,
                "ti_bs_return_window_days": 30,
                "ti_received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
            },
            now=now,
            tz_name="America/Los_Angeles",
        )
        self.assertTrue(fields["returnable"])
        self.assertEqual(fields["return_window_days"], 30)
        self.assertFalse(fields["return_window_expired"])
        self.assertEqual(fields["return_window_expires_at"], "2025-09-13T12:00:00Z")
        self.assertEqual(fields["return_window_closes_label"], "Sep 13, 2025")

    def test_expired_window(self):
        received_at = datetime(2025, 8, 14, 12, 0, 0, tzinfo=timezone.utc)
        now = received_at + timedelta(days=31)
        fields = line_return_window_api_fields(
            {
                "ti_bs_is_returnable": 1,
                "ti_bs_return_window_days": 30,
                "ti_received_at": received_at.strftime("%Y-%m-%d %H:%M:%S"),
            },
            now=now,
            tz_name="America/Los_Angeles",
        )
        self.assertTrue(fields["return_window_expired"])
        self.assertEqual(fields["return_window_closes_label"], "Closed")

    def test_unreceived_line_is_dash(self):
        fields = line_return_window_api_fields(
            {"ti_bs_is_returnable": 1, "ti_bs_return_window_days": 30}
        )
        self.assertEqual(fields["return_window_closes_label"], "—")
        self.assertIsNone(fields["return_window_expires_at"])


if __name__ == "__main__":
    unittest.main()
