"""BusinessInfo.get hides owner-removed (soft-deleted) businesses but keeps
moderation take-downs visible."""

import unittest
from unittest.mock import MagicMock, patch

from business_info import BusinessInfo


def _db_with_business(row):
    db = MagicMock()
    db.select.return_value = {"result": [row]}
    db.execute.return_value = {"result": []}
    db.__enter__.return_value = db
    db.__exit__.return_value = False
    return db


class BusinessGetSoftDeleteTests(unittest.TestCase):
    def setUp(self):
        self.resource = BusinessInfo()

    def test_owner_removed_business_is_404(self):
        db = _db_with_business({"business_uid": "200-x", "business_is_active": 0, "business_moderated": 0})
        with patch("business_info.connect", return_value=db):
            body, code = self.resource.get("200-x")
        self.assertEqual(code, 404)

    def test_owner_removed_business_is_404_when_moderated_null(self):
        db = _db_with_business({"business_uid": "200-x", "business_is_active": 0})
        with patch("business_info.connect", return_value=db):
            body, code = self.resource.get("200-x")
        self.assertEqual(code, 404)

    def test_moderation_takedown_still_returned(self):
        # inactive but under moderation -> owner must still see it (banner)
        db = _db_with_business({"business_uid": "200-x", "business_is_active": 0, "business_moderated": 1})
        with patch("business_info.connect", return_value=db):
            body, code = self.resource.get("200-x")
        self.assertNotEqual(code, 404)

    def test_active_business_not_blocked_by_this_check(self):
        db = _db_with_business({"business_uid": "200-x", "business_is_active": 1, "business_moderated": 0})
        with patch("business_info.connect", return_value=db):
            body, code = self.resource.get("200-x")
        self.assertNotEqual(code, 404)


if __name__ == "__main__":
    unittest.main()
