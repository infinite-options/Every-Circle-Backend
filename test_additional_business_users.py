"""Unit tests for BusinessInfo._handle_additional_business_users summary."""

import unittest
from unittest.mock import MagicMock

from business_info import BusinessInfo


def _select_router(users_by_email=None, business_user_rows_by_key=None, existing_for_business=None):
    """Build a db.select side_effect keyed on table + where clause."""
    users_by_email = users_by_email or {}
    business_user_rows_by_key = business_user_rows_by_key or {}
    existing_for_business = existing_for_business or []

    def _select(table, where=None):
        where = where or {}
        if table == "every_circle.users":
            email = where.get("user_email_id")
            row = users_by_email.get(email)
            return {"result": [row] if row else []}
        if table == "every_circle.business_user":
            if "bu_user_id" in where:
                key = (where.get("bu_business_id"), where.get("bu_user_id"))
                row = business_user_rows_by_key.get(key)
                return {"result": [row] if row else []}
            return {"result": list(existing_for_business)}
        return {"result": []}

    return _select


OWNER_MEMBERSHIP = {"bu_user_id": "100-owner", "bu_role": "owner", "bu_uid": "700-owner"}


class HandleAdditionalBusinessUsersTests(unittest.TestCase):
    def setUp(self):
        self.resource = BusinessInfo()

    def _call(self, db, emails, roles, actor="100-owner", exclude_user_id="100-owner"):
        return self.resource._handle_additional_business_users(
            db, "200-biz", emails, roles, exclude_user_id=exclude_user_id, actor_user_id=actor
        )

    def test_email_without_account_is_reported_not_saved(self):
        db = MagicMock()
        db.select.side_effect = _select_router(
            users_by_email={}, existing_for_business=[OWNER_MEMBERSHIP]
        )  # nobody matches
        summary = self._call(db, ["ghost@example.com"], ["employee"])
        self.assertEqual(summary["not_found"], ["ghost@example.com"])
        self.assertEqual(summary["added"], [])
        db.insert.assert_not_called()

    def test_new_known_user_is_inserted_and_reported(self):
        db = MagicMock()
        db.select.side_effect = _select_router(
            users_by_email={"alex@example.com": {"user_uid": "100-alex"}},
            existing_for_business=[OWNER_MEMBERSHIP],
        )
        db.call.return_value = {"result": [{"new_id": "700-new"}]}
        summary = self._call(db, ["alex@example.com"], ["employee"])
        self.assertEqual(summary["added"], ["alex@example.com"])
        self.assertEqual(summary["not_found"], [])
        db.insert.assert_called_once()

    def test_non_owner_actor_cannot_add_member(self):
        db = MagicMock()
        db.select.side_effect = _select_router(
            users_by_email={"alex@example.com": {"user_uid": "100-alex"}},
            existing_for_business=[{"bu_user_id": "100-emp", "bu_role": "employee", "bu_uid": "700-emp"}],
        )
        db.call.return_value = {"result": [{"new_id": "700-new"}]}
        summary = self._call(db, ["alex@example.com"], ["employee"], actor="100-emp", exclude_user_id="100-emp")
        self.assertEqual(summary["forbidden"], ["alex@example.com"])
        self.assertEqual(summary["added"], [])
        db.insert.assert_not_called()

    def test_owner_target_role_is_protected(self):
        db = MagicMock()
        db.select.side_effect = _select_router(
            users_by_email={"co@example.com": {"user_uid": "100-co"}},
            business_user_rows_by_key={("200-biz", "100-co"): {"bu_role": "owner", "bu_uid": "700-co"}},
            existing_for_business=[OWNER_MEMBERSHIP],
        )
        summary = self._call(db, ["co@example.com"], ["employee"])
        self.assertEqual(summary["protected"], ["co@example.com"])
        db.update.assert_not_called()

    def test_owner_can_promote_employee(self):
        db = MagicMock()
        db.select.side_effect = _select_router(
            users_by_email={"emp@example.com": {"user_uid": "100-emp"}},
            business_user_rows_by_key={("200-biz", "100-emp"): {"bu_role": "employee", "bu_uid": "700-emp"}},
            existing_for_business=[OWNER_MEMBERSHIP],
        )
        summary = self._call(db, ["emp@example.com"], ["partner"])
        self.assertEqual(summary["updated"], ["emp@example.com"])
        db.update.assert_called_once_with("every_circle.business_user", {"bu_uid": "700-emp"}, {"bu_role": "partner"})

    def test_length_mismatch_processes_overlap_and_flags(self):
        db = MagicMock()
        db.select.side_effect = _select_router(
            users_by_email={"alex@example.com": {"user_uid": "100-alex"}},
            existing_for_business=[OWNER_MEMBERSHIP],
        )
        db.call.return_value = {"result": [{"new_id": "700-new"}]}
        summary = self._call(db, ["alex@example.com", "second@example.com"], ["employee"])
        self.assertTrue(summary.get("length_mismatch"))
        self.assertEqual(summary["added"], ["alex@example.com"])


if __name__ == "__main__":
    unittest.main()
