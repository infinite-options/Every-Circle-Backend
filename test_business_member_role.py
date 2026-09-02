"""Permission tests for BusinessMemberRole (PUT /api/v1/business_member_role)."""

import unittest
from unittest.mock import MagicMock, patch

from flask import Flask
from flask_restful import Api

from business import BusinessMemberRole


def _db_for(actor_rows, target_rows, business_exists=True):
    db = MagicMock()

    def _select(table, where=None):
        where = where or {}
        if table == "every_circle.business":
            return {"result": [{"business_uid": where.get("business_uid")}] if business_exists else []}
        if table == "every_circle.business_user":
            uid = where.get("bu_user_id")
            if uid == "100-actor":
                return {"result": list(actor_rows)}
            if uid == "100-target":
                return {"result": list(target_rows)}
            return {"result": []}
        return {"result": []}

    db.select.side_effect = _select
    db.__enter__.return_value = db
    db.__exit__.return_value = False
    return db


class BusinessMemberRoleTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["TESTING"] = True
        Api(self.app).add_resource(BusinessMemberRole, "/api/v1/business_member_role")
        self.client = self.app.test_client()

    def _put(self, db, **body):
        body.setdefault("business_uid", "200-biz")
        body.setdefault("target_user_id", "100-target")
        body.setdefault("user_uid", "100-actor")
        with patch("business.connect", return_value=db):
            return self.client.put("/api/v1/business_member_role", json=body)

    def test_owner_can_change_employee_role(self):
        db = _db_for([{"bu_role": "owner", "bu_uid": "700-a"}], [{"bu_role": "employee", "bu_uid": "700-t"}])
        res = self._put(db, role="admin")
        self.assertEqual(res.status_code, 200)
        db.update.assert_called_once_with("every_circle.business_user", {"bu_uid": "700-t"}, {"bu_role": "admin"})

    def test_owner_can_promote_employee_to_owner(self):
        db = _db_for([{"bu_role": "owner", "bu_uid": "700-a"}], [{"bu_role": "employee", "bu_uid": "700-t"}])
        res = self._put(db, role="owner")
        self.assertEqual(res.status_code, 200)
        db.update.assert_called_once()

    def test_owner_cannot_change_partner(self):
        db = _db_for([{"bu_role": "owner", "bu_uid": "700-a"}], [{"bu_role": "partner", "bu_uid": "700-t"}])
        res = self._put(db, role="employee")
        self.assertEqual(res.status_code, 403)
        db.update.assert_not_called()

    def test_owner_cannot_change_another_owner(self):
        db = _db_for([{"bu_role": "owner", "bu_uid": "700-a"}], [{"bu_role": "owner", "bu_uid": "700-t"}])
        res = self._put(db, role="employee")
        self.assertEqual(res.status_code, 403)
        db.update.assert_not_called()

    def test_partner_cannot_change_owner(self):
        db = _db_for([{"bu_role": "partner", "bu_uid": "700-a"}], [{"bu_role": "owner", "bu_uid": "700-t"}])
        res = self._put(db, role="employee")
        self.assertEqual(res.status_code, 403)
        db.update.assert_not_called()

    def test_partner_can_change_employee(self):
        db = _db_for([{"bu_role": "partner", "bu_uid": "700-a"}], [{"bu_role": "employee", "bu_uid": "700-t"}])
        res = self._put(db, role="admin")
        self.assertEqual(res.status_code, 200)
        db.update.assert_called_once()

    def test_employee_actor_is_forbidden(self):
        db = _db_for([{"bu_role": "employee", "bu_uid": "700-a"}], [{"bu_role": "other", "bu_uid": "700-t"}])
        res = self._put(db, role="admin")
        self.assertEqual(res.status_code, 403)
        db.update.assert_not_called()

    def test_non_member_actor_is_forbidden(self):
        db = _db_for([], [{"bu_role": "employee", "bu_uid": "700-t"}])
        res = self._put(db, role="admin")
        self.assertEqual(res.status_code, 403)

    def test_unknown_target_is_404(self):
        db = _db_for([{"bu_role": "owner", "bu_uid": "700-a"}], [])
        res = self._put(db, role="admin")
        self.assertEqual(res.status_code, 404)

    def test_invalid_role_is_400(self):
        db = _db_for([{"bu_role": "owner", "bu_uid": "700-a"}], [{"bu_role": "employee", "bu_uid": "700-t"}])
        res = self._put(db, role="ceo")
        self.assertEqual(res.status_code, 400)
        db.update.assert_not_called()


if __name__ == "__main__":
    unittest.main()
