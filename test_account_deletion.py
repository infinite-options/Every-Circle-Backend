"""Tests for account soft-delete, reactivate, purge cron, and pending_deletion auth."""

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from flask import Flask

from profile_status import (
    batch_deleted_status,
    build_connection_path_nodes,
    is_permanently_deleted,
    is_profile_deleted,
    is_soft_deleted,
    stub_deleted_profile_row,
)
from wallet_service import wallet_row_is_frozen, _frozen_wallet_response


def _future_purge():
    return (datetime.now(timezone.utc) + timedelta(days=30)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _past_purge():
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _app_request(json_body=None):
    app = Flask(__name__)
    return app.test_request_context(json=json_body or {})


class ProfileStatusTests(unittest.TestCase):
    def test_is_profile_deleted_from_row(self):
        self.assertTrue(is_profile_deleted({"profile_personal_is_deleted": 1}))
        self.assertFalse(is_profile_deleted({"profile_personal_is_deleted": 0}))

    def test_is_soft_deleted(self):
        row = {
            "profile_personal_is_deleted": 1,
            "profile_personal_user_id": "100-1",
            "profile_personal_purge_scheduled_at": _future_purge(),
        }
        self.assertTrue(is_soft_deleted(row))
        self.assertFalse(is_permanently_deleted(row))
        self.assertTrue(is_profile_deleted(row))

    def test_is_permanently_deleted_null_user(self):
        row = {
            "profile_personal_is_deleted": 1,
            "profile_personal_user_id": None,
            "profile_personal_purge_scheduled_at": None,
        }
        self.assertFalse(is_soft_deleted(row))
        self.assertTrue(is_permanently_deleted(row))
        self.assertTrue(is_profile_deleted(row))

    def test_is_permanently_deleted_purge_past(self):
        row = {
            "profile_personal_is_deleted": 1,
            "profile_personal_user_id": "100-1",
            "profile_personal_purge_scheduled_at": _past_purge(),
        }
        self.assertFalse(is_soft_deleted(row))
        self.assertTrue(is_permanently_deleted(row))

    def test_stub_deleted_profile_row(self):
        stub = stub_deleted_profile_row("110-000001")
        self.assertEqual(stub["profile_personal_uid"], "110-000001")
        self.assertTrue(stub["is_deleted"])

    def test_batch_deleted_status(self):
        db = MagicMock()
        db.execute.return_value = {
            "result": [
                {"profile_personal_uid": "110-a", "profile_personal_is_deleted": 1},
                {"profile_personal_uid": "110-b", "profile_personal_is_deleted": 0},
            ]
        }
        status = batch_deleted_status(db, ["110-a", "110-b", "110-missing"])
        self.assertTrue(status["110-a"])
        self.assertFalse(status["110-b"])
        self.assertFalse(status["110-missing"])

    def test_build_connection_path_nodes(self):
        db = MagicMock()
        db.execute.side_effect = [
            {
                "result": [
                    {"profile_personal_uid": "110-a", "profile_personal_is_deleted": 0},
                    {"profile_personal_uid": "110-b", "profile_personal_is_deleted": 1},
                ]
            },
            {
                "result": [
                    {
                        "profile_personal_uid": "110-a",
                        "profile_personal_first_name": "Alice",
                        "profile_personal_last_name": "Smith",
                        "profile_personal_image": "http://img/a",
                        "profile_personal_image_is_public": 1,
                    }
                ]
            },
        ]
        nodes = build_connection_path_nodes(db, "110-a,110-b")
        self.assertEqual(len(nodes), 2)
        self.assertFalse(nodes[0]["is_deleted"])
        self.assertEqual(nodes[0]["profile_personal_first_name"], "Alice")
        self.assertTrue(nodes[1]["is_deleted"])


class WalletFreezeTests(unittest.TestCase):
    def test_wallet_row_is_frozen(self):
        self.assertTrue(wallet_row_is_frozen({"wallet_is_frozen": 1}))
        self.assertFalse(wallet_row_is_frozen({"wallet_is_frozen": 0}))
        self.assertFalse(wallet_row_is_frozen(None))

    def test_frozen_wallet_response(self):
        resp = _frozen_wallet_response("110-1")
        self.assertEqual(resp["code"], 403)
        self.assertTrue(resp["skipped"])


class SoftDeleteTests(unittest.TestCase):
    def test_soft_delete_sets_flags_without_scrubbing(self):
        from account_deletion import soft_delete_user_account

        db = MagicMock()
        profile = {
            "profile_personal_uid": "110-1",
            "profile_personal_user_id": "100-1",
            "profile_personal_is_deleted": 0,
            "profile_personal_first_name": "Ada",
        }
        db.select.side_effect = [
            {"result": [profile]},
            {"result": [{"user_uid": "100-1", "user_email_id": "a@b.com"}]},
        ]
        db.execute.return_value = {"code": 200}

        with patch(
            "account_deletion.freeze_wallet", return_value=True
        ) as freeze, patch(
            "account_deletion._hide_public_listings"
        ) as hide:
            confirmation = soft_delete_user_account(db, "100-1", profile_uid="110-1")

        self.assertFalse(confirmation["personal_data_deleted"])
        self.assertTrue(confirmation["reactivation_available"])
        self.assertEqual(confirmation["grace_days"], 30)
        self.assertTrue(confirmation["wallet_frozen"])
        freeze.assert_called_once_with(db, "110-1")
        hide.assert_called_once_with(db, "110-1")

        # Soft-delete UPDATE on profile_personal (not anonymize / user delete)
        update_sql = db.execute.call_args_list[0][0][0]
        self.assertIn("profile_personal_is_deleted = 1", update_sql)
        self.assertIn("profile_personal_purge_scheduled_at", update_sql)

        # Must not delete users or scrub name during soft-delete
        for call in db.execute.call_args_list:
            sql = call[0][0] if call[0] else ""
            self.assertNotIn("DELETE FROM every_circle.users", sql)
            self.assertNotIn("profile_personal_first_name = NULL", sql)

    def test_soft_delete_rejects_already_deleted(self):
        from account_deletion import AccountAlreadyDeletedError, soft_delete_user_account

        db = MagicMock()
        db.select.return_value = {
            "result": [
                {
                    "profile_personal_uid": "110-1",
                    "profile_personal_user_id": "100-1",
                    "profile_personal_is_deleted": 1,
                    "profile_personal_purge_scheduled_at": _future_purge(),
                }
            ]
        }
        with self.assertRaises(AccountAlreadyDeletedError):
            soft_delete_user_account(db, "100-1")


class PermanentPurgeTests(unittest.TestCase):
    def test_permanently_purge_anonymizes_and_logs(self):
        from account_deletion import permanently_purge_account

        db = MagicMock()
        profile = {
            "profile_personal_uid": "110-1",
            "profile_personal_user_id": "100-1",
            "profile_personal_is_deleted": 1,
            "profile_personal_purge_scheduled_at": _past_purge(),
        }
        db.select.side_effect = lambda *args, **kwargs: {
            "result": (
                [profile]
                if "profile_personal" in str(args)
                or kwargs.get("where", {}).get("profile_personal_uid")
                or kwargs.get("where", {}).get("profile_personal_user_id")
                else [{"user_uid": "100-1", "user_email_id": "a@b.com"}]
            )
        }
        # Simpler: patch _resolve and helpers
        with patch(
            "account_deletion._resolve_profile_row", return_value=profile
        ), patch(
            "account_deletion._delete_profile_personal_s3_assets"
        ), patch(
            "account_deletion._delete_listing_s3_assets"
        ), patch(
            "account_deletion._delete_listings"
        ), patch(
            "account_deletion._delete_profile_children"
        ), patch(
            "account_deletion._delete_conversations_and_messages"
        ), patch(
            "account_deletion._delete_views_and_blocks"
        ), patch(
            "account_deletion._delete_owned_circles"
        ), patch(
            "account_deletion._delete_ratings_on_profile"
        ), patch(
            "account_deletion._delete_feedback_for_user"
        ), patch(
            "account_deletion._remove_business_membership"
        ), patch(
            "account_deletion.freeze_wallet", return_value=True
        ), patch(
            "account_deletion._anonymize_profile_tombstone"
        ) as anonymize, patch(
            "account_deletion._delete_auth_user"
        ) as delete_user, patch(
            "account_deletion._log_account_deletion"
        ) as log_del:
            db.select.return_value = {
                "result": [{"user_uid": "100-1", "user_email_id": "a@b.com"}]
            }
            confirmation = permanently_purge_account(db, "100-1", profile_uid="110-1")

        self.assertTrue(confirmation["personal_data_deleted"])
        anonymize.assert_called_once()
        delete_user.assert_called_once_with(db, "100-1")
        log_del.assert_called_once()


class ReactivateTests(unittest.TestCase):
    def test_reactivate_requires_confirmation(self):
        from account_deletion import AccountReactivate

        resource = AccountReactivate()
        with _app_request({"email": "a@b.com", "password": "secret1"}):
            body, code = resource.post()
        self.assertEqual(code, 400)
        self.assertIn("confirm_reactivation", body["message"])

    def test_reactivate_restores_flags_and_unfreezes(self):
        from account_deletion import reactivate_user_account

        db = MagicMock()
        user = {
            "user_uid": "100-1",
            "user_email_id": "a@b.com",
            "user_password_salt": "salt",
            "user_password_hash": "hash",
        }
        profile = {
            "profile_personal_uid": "110-1",
            "profile_personal_user_id": "100-1",
            "profile_personal_is_deleted": 1,
            "profile_personal_purge_scheduled_at": _future_purge(),
        }
        with patch(
            "auth._load_user_for_login", return_value=user
        ), patch(
            "auth._profile_for_user", return_value=profile
        ), patch(
            "auth.verify_password", return_value=True
        ), patch(
            "account_deletion.unfreeze_wallet", return_value=True
        ) as unfreeze, patch(
            "account_deletion.get_wallet_row",
            return_value={"wallet_is_frozen": 0},
        ):
            result = reactivate_user_account(db, "a@b.com", "secret1")

        self.assertEqual(result["confirmation"]["profile_personal_uid"], "110-1")
        self.assertFalse(result["confirmation"]["wallet_frozen"])
        unfreeze.assert_called_once_with(db, "110-1")
        update_sql = db.execute.call_args[0][0]
        self.assertIn("profile_personal_is_deleted = 0", update_sql)

    def test_reactivate_after_purge_window_fails(self):
        from account_deletion import (
            AccountPurgeWindowExpiredError,
            reactivate_user_account,
        )

        db = MagicMock()
        user = {
            "user_uid": "100-1",
            "user_email_id": "a@b.com",
            "user_password_salt": "salt",
            "user_password_hash": "hash",
        }
        profile = {
            "profile_personal_uid": "110-1",
            "profile_personal_user_id": "100-1",
            "profile_personal_is_deleted": 1,
            "profile_personal_purge_scheduled_at": _past_purge(),
        }
        with patch(
            "auth._load_user_for_login", return_value=user
        ), patch(
            "auth._profile_for_user", return_value=profile
        ):
            with self.assertRaises(AccountPurgeWindowExpiredError):
                reactivate_user_account(db, "a@b.com", "secret1")


class AuthPendingDeletionTests(unittest.TestCase):
    def _soft_profile(self):
        return {
            "profile_personal_uid": "110-1",
            "profile_personal_user_id": "100-1",
            "profile_personal_is_deleted": 1,
            "profile_personal_purge_scheduled_at": _future_purge(),
        }

    def test_login_soft_deleted_returns_pending_deletion(self):
        from auth import AuthLogin

        resource = AuthLogin()
        user = {
            "user_uid": "100-1",
            "user_email_id": "a@b.com",
            "user_password_salt": "s",
            "user_password_hash": "h",
        }
        with _app_request({"email": "a@b.com", "password": "secret1"}), patch(
            "auth.connect"
        ) as mock_connect, patch(
            "auth._load_user_for_login", return_value=user
        ), patch(
            "auth.verify_password", return_value=True
        ), patch(
            "auth._profile_for_user", return_value=self._soft_profile()
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            body, code = resource.post()

        self.assertEqual(code, 403)
        self.assertTrue(body["pending_deletion"])
        self.assertTrue(body["can_reactivate"])
        self.assertIn("purge_scheduled_at", body)

    def test_register_soft_deleted_email_returns_409(self):
        from auth import AuthRegister

        resource = AuthRegister()
        user = {"user_uid": "100-1", "user_email_id": "a@b.com"}
        with _app_request({"email": "a@b.com", "password": "secret1"}), patch(
            "auth.connect"
        ) as mock_connect, patch(
            "auth._load_user_for_login", return_value=user
        ), patch(
            "auth._profile_for_user", return_value=self._soft_profile()
        ):
            mock_connect.return_value.__enter__.return_value = MagicMock()
            body, code = resource.post()

        self.assertEqual(code, 409)
        self.assertEqual(body["message"], "Reactivate existing account")
        self.assertTrue(body["pending_deletion"])


class AccountPurgeCronTests(unittest.TestCase):
    def test_purge_cron_calls_permanent_purge(self):
        from account_purge import AccountPurgeJob

        db = MagicMock()
        db.execute.return_value = {
            "result": [
                {
                    "profile_personal_uid": "110-1",
                    "profile_personal_user_id": "100-1",
                    "profile_personal_purge_scheduled_at": _past_purge(),
                }
            ]
        }
        with patch("account_purge.connect") as mock_connect, patch(
            "account_purge.permanently_purge_account",
            return_value={
                "deleted_at": "2026-09-02T00:00:00Z",
                "profile_personal_uid": "110-1",
                "personal_data_deleted": True,
            },
        ) as purge:
            mock_connect.return_value.__enter__.return_value = db
            response = AccountPurgeJob.get()

        self.assertEqual(response["purged_count"], 1)
        self.assertEqual(response["failed_count"], 0)
        purge.assert_called_once_with(db, "100-1", profile_uid="110-1")


class BountyTombstoneFilterTests(unittest.TestCase):
    def test_middle_path_nodes_filters_deleted_with_db(self):
        from transactions import _middle_path_nodes

        db = MagicMock()
        db.execute.return_value = {
            "result": [
                {"profile_personal_uid": "110-mid", "profile_personal_is_deleted": 1},
            ]
        }
        middle = _middle_path_nodes("110-a,110-mid,110-b", {"110-a", "110-b"}, db=db)
        self.assertEqual(middle, [])

    def test_profile_id_is_deleted_tombstone(self):
        from transactions import _profile_id_is_deleted_tombstone

        db = MagicMock()
        db.execute.return_value = {
            "result": [{"profile_personal_uid": "110-del", "profile_personal_is_deleted": 1}]
        }
        self.assertTrue(_profile_id_is_deleted_tombstone(db, "110-del"))
        self.assertFalse(_profile_id_is_deleted_tombstone(db, "ec-wallet"))

    def test_seeking_plan_skips_deleted_recommender(self):
        from transactions import CHARITY_PROFILE_ID, _plan_seeking_bounty_shares

        db = MagicMock()
        db.execute.return_value = {
            "result": [
                {"profile_personal_uid": "110-rec", "profile_personal_is_deleted": 1},
            ]
        }
        buyer, recommender, node = "110-buyer", "110-rec", "110-node"
        known, network = _plan_seeking_bounty_shares(
            50.0,
            buyer,
            recommender,
            f"{recommender},{node},{buyer}",
            seller_id="110-seller",
            db=db,
        )
        known_ids = {p["tb_profile_id"] for p in known}
        self.assertNotIn(recommender, known_ids)
        network_map = {p["tb_profile_id"]: p for p in network}
        self.assertIn(CHARITY_PROFILE_ID, network_map)


class AccountDeleteEndpointTests(unittest.TestCase):
    def test_delete_requires_confirmation(self):
        from account_deletion import AccountDelete

        resource = AccountDelete()
        with _app_request({}):
            body, code = resource.delete()
        self.assertEqual(code, 400)
        self.assertIn("confirm_deletion", body["message"])


if __name__ == "__main__":
    unittest.main()
