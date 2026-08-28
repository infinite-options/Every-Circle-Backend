"""Tests for account deletion tombstones, wallet freeze, and bounty filtering."""

import unittest
from unittest.mock import MagicMock, patch

from profile_status import (
    batch_deleted_status,
    build_connection_path_nodes,
    is_profile_deleted,
    stub_deleted_profile_row,
)
from wallet_service import wallet_row_is_frozen, _frozen_wallet_response


class ProfileStatusTests(unittest.TestCase):
    def test_is_profile_deleted_from_row(self):
        self.assertTrue(is_profile_deleted({"profile_personal_is_deleted": 1}))
        self.assertFalse(is_profile_deleted({"profile_personal_is_deleted": 0}))

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
        with patch("account_deletion.request") as mock_request:
            mock_request.get_json.return_value = {}
            body, code = resource.delete()
        self.assertEqual(code, 400)
        self.assertIn("confirm_deletion", body["message"])


if __name__ == "__main__":
    unittest.main()
