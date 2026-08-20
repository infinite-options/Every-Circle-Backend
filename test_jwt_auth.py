"""Unit tests for JWT auth helpers and endpoints."""

import os
import unittest
from unittest.mock import MagicMock, patch

os.environ.setdefault("JWT_SECRET_KEY", "test-jwt-secret")
os.environ["JWT_AUTH_REQUIRED"] = "false"

from flask import Flask, jsonify
from flask_jwt_extended import JWTManager, create_access_token, jwt_required

from auth import (
    actor_may_use_uid,
    bind_actor,
    bind_user_uid,
    generate_password_salt,
    get_current_identity,
    get_current_profile_id,
    hash_password,
    issue_tokens,
    jwt_auth_required,
    path_requires_jwt,
    register_jwt_auth,
    require_actor_or_admin,
    require_admin,
    require_owned_business,
    verify_password,
    AuthLogin,
    AuthSalt,
)


class PasswordHashTests(unittest.TestCase):
    def test_hash_is_64_char_hex(self):
        salt = generate_password_salt()
        digest = hash_password("secret12", salt)
        self.assertEqual(len(salt), 64)
        self.assertEqual(len(digest), 64)

    def test_verify_plaintext_and_prehashed(self):
        salt = generate_password_salt()
        digest = hash_password("secret12", salt)
        self.assertTrue(verify_password("secret12", salt, digest))
        self.assertTrue(verify_password(digest, salt, digest))
        self.assertFalse(verify_password("wrong-password", salt, digest))

    def test_verify_rejects_missing_fields(self):
        self.assertFalse(verify_password("", "salt", "hash"))
        self.assertFalse(verify_password("pw", "", "hash"))
        self.assertFalse(verify_password("pw", "salt", ""))


class PathGateTests(unittest.TestCase):
    def test_auth_and_cron_are_public(self):
        self.assertFalse(path_requires_jwt("POST", "/api/v1/auth/login"))
        self.assertFalse(path_requires_jwt("POST", "/api/v1/auth/salt"))
        self.assertFalse(path_requires_jwt("POST", "/api/v1/auth/social"))
        self.assertFalse(path_requires_jwt("POST", "/api/v1/auth/logout"))
        self.assertFalse(path_requires_jwt("GET", "/api/v1/escrow_release_cron"))
        self.assertFalse(path_requires_jwt("OPTIONS", "/api/v1/transactions"))

    def test_writes_are_protected(self):
        self.assertTrue(path_requires_jwt("POST", "/api/v1/transactions"))
        self.assertTrue(path_requires_jwt("PUT", "/api/v1/userprofileinfo"))
        self.assertTrue(path_requires_jwt("DELETE", "/api/v1/blocked-users"))

    def test_sensitive_gets_are_protected(self):
        self.assertTrue(path_requires_jwt("GET", "/api/v1/orders/500-1"))
        self.assertTrue(path_requires_jwt("GET", "/api/v1/chat/messages/800-1"))
        self.assertTrue(path_requires_jwt("GET", "/api/v1/account-screen/personal/110-1"))
        self.assertTrue(path_requires_jwt("GET", "/api/v1/auth/me"))

    def test_public_profile_gets_remain_open(self):
        self.assertFalse(path_requires_jwt("GET", "/api/v1/userprofileinfo/110-000001"))
        self.assertFalse(path_requires_jwt("GET", "/api/v1/businessinfo/200-000001"))
        self.assertFalse(path_requires_jwt("GET", "/api/v1/business_map"))


class JwtEndpointTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["JWT_SECRET_KEY"] = "test-jwt-secret"
        self.app.config["TESTING"] = True
        jwt = JWTManager(self.app)
        register_jwt_auth(self.app, jwt)

        @self.app.route("/protected-echo", methods=["POST"])
        @jwt_required()
        def protected_echo():
            identity = get_current_identity()
            return jsonify({"profile_id": get_current_profile_id(), "user": identity})

        from flask_restful import Api

        api = Api(self.app)
        api.add_resource(AuthSalt, "/api/v1/auth/salt")
        api.add_resource(AuthLogin, "/api/v1/auth/login")
        self.client = self.app.test_client()

    def test_issue_tokens_and_read_claims(self):
        user = {
            "user_uid": "100-000099",
            "user_email_id": "jwt@example.com",
            "user_role": "ADMIN",
        }
        profile = {"profile_personal_uid": "110-000099"}
        with self.app.app_context():
            tokens = issue_tokens(user, profile)
        self.assertTrue(tokens["access_token"])
        self.assertTrue(tokens["refresh_token"])
        headers = {"Authorization": f"Bearer {tokens['access_token']}"}
        res = self.client.post("/protected-echo", headers=headers)
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertEqual(body["profile_id"], "110-000099")
        self.assertEqual(body["user"]["user_uid"], "100-000099")
        self.assertTrue(body["user"]["is_admin"])

    def test_protected_echo_rejects_missing_token(self):
        res = self.client.post("/protected-echo")
        self.assertEqual(res.status_code, 401)

    def _mock_user(self, **overrides):
        salt = generate_password_salt()
        user = {
            "user_uid": "100-000050",
            "user_email_id": "pat@example.com",
            "user_password_salt": salt,
            "user_password_hash": hash_password("hunter2", salt),
            "user_role": None,
        }
        user.update(overrides)
        return user

    def test_salt_and_login(self):
        user = self._mock_user()
        profile = {"profile_personal_uid": "110-000050"}

        db = MagicMock()
        db.select.side_effect = [
            {"result": [user]},
            {"result": [user]},
            {"result": [profile]},
        ]
        db.__enter__.return_value = db
        db.__exit__.return_value = False

        with patch("auth.connect", return_value=db):
            salt_res = self.client.post(
                "/api/v1/auth/salt", json={"email": "pat@example.com"}
            )
            self.assertEqual(salt_res.status_code, 200)
            self.assertEqual(
                salt_res.get_json()["result"][0]["password_salt"],
                user["user_password_salt"],
            )

            login_res = self.client.post(
                "/api/v1/auth/login",
                json={"email": "pat@example.com", "password": "hunter2"},
            )
        self.assertEqual(login_res.status_code, 200)
        result = login_res.get_json()["result"]
        self.assertEqual(result["user_uid"], "100-000050")
        self.assertEqual(result["profile_id"], "110-000050")
        self.assertIn("access_token", result)
        self.assertIn("refresh_token", result)

    def test_login_rejects_bad_password(self):
        user = self._mock_user()
        db = MagicMock()
        db.select.return_value = {"result": [user]}
        db.__enter__.return_value = db
        db.__exit__.return_value = False
        with patch("auth.connect", return_value=db):
            res = self.client.post(
                "/api/v1/auth/login",
                json={"email": "pat@example.com", "password": "nope"},
            )
        self.assertEqual(res.status_code, 401)

    def test_enforce_flag_blocks_writes(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            self.assertTrue(jwt_auth_required())
            res = self.client.post("/api/v1/transactions", json={})
            self.assertEqual(res.status_code, 401)

    def test_actor_may_use_uid_without_jwt_in_legacy_mode(self):
        with self.app.test_request_context("/"):
            self.assertTrue(actor_may_use_uid("110-000001"))

    def test_actor_may_use_uid_without_jwt_when_required(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            with self.app.test_request_context("/"):
                self.assertFalse(actor_may_use_uid("110-000001"))


_ALICE = {
    "user_uid": "100-alice",
    "profile_id": "110-alice",
    "email": "alice@example.com",
    "role": None,
    "is_admin": False,
}
_ALICE_ADMIN = {**_ALICE, "role": "ADMIN", "is_admin": True}


class ActorHelperTests(unittest.TestCase):
    """bind_actor / require_owned_business / require_admin without the full API app."""

    def test_bind_actor_legacy_accepts_mismatch(self):
        with patch("auth.jwt_auth_required", return_value=False):
            actor, error = bind_actor("110-bob")
        self.assertEqual(actor, "110-bob")
        self.assertIsNone(error)

    def test_bind_actor_legacy_returns_requested_even_with_jwt(self):
        with patch("auth.jwt_auth_required", return_value=False), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            actor, error = bind_actor("110-bob")
        self.assertEqual(actor, "110-bob")
        self.assertIsNone(error)

    def test_bind_actor_flag_on_uses_jwt_when_requested_omitted(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            actor, error = bind_actor(None)
        self.assertEqual(actor, "110-alice")
        self.assertIsNone(error)

    def test_bind_actor_flag_on_falls_back_to_user_uid(self):
        identity = {**_ALICE, "profile_id": None}
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=identity
        ):
            actor, error = bind_actor()
        self.assertEqual(actor, "100-alice")
        self.assertIsNone(error)

    def test_bind_actor_flag_on_matching_profile_proceeds(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            actor, error = bind_actor("110-alice")
        self.assertEqual(actor, "110-alice")
        self.assertIsNone(error)

    def test_bind_actor_flag_on_matching_user_uid_proceeds(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            actor, error = bind_actor("100-alice")
        self.assertEqual(actor, "100-alice")
        self.assertIsNone(error)

    def test_bind_actor_flag_on_mismatch_is_403(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            actor, error = bind_actor("110-bob")
        self.assertIsNone(actor)
        self.assertEqual(error["code"], 403)
        self.assertIn("does not match", error["message"])

    def test_actor_may_use_owned_profile_when_token_has_no_profile_id(self):
        identity = {**_ALICE, "profile_id": None}
        db = MagicMock()
        db.select.return_value = {"result": [{"profile_personal_uid": "110-alice"}]}
        db.__enter__.return_value = db
        db.__exit__.return_value = False
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=identity
        ), patch("auth.connect", return_value=db):
            self.assertTrue(actor_may_use_uid("110-alice"))
            self.assertFalse(actor_may_use_uid("110-bob"))

    def test_bind_actor_flag_on_without_jwt_is_401(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=None
        ):
            actor, error = bind_actor("110-alice")
        self.assertIsNone(actor)
        self.assertEqual(error["code"], 401)

    def test_bind_actor_owned_business_allowed_when_enabled(self):
        db = MagicMock()
        db.execute.return_value = {"result": [{"1": 1}]}
        db.__enter__.return_value = db
        db.__exit__.return_value = False
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ), patch("auth.connect", return_value=db):
            actor, error = bind_actor("200-owned", allow_business=True)
        self.assertEqual(actor, "200-owned")
        self.assertIsNone(error)

    def test_bind_actor_other_business_is_403(self):
        db = MagicMock()
        db.execute.return_value = {"result": []}
        db.__enter__.return_value = db
        db.__exit__.return_value = False
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ), patch("auth.connect", return_value=db):
            actor, error = bind_actor("200-other", allow_business=True)
        self.assertIsNone(actor)
        self.assertEqual(error["code"], 403)

    def test_bind_actor_rejects_business_when_not_allowed(self):
        db = MagicMock()
        db.execute.return_value = {"result": [{"1": 1}]}
        db.__enter__.return_value = db
        db.__exit__.return_value = False
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ), patch("auth.connect", return_value=db):
            actor, error = bind_actor("200-owned", allow_business=False)
        self.assertIsNone(actor)
        self.assertEqual(error["code"], 403)
        db.execute.assert_not_called()

    def test_bind_user_uid_legacy_returns_requested(self):
        with patch("auth.jwt_auth_required", return_value=False):
            uid, error = bind_user_uid("100-bob")
        self.assertEqual(uid, "100-bob")
        self.assertIsNone(error)

    def test_bind_user_uid_flag_on_returns_jwt_user_not_profile(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            uid, error = bind_user_uid("110-alice")
        self.assertEqual(uid, "100-alice")
        self.assertIsNone(error)

    def test_bind_user_uid_flag_on_omitted_uses_jwt_user(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            uid, error = bind_user_uid(None)
        self.assertEqual(uid, "100-alice")
        self.assertIsNone(error)

    def test_bind_user_uid_flag_on_mismatch_is_403(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            uid, error = bind_user_uid("100-bob")
        self.assertIsNone(uid)
        self.assertEqual(error["code"], 403)

    def test_require_owned_business_legacy_skips_check(self):
        with patch("auth.jwt_auth_required", return_value=False):
            uid, error = require_owned_business("200-other")
        self.assertEqual(uid, "200-other")
        self.assertIsNone(error)

    def test_require_owned_business_flag_on_owned(self):
        db = MagicMock()
        db.execute.return_value = {"result": [{"1": 1}]}
        db.__enter__.return_value = db
        db.__exit__.return_value = False
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ), patch("auth.connect", return_value=db):
            uid, error = require_owned_business("200-owned")
        self.assertEqual(uid, "200-owned")
        self.assertIsNone(error)

    def test_require_owned_business_flag_on_not_owned(self):
        db = MagicMock()
        db.execute.return_value = {"result": []}
        db.__enter__.return_value = db
        db.__exit__.return_value = False
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ), patch("auth.connect", return_value=db):
            uid, error = require_owned_business("200-other")
        self.assertIsNone(uid)
        self.assertEqual(error["code"], 403)

    def test_require_owned_business_missing_uid(self):
        uid, error = require_owned_business(None)
        self.assertIsNone(uid)
        self.assertEqual(error["code"], 400)

    def test_require_admin_legacy_does_not_enforce(self):
        with patch("auth.jwt_auth_required", return_value=False), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            admin_uid, error = require_admin()
        self.assertIsNone(admin_uid)
        self.assertIsNone(error)

    def test_require_admin_flag_on_admin_returns_jwt_actor(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE_ADMIN
        ):
            admin_uid, error = require_admin()
        self.assertEqual(admin_uid, "110-alice")
        self.assertIsNone(error)

    def test_require_admin_flag_on_non_admin_is_403(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            admin_uid, error = require_admin()
        self.assertIsNone(admin_uid)
        self.assertEqual(error["code"], 403)
        self.assertIn("Admin", error["message"])

    def test_require_actor_or_admin_legacy_allows_other(self):
        with patch("auth.jwt_auth_required", return_value=False):
            uid, error = require_actor_or_admin("110-bob")
        self.assertEqual(uid, "110-bob")
        self.assertIsNone(error)

    def test_require_actor_or_admin_flag_on_mismatch_is_403(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            uid, error = require_actor_or_admin("110-bob")
        self.assertIsNone(uid)
        self.assertEqual(error["code"], 403)

    def test_require_actor_or_admin_flag_on_matching_profile(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            uid, error = require_actor_or_admin("110-alice")
        self.assertEqual(uid, "110-alice")
        self.assertIsNone(error)

    def test_require_actor_or_admin_flag_on_admin_can_view_other(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE_ADMIN
        ):
            uid, error = require_actor_or_admin("110-bob")
        self.assertEqual(uid, "110-bob")
        self.assertIsNone(error)

    def test_require_actor_or_admin_owned_business(self):
        db = MagicMock()
        db.execute.return_value = {"result": [{"1": 1}]}
        db.__enter__.return_value = db
        db.__exit__.return_value = False
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ), patch("auth.connect", return_value=db):
            uid, error = require_actor_or_admin("200-owned", allow_business=True)
        self.assertEqual(uid, "200-owned")
        self.assertIsNone(error)


def _mock_db(select_row=None):
    db = MagicMock()
    db.select.return_value = {"result": [select_row] if select_row else []}
    db.update.return_value = {"code": 200, "message": "Success"}
    db.delete.return_value = {"code": 200}
    db.__enter__.return_value = db
    db.__exit__.return_value = False
    return db


class ProfileActorBindingTests(unittest.TestCase):
    """UserProfileInfo PUT/DELETE and UserInfo PUT bind to the JWT actor."""

    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["JWT_SECRET_KEY"] = "test-jwt-secret"
        self.app.config["TESTING"] = True
        jwt = JWTManager(self.app)
        register_jwt_auth(self.app, jwt)
        from flask_restful import Api
        from user_profile_info import UserProfileInfo
        from users import UserInfo

        api = Api(self.app)
        api.add_resource(
            UserProfileInfo,
            "/api/v1/userprofileinfo",
            "/api/v1/userprofileinfo/<string:uid>",
        )
        api.add_resource(UserInfo, "/userinfo", "/userinfo/<string:user_id>")
        self.client = self.app.test_client()

    def _alice_headers(self, profile_id="110-alice"):
        with self.app.app_context():
            token = create_access_token(
                identity="100-alice",
                additional_claims={
                    "user_uid": "100-alice",
                    "profile_id": profile_id,
                    "is_admin": False,
                },
            )
        return {"Authorization": f"Bearer {token}"}

    def test_put_profile_legacy_accepts_mismatched_profile_uid(self):
        db = _mock_db(
            {
                "profile_personal_uid": "110-bob",
                "profile_personal_user_id": "100-bob",
            }
        )
        with patch("user_profile_info.connect", return_value=db):
            res = self.client.put(
                "/api/v1/userprofileinfo",
                data={"profile_uid": "110-bob"},
            )
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertEqual(body.get("message"), "Profile updated successfully")
        db.select.assert_called()

    def test_put_profile_flag_on_mismatched_uid_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "user_profile_info.connect", return_value=db
        ):
            res = self.client.put(
                "/api/v1/userprofileinfo",
                data={"profile_uid": "110-bob"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        body = res.get_json()
        self.assertEqual(body.get("code"), 403)
        self.assertIn("does not match", body.get("message", ""))
        db.select.assert_not_called()

    def test_put_profile_flag_on_matching_uid_hits_db(self):
        db = _mock_db(
            {
                "profile_personal_uid": "110-alice",
                "profile_personal_user_id": "100-alice",
            }
        )
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "user_profile_info.connect", return_value=db
        ):
            res = self.client.put(
                "/api/v1/userprofileinfo",
                data={"profile_uid": "110-alice"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 200)
        self.assertEqual(
            res.get_json().get("message"), "Profile updated successfully"
        )
        db.select.assert_called()

    def test_delete_profile_flag_on_mismatched_uid_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "user_profile_info.connect", return_value=db
        ):
            res = self.client.delete(
                "/api/v1/userprofileinfo/110-bob",
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.select.assert_not_called()
        db.delete.assert_not_called()

    def test_delete_nested_listing_flag_on_other_profile_is_403(self):
        db = _mock_db(
            {
                "profile_expertise_uid": "150-1",
                "profile_expertise_profile_personal_id": "110-bob",
            }
        )
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "user_profile_info.connect", return_value=db
        ), patch("user_profile_info._delete_expertise_s3_assets"):
            res = self.client.delete(
                "/api/v1/userprofileinfo/150-1",
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.delete.assert_not_called()

    def test_delete_nested_listing_flag_on_own_profile_deletes(self):
        db = _mock_db(
            {
                "profile_expertise_uid": "150-1",
                "profile_expertise_profile_personal_id": "110-alice",
            }
        )
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "user_profile_info.connect", return_value=db
        ), patch("user_profile_info._delete_expertise_s3_assets"):
            res = self.client.delete(
                "/api/v1/userprofileinfo/150-1",
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 200)
        db.delete.assert_called()

    def test_put_userinfo_flag_on_mismatched_uid_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "users.connect", return_value=db
        ):
            res = self.client.put(
                "/userinfo",
                json={"user_uid": "100-bob", "user_email_id": "eve@example.com"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.update.assert_not_called()

    def test_put_userinfo_flag_on_matching_uid_hits_db(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "users.connect", return_value=db
        ):
            res = self.client.put(
                "/userinfo",
                json={"user_uid": "100-alice", "user_first_name": "Alice"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 200)
        db.update.assert_called_once()
        args, _kwargs = db.update.call_args
        self.assertEqual(args[1], {"user_uid": "100-alice"})
        self.assertNotIn("user_uid", args[2])


class BusinessActorBindingTests(unittest.TestCase):
    """Business create/update/delete, claim, restock/purchase bind to JWT actor."""

    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["JWT_SECRET_KEY"] = "test-jwt-secret"
        self.app.config["TESTING"] = True
        jwt = JWTManager(self.app)
        register_jwt_auth(self.app, jwt)
        from flask_restful import Api
        from business import (
            Business,
            BusinessClaim,
            BusinessServicePurchase,
            BusinessServiceRestock,
        )
        from business_info import BusinessInfo
        from business_services_options import BusinessServiceOptions
        from business_v3 import Business_v3
        from user_profile_info import ProfileExpertiseRestock

        api = Api(self.app)
        api.add_resource(
            BusinessInfo, "/api/v1/businessinfo", "/api/v1/businessinfo/<string:uid>"
        )
        api.add_resource(Business, "/business")
        api.add_resource(BusinessClaim, "/api/v1/business_claim")
        api.add_resource(BusinessServiceRestock, "/business/service/restock")
        api.add_resource(BusinessServicePurchase, "/business/service/purchase")
        api.add_resource(
            BusinessServiceOptions, "/api/business_service_options/<string:bs_uid>"
        )
        api.add_resource(ProfileExpertiseRestock, "/api/v1/profile-expertise/restock")
        api.add_resource(Business_v3, "/api/v3/business_v3")
        self.client = self.app.test_client()

    def _alice_headers(self, profile_id="110-alice"):
        with self.app.app_context():
            token = create_access_token(
                identity="100-alice",
                additional_claims={
                    "user_uid": "100-alice",
                    "profile_id": profile_id,
                    "is_admin": False,
                },
            )
        return {"Authorization": f"Bearer {token}"}

    def test_post_businessinfo_legacy_accepts_other_user_uid(self):
        db = _mock_db()
        with patch("business_info.connect", return_value=db):
            res = self.client.post(
                "/api/v1/businessinfo",
                data={"user_uid": "100-bob", "business_name": "Bob's"},
            )
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.get_json().get("message"), "User does not exist")

    def test_post_businessinfo_flag_on_mismatched_user_uid_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "business_info.connect", return_value=db
        ):
            res = self.client.post(
                "/api/v1/businessinfo",
                data={"user_uid": "100-bob", "business_name": "Bob's"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.select.assert_not_called()

    def test_post_businessinfo_flag_on_matching_user_uid_hits_db(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "business_info.connect", return_value=db
        ):
            res = self.client.post(
                "/api/v1/businessinfo",
                data={"user_uid": "100-alice", "business_name": "Alice Co"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 404)
        db.select.assert_called()

    def test_put_businessinfo_legacy_skips_ownership(self):
        db = _mock_db()
        with patch("business_info.connect", return_value=db):
            res = self.client.put(
                "/api/v1/businessinfo",
                data={"business_uid": "200-other"},
            )
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.get_json().get("message"), "Business does not exist")

    def test_put_businessinfo_flag_on_not_owned_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ), patch("business_info.connect", return_value=db):
            res = self.client.put(
                "/api/v1/businessinfo",
                data={"business_uid": "200-other"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.select.assert_not_called()

    def test_put_businessinfo_flag_on_owned_hits_db(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=True
        ), patch("business_info.connect", return_value=db):
            res = self.client.put(
                "/api/v1/businessinfo",
                data={"business_uid": "200-owned"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 404)
        db.select.assert_called()

    def test_put_business_v3_flag_on_not_owned_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ), patch("business_v3.connect", return_value=db):
            res = self.client.put(
                "/api/v3/business_v3",
                data={"business_uid": "200-other"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.select.assert_not_called()

    def test_delete_businessinfo_flag_on_not_owned_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ), patch("business_info.connect", return_value=db):
            res = self.client.delete(
                "/api/v1/businessinfo/200-other",
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.delete.assert_not_called()

    def test_post_business_flag_on_mismatched_user_uid_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "business.connect", return_value=db
        ):
            res = self.client.post(
                "/business",
                data={"user_uid": "100-bob", "business_name": "Bob's"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.select.assert_not_called()

    def test_claim_flag_on_mismatched_profile_uid_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "business.connect", return_value=db
        ):
            res = self.client.post(
                "/api/v1/business_claim",
                data={
                    "profile_uid": "110-bob",
                    "business_uid": "200-1",
                    "claim_role": "owner",
                },
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.select.assert_not_called()

    def test_claim_flag_on_matching_profile_hits_db(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "business.connect", return_value=db
        ):
            res = self.client.post(
                "/api/v1/business_claim",
                data={
                    "profile_uid": "110-alice",
                    "business_uid": "200-1",
                    "claim_role": "owner",
                },
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.get_json().get("message"), "Business not found")

    def test_restock_legacy_omitted_seller_id_accepted(self):
        db = _mock_db()
        with patch("business.connect", return_value=db):
            res = self.client.post(
                "/business/service/restock",
                json={"bs_uid": "250-1", "quantity": 1},
            )
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.get_json().get("message"), "Service not found")

    def test_restock_flag_on_omitted_seller_id_is_400(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.post(
                "/business/service/restock",
                json={"bs_uid": "250-1", "quantity": 1},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 400)
        self.assertIn("seller_id", res.get_json().get("message", ""))

    def test_restock_flag_on_other_business_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ), patch("business.connect", return_value=db):
            res = self.client.post(
                "/business/service/restock",
                json={"bs_uid": "250-1", "quantity": 1, "seller_id": "200-other"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.select.assert_not_called()

    def test_restock_flag_on_owned_business_allowed(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=True
        ), patch("business.connect", return_value=db):
            res = self.client.post(
                "/business/service/restock",
                json={"bs_uid": "250-1", "quantity": 1, "seller_id": "200-owned"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 404)
        db.select.assert_called()

    def test_purchase_flag_on_other_business_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ), patch("business.connect", return_value=db):
            res = self.client.post(
                "/business/service/purchase",
                json={"bs_uid": "250-1", "quantity": 1, "seller_id": "200-other"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.select.assert_not_called()

    def test_purchase_flag_on_owned_business_hits_db(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=True
        ), patch("business.connect", return_value=db):
            res = self.client.post(
                "/business/service/purchase",
                json={"bs_uid": "250-1", "quantity": 1, "seller_id": "200-owned"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 404)
        db.select.assert_called()

    def test_expertise_restock_flag_on_omitted_seller_id_is_400(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.post(
                "/api/v1/profile-expertise/restock",
                json={"profile_expertise_uid": "150-1", "quantity": 1},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 400)

    def test_expertise_restock_flag_on_mismatched_seller_is_403(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "user_profile_info.connect", return_value=db
        ):
            res = self.client.post(
                "/api/v1/profile-expertise/restock",
                json={
                    "profile_expertise_uid": "150-1",
                    "quantity": 1,
                    "seller_id": "110-bob",
                },
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.select.assert_not_called()

    def test_expertise_restock_flag_on_matching_seller_hits_db(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "user_profile_info.connect", return_value=db
        ):
            res = self.client.post(
                "/api/v1/profile-expertise/restock",
                json={
                    "profile_expertise_uid": "150-1",
                    "quantity": 1,
                    "seller_id": "110-alice",
                },
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 404)
        db.select.assert_called()

    def test_service_options_flag_on_not_owned_is_403(self):
        db = _mock_db({"bs_uid": "250-1", "bs_business_id": "200-other"})
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ), patch("business_services_options.connect", return_value=db):
            res = self.client.post(
                "/api/business_service_options/250-1",
                json={"choice_groups": []},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_service_options_flag_on_owned_hits_db(self):
        db = _mock_db({"bs_uid": "250-1", "bs_business_id": "200-owned"})
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=True
        ), patch("business_services_options.connect", return_value=db):
            res = self.client.post(
                "/api/business_service_options/250-1",
                json={"choice_groups": []},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 200)
        db.update.assert_called()

    def test_service_options_delete_flag_on_not_owned_is_403(self):
        db = _mock_db({"bs_uid": "250-1", "bs_business_id": "200-other"})
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ), patch("business_services_options.connect", return_value=db):
            res = self.client.delete(
                "/api/business_service_options/250-1",
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_google_places_flag_on_mismatched_user_uid_is_403(self):
        with patch("auth.jwt_auth_required", return_value=True), patch(
            "auth.get_current_identity", return_value=_ALICE
        ):
            from business_info import BusinessInfo

            body, code = BusinessInfo().get_google_places_info("place", "100-bob")
        self.assertEqual(code, 403)
        self.assertEqual(body.get("code"), 403)


class JwtRequiredGateTests(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["JWT_SECRET_KEY"] = "test-jwt-secret"
        jwt = JWTManager(self.app)
        register_jwt_auth(self.app, jwt)

        @self.app.route("/api/v1/transactions", methods=["POST"])
        def fake_tx():
            return jsonify({"ok": True})

        self.client = self.app.test_client()

    def test_required_mode_rejects_then_accepts_bearer(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            denied = self.client.post("/api/v1/transactions")
            self.assertEqual(denied.status_code, 401)
            with self.app.app_context():
                token = create_access_token(
                    identity="100-000001",
                    additional_claims={
                        "user_uid": "100-000001",
                        "profile_id": "110-000001",
                        "is_admin": False,
                    },
                )
            allowed = self.client.post(
                "/api/v1/transactions",
                headers={"Authorization": f"Bearer {token}"},
            )
        self.assertEqual(allowed.status_code, 200)


class CommerceActorBindingTests(unittest.TestCase):
    """Checkout, returns, seller confirm, fulfillment, and private commerce GETs."""

    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["JWT_SECRET_KEY"] = "test-jwt-secret"
        self.app.config["TESTING"] = True
        jwt = JWTManager(self.app)
        register_jwt_auth(self.app, jwt)
        from flask_restful import Api
        from account_screen import AccountScreenPersonal
        from transactions import (
            ConfirmReturnTransaction,
            DeclinedReturns,
            ReturnTransaction,
            SellerTransactions,
            Transactions,
        )
        from wallet_reconcile import WalletReconcile, WalletReconcileAll

        api = Api(self.app)
        api.add_resource(ReturnTransaction, "/api/v1/transactions/return")
        api.add_resource(
            ConfirmReturnTransaction, "/api/v1/transactions/return/confirm"
        )
        api.add_resource(DeclinedReturns, "/api/v1/transactions/returns/declined")
        api.add_resource(
            Transactions,
            "/api/v1/transactions",
            "/api/v1/transactions/<string:profile_id>",
        )
        api.add_resource(
            SellerTransactions, "/api/v1/transactions/seller/<string:profile_id>"
        )
        api.add_resource(
            AccountScreenPersonal,
            "/api/v1/account-screen/personal/<string:profile_id>",
        )
        api.add_resource(WalletReconcileAll, "/api/v1/wallet_reconcile")
        api.add_resource(
            WalletReconcile, "/api/v1/wallet_reconcile/<string:profile_id>"
        )
        self.client = self.app.test_client()

    def _alice_headers(self, profile_id="110-alice", is_admin=False):
        with self.app.app_context():
            token = create_access_token(
                identity="100-alice",
                additional_claims={
                    "user_uid": "100-alice",
                    "profile_id": profile_id,
                    "is_admin": is_admin,
                },
            )
        return {"Authorization": f"Bearer {token}"}

    def test_checkout_legacy_mismatch_reaches_validation(self):
        res = self.client.post(
            "/api/v1/transactions",
            json={"profile_id": "110-bob"},
        )
        self.assertEqual(res.status_code, 400)
        self.assertIn("Missing required fields", res.get_json().get("message", ""))

    def test_checkout_flag_on_mismatched_profile_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.post(
                "/api/v1/transactions",
                json={"profile_id": "110-bob"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        self.assertIn("does not match", res.get_json().get("message", ""))

    def test_checkout_flag_on_matching_profile_reaches_validation(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.post(
                "/api/v1/transactions",
                json={"profile_id": "110-alice"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 400)
        self.assertIn("Missing required fields", res.get_json().get("message", ""))

    def test_return_flag_on_mismatched_profile_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.post(
                "/api/v1/transactions/return",
                json={"profile_id": "110-bob", "transaction_uid": "500-1"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_return_flag_on_matching_profile_hits_db(self):
        db = _mock_db()
        db.execute.return_value = {"result": []}
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "transactions.connect", return_value=db
        ):
            res = self.client.post(
                "/api/v1/transactions/return",
                json={"profile_id": "110-alice", "transaction_uid": "500-1"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 404)
        self.assertEqual(res.get_json().get("message"), "Original transaction not found")

    def test_confirm_return_flag_on_other_business_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ):
            res = self.client.put(
                "/api/v1/transactions/return/confirm",
                json={
                    "transaction_uid": "500-1",
                    "seller_id": "200-other",
                },
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_confirm_return_flag_on_owned_business_hits_db(self):
        db = _mock_db()
        db.execute.return_value = {
            "result": [
                {
                    "transaction_uid": "500-1",
                    "transaction_profile_id": "110-alice",
                    "transaction_business_id": "200-owned",
                    "transaction_type": "sale",
                }
            ]
        }
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=True
        ), patch("transactions.connect", return_value=db):
            res = self.client.put(
                "/api/v1/transactions/return/confirm",
                json={
                    "transaction_uid": "500-1",
                    "seller_id": "200-owned",
                },
                headers=self._alice_headers(),
            )
        self.assertNotEqual(res.status_code, 403)
        db.execute.assert_called()

    def test_fulfillment_flag_on_body_profile_id_mismatch_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.put(
                "/api/v1/transactions",
                json={
                    "transaction_uid": "500-1",
                    "fulfillment_updates": [
                        {
                            "transaction_item_uid": "ti-1",
                            "fulfillment_status": "in_transit",
                        }
                    ],
                    "profile_id": "110-bob",
                },
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_fulfillment_flag_on_other_business_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ):
            res = self.client.put(
                "/api/v1/transactions",
                json={
                    "transaction_uid": "500-1",
                    "fulfillment_updates": [
                        {
                            "transaction_item_uid": "ti-1",
                            "fulfillment_status": "in_transit",
                        }
                    ],
                    "seller_id": "200-other",
                },
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_fulfillment_flag_on_owned_business_hits_db(self):
        db = _mock_db()
        db.execute.return_value = {
            "result": [
                {
                    "transaction_uid": "500-1",
                    "transaction_business_id": "200-owned",
                    "transaction_profile_id": "110-buyer",
                    "transaction_type": "sale",
                }
            ]
        }
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=True
        ), patch("transactions.connect", return_value=db):
            res = self.client.put(
                "/api/v1/transactions",
                json={
                    "transaction_uid": "500-1",
                    "fulfillment_updates": [
                        {
                            "transaction_item_uid": "ti-1",
                            "fulfillment_status": "in_transit",
                        }
                    ],
                    "seller_id": "200-owned",
                },
                headers=self._alice_headers(),
            )
        self.assertNotEqual(res.status_code, 403)
        db.execute.assert_called()

    def test_delivery_flag_on_mismatched_profile_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.put(
                "/api/v1/transactions",
                json={
                    "transaction_uid": "500-1",
                    "transaction_in_escrow": 1,
                    "delivery_verification_items": [
                        {"transaction_item_uid": "ti-1", "received_quantity": 1}
                    ],
                    "profile_id": "110-bob",
                },
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_bare_put_flag_on_non_party_is_403(self):
        db = _mock_db()
        db.execute.return_value = {
            "result": [
                {
                    "transaction_uid": "500-1",
                    "transaction_profile_id": "110-bob",
                    "transaction_business_id": "200-other",
                }
            ]
        }
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ), patch("transactions.connect", return_value=db):
            res = self.client.put(
                "/api/v1/transactions",
                json={"transaction_uid": "500-1", "transaction_in_escrow": 0},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.update.assert_not_called()

    def test_purchase_list_flag_on_other_profile_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.get(
                "/api/v1/transactions/110-bob",
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_account_screen_flag_on_other_profile_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.get(
                "/api/v1/account-screen/personal/110-bob",
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_wallet_reconcile_flag_on_other_profile_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.get(
                "/api/v1/wallet_reconcile/110-bob",
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_wallet_reconcile_all_flag_on_non_admin_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.get(
                "/api/v1/wallet_reconcile",
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_declined_returns_put_flag_on_non_seller_is_403(self):
        db = _mock_db()
        db.execute.return_value = {
            "result": [
                {
                    "transaction_uid": "500-1",
                    "transaction_profile_id": "110-bob",
                    "transaction_business_id": "200-other",
                    "transaction_type": "sale",
                }
            ]
        }
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "auth._user_owns_business", return_value=False
        ), patch("transactions.connect", return_value=db):
            res = self.client.put(
                "/api/v1/transactions/returns/declined",
                json={"transaction_uid": "500-1", "action": "decline"},
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_declined_returns_get_flag_on_non_admin_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.get(
                "/api/v1/transactions/returns/declined",
                headers=self._alice_headers(),
            )
        self.assertEqual(res.status_code, 403)
        self.assertIn("Admin", res.get_json().get("message", ""))


class AdminModerationAuthorizationTests(unittest.TestCase):
    """Moderation review, report dismiss, and admin queues require JWT admin."""

    def setUp(self):
        self.app = Flask(__name__)
        self.app.config["JWT_SECRET_KEY"] = "test-jwt-secret"
        self.app.config["TESTING"] = True
        jwt = JWTManager(self.app)
        register_jwt_auth(self.app, jwt)
        from flask_restful import Api
        from content_reports import ContentModerationReview, ContentReports

        api = Api(self.app)
        api.add_resource(ContentReports, "/api/v1/reports", "/api/v1/reports/<string:report_uid>")
        api.add_resource(
            ContentModerationReview,
            "/api/v1/moderation/offerings/review-queue",
            "/api/v1/moderation/offerings/<string:profile_expertise_uid>",
            "/api/v1/moderation/offerings/<string:profile_expertise_uid>/review",
        )
        self.client = self.app.test_client()

    def _headers(self, profile_id="110-alice", is_admin=False):
        with self.app.app_context():
            token = create_access_token(
                identity="100-alice",
                additional_claims={
                    "user_uid": "100-alice",
                    "profile_id": profile_id,
                    "is_admin": is_admin,
                },
            )
        return {"Authorization": f"Bearer {token}"}

    def test_report_list_flag_on_non_admin_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.get(
                "/api/v1/reports",
                headers=self._headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_report_dismiss_flag_on_non_admin_is_403(self):
        db = _mock_db({"report_uid": "900-1", "report_status": "pending"})
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "content_reports.connect", return_value=db
        ):
            res = self.client.put(
                "/api/v1/reports/900-1",
                json={"admin_uid": "110-bob"},
                headers=self._headers(),
            )
        self.assertEqual(res.status_code, 403)
        db.update.assert_not_called()

    def test_report_dismiss_legacy_accepts_body_admin_uid(self):
        db = _mock_db({"report_uid": "900-1", "report_status": "pending"})
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "false"}), patch(
            "content_reports.connect", return_value=db
        ):
            res = self.client.put(
                "/api/v1/reports/900-1",
                json={"admin_uid": "110-bob"},
            )
        self.assertEqual(res.status_code, 200)
        body = res.get_json()
        self.assertEqual(body["data"]["admin_uid"], "110-bob")

    def test_moderation_review_queue_flag_on_non_admin_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.get(
                "/api/v1/moderation/offerings/review-queue",
                headers=self._headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_moderation_review_put_flag_on_non_admin_is_403(self):
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}):
            res = self.client.put(
                "/api/v1/moderation/offerings/120-1/review",
                json={"action": "approve", "admin_uid": "110-bob"},
                headers=self._headers(),
            )
        self.assertEqual(res.status_code, 403)

    def test_moderation_review_put_flag_on_admin_uses_jwt_actor(self):
        db = _mock_db()
        with patch.dict(os.environ, {"JWT_AUTH_REQUIRED": "true"}), patch(
            "content_reports.connect", return_value=db
        ), patch(
            "content_reports.get_offering",
            return_value={"profile_expertise_uid": "120-1"},
        ), patch(
            "content_reports.approve_offering_review",
            return_value={"ok": True},
        ) as approve_mock:
            res = self.client.put(
                "/api/v1/moderation/offerings/120-1/review",
                json={"action": "approve", "admin_uid": "110-bob"},
                headers=self._headers(is_admin=True),
            )
        self.assertEqual(res.status_code, 200)
        approve_mock.assert_called_once()
        self.assertEqual(approve_mock.call_args[0][2], "110-alice")
        self.assertEqual(res.get_json()["data"]["admin_uid"], "110-alice")


if __name__ == "__main__":
    unittest.main()
