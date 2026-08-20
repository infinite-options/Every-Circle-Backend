"""JWT authentication for the Every Circle Flask API.

Account records already live in `every_circle.users` (email, SHA-256 hash + salt,
Google/Apple social ids). This module issues access/refresh tokens, resolves the
caller from the Bearer token, and optionally requires JWT on protected routes.

Set JWT_AUTH_REQUIRED=true after the frontend sends Authorization headers.
Until then, tokens are issued and used when present, but missing tokens do not
fail requests (legacy profile_id / user_uid query params still work).
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone

from flask import g, jsonify, request
from flask_jwt_extended import (
    create_access_token,
    create_refresh_token,
    get_jwt,
    get_jwt_identity,
    jwt_required,
    verify_jwt_in_request,
)
from flask_restful import Resource

from data_ec import connect

_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")

ACCESS_TOKEN_HOURS = int(os.getenv("JWT_ACCESS_TOKEN_HOURS", "1"))
REFRESH_TOKEN_DAYS = int(os.getenv("JWT_REFRESH_TOKEN_DAYS", "30"))

_PUBLIC_PATHS = (
    "/api/v1/auth/salt",
    "/api/v1/auth/login",
    "/api/v1/auth/register",
    "/api/v1/auth/refresh",
    "/api/v1/auth/social",
    "/api/v1/auth/logout",
    "/stripe_key",
    "/decode",
    "/api/v1/lists_cron",
    "/api/v1/escrow_release_cron",
    "/api/v1/seller_hold_release_cron",
)

_PROTECTED_GET_PREFIXES = (
    "/api/v1/orders",
    "/api/v1/transactions",
    "/api/v1/account-screen",
    "/api/v1/wallet_ledger",
    "/api/v1/wallet_reconcile",
    "/api/v1/chat",
    "/api/v1/blocked-users",
    "/api/v1/nearby",
    "/api/v1/ably",
    "/api/v1/moderation",
    "/api/v1/reports",
    "/api/transactionreceipt",
    "/api/bountyresults",
    "/api/business-bountyresults",
    "/userinfo",
    "/business-budget",
    "/api/v1/businessrevenue",
    "/api/v1/circles",
    "/api/profilewishresponse",
    "/api/profileexpertiseresponse",
    "/api/v1/auth/me",
    "/api/v1/auth/logout",
)


def jwt_auth_required():
    return os.getenv("JWT_AUTH_REQUIRED", "false").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _normalize_email(email):
    return (email or "").strip().lower()


def generate_password_salt():
    return secrets.token_hex(32)


def hash_password(password, salt):
    """Match the existing frontend/account-service scheme: SHA-256(password + salt) hex."""
    value = f"{password}{salt}"
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def verify_password(password, salt, stored_hash):
    if not password or not salt or not stored_hash:
        return False
    candidate = (
        password.lower()
        if _HEX64.match(password)
        else hash_password(password, salt)
    )
    return hmac.compare_digest(candidate, stored_hash.lower())


def _user_row_by_email(db, email):
    return db.select("every_circle.users", where={"user_email_id": email})


def _profile_for_user(db, user_uid):
    result = db.select(
        "every_circle.profile_personal",
        where={"profile_personal_user_id": user_uid},
    )
    rows = (result or {}).get("result") or []
    return rows[0] if rows else None


def _identity_payload(user, profile=None):
    profile_id = None
    if profile:
        profile_id = profile.get("profile_personal_uid")
    role = (user.get("user_role") or "").strip().upper() or None
    return {
        "user_uid": user.get("user_uid"),
        "profile_id": profile_id,
        "email": user.get("user_email_id"),
        "role": role,
        "is_admin": role == "ADMIN",
    }


def issue_tokens(user, profile=None):
    identity = _identity_payload(user, profile)
    user_uid = str(identity["user_uid"])
    claims = {
        "user_uid": user_uid,
        "profile_id": identity["profile_id"],
        "email": identity["email"],
        "role": identity["role"],
        "is_admin": identity["is_admin"],
    }
    access = create_access_token(identity=user_uid, additional_claims=claims)
    refresh = create_refresh_token(identity=user_uid, additional_claims=claims)
    return {
        "access_token": access,
        "refresh_token": refresh,
        "token_type": "Bearer",
        "expires_in": ACCESS_TOKEN_HOURS * 3600,
        "user": identity,
    }


def _verify_optional():
    try:
        verify_jwt_in_request(optional=True)
        return True
    except RuntimeError:
        return False
    except Exception:
        return False


def get_current_identity():
    """Return JWT claims for the caller, or None."""
    try:
        cached = getattr(g, "_jwt_identity", None)
    except RuntimeError:
        return None
    if cached is not None:
        return cached or None

    if not _verify_optional():
        try:
            g._jwt_identity = {}
        except RuntimeError:
            return None
        return None

    try:
        user_uid = get_jwt_identity()
        claims = get_jwt() or {}
    except Exception:
        g._jwt_identity = {}
        return None

    if not user_uid:
        g._jwt_identity = {}
        return None

    identity = {
        "user_uid": str(claims.get("user_uid") or user_uid),
        "profile_id": claims.get("profile_id"),
        "email": claims.get("email"),
        "role": claims.get("role"),
        "is_admin": bool(claims.get("is_admin")),
    }
    if identity["profile_id"]:
        identity["profile_id"] = str(identity["profile_id"])
    g._jwt_identity = identity
    return identity


def get_current_profile_id():
    identity = get_current_identity()
    if not identity:
        return None
    if identity.get("profile_id"):
        return str(identity["profile_id"])
    user_uid = identity.get("user_uid")
    if not user_uid:
        return None
    try:
        with connect() as db:
            profile = _profile_for_user(db, user_uid)
        if profile and profile.get("profile_personal_uid"):
            return str(profile["profile_personal_uid"])
    except Exception:
        pass
    return str(user_uid)


def get_current_user_uid():
    identity = get_current_identity()
    if identity and identity.get("user_uid"):
        return str(identity["user_uid"])
    return None


def current_user_is_admin():
    identity = get_current_identity()
    return bool(identity and identity.get("is_admin"))


def _actor_error(message, code=403):
    return {"message": message, "code": code}


def _user_owns_business(user_uid, business_uid):
    """True when `business_user` links this user to the business."""
    if not user_uid or not business_uid:
        return False
    try:
        with connect() as db:
            owned = db.execute(
                """
                SELECT 1
                FROM every_circle.business_user
                WHERE bu_business_id = %s AND bu_user_id = %s
                LIMIT 1
                """,
                (str(business_uid), str(user_uid)),
            )
        return bool((owned or {}).get("result"))
    except Exception:
        return False


def actor_may_use_uid(uid, *, allow_business=True):
    """True when uid is the JWT user, their personal profile, or (if no JWT) always in legacy mode.

    When JWT_AUTH_REQUIRED is on and there is no JWT, returns False (the request
    gate should already have 401'd). If the token has no ``profile_id`` (until
    refresh after create-profile), a ``110-*`` uid that belongs to the JWT user
    in ``profile_personal`` is still allowed. Owned ``200-*`` businesses are
    allowed only when allow_business is True.
    """
    if not uid:
        return False
    identity = get_current_identity()
    if not identity:
        return not jwt_auth_required()
    uid = str(uid)
    if uid == identity.get("user_uid") or uid == identity.get("profile_id"):
        return True
    # profile_id may be absent on the token until refresh after create-profile
    if uid.startswith("110-") and not identity.get("profile_id") and identity.get(
        "user_uid"
    ):
        try:
            with connect() as db:
                profile = _profile_for_user(db, identity["user_uid"])
            owned = profile.get("profile_personal_uid") if profile else None
            if owned and str(owned) == uid:
                return True
        except Exception:
            pass
    if allow_business and uid.startswith("200-") and identity.get("user_uid"):
        return _user_owns_business(identity["user_uid"], uid)
    return False


def bind_actor(requested=None, *, allow_business=False):
    """Resolve the write actor from the JWT when JWT_AUTH_REQUIRED is on.

    Flag off → return ``requested`` unchanged (legacy Postman / old clients).
    Flag on → require JWT; if ``requested`` is set it must pass
    ``actor_may_use_uid`` (403 on mismatch); otherwise use JWT ``profile_id``
    or ``user_uid``.

    Returns ``(actor_uid, error)``. On denial ``error`` is a dict with
    ``message`` and ``code`` so handlers can ``return error, error["code"]``
    (typically 403).
    """
    requested = str(requested).strip() if requested else None
    if not requested:
        requested = None

    if not jwt_auth_required():
        return requested, None

    identity = get_current_identity()
    if not identity:
        return None, _actor_error("Missing or invalid authorization token", 401)

    if requested:
        if not actor_may_use_uid(requested, allow_business=allow_business):
            return None, _actor_error(
                "Actor id does not match the authenticated user"
            )
        return requested, None

    actor_uid = identity.get("profile_id") or identity.get("user_uid")
    if not actor_uid:
        return None, _actor_error("Missing or invalid authorization token", 401)
    return str(actor_uid), None


def bind_user_uid(requested=None):
    """Bind a ``users.user_uid`` write actor.

    Flag off → return ``requested`` unchanged (legacy).
    Flag on → same mismatch rules as ``bind_actor``, then always return the
    JWT ``user_uid`` so a matching profile_id in the body cannot be persisted
    as the account id.
    """
    actor, error = bind_actor(requested)
    if error:
        return None, error
    if jwt_auth_required():
        user_uid = get_current_user_uid()
        if not user_uid:
            return None, _actor_error("Missing or invalid authorization token", 401)
        return str(user_uid), None
    return actor, None


def require_owned_business(business_uid):
    """Require a ``business_user`` row for the JWT user when the flag is on.

    Flag off → return ``business_uid`` without checking ownership.
    Flag on → JWT ``user_uid`` must own the business.

    Returns ``(business_uid, error)`` in the same shape as ``bind_actor``.
    """
    business_uid = str(business_uid).strip() if business_uid else None
    if not business_uid:
        return None, _actor_error("business_uid is required", 400)

    if not jwt_auth_required():
        return business_uid, None

    identity = get_current_identity()
    if not identity or not identity.get("user_uid"):
        return None, _actor_error("Missing or invalid authorization token", 401)
    if not _user_owns_business(identity["user_uid"], business_uid):
        return None, _actor_error("Not authorized for this business")
    return business_uid, None


def require_admin():
    """Require ``user_role=ADMIN`` on the JWT when the flag is on.

    Flag off → ``(None, None)`` so handlers may still read body ``admin_uid``.
    Flag on → ignore client ``admin_uid`` / ``viewer_is_admin``; return the
    JWT ``profile_id`` or ``user_uid`` as the admin actor, or 403.

    Returns ``(admin_uid, error)`` in the same shape as ``bind_actor``.
    """
    if not jwt_auth_required():
        return None, None

    identity = get_current_identity()
    if not identity:
        return None, _actor_error("Missing or invalid authorization token", 401)
    if not identity.get("is_admin"):
        return None, _actor_error("Admin privileges required")
    admin_uid = identity.get("profile_id") or identity.get("user_uid")
    if not admin_uid:
        return None, _actor_error("Admin privileges required")
    return str(admin_uid), None


def require_actor_or_admin(requested_uid, *, allow_business=True):
    """GET/private data: flag on → JWT actor, owned business, or admin.

    Flag off → return ``requested_uid`` without checking (legacy).
    Flag on → admin may use any uid; otherwise ``actor_may_use_uid``.

    Returns ``(requested_uid, error)`` in the same shape as ``bind_actor``.
    """
    requested_uid = str(requested_uid).strip() if requested_uid else None
    if not requested_uid:
        return None, _actor_error("Actor id is required", 400)

    if not jwt_auth_required():
        return requested_uid, None

    identity = get_current_identity()
    if not identity:
        return None, _actor_error("Missing or invalid authorization token", 401)
    if identity.get("is_admin"):
        return requested_uid, None
    if actor_may_use_uid(requested_uid, allow_business=allow_business):
        return requested_uid, None
    return None, _actor_error("Actor id does not match the authenticated user")


def _path_is_public(path):
    for prefix in _PUBLIC_PATHS:
        if path == prefix or path.startswith(prefix + "/"):
            return True
    return False


def _path_is_protected_get(path):
    for prefix in _PROTECTED_GET_PREFIXES:
        if path == prefix or path.startswith(prefix + "/"):
            return True
    return False


def path_requires_jwt(method, path):
    """Used by tests and the request gate."""
    method = (method or "GET").upper()
    path = path or ""
    if method == "OPTIONS":
        return False
    if _path_is_public(path):
        return False
    if method in ("POST", "PUT", "PATCH", "DELETE"):
        return True
    if method == "GET" and _path_is_protected_get(path):
        return True
    return False


def _unauthorized(message="Missing or invalid authorization token"):
    return jsonify({"message": message, "code": 401}), 401


def register_jwt_auth(app, jwt_manager):
    app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(hours=ACCESS_TOKEN_HOURS)
    app.config["JWT_REFRESH_TOKEN_EXPIRES"] = timedelta(days=REFRESH_TOKEN_DAYS)
    app.config["JWT_ERROR_MESSAGE_KEY"] = "message"

    @jwt_manager.unauthorized_loader
    def _missing_token(reason):
        return jsonify({"message": reason or "Missing authorization token", "code": 401}), 401

    @jwt_manager.invalid_token_loader
    def _invalid_token(reason):
        return jsonify({"message": reason or "Invalid authorization token", "code": 401}), 401

    @jwt_manager.expired_token_loader
    def _expired_token(_header, _payload):
        return jsonify({"message": "Token has expired", "code": 401}), 401

    @app.before_request
    def _enforce_jwt():
        if request.method == "OPTIONS":
            return None
        if not jwt_auth_required():
            _verify_optional()
            return None
        if _path_is_public(request.path):
            return None
        if not path_requires_jwt(request.method, request.path):
            _verify_optional()
            return None
        try:
            verify_jwt_in_request()
        except Exception:
            return _unauthorized()
        return None


def _load_user_for_login(db, email):
    result = _user_row_by_email(db, email)
    rows = (result or {}).get("result") or []
    return rows[0] if rows else None


def _auth_success(user, profile=None, extra=None):
    tokens = issue_tokens(user, profile)
    body = {
        "message": "Success",
        "code": 200,
        "result": {
            **tokens["user"],
            "user_uid": tokens["user"]["user_uid"],
            "user_email_id": tokens["user"]["email"],
            "access_token": tokens["access_token"],
            "refresh_token": tokens["refresh_token"],
            "token_type": tokens["token_type"],
            "expires_in": tokens["expires_in"],
        },
    }
    if extra:
        body["result"].update(extra)
    return body, 200


class AuthSalt(Resource):
    """POST { email } → { result: [{ password_salt }] } — same shape as AccountSalt."""

    def post(self):
        payload = request.get_json(silent=True) or {}
        email = _normalize_email(payload.get("email"))
        if not email:
            return {"message": "email is required", "code": 400}, 400
        try:
            with connect() as db:
                user = _load_user_for_login(db, email)
            if not user or not user.get("user_password_salt"):
                return {"message": "Email is not valid", "code": 404}, 404
            return {
                "message": "Success",
                "code": 200,
                "result": [{"password_salt": user["user_password_salt"]}],
            }, 200
        except Exception as e:
            print(f"AuthSalt error: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500


class AuthLogin(Resource):
    """POST { email, password } — password may be plaintext or SHA-256 hex."""

    def post(self):
        payload = request.get_json(silent=True) or {}
        email = _normalize_email(payload.get("email"))
        password = payload.get("password") or ""
        if not email or not password:
            return {"message": "email and password are required", "code": 400}, 400
        try:
            with connect() as db:
                user = _load_user_for_login(db, email)
                if not user:
                    return {"message": "Invalid email or password", "code": 401}, 401
                if not verify_password(
                    password,
                    user.get("user_password_salt"),
                    user.get("user_password_hash"),
                ):
                    return {"message": "Invalid email or password", "code": 401}, 401
                profile = _profile_for_user(db, user["user_uid"])
            return _auth_success(user, profile)
        except Exception as e:
            print(f"AuthLogin error: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500


class AuthRegister(Resource):
    """POST { email, password, first_name?, last_name?, phone_number? }."""

    def post(self):
        payload = request.get_json(silent=True) or {}
        email = _normalize_email(payload.get("email"))
        password = payload.get("password") or ""
        if not email or not password:
            return {"message": "email and password are required", "code": 400}, 400
        if len(password) < 6:
            return {"message": "password must be at least 6 characters", "code": 400}, 400
        try:
            with connect() as db:
                existing = _load_user_for_login(db, email)
                if existing:
                    return {
                        "message": "User already exists",
                        "code": 409,
                        "user_uid": existing.get("user_uid"),
                    }, 409

                uid_result = db.call(procedure="new_user_uid")
                rows = (uid_result or {}).get("result") or []
                if not rows or not rows[0].get("new_id"):
                    return {"message": "Failed to allocate user_uid", "code": 500}, 500
                user_uid = rows[0]["new_id"]
                salt = generate_password_salt()
                user = {
                    "user_uid": user_uid,
                    "user_email_id": email,
                    "user_first_name": (payload.get("first_name") or "").strip() or None,
                    "user_last_name": (payload.get("last_name") or "").strip() or None,
                    "user_phone_number": (payload.get("phone_number") or "").strip()
                    or None,
                    "user_password_salt": salt,
                    "user_password_hash": hash_password(password, salt),
                    "user_created_date": datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S"
                    ),
                }
                insert = db.insert("every_circle.users", user)
                if not insert or insert.get("code") not in (None, 200):
                    return {
                        "message": (insert or {}).get("message") or "Failed to create user",
                        "code": 500,
                    }, 500
            return _auth_success(user, None)
        except Exception as e:
            print(f"AuthRegister error: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500


class AuthRefresh(Resource):
    """POST with Authorization: Bearer <refresh_token>."""

    @jwt_required(refresh=True)
    def post(self):
        try:
            user_uid = get_jwt_identity()
            with connect() as db:
                result = db.select("every_circle.users", where={"user_uid": user_uid})
                rows = (result or {}).get("result") or []
                if not rows:
                    return {"message": "User not found", "code": 401}, 401
                user = rows[0]
                profile = _profile_for_user(db, user_uid)
            return _auth_success(user, profile)
        except Exception as e:
            print(f"AuthRefresh error: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500


class AuthMe(Resource):
    @jwt_required()
    def get(self):
        identity = get_current_identity() or {}
        user_uid = identity.get("user_uid") or get_jwt_identity()
        try:
            with connect() as db:
                result = db.select("every_circle.users", where={"user_uid": user_uid})
                rows = (result or {}).get("result") or []
                if not rows:
                    return {"message": "User not found", "code": 404}, 404
                user = rows[0]
                profile = _profile_for_user(db, user_uid)
            payload = _identity_payload(user, profile)
            return {"message": "Success", "code": 200, "result": payload}, 200
        except Exception as e:
            print(f"AuthMe error: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500


class AuthLogout(Resource):
    """Client should discard tokens. Endpoint exists so the FE has a single logout call."""

    @jwt_required(optional=True)
    def post(self):
        return {"message": "Logged out", "code": 200}, 200


def _http_get_json(url, timeout=8):
    req = urllib.request.Request(url, headers={"User-Agent": "every-circle-auth"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _verify_google_token(id_token=None, access_token=None):
    tokens = []
    if id_token:
        tokens.append(("id_token", id_token))
    if access_token:
        tokens.append(("access_token", access_token))
    last_error = None
    for param, value in tokens:
        url = "https://oauth2.googleapis.com/tokeninfo?" + urllib.parse.urlencode(
            {param: value}
        )
        try:
            info = _http_get_json(url)
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, ValueError) as e:
            last_error = e
            continue
        email = _normalize_email(info.get("email"))
        if not email:
            continue
        audience = os.getenv("GOOGLE_CLIENT_ID")
        token_aud = info.get("aud")
        if audience and token_aud and token_aud != audience:
            continue
        return {
            "email": email,
            "social_id": info.get("sub") or info.get("user_id"),
            "first_name": info.get("given_name"),
            "last_name": info.get("family_name"),
        }
    if last_error:
        print(f"Google token verify failed: {last_error}")
    return None


def _verify_apple_token(id_token):
    if not id_token:
        return None
    try:
        import jwt as pyjwt

        jwks_client = pyjwt.PyJWKClient("https://appleid.apple.com/auth/keys")
        signing_key = jwks_client.get_signing_key_from_jwt(id_token)
        decode_kwargs = {
            "algorithms": ["RS256"],
            "issuer": "https://appleid.apple.com",
        }
        audience = os.getenv("APPLE_CLIENT_ID")
        if audience:
            decode_kwargs["audience"] = audience
        else:
            decode_kwargs["options"] = {"verify_aud": False}
        info = pyjwt.decode(id_token, signing_key.key, **decode_kwargs)
    except Exception as e:
        print(f"Apple token verify failed: {e}")
        return None
    email = _normalize_email(info.get("email"))
    if not email:
        return None
    return {
        "email": email,
        "social_id": info.get("sub"),
        "first_name": None,
        "last_name": None,
    }


def _find_social_user(db, email, social_id):
    user = _load_user_for_login(db, email) if email else None
    if user:
        return user
    if social_id:
        result = db.select("every_circle.users", where={"user_social_id": social_id})
        rows = (result or {}).get("result") or []
        if rows:
            return rows[0]
    return None


class AuthSocial(Resource):
    """POST { provider: google|apple, id_token?, access_token? } — verifies the IdP token."""

    def post(self):
        payload = request.get_json(silent=True) or {}
        provider = (payload.get("provider") or "").strip().lower()
        id_token = payload.get("id_token") or payload.get("google_auth_token")
        access_token = payload.get("access_token")
        if provider not in ("google", "apple"):
            return {"message": "provider must be google or apple", "code": 400}, 400
        try:
            if provider == "google":
                social = _verify_google_token(id_token=id_token, access_token=access_token)
            else:
                social = _verify_apple_token(id_token)
            if not social:
                return {"message": "Invalid social token", "code": 401}, 401
            with connect() as db:
                user = _find_social_user(db, social.get("email"), social.get("social_id"))
                if not user:
                    return {
                        "message": "No account for this social login. Sign up first.",
                        "code": 404,
                    }, 404
                profile = _profile_for_user(db, user["user_uid"])
            return _auth_success(user, profile)
        except Exception as e:
            print(f"AuthSocial error: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500
