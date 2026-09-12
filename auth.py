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
from datetime_utils import format_utc_iso, utc_now_str
from notifications_service import _to_e164, send_sms
from profile_status import is_permanently_deleted, is_soft_deleted

_HEX64 = re.compile(r"^[0-9a-fA-F]{64}$")

ACCESS_TOKEN_HOURS = int(os.getenv("JWT_ACCESS_TOKEN_HOURS", "1"))
REFRESH_TOKEN_DAYS = int(os.getenv("JWT_REFRESH_TOKEN_DAYS", "30"))

OTP_EXPIRES_SECONDS = 600
OTP_MAX_ATTEMPTS = 5
OTP_SEND_COOLDOWN_SECONDS = 60
OTP_MAX_SENDS_PER_HOUR = 5
OTP_SMS_TEMPLATE = "Your Every Circle code is {code}. Expires in 10 minutes."

_PUBLIC_PATHS = (
    "/api/v1/auth/salt",
    "/api/v1/auth/login",
    "/api/v1/auth/register",
    "/api/v1/auth/refresh",
    "/api/v1/auth/social",
    "/api/v1/auth/logout",
    "/api/v1/account/reactivate",
    "/stripe_key",
    "/decode",
    "/api/v1/lists_cron",
    "/api/v1/seller_hold_release_cron",
    "/api/v1/account_purge_cron",
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


def _normalize_phone(raw):
    """Normalize a US phone to E.164 via notifications_service._to_e164."""
    return _to_e164(raw)


def _generate_otp_code():
    return f"{secrets.randbelow(1_000_000):06d}"


def _otp_salt():
    return secrets.token_hex(32)


def _hash_otp(code, salt):
    return hashlib.sha256(f"{code}{salt}".encode("utf-8")).hexdigest()


def _verify_otp(code, salt, stored_hash):
    if not code or not salt or not stored_hash:
        return False
    return hmac.compare_digest(_hash_otp(str(code).strip(), salt), stored_hash)


def _dt_str(dt):
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _count_otp_sends_since(db, user_uid, since_str):
    res = db.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM every_circle.phone_otp_challenges
        WHERE user_uid = %s AND created_at >= %s
        """,
        (user_uid, since_str),
    )
    rows = (res or {}).get("result") or []
    if not rows:
        return 0
    try:
        return int(rows[0].get("cnt") or 0)
    except (TypeError, ValueError):
        return 0


def _otp_send_rate_limited(db, user_uid):
    """Return an error body/status when the user is rate-limited, else None."""
    now = datetime.now(timezone.utc)
    hour_ago = _dt_str(now - timedelta(hours=1))
    cooldown_ago = _dt_str(now - timedelta(seconds=OTP_SEND_COOLDOWN_SECONDS))

    if _count_otp_sends_since(db, user_uid, cooldown_ago) >= 1:
        return {
            "message": "Please wait before requesting another code",
            "code": 429,
        }, 429
    if _count_otp_sends_since(db, user_uid, hour_ago) >= OTP_MAX_SENDS_PER_HOUR:
        return {
            "message": "Too many verification codes requested. Try again later.",
            "code": 429,
        }, 429
    return None


def _invalidate_open_otp_challenges(db, user_uid):
    now_str = utc_now_str()
    db.execute(
        """
        UPDATE every_circle.phone_otp_challenges
        SET consumed_at = %s
        WHERE user_uid = %s AND consumed_at IS NULL
        """,
        (now_str, user_uid),
        cmd="post",
    )


def _latest_open_otp_challenge(db, user_uid, phone_e164):
    now_str = utc_now_str()
    res = db.execute(
        """
        SELECT *
        FROM every_circle.phone_otp_challenges
        WHERE user_uid = %s
          AND phone_e164 = %s
          AND consumed_at IS NULL
          AND expires_at > %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (user_uid, phone_e164, now_str),
    )
    rows = (res or {}).get("result") or []
    return rows[0] if rows else None


def _apply_verified_phone(db, user_uid, phone_e164):
    db.update(
        "every_circle.users",
        {"user_uid": user_uid},
        {
            "user_phone_number": phone_e164,
            "user_phone_verified": 1,
        },
    )
    profile = _profile_for_user(db, user_uid)
    if profile and profile.get("profile_personal_uid"):
        db.update(
            "every_circle.profile_personal",
            {"profile_personal_uid": profile["profile_personal_uid"]},
            {"profile_personal_phone_number": phone_e164},
        )
    return profile


def _phone_verified_flag(raw):
    try:
        return bool(int(raw)) if raw is not None else False
    except (TypeError, ValueError):
        return bool(raw)


def normalize_phone_for_storage(raw):
    """Return E.164 when possible, else stripped raw (or None if empty)."""
    if raw is None:
        return None
    stripped = str(raw).strip()
    if not stripped:
        return None
    return _to_e164(stripped) or stripped


def sync_user_phone_from_profile_edit(db, user_uid, new_phone_raw):
    """Keep ``users`` phone in sync with profile edit; clear verified on change.

    Returns ``{phone_number, phone_verified, phone_needs_verification}``.
    """
    store_value = normalize_phone_for_storage(new_phone_raw)
    new_e164 = _to_e164(store_value) if store_value else None

    user_res = db.select("every_circle.users", where={"user_uid": user_uid})
    users = (user_res or {}).get("result") or []
    if not users:
        return {
            "phone_number": store_value,
            "phone_verified": False,
            "phone_needs_verification": bool(store_value),
        }

    user = users[0]
    old_phone = user.get("user_phone_number")
    old_e164 = _to_e164(old_phone) if old_phone else None
    was_verified = _phone_verified_flag(user.get("user_phone_verified"))

    same_number = bool(new_e164 and old_e164 and new_e164 == old_e164)
    keep_verified = same_number and was_verified and bool(store_value)
    verified = 1 if keep_verified else 0

    db.update(
        "every_circle.users",
        {"user_uid": user_uid},
        {
            "user_phone_number": store_value,
            "user_phone_verified": verified,
        },
    )
    return {
        "phone_number": store_value,
        "phone_verified": bool(verified),
        "phone_needs_verification": bool(store_value) and not bool(verified),
    }


def _user_row_by_email(db, email):
    return db.select("every_circle.users", where={"user_email_id": email})


def _deleted_account_lookup(db, *, email=None, user_uid=None, social_id=None):
    """Return True when the identifier matches a prior account deletion."""
    email = _normalize_email(email) if email else None
    user_uid = str(user_uid or "").strip() or None
    social_id = str(social_id or "").strip() or None
    if not email and not user_uid and not social_id:
        return False

    conditions = []
    params = []
    if email:
        conditions.append("user_email_id = %s")
        params.append(email)
    if user_uid:
        conditions.append("user_uid = %s")
        params.append(user_uid)
    if social_id:
        conditions.append("user_social_id = %s")
        params.append(social_id)

    try:
        res = db.execute(
            f"""
            SELECT 1
            FROM every_circle.account_deletion_log
            WHERE {' OR '.join(conditions)}
            LIMIT 1
            """,
            tuple(params),
        )
        return bool(res.get("result"))
    except Exception:
        if not user_uid:
            return False
        res = db.execute(
            """
            SELECT 1
            FROM every_circle.account_deletion_log
            WHERE user_uid = %s
            LIMIT 1
            """,
            (user_uid,),
        )
        return bool(res.get("result"))


def _login_not_found_response(db, email):
    if _deleted_account_lookup(db, email=email):
        return {"message": "Account deleted", "code": 401}, 401
    return {"message": "Invalid email or password", "code": 401}, 401


def _deleted_user_response(db, *, email=None, user_uid=None, social_id=None):
    if _deleted_account_lookup(
        db, email=email, user_uid=user_uid, social_id=social_id
    ):
        return {"message": "Account deleted", "code": 401}, 401
    return None


def _pending_deletion_response(profile):
    """403 when soft-deleted account is still within the grace window."""
    return {
        "message": "Account scheduled for deletion",
        "code": 403,
        "pending_deletion": True,
        "purge_scheduled_at": format_utc_iso(
            profile.get("profile_personal_purge_scheduled_at")
        ),
        "can_reactivate": True,
    }, 403


def _soft_delete_gate(db, user):
    """
    Block login/salt/social for soft-deleted (or expired-grace) profiles.

    Returns (body, status) when blocked, else None.
    """
    if not user:
        return None
    profile = _profile_for_user(db, user.get("user_uid"))
    if not profile:
        return None
    if is_soft_deleted(profile):
        return _pending_deletion_response(profile)
    if is_permanently_deleted(profile):
        return {"message": "Account deleted", "code": 401}, 401
    return None


def _profile_for_user(db, user_uid):
    result = db.select(
        "every_circle.profile_personal",
        where={"profile_personal_user_id": user_uid},
    )
    rows = (result or {}).get("result") or []
    return rows[0] if rows else None


def _identity_payload(user, profile=None):
    profile_id = None
    phone_number = user.get("user_phone_number")
    if profile:
        profile_id = profile.get("profile_personal_uid")
        # Profile edit phone is the source of truth for display.
        if profile.get("profile_personal_phone_number") is not None:
            phone_number = profile.get("profile_personal_phone_number")

    role = (user.get("user_role") or "").strip().upper() or None
    phone_verified = _phone_verified_flag(user.get("user_phone_verified"))

    # Defensive: if profile/user phones diverge, treat as unverified.
    profile_e164 = _to_e164(phone_number) if phone_number else None
    user_e164 = _to_e164(user.get("user_phone_number")) if user.get(
        "user_phone_number"
    ) else None
    if phone_verified and profile_e164 and user_e164 and profile_e164 != user_e164:
        phone_verified = False
    elif phone_verified and phone_number and not profile_e164:
        # Non-E.164 / empty after change — require verify again.
        phone_verified = False

    return {
        "user_uid": user.get("user_uid"),
        "profile_id": profile_id,
        "email": user.get("user_email_id"),
        "role": role,
        "is_admin": role == "ADMIN",
        "phone_number": phone_number,
        "phone_verified": phone_verified,
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
                if user:
                    blocked = _soft_delete_gate(db, user)
                    if blocked:
                        return blocked
                if not user or not user.get("user_password_salt"):
                    deleted = _deleted_user_response(db, email=email)
                    if deleted:
                        return deleted
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
                    return _login_not_found_response(db, email)
                if not verify_password(
                    password,
                    user.get("user_password_salt"),
                    user.get("user_password_hash"),
                ):
                    return {"message": "Invalid email or password", "code": 401}, 401
                blocked = _soft_delete_gate(db, user)
                if blocked:
                    return blocked
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
                    profile = _profile_for_user(db, existing.get("user_uid"))
                    if profile and is_soft_deleted(profile):
                        return {
                            "message": "Reactivate existing account",
                            "code": 409,
                            "pending_deletion": True,
                            "purge_scheduled_at": format_utc_iso(
                                profile.get("profile_personal_purge_scheduled_at")
                            ),
                            "can_reactivate": True,
                            "user_uid": existing.get("user_uid"),
                        }, 409
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
                    deleted = _deleted_user_response(db, user_uid=user_uid)
                    if deleted:
                        return deleted
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
                    deleted = _deleted_user_response(db, user_uid=user_uid)
                    if deleted:
                        return deleted
                    return {"message": "User not found", "code": 404}, 404
                user = rows[0]
                profile = _profile_for_user(db, user_uid)
            payload = _identity_payload(user, profile)
            return {"message": "Success", "code": 200, "result": payload}, 200
        except Exception as e:
            print(f"AuthMe error: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500


class PhoneSendOtp(Resource):
    """POST { phone_number } — send a 6-digit SMS OTP to verify/change phone."""

    @jwt_required()
    def post(self):
        user_uid = get_current_user_uid() or get_jwt_identity()
        if not user_uid:
            return {"message": "Missing or invalid authorization token", "code": 401}, 401

        payload = request.get_json(silent=True) or {}
        phone_e164 = _normalize_phone(payload.get("phone_number"))
        if not phone_e164:
            return {"message": "Invalid US phone number", "code": 400}, 400

        try:
            with connect() as db:
                limited = _otp_send_rate_limited(db, user_uid)
                if limited:
                    return limited

                code = _generate_otp_code()
                salt = _otp_salt()
                now = datetime.now(timezone.utc)
                expires_at = now + timedelta(seconds=OTP_EXPIRES_SECONDS)
                created_str = _dt_str(now)
                expires_str = _dt_str(expires_at)

                _invalidate_open_otp_challenges(db, user_uid)
                insert = db.insert(
                    "every_circle.phone_otp_challenges",
                    {
                        "user_uid": user_uid,
                        "phone_e164": phone_e164,
                        "code_hash": _hash_otp(code, salt),
                        "code_salt": salt,
                        "expires_at": expires_str,
                        "attempts": 0,
                        "consumed_at": None,
                        "created_at": created_str,
                    },
                )
                if not insert or insert.get("code") not in (None, 200):
                    return {
                        "message": (insert or {}).get("message")
                        or "Failed to create verification challenge",
                        "code": 500,
                    }, 500

                sent = send_sms(phone_e164, OTP_SMS_TEMPLATE.format(code=code))
                if not sent:
                    _invalidate_open_otp_challenges(db, user_uid)
                    return {
                        "message": "Failed to send verification code",
                        "code": 503,
                    }, 503

            return {
                "message": "Success",
                "code": 200,
                "result": {
                    "phone_number": phone_e164,
                    "expires_in": OTP_EXPIRES_SECONDS,
                },
            }, 200
        except Exception as e:
            print(f"PhoneSendOtp error: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500


class PhoneVerifyOtp(Resource):
    """POST { phone_number, otp } — verify SMS OTP and persist phone on the user."""

    @jwt_required()
    def post(self):
        user_uid = get_current_user_uid() or get_jwt_identity()
        if not user_uid:
            return {"message": "Missing or invalid authorization token", "code": 401}, 401

        payload = request.get_json(silent=True) or {}
        phone_e164 = _normalize_phone(payload.get("phone_number"))
        otp = str(payload.get("otp") or "").strip()
        if not phone_e164:
            return {"message": "Invalid US phone number", "code": 400}, 400
        if not otp or not otp.isdigit() or len(otp) != 6:
            return {"message": "otp must be a 6-digit code", "code": 400}, 400

        try:
            with connect() as db:
                challenge = _latest_open_otp_challenge(db, user_uid, phone_e164)
                if not challenge:
                    return {
                        "message": "No valid verification code found",
                        "code": 400,
                    }, 400

                attempts = int(challenge.get("attempts") or 0)
                if attempts >= OTP_MAX_ATTEMPTS:
                    return {
                        "message": "Too many invalid attempts. Request a new code.",
                        "code": 429,
                    }, 429

                new_attempts = attempts + 1
                db.update(
                    "every_circle.phone_otp_challenges",
                    {"id": challenge["id"]},
                    {"attempts": new_attempts},
                )

                if not _verify_otp(
                    otp, challenge.get("code_salt"), challenge.get("code_hash")
                ):
                    if new_attempts >= OTP_MAX_ATTEMPTS:
                        return {
                            "message": "Too many invalid attempts. Request a new code.",
                            "code": 429,
                        }, 429
                    return {"message": "Invalid verification code", "code": 401}, 401

                now_str = utc_now_str()
                db.update(
                    "every_circle.phone_otp_challenges",
                    {"id": challenge["id"]},
                    {"consumed_at": now_str},
                )
                profile = _apply_verified_phone(db, user_uid, phone_e164)

                user_res = db.select(
                    "every_circle.users", where={"user_uid": user_uid}
                )
                users = (user_res or {}).get("result") or []
                if not users:
                    return {"message": "User not found", "code": 404}, 404
                identity = _identity_payload(users[0], profile)

            return {
                "message": "Success",
                "code": 200,
                "result": identity,
            }, 200
        except Exception as e:
            print(f"PhoneVerifyOtp error: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500


class AuthLogout(Resource):
    """Client should discard tokens. Endpoint exists so the FE has a single logout call."""

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
                    deleted = _deleted_user_response(
                        db,
                        email=social.get("email"),
                        social_id=social.get("social_id"),
                    )
                    if deleted:
                        return deleted
                    return {
                        "message": "No account for this social login. Sign up first.",
                        "code": 404,
                    }, 404
                blocked = _soft_delete_gate(db, user)
                if blocked:
                    return blocked
                profile = _profile_for_user(db, user["user_uid"])
            return _auth_success(user, profile)
        except Exception as e:
            print(f"AuthSocial error: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500
