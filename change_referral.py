"""One-time referral change for users currently under the zero node."""

from flask import request
from flask_restful import Resource

from data_ec import connect
from network_connection import ZERO_NODE


def _resolve_referrer_uid(db, referred_by_value):
    """Resolve profile uid / user uid / email to a profile_personal_uid."""
    value = (referred_by_value or "").strip()
    if not value or value in ("null", "None"):
        return None

    if value[:3] == "110":
        return value

    if value[:3] == "100":
        uid_result = db.execute(
            """
            SELECT profile_personal_uid
            FROM every_circle.profile_personal
            WHERE profile_personal_user_id = %s
            LIMIT 1
            """,
            (value,),
        )
        rows = uid_result.get("result") or []
        return rows[0]["profile_personal_uid"] if rows else None

    if "@" in value:
        email_result = db.execute(
            """
            SELECT profile_personal_uid
            FROM every_circle.users
            LEFT JOIN every_circle.profile_personal
              ON user_uid = profile_personal_user_id
            WHERE user_email_id = %s
            LIMIT 1
            """,
            (value,),
        )
        rows = email_result.get("result") or []
        if rows and rows[0].get("profile_personal_uid"):
            return rows[0]["profile_personal_uid"]
        return None

    return None


def _profile_exists(db, profile_uid):
    result = db.execute(
        """
        SELECT profile_personal_uid, profile_personal_path, profile_personal_referred_by,
               profile_personal_first_name, profile_personal_last_name
        FROM every_circle.profile_personal
        WHERE profile_personal_uid = %s
        LIMIT 1
        """,
        (profile_uid,),
    )
    rows = result.get("result") or []
    return rows[0] if rows else None


def _compute_path_for_uid(db, profile_uid):
    """Same recursive path-from-zero-node query used at profile create."""
    path_query = """
        WITH RECURSIVE ReferralPath AS (
            SELECT
                profile_personal_uid AS user_id,
                profile_personal_referred_by,
                CAST(CONCAT("'", profile_personal_uid, "'") AS CHAR(255)) AS path
            FROM every_circle.profile_personal
            WHERE profile_personal_uid = %s

            UNION ALL

            SELECT
                p.profile_personal_uid,
                p.profile_personal_referred_by,
                CONCAT(r.path, ',', "'", p.profile_personal_uid, "'")
            FROM every_circle.profile_personal p
            JOIN ReferralPath r ON p.profile_personal_referred_by = r.user_id
            WHERE LOCATE(p.profile_personal_uid, r.path) = 0
        )
        SELECT path
        FROM ReferralPath
        WHERE user_id = %s
    """
    response = db.execute(path_query, (ZERO_NODE, profile_uid))
    rows = response.get("result") or []
    return rows[0]["path"] if rows else None


def _rewrite_paths_for_subtree(db, profile_uid, old_path, new_path):
    """Rewrite cached paths for the user and all descendants that shared old_path as prefix."""
    if old_path and new_path and old_path != new_path:
        db.execute(
            """
            UPDATE every_circle.profile_personal
            SET profile_personal_path = CONCAT(
                %s,
                SUBSTRING(profile_personal_path, %s)
            )
            WHERE profile_personal_path LIKE %s
            """,
            (new_path, len(old_path) + 1, f"{old_path}%"),
            cmd="post",
        )
        return

    # Fallback: recompute path for the user and every descendant via referred_by walk.
    descendants = db.execute(
        """
        WITH RECURSIVE Subtree AS (
            SELECT profile_personal_uid
            FROM every_circle.profile_personal
            WHERE profile_personal_uid = %s

            UNION ALL

            SELECT p.profile_personal_uid
            FROM every_circle.profile_personal p
            JOIN Subtree s ON p.profile_personal_referred_by = s.profile_personal_uid
        )
        SELECT profile_personal_uid FROM Subtree
        """,
        (profile_uid,),
    )
    for row in descendants.get("result") or []:
        uid = row["profile_personal_uid"]
        path = _compute_path_for_uid(db, uid)
        if path:
            db.update(
                "every_circle.profile_personal",
                {"profile_personal_uid": uid},
                {"profile_personal_path": path},
            )


def _is_in_subtree(db, candidate_uid, root_uid):
    """True if candidate_uid is root_uid or a descendant of root_uid."""
    if candidate_uid == root_uid:
        return True
    result = db.execute(
        """
        WITH RECURSIVE Subtree AS (
            SELECT profile_personal_uid
            FROM every_circle.profile_personal
            WHERE profile_personal_uid = %s

            UNION ALL

            SELECT p.profile_personal_uid
            FROM every_circle.profile_personal p
            JOIN Subtree s ON p.profile_personal_referred_by = s.profile_personal_uid
        )
        SELECT 1 AS found
        FROM Subtree
        WHERE profile_personal_uid = %s
        LIMIT 1
        """,
        (root_uid, candidate_uid),
    )
    return bool(result.get("result"))


class ChangeReferral(Resource):
    """
    POST /api/v1/change_referral

    Allow a user under the zero node (110-000001) to set a real referrer once.
    After success, referred_by is no longer the zero node, so the change cannot repeat.
    """

    def post(self):
        response = {}
        data = request.get_json(silent=True) or {}
        if not data and request.form:
            data = request.form.to_dict()

        from auth import (
            actor_may_use_uid,
            get_current_profile_id,
            jwt_auth_required,
        )

        profile_uid = get_current_profile_id()
        requested_uid = (
            data.get("profile_uid")
            or data.get("profile_personal_uid")
            or ""
        ).strip()

        if profile_uid:
            if requested_uid and not actor_may_use_uid(requested_uid):
                response["message"] = "profile_uid does not match the authenticated user"
                response["code"] = 403
                return response, 403
        elif requested_uid:
            if jwt_auth_required():
                response["message"] = "Missing or invalid authorization token"
                response["code"] = 401
                return response, 401
            profile_uid = requested_uid
        else:
            response["message"] = "Authentication required"
            response["code"] = 401
            return response, 401

        referred_by_raw = (
            data.get("profile_personal_referred_by")
            or data.get("referred_by")
            or ""
        ).strip()
        if not referred_by_raw:
            response["message"] = "profile_personal_referred_by is required"
            response["code"] = 400
            return response, 400

        try:
            with connect() as db:
                profile = _profile_exists(db, profile_uid)
                if not profile:
                    response["message"] = "Profile not found"
                    response["code"] = 404
                    return response, 404

                current_referred_by = (profile.get("profile_personal_referred_by") or "").strip()
                if current_referred_by != ZERO_NODE:
                    response["message"] = "Referral can only be set when connected to the zero node"
                    response["code"] = 400
                    return response, 400

                new_referrer_uid = _resolve_referrer_uid(db, referred_by_raw)
                if not new_referrer_uid:
                    response["message"] = "Referrer not found"
                    response["code"] = 404
                    return response, 404

                if new_referrer_uid == ZERO_NODE:
                    response["message"] = "Cannot set the zero node as referral"
                    response["code"] = 400
                    return response, 400

                if new_referrer_uid == profile_uid:
                    response["message"] = "Cannot refer yourself"
                    response["code"] = 400
                    return response, 400

                referrer = _profile_exists(db, new_referrer_uid)
                if not referrer:
                    response["message"] = "Referrer not found"
                    response["code"] = 404
                    return response, 404

                if _is_in_subtree(db, new_referrer_uid, profile_uid):
                    response["message"] = "Cannot create a referral cycle"
                    response["code"] = 400
                    return response, 400

                old_path = profile.get("profile_personal_path")

                db.update(
                    "every_circle.profile_personal",
                    {"profile_personal_uid": profile_uid},
                    {"profile_personal_referred_by": new_referrer_uid},
                )

                new_path = _compute_path_for_uid(db, profile_uid)
                if not new_path:
                    # Roll back referred_by if path cannot be built
                    db.update(
                        "every_circle.profile_personal",
                        {"profile_personal_uid": profile_uid},
                        {"profile_personal_referred_by": ZERO_NODE},
                    )
                    response["message"] = "Unable to compute referral path"
                    response["code"] = 400
                    return response, 400

                _rewrite_paths_for_subtree(db, profile_uid, old_path, new_path)

                try:
                    from notifications_service import notify_uid_if_away

                    first = (profile.get("profile_personal_first_name") or "").strip()
                    last = (profile.get("profile_personal_last_name") or "").strip()
                    name = f"{first} {last}".strip() or "Someone"
                    notify_uid_if_away(
                        new_referrer_uid,
                        f"{name} just added you as their referral and is now in your circle!",
                    )
                except Exception as notify_err:
                    print(f"Change referral notification error: {notify_err}")

                response["message"] = "Referral updated successfully"
                response["code"] = 200
                response["profile_personal_uid"] = profile_uid
                response["profile_personal_referred_by"] = new_referrer_uid
                response["profile_personal_path"] = new_path
                return response, 200

        except Exception as e:
            print(f"Error in ChangeReferral POST: {e}")
            response["message"] = "Internal server error"
            response["code"] = 500
            response["error"] = str(e)
            return response, 500
