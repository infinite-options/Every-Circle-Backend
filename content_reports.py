import json
import traceback
from datetime import datetime

from flask import request
from flask_restful import Resource

from data_ec import connect
from moderation import (
    MODERATED_PENDING_REVIEW,
    TARGET_TYPE_OFFERING,
    TARGET_TYPE_SEEKING,
    TARGET_TYPE_USER,
    acknowledge_offering_takedown,
    acknowledge_user_takedown,
    acknowledge_wish_takedown,
    apply_content_takedown_if_threshold,
    approve_offering_review,
    approve_user_review,
    approve_wish_review,
    build_offering_moderation_metadata,
    build_user_moderation_metadata,
    build_wish_moderation_metadata,
    get_offering,
    get_offering_owner_profile_uid,
    get_user,
    get_wish,
    get_wish_owner_profile_uid,
    reject_offering_review,
    reject_user_review,
    reject_wish_review,
    resolve_content_target,
)

_CONTENT_REPORTS_TABLE = "every_circle.content_reports"


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _serialize_datetime(value):
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    return value


def _db_write_succeeded(res):
    return bool(res) and res.get("code") == 200


def _format_report_row(row):
    if not row:
        return row
    formatted = dict(row)
    for key, value in formatted.items():
        formatted[key] = _serialize_datetime(value)
    return formatted


def _parse_snapshot(raw):
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw
    return raw


def _new_report_uid(db):
    uid_res = db.call(procedure="every_circle.new_content_reports_uid")
    rows = uid_res.get("result") or []
    if not rows:
        return None
    return rows[0].get("new_id")


def _report_exists(db, reporter_profile_uid, target_uid):
    query = """
        SELECT report_uid
        FROM every_circle.content_reports
        WHERE report_reporter_profile_uid = %s
          AND report_target_uid = %s
        LIMIT 1
    """
    res = db.execute(query, (reporter_profile_uid, target_uid))
    rows = res.get("result") or []
    return rows[0] if rows else None


def _get_pending_flags_for_target(db, target_uid):
    query = """
        SELECT cr.*,
               pp.profile_personal_first_name AS reporter_first_name,
               pp.profile_personal_last_name AS reporter_last_name
        FROM every_circle.content_reports cr
        LEFT JOIN every_circle.profile_personal pp
               ON pp.profile_personal_uid = cr.report_reporter_profile_uid
        WHERE cr.report_target_uid = %s
          AND cr.report_status = 'pending'
        ORDER BY cr.report_created_at ASC
    """
    res = db.execute(query, (target_uid,))
    rows = res.get("result") or []
    return [_format_report_row(row) for row in rows]


def _get_latest_resubmission(db, target_uid):
    query = """
        SELECT *
        FROM every_circle.content_resubmissions
        WHERE resubmission_target_uid = %s
        ORDER BY resubmission_created_at DESC
        LIMIT 1
    """
    res = db.execute(query, (target_uid,))
    rows = res.get("result") or []
    return rows[0] if rows else None


class ContentReports(Resource):
    def post(self):
        """Submit a content flag against an offering, seeking post, or user profile."""
        print("In ContentReports POST")
        response = {}
        try:
            payload = request.get_json(force=True) or {}
            reporter_profile_uid = str(payload.get("reporter_profile_uid", "")).strip()
            target_uid = str(payload.get("target_uid", "")).strip()
            reason_category = str(payload.get("reason_category", "")).strip()
            reason_text = payload.get("reason_text")

            if not reporter_profile_uid or not target_uid or not reason_category:
                response["message"] = (
                    "reporter_profile_uid, target_uid, and reason_category are required"
                )
                response["code"] = 400
                return response, 400

            with connect() as db:
                target_type, target_row = resolve_content_target(db, target_uid)
                if not target_row:
                    response["message"] = "Content not found"
                    response["code"] = 404
                    return response, 404

                if target_type == TARGET_TYPE_OFFERING:
                    owner_uid = get_offering_owner_profile_uid(db, target_uid)
                    content_label = "offering"
                elif target_type == TARGET_TYPE_SEEKING:
                    owner_uid = get_wish_owner_profile_uid(db, target_uid)
                    content_label = "seeking post"
                else:
                    owner_uid = target_uid
                    content_label = "user profile"

                if owner_uid and str(reporter_profile_uid) == str(owner_uid):
                    response["message"] = f"You cannot report your own {content_label}"
                    response["code"] = 403
                    return response, 403

                if _report_exists(db, reporter_profile_uid, target_uid):
                    response["message"] = f"You have already reported this {content_label}"
                    response["code"] = 409
                    return response, 409

                report_uid = _new_report_uid(db)
                if not report_uid:
                    response["message"] = "Failed to generate report UID"
                    response["code"] = 500
                    return response, 500

                ins_res = db.insert(
                    _CONTENT_REPORTS_TABLE,
                    {
                        "report_uid": report_uid,
                        "report_reporter_profile_uid": reporter_profile_uid,
                        "report_target_uid": target_uid,
                        "report_target_type": target_type,
                        "report_reason_category": reason_category[:50],
                        "report_reason_text": reason_text,
                        "report_status": "pending",
                        "report_created_at": _now_str(),
                    },
                )
                if not _db_write_succeeded(ins_res):
                    response["message"] = "Failed to submit report"
                    response["code"] = 500
                    return response, 500

                taken_down = apply_content_takedown_if_threshold(db, target_uid)

            if taken_down:
                response["message"] = (
                    f"Report submitted. This {content_label} has been taken down and "
                    "queued for admin review due to multiple reports."
                )
            else:
                response["message"] = "Report submitted successfully"
            response["code"] = 200
            response["data"] = {
                "report_uid": report_uid,
                "taken_down": taken_down,
                "queued_for_review": taken_down,
            }
            return response, 200

        except Exception as e:
            print(f"Error in ContentReports POST: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def get(self):
        """Admin list of content reports."""
        print("In ContentReports GET")
        response = {}
        try:
            status = request.args.get("status", "pending").strip()
            target_uid = request.args.get("target_uid", "").strip()

            where_parts = ["cr.report_status = %s"]
            params = [status]
            if target_uid:
                where_parts.append("cr.report_target_uid = %s")
                params.append(target_uid)

            where_clause = " AND ".join(where_parts)
            query = f"""
                SELECT cr.*,
                       pp.profile_personal_first_name AS reporter_first_name,
                       pp.profile_personal_last_name AS reporter_last_name
                FROM every_circle.content_reports cr
                LEFT JOIN every_circle.profile_personal pp
                       ON pp.profile_personal_uid = cr.report_reporter_profile_uid
                WHERE {where_clause}
                ORDER BY cr.report_created_at DESC
            """

            with connect() as db:
                result = db.execute(query, tuple(params))
                rows = result.get("result") or []

            response["message"] = "Reports retrieved successfully"
            response["code"] = 200
            response["result"] = [_format_report_row(row) for row in rows]
            return response, 200

        except Exception as e:
            print(f"Error in ContentReports GET: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def put(self, report_uid=None):
        """Admin dismisses a single pending report."""
        print("In ContentReports PUT")
        response = {}
        try:
            payload = request.get_json(force=True) or {}
            report_uid = str(
                report_uid or payload.get("report_uid") or payload.get("content_reports_uid", "")
            ).strip()
            admin_uid = str(payload.get("admin_uid", "")).strip()

            if not report_uid:
                response["message"] = "report_uid is required"
                response["code"] = 400
                return response, 400

            with connect() as db:
                existing_res = db.select(
                    _CONTENT_REPORTS_TABLE,
                    where={"report_uid": report_uid},
                )
                rows = existing_res.get("result") or []
                if not rows:
                    response["message"] = "Report not found"
                    response["code"] = 404
                    return response, 404

                report = rows[0]
                if report.get("report_status") == "dismissed":
                    response["message"] = "Report is already dismissed"
                    response["code"] = 200
                    response["data"] = {"report_uid": report_uid}
                    return response, 200

                upd_res = db.update(
                    _CONTENT_REPORTS_TABLE,
                    {"report_uid": report_uid},
                    {"report_status": "dismissed"},
                )
                if not _db_write_succeeded(upd_res):
                    response["message"] = "Failed to dismiss report"
                    response["code"] = 500
                    return response, 500

            response["message"] = "Report dismissed successfully"
            response["code"] = 200
            response["data"] = {
                "report_uid": report_uid,
                "admin_uid": admin_uid or None,
            }
            return response, 200

        except Exception as e:
            print(f"Error in ContentReports PUT: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500


class ContentModerationReview(Resource):
    def get(self, profile_expertise_uid=None):
        """Review queue or single offering moderation detail."""
        print("In ContentModerationReview GET")
        response = {}
        try:
            with connect() as db:
                if not profile_expertise_uid or profile_expertise_uid == "review-queue":
                    query = """
                        SELECT pe.*,
                               pp.profile_personal_first_name AS owner_first_name,
                               pp.profile_personal_last_name AS owner_last_name
                        FROM every_circle.profile_expertise pe
                        LEFT JOIN every_circle.profile_personal pp
                               ON pp.profile_personal_uid = pe.profile_expertise_profile_personal_id
                        WHERE pe.profile_expertise_moderated = %s
                          AND EXISTS (
                              SELECT 1
                              FROM every_circle.content_resubmissions cr
                              WHERE cr.resubmission_target_uid = pe.profile_expertise_uid
                                AND cr.resubmission_status = 'pending'
                          )
                        ORDER BY pe.profile_expertise_uid ASC
                    """
                    result = db.execute(query, (MODERATED_PENDING_REVIEW,))
                    rows = result.get("result") or []
                    queue = []
                    for row in rows:
                        item = {k: _serialize_datetime(v) for k, v in row.items()}
                        expertise_uid = row.get("profile_expertise_uid")
                        item["moderation"] = build_offering_moderation_metadata(db, expertise_uid)
                        queue.append(item)

                    response["message"] = "Review queue retrieved successfully"
                    response["code"] = 200
                    response["result"] = queue
                    return response, 200

                offering = get_offering(db, profile_expertise_uid)
                if not offering:
                    response["message"] = "Offering not found"
                    response["code"] = 404
                    return response, 404

                latest_resubmission = _get_latest_resubmission(db, profile_expertise_uid)
                resubmission_data = None
                if latest_resubmission:
                    resubmission_data = {
                        key: _serialize_datetime(value)
                        for key, value in latest_resubmission.items()
                    }
                    resubmission_data["resubmission_snapshot"] = _parse_snapshot(
                        latest_resubmission.get("resubmission_snapshot")
                    )

                response["message"] = "Offering moderation detail retrieved successfully"
                response["code"] = 200
                response["data"] = {
                    "offering": {k: _serialize_datetime(v) for k, v in offering.items()},
                    "pendingFlags": _get_pending_flags_for_target(db, profile_expertise_uid),
                    "moderation": build_offering_moderation_metadata(db, profile_expertise_uid),
                    "latestResubmission": resubmission_data,
                }
                return response, 200

        except Exception as e:
            print(f"Error in ContentModerationReview GET: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def put(self, profile_expertise_uid):
        """Admin approves or rejects a moderated offering resubmission."""
        print("In ContentModerationReview PUT")
        response = {}
        try:
            if not request.path.rstrip("/").endswith("/review"):
                response["message"] = "Use the /review endpoint to submit a moderation decision"
                response["code"] = 400
                return response, 400

            payload = request.get_json(force=True) or {}
            action = str(payload.get("action", "")).strip().lower()
            admin_uid = str(payload.get("admin_uid", "")).strip()
            note = payload.get("note")

            if not profile_expertise_uid or action not in ("approve", "reject"):
                response["message"] = (
                    "profile_expertise_uid and action ('approve' or 'reject') are required"
                )
                response["code"] = 400
                return response, 400

            if action == "reject":
                note_text = str(note or "").strip()
                if not note_text:
                    response["message"] = "note is required when rejecting an offering"
                    response["code"] = 400
                    return response, 400

            with connect() as db:
                offering = get_offering(db, profile_expertise_uid)
                if not offering:
                    response["message"] = "Offering not found"
                    response["code"] = 404
                    return response, 404

                if action == "approve":
                    result = approve_offering_review(db, profile_expertise_uid, admin_uid, note)
                else:
                    result = reject_offering_review(db, profile_expertise_uid, admin_uid, note)

                if not result.get("ok"):
                    response["message"] = result.get("message", "Review action failed")
                    response["code"] = 400 if action == "reject" else 500
                    return response, response["code"]

            response["message"] = f"Offering {action}d successfully"
            response["code"] = 200
            response["data"] = {
                "profile_expertise_uid": profile_expertise_uid,
                "action": action,
                "admin_uid": admin_uid or None,
            }
            return response, 200

        except Exception as e:
            print(f"Error in ContentModerationReview PUT: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def post(self, profile_expertise_uid):
        """Owner acknowledges a rejected / taken-down offering (moderated = 3)."""
        print("In ContentModerationReview POST (acknowledge)")
        response = {}
        try:
            if not request.path.rstrip("/").endswith("/acknowledge"):
                response["message"] = (
                    "Use the /acknowledge endpoint to acknowledge a taken-down offering"
                )
                response["code"] = 400
                return response, 400

            payload = request.get_json(force=True) or {}
            requester_profile_uid = str(
                payload.get("profile_uid")
                or payload.get("requester_profile_uid")
                or ""
            ).strip()

            if not profile_expertise_uid or not requester_profile_uid:
                response["message"] = (
                    "profile_expertise_uid and profile_uid are required"
                )
                response["code"] = 400
                return response, 400

            with connect() as db:
                result = acknowledge_offering_takedown(
                    db, profile_expertise_uid, requester_profile_uid
                )

            if not result.get("ok"):
                code = result.get("code", 400)
                response["message"] = result.get("message", "Acknowledge failed")
                response["code"] = code
                return response, code

            response["message"] = (
                "Offering already acknowledged"
                if result.get("already_acknowledged")
                else "Offering acknowledged successfully"
            )
            response["code"] = 200
            response["data"] = {
                "profile_expertise_uid": profile_expertise_uid,
                "moderated": 3,
                "already_acknowledged": bool(result.get("already_acknowledged")),
            }
            return response, 200

        except Exception as e:
            print(f"Error in ContentModerationReview POST: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500


class SeekingContentModerationReview(Resource):
    def get(self, profile_wish_uid=None):
        """Review queue or single seeking post moderation detail."""
        print("In SeekingContentModerationReview GET")
        response = {}
        try:
            with connect() as db:
                if not profile_wish_uid or profile_wish_uid == "review-queue":
                    query = """
                        SELECT pw.*,
                               pp.profile_personal_first_name AS owner_first_name,
                               pp.profile_personal_last_name AS owner_last_name
                        FROM every_circle.profile_wish pw
                        LEFT JOIN every_circle.profile_personal pp
                               ON pp.profile_personal_uid = pw.profile_wish_profile_personal_id
                        WHERE pw.profile_wish_moderated = %s
                          AND EXISTS (
                              SELECT 1
                              FROM every_circle.content_resubmissions cr
                              WHERE cr.resubmission_target_uid = pw.profile_wish_uid
                                AND cr.resubmission_status = 'pending'
                          )
                        ORDER BY pw.profile_wish_uid ASC
                    """
                    result = db.execute(query, (MODERATED_PENDING_REVIEW,))
                    rows = result.get("result") or []
                    queue = []
                    for row in rows:
                        item = {k: _serialize_datetime(v) for k, v in row.items()}
                        wish_uid = row.get("profile_wish_uid")
                        item["moderation"] = build_wish_moderation_metadata(db, wish_uid)
                        queue.append(item)

                    response["message"] = "Review queue retrieved successfully"
                    response["code"] = 200
                    response["result"] = queue
                    return response, 200

                wish = get_wish(db, profile_wish_uid)
                if not wish:
                    response["message"] = "Seeking post not found"
                    response["code"] = 404
                    return response, 404

                latest_resubmission = _get_latest_resubmission(db, profile_wish_uid)
                resubmission_data = None
                if latest_resubmission:
                    resubmission_data = {
                        key: _serialize_datetime(value)
                        for key, value in latest_resubmission.items()
                    }
                    resubmission_data["resubmission_snapshot"] = _parse_snapshot(
                        latest_resubmission.get("resubmission_snapshot")
                    )

                response["message"] = "Seeking moderation detail retrieved successfully"
                response["code"] = 200
                response["data"] = {
                    "seeking": {k: _serialize_datetime(v) for k, v in wish.items()},
                    "pendingFlags": _get_pending_flags_for_target(db, profile_wish_uid),
                    "moderation": build_wish_moderation_metadata(db, profile_wish_uid),
                    "latestResubmission": resubmission_data,
                }
                return response, 200

        except Exception as e:
            print(f"Error in SeekingContentModerationReview GET: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def put(self, profile_wish_uid):
        """Admin approves or rejects a moderated seeking post resubmission."""
        print("In SeekingContentModerationReview PUT")
        response = {}
        try:
            if not request.path.rstrip("/").endswith("/review"):
                response["message"] = "Use the /review endpoint to submit a moderation decision"
                response["code"] = 400
                return response, 400

            payload = request.get_json(force=True) or {}
            action = str(payload.get("action", "")).strip().lower()
            admin_uid = str(payload.get("admin_uid", "")).strip()
            note = payload.get("note")

            if not profile_wish_uid or action not in ("approve", "reject"):
                response["message"] = (
                    "profile_wish_uid and action ('approve' or 'reject') are required"
                )
                response["code"] = 400
                return response, 400

            if action == "reject":
                note_text = str(note or "").strip()
                if not note_text:
                    response["message"] = "note is required when rejecting a seeking post"
                    response["code"] = 400
                    return response, 400

            with connect() as db:
                wish = get_wish(db, profile_wish_uid)
                if not wish:
                    response["message"] = "Seeking post not found"
                    response["code"] = 404
                    return response, 404

                if action == "approve":
                    result = approve_wish_review(db, profile_wish_uid, admin_uid, note)
                else:
                    result = reject_wish_review(db, profile_wish_uid, admin_uid, note)

                if not result.get("ok"):
                    response["message"] = result.get("message", "Review action failed")
                    response["code"] = 400 if action == "reject" else 500
                    return response, response["code"]

            response["message"] = f"Seeking post {action}d successfully"
            response["code"] = 200
            response["data"] = {
                "profile_wish_uid": profile_wish_uid,
                "action": action,
                "admin_uid": admin_uid or None,
            }
            return response, 200

        except Exception as e:
            print(f"Error in SeekingContentModerationReview PUT: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def post(self, profile_wish_uid):
        """Owner acknowledges a rejected / taken-down seeking post (moderated = 3)."""
        print("In SeekingContentModerationReview POST (acknowledge)")
        response = {}
        try:
            if not request.path.rstrip("/").endswith("/acknowledge"):
                response["message"] = (
                    "Use the /acknowledge endpoint to acknowledge a taken-down seeking post"
                )
                response["code"] = 400
                return response, 400

            payload = request.get_json(force=True) or {}
            requester_profile_uid = str(
                payload.get("profile_uid")
                or payload.get("requester_profile_uid")
                or ""
            ).strip()

            if not profile_wish_uid or not requester_profile_uid:
                response["message"] = (
                    "profile_wish_uid and profile_uid are required"
                )
                response["code"] = 400
                return response, 400

            with connect() as db:
                result = acknowledge_wish_takedown(
                    db, profile_wish_uid, requester_profile_uid
                )

            if not result.get("ok"):
                code = result.get("code", 400)
                response["message"] = result.get("message", "Acknowledge failed")
                response["code"] = code
                return response, code

            response["message"] = (
                "Seeking post already acknowledged"
                if result.get("already_acknowledged")
                else "Seeking post acknowledged successfully"
            )
            response["code"] = 200
            response["data"] = {
                "profile_wish_uid": profile_wish_uid,
                "moderated": 3,
                "already_acknowledged": bool(result.get("already_acknowledged")),
            }
            return response, 200

        except Exception as e:
            print(f"Error in SeekingContentModerationReview POST: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500


class UserModerationReview(Resource):
    def get(self, profile_personal_uid=None):
        """Review queue or single user profile moderation detail."""
        print("In UserModerationReview GET")
        response = {}
        try:
            with connect() as db:
                if not profile_personal_uid or profile_personal_uid == "review-queue":
                    query = """
                        SELECT pp.*,
                               u.user_email_id AS owner_email
                        FROM every_circle.profile_personal pp
                        LEFT JOIN every_circle.users u
                               ON u.user_uid = pp.profile_personal_user_id
                        WHERE pp.profile_personal_moderated = %s
                          AND EXISTS (
                              SELECT 1
                              FROM every_circle.content_resubmissions cr
                              WHERE cr.resubmission_target_uid = pp.profile_personal_uid
                                AND cr.resubmission_status = 'pending'
                          )
                        ORDER BY pp.profile_personal_uid ASC
                    """
                    result = db.execute(query, (MODERATED_PENDING_REVIEW,))
                    rows = result.get("result") or []
                    queue = []
                    for row in rows:
                        item = {k: _serialize_datetime(v) for k, v in row.items()}
                        profile_uid = row.get("profile_personal_uid")
                        item["moderation"] = build_user_moderation_metadata(db, profile_uid)
                        queue.append(item)

                    response["message"] = "Review queue retrieved successfully"
                    response["code"] = 200
                    response["result"] = queue
                    return response, 200

                user = get_user(db, profile_personal_uid)
                if not user:
                    response["message"] = "User profile not found"
                    response["code"] = 404
                    return response, 404

                latest_resubmission = _get_latest_resubmission(db, profile_personal_uid)
                resubmission_data = None
                if latest_resubmission:
                    resubmission_data = {
                        key: _serialize_datetime(value)
                        for key, value in latest_resubmission.items()
                    }
                    resubmission_data["resubmission_snapshot"] = _parse_snapshot(
                        latest_resubmission.get("resubmission_snapshot")
                    )

                response["message"] = "User moderation detail retrieved successfully"
                response["code"] = 200
                response["data"] = {
                    "user": {k: _serialize_datetime(v) for k, v in user.items()},
                    "pendingFlags": _get_pending_flags_for_target(db, profile_personal_uid),
                    "moderation": build_user_moderation_metadata(db, profile_personal_uid),
                    "latestResubmission": resubmission_data,
                }
                return response, 200

        except Exception as e:
            print(f"Error in UserModerationReview GET: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def put(self, profile_personal_uid):
        """Admin approves or rejects a moderated user profile resubmission."""
        print("In UserModerationReview PUT")
        response = {}
        try:
            if not request.path.rstrip("/").endswith("/review"):
                response["message"] = "Use the /review endpoint to submit a moderation decision"
                response["code"] = 400
                return response, 400

            payload = request.get_json(force=True) or {}
            action = str(payload.get("action", "")).strip().lower()
            admin_uid = str(payload.get("admin_uid", "")).strip()
            note = payload.get("note")

            if not profile_personal_uid or action not in ("approve", "reject"):
                response["message"] = (
                    "profile_personal_uid and action ('approve' or 'reject') are required"
                )
                response["code"] = 400
                return response, 400

            if action == "reject":
                note_text = str(note or "").strip()
                if not note_text:
                    response["message"] = "note is required when rejecting a user profile"
                    response["code"] = 400
                    return response, 400

            with connect() as db:
                user = get_user(db, profile_personal_uid)
                if not user:
                    response["message"] = "User profile not found"
                    response["code"] = 404
                    return response, 404

                if action == "approve":
                    result = approve_user_review(db, profile_personal_uid, admin_uid, note)
                else:
                    result = reject_user_review(db, profile_personal_uid, admin_uid, note)

                if not result.get("ok"):
                    response["message"] = result.get("message", "Review action failed")
                    response["code"] = 400 if action == "reject" else 500
                    return response, response["code"]

            response["message"] = f"User profile {action}d successfully"
            response["code"] = 200
            response["data"] = {
                "profile_personal_uid": profile_personal_uid,
                "action": action,
                "admin_uid": admin_uid or None,
            }
            return response, 200

        except Exception as e:
            print(f"Error in UserModerationReview PUT: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def post(self, profile_personal_uid):
        """Owner acknowledges a rejected / taken-down user profile (moderated = 3)."""
        print("In UserModerationReview POST (acknowledge)")
        response = {}
        try:
            if not request.path.rstrip("/").endswith("/acknowledge"):
                response["message"] = (
                    "Use the /acknowledge endpoint to acknowledge a taken-down user profile"
                )
                response["code"] = 400
                return response, 400

            payload = request.get_json(force=True) or {}
            requester_profile_uid = str(
                payload.get("profile_uid")
                or payload.get("requester_profile_uid")
                or ""
            ).strip()

            if not profile_personal_uid or not requester_profile_uid:
                response["message"] = (
                    "profile_personal_uid and profile_uid are required"
                )
                response["code"] = 400
                return response, 400

            with connect() as db:
                result = acknowledge_user_takedown(
                    db, profile_personal_uid, requester_profile_uid
                )

            if not result.get("ok"):
                code = result.get("code", 400)
                response["message"] = result.get("message", "Acknowledge failed")
                response["code"] = code
                return response, code

            response["message"] = (
                "User profile already acknowledged"
                if result.get("already_acknowledged")
                else "User profile acknowledged successfully"
            )
            response["code"] = 200
            response["data"] = {
                "profile_personal_uid": profile_personal_uid,
                "moderated": 3,
                "already_acknowledged": bool(result.get("already_acknowledged")),
            }
            return response, 200

        except Exception as e:
            print(f"Error in UserModerationReview POST: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500
