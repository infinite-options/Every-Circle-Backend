from flask import request
from flask_restful import Resource
from datetime import datetime
import uuid

from data_ec import connect
from moderation import MODERATED_ACTIVE, get_offering


def _generate_expertise_response_uid():
    try:
        with connect() as db:
            uid_result = db.call(procedure="new_expertise_response_uid")
        rows = (uid_result or {}).get("result") or []
        if rows and rows[0].get("new_id"):
            return rows[0]["new_id"]
    except Exception as e:
        print(f"new_expertise_response_uid failed, using fallback: {e}")
    return f"175-{uuid.uuid4().hex[:12]}"


class ProfileExpertiseResponse(Resource):
    def get(self, responder_id):
        print(f"In ProfileExpertiseResponse GET - responses by responder: {responder_id}")
        response = {}

        try:
            if not responder_id:
                response["message"] = "responder_id is required"
                response["code"] = 400
                return response, 400

            with connect() as db:
                query = """
                    SELECT *
                    FROM every_circle.expertise_response
                    WHERE er_responder_id = %s
                    ORDER BY er_datetime DESC
                """
                query_response = db.execute(query, (responder_id,))

            if query_response.get("code") == 200:
                response["message"] = "Expertise responses retrieved successfully"
                response["code"] = 200
                response["data"] = query_response.get("result", [])
                response["count"] = len(query_response.get("result", []))
                return response, 200

            response["message"] = "Query execution failed"
            response["code"] = query_response.get("code", 500)
            response["error"] = query_response.get("error", "Unknown error")
            return response, response["code"]

        except Exception as e:
            print(f"Error in ProfileExpertiseResponse GET: {str(e)}")
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500

    def post(self):
        print("In ProfileExpertiseResponse POST - Record offering message")
        response = {}

        try:
            payload = request.get_json() or {}
            profile_expertise_id = payload.get("profile_expertise_id")
            responder_id = payload.get("responder_id")

            missing = [
                field
                for field, value in [
                    ("profile_expertise_id", profile_expertise_id),
                    ("responder_id", responder_id),
                ]
                if not value
            ]
            if missing:
                response["message"] = f"Missing required fields: {', '.join(missing)}"
                response["code"] = 400
                return response, 400

            with connect() as db:
                offering = get_offering(db, profile_expertise_id)
                if not offering:
                    response["message"] = "Offering not found"
                    response["code"] = 404
                    return response, 404

                if int(offering.get("profile_expertise_moderated") or 0) != MODERATED_ACTIVE:
                    response["message"] = "Offering is not available"
                    response["code"] = 403
                    return response, 403

                new_uid = _generate_expertise_response_uid()
                expertise_response_data = {
                    "expertise_response_uid": new_uid,
                    "er_profile_expertise_id": profile_expertise_id,
                    "er_responder_id": responder_id,
                    "er_datetime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                insert_response = db.insert(
                    "every_circle.expertise_response", expertise_response_data
                )

            if insert_response.get("code") != 200:
                response["message"] = insert_response.get(
                    "message", "Failed to insert expertise response"
                )
                response["code"] = insert_response.get("code", 500)
                return response, response["code"]

            response["expertise_response_uid"] = new_uid
            response["er_datetime"] = expertise_response_data["er_datetime"]
            response["message"] = "Expertise response recorded successfully"
            response["code"] = 200
            return response, 200

        except Exception as e:
            print(f"Error in ProfileExpertiseResponse POST: {str(e)}")
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500


class ProfileExpertiseResponsesForOffering(Resource):
    def get(self, profile_expertise_id):
        print(f"In ProfileExpertiseResponsesForOffering GET - offering: {profile_expertise_id}")
        response = {}

        try:
            if not profile_expertise_id:
                response["message"] = "profile_expertise_id is required"
                response["code"] = 400
                return response, 400

            with connect() as db:
                query = """
                    SELECT er.*, m.message_body,
                        responder.profile_personal_first_name AS responder_first_name,
                        responder.profile_personal_last_name AS responder_last_name,
                        responder.profile_personal_image AS responder_image,
                        responder.profile_personal_image_is_public AS responder_image_is_public,
                        responder.profile_personal_tag_line AS responder_tag_line,
                        responder.profile_personal_tag_line_is_public AS responder_tag_line_is_public,
                        responder.profile_personal_email_is_public AS responder_email_is_public,
                        responder.profile_personal_phone_number_is_public AS responder_phone_is_public,
                        if (responder.profile_personal_email_is_public = 1, u.user_email_id, null) AS responder_email,
                        if (responder.profile_personal_phone_number_is_public = 1, responder.profile_personal_phone_number, null) AS responder_phone
                    FROM every_circle.expertise_response er
                    LEFT JOIN every_circle.profile_personal AS responder
                        ON er.er_responder_id = responder.profile_personal_uid
                    LEFT JOIN every_circle.users u
                        ON u.user_uid = responder.profile_personal_user_id
                    LEFT JOIN every_circle.messages m
                        ON m.message_uid = (
                            SELECT message_uid
                            FROM every_circle.messages
                            WHERE message_context_response_uid = er.expertise_response_uid
                              AND message_context_type = 'offering'
                              AND message_context_uid = er.er_profile_expertise_id
                            ORDER BY message_sent_at ASC
                            LIMIT 1
                        )
                    WHERE er.er_profile_expertise_id = %s
                    ORDER BY er.er_datetime DESC
                """
                query_response = db.execute(query, (profile_expertise_id,))

            if query_response.get("code") == 200:
                response["message"] = "Offering responses retrieved successfully"
                response["code"] = 200
                response["data"] = query_response.get("result", [])
                response["count"] = len(query_response.get("result", []))
                return response, 200

            response["message"] = "Query execution failed"
            response["code"] = query_response.get("code", 500)
            response["error"] = query_response.get("error", "Unknown error")
            return response, response["code"]

        except Exception as e:
            print(f"Error in ProfileExpertiseResponsesForOffering GET: {str(e)}")
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500
