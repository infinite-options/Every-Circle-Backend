from flask import request
from flask_restful import Resource
from datetime import datetime
from data_ec import connect


class ProfileViews(Resource):

    def get(self, profile_uid):

        print(f"In ProfileViews GET for profile_uid: {profile_uid}")
        response = {}
        try:
            with connect() as db:
                query = """
                    SELECT
                        pv.view_viewer_id,
                        pv.view_timestamp,
                        pp.profile_personal_first_name      AS viewer_first_name,
                        pp.profile_personal_last_name       AS viewer_last_name,
                        pp.profile_personal_image           AS viewer_image,
                        pp.profile_personal_image_is_public AS viewer_image_is_public,
                        pp.profile_personal_tag_line        AS viewer_tag_line,
                        pp.profile_personal_tag_line_is_public AS viewer_tag_line_is_public,
                        pp.profile_personal_phone_number    AS viewer_phone,
                        pp.profile_personal_phone_number_is_public AS viewer_phone_is_public,
                        pp.profile_personal_city            AS viewer_city,
                        pp.profile_personal_state           AS viewer_state,
                        pp.profile_personal_location_is_public AS viewer_location_is_public,
                        pp.profile_personal_email_is_public AS viewer_email_is_public,
                        u.user_email_id                     AS viewer_email
                    FROM every_circle.profile_views pv
                    LEFT JOIN every_circle.profile_personal pp
                        ON pp.profile_personal_uid = pv.view_viewer_id
                    LEFT JOIN every_circle.users u
                        ON u.user_uid = pp.profile_personal_user_id
                    WHERE pv.view_profile_id = %s
                    ORDER BY pv.view_timestamp DESC
                """
                result = db.execute(query, (profile_uid,))
                viewers = result.get("result", []) if result else []

                # Deduplicate: keep only the most recent entry per viewer
                seen = set()
                unique_viewers = []
                for row in viewers:
                    vid = row.get("view_viewer_id")
                    if vid not in seen:
                        seen.add(vid)
                        unique_viewers.append(row)

                response["viewers"] = unique_viewers
                response["count"] = len(unique_viewers)
                response["code"] = 200
                return response, 200

        except Exception as e:
            print(f"Error in ProfileViews GET: {e}")
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500

    def post(self):
        print("In ProfileViews POST")
        response = {}
        try:
            payload = request.get_json(force=True, silent=True) or {}
            view_profile_id = (payload.get("profile_view_profile_id") or "").strip()
            view_viewer_id = (payload.get("profile_view_viewer_id") or "").strip()

            if not view_profile_id or not view_viewer_id:
                response["message"] = (
                    "profile_view_profile_id and profile_view_viewer_id are required"
                )
                response["code"] = 400
                return response, 400

            # Don't record self-views
            if view_profile_id == view_viewer_id:
                response["message"] = "Self-views are not recorded"
                response["code"] = 200
                return response, 200

            now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            with connect() as db:
                # If a business UID (200-xxx) is passed, resolve to owner personal UIDs
                if view_profile_id.startswith("200-"):
                    owner_result = db.execute(
                        """
                        SELECT pp.profile_personal_uid AS owner_uid
                        FROM every_circle.business_user bu
                        LEFT JOIN every_circle.profile_personal pp
                            ON bu.bu_user_id = pp.profile_personal_user_id
                        WHERE bu.bu_business_id = %s
                          AND pp.profile_personal_uid IS NOT NULL
                        """,
                        (view_profile_id,),
                    )
                    owner_rows = (
                        (owner_result.get("result") or []) if owner_result else []
                    )
                    owner_uids = [
                        r["owner_uid"] for r in owner_rows if r.get("owner_uid")
                    ]
                    print(
                        f"Resolved business {view_profile_id} to owner UIDs: {owner_uids}"
                    )
                else:
                    owner_uids = [view_profile_id]

                if not owner_uids:
                    response["message"] = (
                        "No owners found for given profile_view_profile_id"
                    )
                    response["code"] = 404
                    return response, 404

                for owner_uid in owner_uids:
                    # Skip self-views per owner
                    if owner_uid == view_viewer_id:
                        continue

                    # Check if this viewer already has a view within the last 2 days
                    check = db.execute(
                        """
                        SELECT profile_view_uid
                        FROM every_circle.profile_views
                        WHERE view_profile_id = %s
                          AND view_viewer_id = %s
                          AND view_timestamp >= DATE_SUB(NOW(), INTERVAL 2 DAY)
                        LIMIT 1
                        """,
                        (owner_uid, view_viewer_id),
                    )
                    existing = (check.get("result") or []) if check else []

                    if existing:
                        existing_uid = existing[0]["profile_view_uid"]
                        print(
                            f"Updating existing view {existing_uid} for {view_viewer_id} -> {owner_uid}"
                        )
                        db.execute(
                            """
                            UPDATE every_circle.profile_views
                            SET view_timestamp = %s
                            WHERE profile_view_uid = %s
                            """,
                            (now, existing_uid),
                        )
                    else:
                        uid_result = db.call("new_profile_view_uid")
                        uid_rows = (
                            (uid_result.get("result") or []) if uid_result else []
                        )
                        new_uid = uid_rows[0].get("new_id") if uid_rows else None
                        if not new_uid:
                            response["message"] = "Failed to generate profile_view_uid"
                            response["code"] = 500
                            return response, 500
                        print(
                            f"Inserting view {new_uid}: {view_viewer_id} -> {owner_uid}"
                        )
                        db.insert(
                            "every_circle.profile_views",
                            {
                                "profile_view_uid": new_uid,
                                "view_profile_id": owner_uid,
                                "view_viewer_id": view_viewer_id,
                                "view_timestamp": now,
                            },
                        )

                response["message"] = "View recorded"
                response["code"] = 200
                return response, 200

        except Exception as e:
            print(f"Error in ProfileViews POST: {e}")
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500
