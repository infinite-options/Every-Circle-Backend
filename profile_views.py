import json
from flask import request
from flask_restful import Resource
from datetime import datetime
from data_ec import connect
from user_path_connection import ConnectionsPath


class ProfileViews(Resource):
    def get(self, profile_uid):
        print(f"In ProfileViews GET for profile_uid: {profile_uid}")
        response = {}
        try:
            viewer_uid = (request.args.get("viewer_uid") or "").strip()

            # ── 1. Record view (upsert) if viewer_uid provided and not a self-view ──
            if viewer_uid and viewer_uid != profile_uid:
                now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                with connect() as db_w:
                    check = db_w.execute(
                        """
                        SELECT profile_view_uid, view_timestamp
                        FROM every_circle.profile_views
                        WHERE view_profile_id = %s AND view_viewer_id = %s
                        LIMIT 1
                        """,
                        (profile_uid, viewer_uid),
                    )
                    existing = (check.get("result") or []) if check else []
                    if existing:
                        existing_uid = existing[0]["profile_view_uid"]
                        raw_ts = existing[0].get("view_timestamp")
                        if isinstance(raw_ts, list):
                            timestamps = raw_ts
                        else:
                            try:
                                parsed = json.loads(raw_ts) if raw_ts else []
                                timestamps = parsed if isinstance(parsed, list) else [str(raw_ts)]
                            except (TypeError, ValueError):
                                timestamps = [str(raw_ts)] if raw_ts else []
                        timestamps.append(now)
                        db_w.execute(
                            """
                            UPDATE every_circle.profile_views
                            SET view_timestamp = %s
                            WHERE profile_view_uid = %s
                            """,
                            (json.dumps(timestamps), existing_uid),
                            "post",
                        )
                        print(f"Updated view {existing_uid} for {viewer_uid} -> {profile_uid}")
                    else:
                        uid_result = db_w.call("new_profile_view_uid")
                        uid_rows = (uid_result.get("result") or []) if uid_result else []
                        new_uid = uid_rows[0].get("new_id") if uid_rows else None
                        if new_uid:
                            db_w.insert(
                                "every_circle.profile_views",
                                {
                                    "profile_view_uid": new_uid,
                                    "view_profile_id": profile_uid,
                                    "view_viewer_id": viewer_uid,
                                    "view_timestamp": json.dumps([now]),
                                },
                            )
                            print(f"Inserted new view {new_uid} for {viewer_uid} -> {profile_uid}")

            # ── 2. Fetch viewer list ──
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

                seen = set()
                unique_viewers = []
                for row in viewers:
                    vid = row.get("view_viewer_id")
                    if vid not in seen:
                        seen.add(vid)
                        raw_ts = row.get("view_timestamp")
                        last_ts = None
                        if isinstance(raw_ts, list):
                            last_ts = raw_ts[-1] if raw_ts else None
                        elif raw_ts:
                            try:
                                parsed = json.loads(raw_ts)
                                last_ts = parsed[-1] if isinstance(parsed, list) and parsed else str(raw_ts)
                            except (TypeError, ValueError):
                                last_ts = str(raw_ts)
                        row["view_timestamp"] = last_ts
                        unique_viewers.append(row)

                unique_viewers.sort(key=lambda r: r.get("view_timestamp") or "", reverse=True)
                response["viewers"] = unique_viewers
                response["count"] = len(unique_viewers)

            # ── 3. Fetch ratings (business profiles only) ──
            ratings_list = []
            if profile_uid[:3] == "200":
                with connect() as db_r:
                    ratings_resp = db_r.select("every_circle.ratings", where={"rating_business_id": profile_uid})
                    ratings_list = ratings_resp.get("result") or []

                for rating in ratings_list:
                    rater_profile_id = rating.get("rating_profile_id")
                    business_id = rating.get("rating_business_id")
                    with connect() as db_r2:
                        tx = db_r2.select(
                            "transactions",
                            where={"transaction_profile_id": rater_profile_id, "transaction_business_id": business_id},
                        )
                        rating["is_verified"] = bool(tx.get("result"))

                        rating["circle_num_nodes"] = None
                        if viewer_uid and rater_profile_id and viewer_uid != rater_profile_id:
                            circle = db_r2.select(
                                "every_circle.circles",
                                where={"circle_profile_id": viewer_uid, "circle_related_person_id": rater_profile_id},
                            )
                            if not circle.get("result"):
                                circle = db_r2.select(
                                    "every_circle.circles",
                                    where={"circle_profile_id": rater_profile_id, "circle_related_person_id": viewer_uid},
                                )
                            if circle.get("result"):
                                stored = circle["result"][0].get("circle_num_nodes")
                                if stored is not None:
                                    rating["circle_num_nodes"] = int(stored)
                                    continue
                            try:
                                cp = ConnectionsPath()
                                path_resp, path_status = cp.get(viewer_uid, rater_profile_id)
                                if path_status == 200 and path_resp.get("combined_path"):
                                    nodes = path_resp["combined_path"].split(",")
                                    rating["circle_num_nodes"] = int(len(nodes) - 1)
                            except Exception as e:
                                print(f"Could not calculate circle_num_nodes: {e}")

                if viewer_uid:
                    ratings_list.sort(
                        key=lambda r: (r["circle_num_nodes"] is None, int(r["circle_num_nodes"]) if r["circle_num_nodes"] is not None else 0)
                    )

            response["ratings"] = ratings_list
            response["code"] = 200
            return response, 200

        except Exception as e:
            print(f"Error in ProfileViews GET: {e}")
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500
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
            print(
                f"ProfileViews POST received - view_profile_id: {view_profile_id}, view_viewer_id: {view_viewer_id}"
            )

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
                # Check if a record already exists for this viewer + profile (any time)
                check = db.execute(
                    """
                    SELECT profile_view_uid, view_timestamp
                    FROM every_circle.profile_views
                    WHERE view_profile_id = %s
                      AND view_viewer_id = %s
                    LIMIT 1
                    """,
                    (view_profile_id, view_viewer_id),
                )
                existing = (check.get("result") or []) if check else []

                if existing:
                    existing_uid = existing[0]["profile_view_uid"]
                    raw_ts = existing[0].get("view_timestamp")

                    # Parse existing value — handle plain string, JSON string, or list
                    if isinstance(raw_ts, list):
                        timestamps = raw_ts
                    else:
                        try:
                            parsed = json.loads(raw_ts) if raw_ts else []
                            timestamps = (
                                parsed if isinstance(parsed, list) else [str(raw_ts)]
                            )
                        except (TypeError, ValueError):
                            timestamps = [str(raw_ts)] if raw_ts else []

                    timestamps.append(now)
                    updated_ts = json.dumps(timestamps)

                    print(
                        f"Appending timestamp to view {existing_uid} for {view_viewer_id} -> {view_profile_id}"
                    )
                    db.execute(
                        """
                        UPDATE every_circle.profile_views
                        SET view_timestamp = %s
                        WHERE profile_view_uid = %s
                        """,
                        (updated_ts, existing_uid),
                    )
                else:
                    uid_result = db.call("new_profile_view_uid")
                    uid_rows = (uid_result.get("result") or []) if uid_result else []
                    new_uid = uid_rows[0].get("new_id") if uid_rows else None

                    if not new_uid:
                        response["message"] = "Failed to generate profile_view_uid"
                        response["code"] = 500
                        return response, 500

                    print(
                        f"Inserting view {new_uid}: {view_viewer_id} -> {view_profile_id}"
                    )
                    db.insert(
                        "every_circle.profile_views",
                        {
                            "profile_view_uid": new_uid,
                            "view_profile_id": view_profile_id,
                            "view_viewer_id": view_viewer_id,
                            "view_timestamp": json.dumps([now]),
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
