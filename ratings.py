from flask import request, abort, jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime
import ast
import json

from data_ec import connect, uploadImage, s3, processImage
from user_path_connection import ConnectionsPath


class Ratings(Resource):
    def get(self, uid):
        print("In Ratings GET")
        response = {}
        try:
            print(uid, type(uid))
            viewer_uid = request.args.get("viewer_uid")
            print("🔍 viewer_uid received:", viewer_uid)

            with connect() as db:
                key = {}
                if uid[:3] == "110":
                    key["rating_profile_id"] = uid
                elif uid[:3] == "200":
                    key["rating_business_id"] = uid
                else:
                    response["message"] = "Invalid UID"
                    response["code"] = 400
                    return response, 400

                response = db.select("every_circle.ratings", where=key)

            if not response["result"]:
                response["result"] = []
                response["message"] = f"No ratings found for {key}"
                response["code"] = 200
                return response, 200

            # Add is_verified and circle_num_nodes for each rating
            for rating in response["result"]:
                profile_id = rating.get("rating_profile_id")
                business_id = rating.get("rating_business_id")

                with connect() as db2:
                    # is_verified check
                    transaction_check = db2.select(
                        "transactions",
                        where={
                            "transaction_profile_id": profile_id,
                            "transaction_business_id": business_id,
                        },
                    )
                    rating["is_verified"] = bool(transaction_check.get("result"))

                    # circle_num_nodes lookup - use same connection
                    rating["circle_num_nodes"] = None
                    print(f"🔍 viewer_uid={viewer_uid}, profile_id={profile_id}")
                    if viewer_uid and profile_id and viewer_uid != profile_id:
                        print(
                            f"🔍 Looking up circle for viewer={viewer_uid} -> reviewer={profile_id}"
                        )
                        circle_check = db2.select(
                            "every_circle.circles",
                            where={
                                "circle_profile_id": viewer_uid,
                                "circle_related_person_id": profile_id,
                            },
                        )
                        if not circle_check.get("result"):
                            circle_check = db2.select(
                                "every_circle.circles",
                                where={
                                    "circle_profile_id": profile_id,
                                    "circle_related_person_id": viewer_uid,
                                },
                            )

                        stored_nodes = None
                        if circle_check.get("result"):
                            stored_nodes = circle_check["result"][0].get(
                                "circle_num_nodes"
                            )
                            print(
                                f"🔍 Found circle record, stored_nodes={stored_nodes}"
                            )
                        else:
                            print(f"🔍 No circle record found in either direction")

                        if stored_nodes is not None:
                            rating["circle_num_nodes"] = int(stored_nodes)
                        else:
                            try:
                                connections_path = ConnectionsPath()
                                path_response, path_status = connections_path.get(
                                    viewer_uid, profile_id
                                )
                                print(f"🔍 path_status: {path_status}")
                                print(f"🔍 path_response: {path_response}")
                                if path_status == 200 and path_response.get(
                                    "combined_path"
                                ):
                                    nodes = path_response["combined_path"].split(",")
                                    rating["circle_num_nodes"] = int(len(nodes) - 1)
                                    print(
                                        f"🔍 Calculated circle_num_nodes for {viewer_uid} -> {profile_id}: {rating['circle_num_nodes']}"
                                    )
                                else:
                                    print(
                                        f"🔍 ConnectionsPath returned no combined_path for {viewer_uid} -> {profile_id}"
                                    )
                            except Exception as e:
                                print(f"Could not calculate circle_num_nodes: {e}")
                                import traceback

                                traceback.print_exc()

            # Sort by circle_num_nodes: lowest first, None (not in network) at the end
            if uid[:3] == "200" and viewer_uid:
                response["result"].sort(
                    key=lambda r: (
                        r["circle_num_nodes"] is None,
                        (
                            int(r["circle_num_nodes"])
                            if r["circle_num_nodes"] is not None
                            else 0
                        ),
                    )
                )
            return response, 200

        except Exception as e:
            print(f"Error in Ratings GET: {e}")
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def post(self):
        print("In Rating POST")
        response = {}
        rating_payload = {}

        # def check_type(sub_type, business_uid):
        #     print("In Check type")
        #     with connect() as db:
        #         type_query = db.select('every_circle.types', where={'sub_type': sub_type})
        #         if not type_query['result']:
        #             type_stored_procedure_response = db.call(procedure='new_type_uid')
        #             type_uid = type_stored_procedure_response['result'][0]['new_id']

        #             type_payload = {}
        #             type_payload['type_uid'] = type_uid
        #             type_payload['sub_type'] = sub_type
        #             type_insert_query = db.insert('every_circle.types', type_payload)

        #         else:
        #             type_uid = type_query['result'][0]['type_uid']

        #         print(type_uid)
        #         business_type_stored_procedure_response = db.call(procedure='new_bt_uid')
        #         bt_uid = business_type_stored_procedure_response['result'][0]['new_id']
        #         business_type_payload = {}
        #         business_type_payload['bt_uid'] = bt_uid
        #         business_type_payload['bt_business_id'] = business_uid
        #         business_type_payload['bt_type_id'] = type_uid
        #         business_type_insert_query = db.insert('every_circle.business_type', business_type_payload)

        # def check_business(payload, business_google_id=None, business_name=None):
        #     print("In Check Business")
        #     if business_google_id:
        #         business_query = db.select('every_circle.business', where={'business_google_id': business_google_id})
        #     elif business_name:
        #         business_query = db.select('every_circle.business', where={'business_name': business_name})

        #     if not business_query['result']:
        #         print("In NOT")
        #         business_stored_procedure_response = db.call(procedure='new_business_uid')
        #         business_uid = business_stored_procedure_response['result'][0]['new_id']

        #         if 'rating_business_types' in payload:
        #             business_types = payload.pop('rating_business_types')
        #             print('\n' + business_types)
        #             business_types = ast.literal_eval(business_types)
        #             print(business_types)
        #             for business_type in business_types:
        #                 check_type(business_type, business_uid)

        #         business_payload = {}
        #         business_payload['business_uid'] = business_uid
        #         business_payload['business_google_id'] = business_google_id
        #         business_payload['business_name'] = business_name
        #         business_payload['business_owner_fn'] = payload.pop('rating_business_owner_fn', None)
        #         business_payload['business_owner_ln'] = payload.pop('rating_business_owner_ln', None)
        #         business_payload['business_phone_number'] = payload.pop('rating_business_phone_number', None)
        #         business_payload['business_email_id'] = payload.pop('rating_business_email_id', None)
        #         business_payload['business_address_line_1'] = payload.pop('rating_business_address_line_1', None)
        #         business_payload['business_address_line_2'] = payload.pop('rating_business_address_line_2', None)
        #         business_payload['business_city'] = payload.pop('rating_business_city', None)
        #         business_payload['business_state'] = payload.pop('rating_business_state', None)
        #         business_payload['business_country'] = payload.pop('rating_business_country', None)
        #         business_payload['business_zip_code'] = payload.pop('rating_business_zip_code', None)
        #         business_payload['business_latitude'] = payload.pop('rating_business_latitude', None)
        #         business_payload['business_longitude'] = payload.pop('rating_business_longitude', None)
        #         business_payload['business_yelp'] = payload.pop('rating_business_yelp', None)
        #         business_payload['business_website'] = payload.pop('rating_business_website', None)
        #         business_payload['business_price_level'] = payload.pop('rating_business_price_level', None)
        #         business_payload['business_google_rating'] = payload.pop('rating_business_google_rating', None)
        #         if 'rating_business_google_photos' in payload:
        #             business_payload['business_google_photos'] = json.dumps(ast.literal_eval(payload.pop('rating_business_google_photos')))
        #         business_payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        #         print('business_payload', business_payload)

        #         business_insert_query = db.insert('every_circle.business', business_payload)

        #         print(business_insert_query)

        #     else:
        #         business_uid = business_query['result'][0]['business_uid']
        #         print("in else", business_uid)
        #         for key in list(payload.keys()):
        #             if 'business' in key:
        #                 print(key, 'key')
        #                 payload.pop(key)

        #     return business_uid, payload

        try:
            payload = request.form.to_dict()
            print("payload: ", payload)

            if "rating_profile_id" not in payload:
                response["message"] = "rating_profile_id is required"
                response["code"] = 400
                return response, 400

            profile_uid = payload.pop("rating_profile_id")
            business_google_id = payload.pop("rating_business_id")

            with connect() as db:

                # Check if the user exists
                # user_exists_query = db.select('every_circle.profile', where={'profile_uid': profile_uid})
                # if not user_exists_query['result']:
                #     response['message'] = 'User does not exist'
                #     response['code'] = 404
                #     return response, 404
                # print("User exists")

                # business_google_id = payload.pop('rating_business_id', None)
                # # business_name = payload.pop('rating_business_name', None)

                # if not business_google_id:
                #     response['message'] = 'rating_business_google_id is required'
                #     response['code'] = 400
                #     return response, 400

                # # business_uid, payload = check_business(payload, business_google_id, business_name)
                # print("Business Check Completed")

                rating_stored_procedure_response = db.call(procedure="new_rating_uid")
                new_rating_uid = rating_stored_procedure_response["result"][0]["new_id"]
                key = {"rating_uid": new_rating_uid}

                rating_payload["rating_uid"] = new_rating_uid
                rating_payload["rating_profile_id"] = profile_uid
                rating_payload["rating_business_id"] = business_google_id
                rating_payload["rating_star"] = payload.pop("rating_star")
                rating_payload["rating_description"] = payload.pop("rating_description")
                rating_payload["rating_receipt_date"] = payload.pop(
                    "rating_receipt_date"
                )
                # payload['rating_rating_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                rating_payload["rating_updated_at_timestamp"] = datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

                # processImage(key, rating_payload)

                response = db.insert("every_circle.ratings", rating_payload)

            response["rating_uid"] = new_rating_uid
            response["business_uid"] = business_google_id

            return response, 200

        except:
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def put(self):
        print("In rating PUT")
        response = {}

        try:
            payload = request.form.to_dict()

            if "rating_uid" not in payload:
                response["message"] = "rating_uid is required"
                response["code"] = 400
                return response, 400

            rating_uid = payload.pop("rating_uid")
            key = {"rating_uid": rating_uid}

            # rating response handling
            if "ratings_response" in payload:
                responding_profile_uid = payload.pop("responding_profile_uid", None)
                if not responding_profile_uid:
                    response["message"] = (
                        "responding_profile_uid is required to post a response"
                    )
                    response["code"] = 400
                    return response, 400

                with connect() as db:
                    # Get the business_id from the rating
                    rating_row = db.select("every_circle.ratings", where=key)
                    if not rating_row["result"]:
                        response["message"] = "Rating does not exist"
                        response["code"] = 404
                        return response, 404

                    business_uid = rating_row["result"][0].get("rating_business_id")

                    # Step 1: Get all bu_user_ids for this business
                    ownership_check = db.select(
                        "every_circle.business_user",
                        where={"bu_business_id": business_uid},
                    )
                    if not ownership_check["result"]:
                        response["message"] = "No owners found for this business"
                        response["code"] = 403
                        return response, 403

                    bu_user_ids = [
                        bu.get("bu_user_id")
                        for bu in ownership_check["result"]
                        if bu.get("bu_user_id")
                    ]
                    print("DEBUG bu_user_ids:", bu_user_ids)

                    # Step 2: For each bu_user_id look up the profile_personal_uid
                    is_authorized = False
                    for bu_user_id in bu_user_ids:
                        profile_check = db.select(
                            "every_circle.profile_personal",
                            where={"profile_personal_user_id": bu_user_id},
                        )
                        if profile_check.get("result"):
                            profile_uid = profile_check["result"][0].get(
                                "profile_personal_uid"
                            )
                            print("DEBUG profile_uid found:", profile_uid)
                            if str(profile_uid) == str(responding_profile_uid):
                                is_authorized = True
                                break

                    if not is_authorized:
                        response["message"] = (
                            "You are not authorized to respond to reviews for this business"
                        )
                        response["code"] = 403
                        return response, 403

            with connect() as db:

                # Check if the rating exists
                rating_exists_query = db.select("every_circle.ratings", where=key)
                print(rating_exists_query)
                if not rating_exists_query["result"]:
                    response["message"] = "ratings does not exist"
                    response["code"] = 404
                    return response, 404
                payload.pop("responding_profile_uid", None)
                processImage(key, payload)

                payload["rating_updated_at_timestamp"] = datetime.now().strftime(
                    "%Y-%m-%d %H:%M:%S"
                )

                response = db.update("every_circle.ratings", key, payload)

            return response, 200

        except Exception as e:
            print(f"Error in Ratings PUT: {e}")
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500
