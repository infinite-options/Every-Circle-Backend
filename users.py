from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest

from data_ec import connect, uploadImage, s3


class UserInfo(Resource):

    def get(self, user_id):
        try:
            print("In UserInfo GET")
            print(user_id)

            with connect() as db:
                userQuery = db.execute("""                     
                        SELECT *
                        FROM every_circle.users 
                        WHERE user_uid = \'""" + user_id + """\';
                        """)
                # print(userQuery)                                    

                if userQuery['code'] == 200 and int(len(userQuery['result']) > 0):                
                    print(userQuery['result'][0]['user_uid'])
                    return userQuery
                else:                
                    abort(404, description="User not found")

        except Exception as e:
            return {"code": 404, "message": str(e)}, 404

    def put(self):
        print("In Update User")
        try:
            payload = request.get_json(silent=True) or {}
            print(payload)

            from auth import bind_actor, get_current_user_uid, jwt_auth_required

            requested = payload.get("user_uid")
            if jwt_auth_required() and not requested:
                requested = get_current_user_uid()
            actor, error = bind_actor(requested)
            if error:
                return error, error["code"]
            if not actor:
                raise BadRequest("Request failed, no UID in payload.")

            # Flag on: persist against the JWT user even if the client sent a
            # matching profile_id as user_uid.
            user_uid = get_current_user_uid() if jwt_auth_required() else actor
            payload.pop("user_uid", None)
            key = {"user_uid": user_uid}
            print(key)

            phone_sync = None
            if "user_phone_number" in payload:
                from auth import (
                    normalize_phone_for_storage,
                    sync_user_phone_from_profile_edit,
                )

                stored_phone = normalize_phone_for_storage(payload.get("user_phone_number"))
                # Drop client-controlled verified flag; sync helper owns it.
                payload.pop("user_phone_verified", None)

            with connect() as db:
                if "user_phone_number" in payload:
                    phone_sync = sync_user_phone_from_profile_edit(
                        db, user_uid, stored_phone
                    )
                    # sync_user_phone_from_profile_edit already wrote users phone fields
                    payload.pop("user_phone_number", None)
                    # Mirror onto personal profile when present
                    profile_res = db.select(
                        "every_circle.profile_personal",
                        where={"profile_personal_user_id": user_uid},
                    )
                    profiles = (profile_res or {}).get("result") or []
                    if profiles and profiles[0].get("profile_personal_uid"):
                        db.update(
                            "every_circle.profile_personal",
                            {
                                "profile_personal_uid": profiles[0][
                                    "profile_personal_uid"
                                ]
                            },
                            {
                                "profile_personal_phone_number": phone_sync.get(
                                    "phone_number"
                                )
                            },
                        )

                if payload:
                    response = db.update("every_circle.users", key, payload)
                else:
                    response = {"code": 200, "message": "Success"}

            if phone_sync is not None and isinstance(response, dict):
                response["phone_number"] = phone_sync.get("phone_number")
                response["phone_verified"] = phone_sync.get("phone_verified")
                response["phone_needs_verification"] = phone_sync.get(
                    "phone_needs_verification"
                )
            return response

        except BadRequest as e:
            return {"code": 400, "message": str(e)}, 400
        except Exception as e:
            return {"code": 500, "message": str(e)}, 500