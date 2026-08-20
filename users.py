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

            with connect() as db:
                response = db.update("every_circle.users", key, payload)

            return response

        except BadRequest as e:
            return {"code": 400, "message": str(e)}, 400
        except Exception as e:
            return {"code": 500, "message": str(e)}, 500