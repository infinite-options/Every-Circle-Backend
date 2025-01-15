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
            with connect() as db:
                payload = request.get_json()
                print(payload)

                if payload["user_uid"] is None:
                    raise BadRequest("Request failed, no UID in payload.")
                
                key = {'user_uid': payload.pop('user_uid')}
                print(key)
                # print(payload)
                
                with connect() as db:
                    response = db.update('every_circle.users', key, payload)

                return response
            
        except Exception as e:
            return {"code": 500, "message": str(e)}, 500