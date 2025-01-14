from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3

class Connections(Resource):
    def post(self):
        print("\nIn Connections POST")
        response = {}
        try:
            payload = request.get_json()
            
            print(payload)

            if 'new_user_id' not in payload or 'referral_code' not in payload:
                response['message'] = 'Both new_user_uid and referral_code are required'
                response['code'] = 400
                return response, 400

            new_user_id = payload.pop('new_user_id')
            referral_code = payload.pop('referral_code')

            with connect() as db:
                connection_stored_procedure_response = db.call(procedure='new_connection_uid')
                new_connection_uid = connection_stored_procedure_response['result'][0]['new_id']
            
                payload['connection_uid'] = new_connection_uid
                payload['connection_existing_user_id'] = "100-000001"
                payload['connection_new_user_id'] = new_user_id
                payload['connection_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                if referral_code == "12345":
                    response = db.insert('every_circle.connections', payload)
            
            response['connection_uid'] = new_connection_uid
            
            return response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500