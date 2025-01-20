from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage


class Business(Resource):
    def get(self):
        pass

    def post(self):
        print("In Business POST")
        response = {}

        try:
            payload = request.form.to_dict()

            if 'user_uid' not in payload:
                    response['message'] = 'user_uid is required to register a business'
                    response['code'] = 400
                    return response, 400

            user_uid = payload.pop('user_uid')

            with connect() as db:

                # Check if the user exists
                user_exists_query = db.select('every_circle.users', where={'user_uid': user_uid})
                if not user_exists_query['result']:
                    response['message'] = 'User does not exist'
                    response['code'] = 404
                    return response, 404

                business_stored_procedure_response = db.call(procedure='new_business_uid')
                new_business_uid = business_stored_procedure_response['result'][0]['new_id']

                payload['business_uid'] = new_business_uid
                payload['business_user_id'] = user_uid
                payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                response = db.insert('every_circle.business', payload)
            
            response['business_uid'] = new_business_uid

            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def put(self):
        print("In Business PUT")
        response = {}

        try:
            payload = request.form.to_dict()

            if 'business_uid' not in payload:
                    response['message'] = 'business_uid is required'
                    response['code'] = 400
                    return response, 400

            business_uid = payload.pop('business_uid')
            key = {'business_uid': business_uid}

            with connect() as db:

                # Check if the business exists
                business_exists_query = db.select('every_circle.business', where=key)
                print(business_exists_query)
                if not business_exists_query['result']:
                    response['message'] = 'Business does not exist'
                    response['code'] = 404
                    return response, 404
                
                processImage(key, payload)
                
                response = db.update('every_circle.business', key, payload)
            
            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500