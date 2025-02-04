from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime
import ast

from data_ec import connect, uploadImage, s3, processImage


class Business(Resource):
    def get(self, uid):
        print("In Business GET")
        response = {}
        try:
            print(uid, type(uid))
            with connect() as db:
                key = {}
                if uid[:3] == "200":
                    key['business_uid'] = uid
                
                elif uid[:3] == "210":
                    key['business_category_id'] = uid
                
                elif uid[:3] == "100":
                    key['business_user_id'] = uid

                else:
                    response['message'] = 'Invalid UID'
                    response['code'] = 400
                    return response, 400
            
                response = db.select('every_circle.business', where=key)

            if not response['result']:
                response.pop('result')
                response['message'] = f'No business found for {key}'
                response['code'] = 404
                return response, 404

            return response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def post(self):
        print("In Business POST")
        response = {}

        def check_category(sub_category, business_uid):
            print("In Check Category")
            with connect() as db:
                category_query = db.select('every_circle.category', where={'sub_category': sub_category})
                if not category_query['result']:
                    category_stored_procedure_response = db.call(procedure='new_category_uid')
                    category_uid = category_stored_procedure_response['result'][0]['new_id']

                    category_payload = {}
                    category_payload['category_uid'] = category_uid
                    category_payload['sub_category'] = sub_category
                    category_insert_query = db.insert('every_circle.category', category_payload)
                
                else:
                    category_uid = category_query['result'][0]['category_uid']
                
                print(category_uid)
                business_type_stored_procedure_response = db.call(procedure='new_bt_uid')
                bt_uid = business_type_stored_procedure_response['result'][0]['new_id']
                business_type_payload = {}
                business_type_payload['bt_uid'] = bt_uid
                business_type_payload['bt_business_id'] = business_uid
                business_type_payload['bt_category_id'] = category_uid
                business_type_insert_query = db.insert('every_circle.business_type', business_type_payload)

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
                
                business_google_id = payload.get('business_google_id', None)
                business_name = payload.get('business_name', None)

                if not business_google_id and not business_name:
                    response['message'] = 'business_google_id or business_name is required'
                    response['code'] = 400
                    return response, 400

                if business_google_id:
                    business_query = db.select('every_circle.business', where={'business_google_id': business_google_id})
                elif business_name:
                    business_query = db.select('every_circle.business', where={'business_name': business_name})
                
                if not business_query['result']:
                    business_stored_procedure_response = db.call(procedure='new_business_uid')
                    new_business_uid = business_stored_procedure_response['result'][0]['new_id']
                    key = {'business_uid': new_business_uid}

                    if 'business_types' in payload:
                        business_types = payload.pop('business_types')
                        print('\n' + business_types)
                        business_types = ast.literal_eval(business_types)
                        print(business_types)
                        for business_type in business_types:
                            check_category(business_type, new_business_uid)

                    payload['business_uid'] = new_business_uid
                    payload['business_user_id'] = user_uid
                    payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    processImage(key, payload)

                    response = db.insert('every_circle.business', payload)
                
                else:
                    response['message'] = 'Business already exists'
                    response['code'] = 409
                    return response, 409
            
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