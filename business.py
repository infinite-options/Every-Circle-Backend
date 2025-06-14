from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime
import ast

from data_ec import connect, uploadImage, s3, processImage

# add google social id in GET api
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
                    key['business_type_id'] = uid
                
                elif uid[:3] == "100":
                    key['business_user_id'] = uid

                else:
                    key['business_google_id'] = uid
                    # response['message'] = 'Invalid UID'
                    # response['code'] = 400
                    # return response, 400
            
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

        def check_type(sub_type, business_uid):
            print("In Check Type")
            with connect() as db:
                type_query = db.select('every_circle.types', where={'sub_type': sub_type})
                if not type_query['result']:
                    type_stored_procedure_response = db.call(procedure='new_type_uid')
                    type_uid = type_stored_procedure_response['result'][0]['new_id']

                    type_payload = {}
                    type_payload['type_uid'] = type_uid
                    type_payload['sub_type'] = sub_type
                    type_insert_query = db.insert('every_circle.types', type_payload)
                
                else:
                    type_uid = type_query['result'][0]['type_uid']
                
                print(type_uid)
                business_type_stored_procedure_response = db.call(procedure='new_bt_uid')
                bt_uid = business_type_stored_procedure_response['result'][0]['new_id']
                business_type_payload = {}
                business_type_payload['bt_uid'] = bt_uid
                business_type_payload['bt_business_id'] = business_uid
                business_type_payload['bt_type_id'] = type_uid
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
                            check_type(business_type, new_business_uid)

                    payload['business_uid'] = new_business_uid
                    payload['business_user_id'] = user_uid
                    payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    processImage(key, payload)

                    response = db.insert('every_circle.business', payload)
                
                else:
                    response['message'] = 'Business: Business already exists'
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

class Business_v2(Resource):
    def get(self, uid):
        print("In Business GET")
        response = {}
        try:
            print(uid, type(uid))
            with connect() as db:
                key = {}
                if uid[:3] == "200":
                    key['business_uid'] = uid
                
                elif uid[:3] == "100":
                    key['business_user_id'] = uid

                else:
                    key['business_google_id'] = uid

                response = db.select('every_circle.business', where=key)

            if not response['result']:
                response.pop('result')
                response['message'] = f'No business found for {key}'
                response['code'] = 404
                return response, 404

            # final_response = {}
            # for business in response['result']:
            #     print(business)
            #     query = f'''
            #                 SELECT *
            #                 FROM every_circle.business_category
            #                 WHERE bc_business_id = "{business['business_uid']}";
            #             '''

            #     category_response = db.execute(query)
            #     # category_response = db.select('every_circle.business_category', where={'bc_business_id': business['business_uid']})
            #     print(category_response, '\n\nCategory Response')
            #     final_response[business['business_uid']] = [business, category_response['result']]

            #     print(final_response)
            return response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def post(self):
        print("In Business POST")
        response = {}

        def check_category(category_uid, business_uid):
            print("In Check Category")
            with connect() as db:
                
                business_category_stored_procedure_response = db.call(procedure='new_bc_uid')
                bc_uid = business_category_stored_procedure_response['result'][0]['new_id']
                business_category_payload = {}
                business_category_payload['bc_uid'] = bc_uid
                business_category_payload['bc_business_id'] = business_uid
                business_category_payload['bc_category_id'] = category_uid
                business_category_insert_query = db.insert('every_circle.business_category', business_category_payload)

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
                    business_look_up_query = db.select('every_circle.business', where={'business_google_id': business_google_id})
                elif business_name:
                    business_look_up_query = db.select('every_circle.business', where={'business_name': business_name})
                
                if not business_look_up_query['result']:
                    business_stored_procedure_response = db.call(procedure='new_business_uid')
                    new_business_uid = business_stored_procedure_response['result'][0]['new_id']
                    key = {'business_uid': new_business_uid}

                    if 'business_categories_uid' in payload:
                        business_categories_uid = payload.pop('business_categories_uid')
                        print('\n' + business_categories_uid)
                        business_categories_uid = ast.literal_eval(business_categories_uid)
                        print(business_categories_uid)
                        for business_category_uid in business_categories_uid:
                            check_category(business_category_uid, new_business_uid)

                    payload['business_uid'] = new_business_uid
                    payload['business_user_id'] = user_uid
                    payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    processImage(key, payload)

                    response = db.insert('every_circle.business', payload)
                
                else:
                    response['message'] = 'Business v2: Business already exists'
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

        def check_category(category_uid, business_uid):
            print("In Check Category")
            with connect() as db:

                check_query = f'''
                                SELECT *
                                FROM every_circle.business_category
                                WHERE bc_business_id = "{business_uid}" AND bc_category_id = "{category_uid}";
                              '''
                check_query_response = db.execute(check_query)
                print('CHECK QUERY RESPOSNE', check_query_response)
                if len(check_query_response['result']) > 0:
                    return
                
                business_category_stored_procedure_response = db.call(procedure='new_bc_uid')
                bc_uid = business_category_stored_procedure_response['result'][0]['new_id']
                business_category_payload = {}
                business_category_payload['bc_uid'] = bc_uid
                business_category_payload['bc_business_id'] = business_uid
                business_category_payload['bc_category_id'] = category_uid
                business_category_insert_query = db.insert('every_circle.business_category', business_category_payload)
                print(business_category_insert_query)

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
                
                if 'business_categories_uid' in payload:
                    print("in business categories uid")
                    business_categories_uid = payload.pop('business_categories_uid')
                    print('\n' + business_categories_uid)
                    business_categories_uid = ast.literal_eval(business_categories_uid)
                    print(business_categories_uid)
                    for business_category_uid in business_categories_uid:
                        check_category(business_category_uid, business_uid)

                processImage(key, payload)
                
                response = db.update('every_circle.business', key, payload)
            
            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
        

class Businesses(Resource):
    def get(self):
        print("In Businesses GET")
        response = {}
        try:
            with connect() as db:
                # Get list of businesses
                business_list = f"""
                        SELECT business_uid, business_name FROM every_circle.business
                        ORDER BY business_name
                """
                businesses = db.execute(business_list)
                response = businesses['result']
                return response, 200
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500