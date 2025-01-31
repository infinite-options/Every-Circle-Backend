from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage


class Ratings(Resource):
    def get(self, uid):
        print("In Ratings GET")
        response = {}
        try:
            print(uid, type(uid))
            with connect() as db:
                key = {}
                if uid[:3] == "110":
                    key['rating_user_id'] = uid
                
                elif uid[:3] == "200":
                    key['rating_business_id'] = uid

                else:
                    response['message'] = 'Invalid UID'
                    response['code'] = 400
                    return response, 400
            
                response = db.select('every_circle.ratings', where=key)

            if not response['result']:
                response.pop('result')
                response['message'] = f'No ratings found for {key}'
                response['code'] = 404
                return response, 404

            return response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def post(self):
        print("In Rating POST")
        response = {}

        def check_business_exists(business_google_id=None, business_name=None):
            business_payload = {}

            if business_google_id:
                business_query = db.select('every_circle.business', where={'business_google_id': business_google_id})

                if not business_query['result']:
                    business_stored_procedure_response = db.call(procedure='new_business_uid')
                    business_uid = business_stored_procedure_response['result'][0]['new_id']
                    
                    business_payload['business_uid'] = business_uid         
                    business_payload['business_google_id'] = business_google_id
                    business_payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    business_insert_query = db.insert('every_circle.business', business_payload)

                else:
                    business_uid = business_query['result'][0]['business_uid']   

                return business_uid         

            elif business_name:
                business_query = db.select('every_circle.business', where={'business_name': business_name})
                
                if not business_query['result']:
                    business_stored_procedure_response = db.call(procedure='new_business_uid')
                    business_uid = business_stored_procedure_response['result'][0]['new_id']

                    business_payload['business_uid'] = business_uid   
                    business_payload['business_name'] = business_name      
                    business_payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    business_insert_query = db.insert('every_circle.business', business_payload)

                else:
                    business_uid = business_query['result'][0]['business_uid']
            
                return business_uid

        try:
            payload = request.form.to_dict()

            if 'profile_uid' not in payload:
                    response['message'] = 'profile_uid is required'
                    response['code'] = 400
                    return response, 400

            profile_uid = payload.pop('profile_uid')

            with connect() as db:

                # Check if the user exists
                user_exists_query = db.select('every_circle.profile', where={'profile_uid': profile_uid})
                if not user_exists_query['result']:
                    response['message'] = 'User does not exist'
                    response['code'] = 404
                    return response, 404

                if 'rating_business_google_id' in payload:
                    business_google_id = payload.pop('rating_business_google_id')
                    business_uid = check_business_exists(business_google_id, None)
                    payload['rating_business_id'] = business_uid

                elif 'rating_business_name' in payload:
                    business_name = payload.pop('rating_business_name')
                    business_uid = check_business_exists(None, business_name)
                    payload['rating_business_id'] = business_uid
                
                else:
                    response['message'] = 'rating_business_google_id or rating_business_name is required'
                    response['code'] = 400
                    return response, 400

                rating_stored_procedure_response = db.call(procedure='new_rating_uid')
                new_rating_uid = rating_stored_procedure_response['result'][0]['new_id']
                key = {'rating_uid': new_rating_uid}

                payload['rating_uid'] = new_rating_uid
                payload['rating_user_id'] = profile_uid
                payload['rating_updated_at_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                processImage(key, payload)

                response = db.insert('every_circle.ratings', payload)
            
            response['rating_uid'] = new_rating_uid

            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def put(self):
        print("In rating PUT")
        response = {}

        try:
            payload = request.form.to_dict()

            if 'rating_uid' not in payload:
                    response['message'] = 'rating_uid is required'
                    response['code'] = 400
                    return response, 400

            rating_uid = payload.pop('rating_uid')
            key = {'rating_uid': rating_uid}

            with connect() as db:

                # Check if the rating exists
                rating_exists_query = db.select('every_circle.ratings', where=key)
                print(rating_exists_query)
                if not rating_exists_query['result']:
                    response['message'] = 'ratings does not exist'
                    response['code'] = 404
                    return response, 404
                
                processImage(key, payload)
                
                payload['rating_updated_at_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                response = db.update('every_circle.ratings', key, payload)
            
            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500