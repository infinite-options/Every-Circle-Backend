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
                if uid[:3] == "100":
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

        try:
            payload = request.form.to_dict()

            if 'user_uid' not in payload or 'business_uid' not in payload:
                    response['message'] = 'Both user_uid and business_uid are required'
                    response['code'] = 400
                    return response, 400

            user_uid = payload.pop('user_uid')
            business_uid = payload.pop('business_uid')

            with connect() as db:

                # Check if the user exists
                user_exists_query = db.select('every_circle.users', where={'user_uid': user_uid})
                if not user_exists_query['result']:
                    response['message'] = 'User does not exist'
                    response['code'] = 404
                    return response, 404
                
                # Check if the business exists
                business_exists_query = db.select('every_circle.business', where={'business_uid': business_uid})
                if not business_exists_query['result']:
                    response['message'] = 'Business does not exist'
                    response['code'] = 404
                    return response, 404

                rating_stored_procedure_response = db.call(procedure='new_rating_uid')
                new_rating_uid = rating_stored_procedure_response['result'][0]['new_id']

                payload['rating_uid'] = new_rating_uid
                payload['rating_user_id'] = user_uid
                payload['rating_business_id'] = business_uid
                payload['rating_updated_at_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

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