from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage

class Profile(Resource):
    def get(self, uid):
        print("In Profile GET")
        response = {}
        try:
            print(uid, type(uid))
            with connect() as db:
                key = {}
                if uid[:3] == "100":
                    user_response = db.select('every_circle.users', where={'user_uid': uid})
                    if not user_response['result']:
                        response['message'] = f'No user found for {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    user_data = user_response['result'][0]
                    if user_data['user_role'] == "user":
                        response = db.select('every_circle.profile', where={'profile_user_id': uid})
                        profile_id = response['result'][0]['profile_uid']

                        rating_query = f'''
                                        SELECT *
                                        FROM every_circle.ratings
                                        LEFT JOIN every_circle.business ON rating_business_id = business_uid
                                        WHERE rating_profile_id = '{profile_id}';
                                        '''
                        
                        rating_response = db.execute(rating_query)

                        # rating_response = db.select('every_circle.ratings', where={'rating_profile_id': profile_id})
                        response['ratings result'] = rating_response['result']
                    else:
                        response = db.select('every_circle.business', where={'business_user_id': uid})
                    
                    if response['result']:
                        response['result'][0]['user_role'] = user_data['user_role']
                    else:
                        response['result'] = [{'user_role': user_data['user_role']}]
                    
                    return response, 200

                elif uid[:3] == "110":
                    key['profile_uid'] = uid
                    response = db.select('every_circle.profile', where={'profile_uid': uid})
                    rating_response = db.select('every_circle.ratings', where={'rating_profile_id': uid})

                    response['ratings result'] = rating_response['result']

                    return response, 200

                else:
                    response['message'] = 'Invalid UID'
                    response['code'] = 400
                    return response, 400

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500


    def post(self):
        print("In Profile POST")
        response = {}

        try:
            payload = request.form.to_dict()

            if 'user_uid' not in payload:
                    response['message'] = 'user_uid is required'
                    response['code'] = 400
                    return response, 400

            user_uid = payload.pop('user_uid')
            # referred_by_code = payload.pop('referred_by_code')

            with connect() as db:

                # Check if the user exists
                user_exists_query = db.select('every_circle.users', where={'user_uid': user_uid})
                if not user_exists_query['result']:
                    response['message'] = 'User does not exist'
                    response['code'] = 404
                    return response, 404

                profile_stored_procedure_response = db.call(procedure='new_profile_uid')
                new_profile_uid = profile_stored_procedure_response['result'][0]['new_id']
                key = {'profile_uid': new_profile_uid}

                if 'profile_referred_by_user_id' not in payload:
                    payload['profile_referred_by_user_id'] = "110-000001"
                elif payload['profile_referred_by_user_id'].strip() in ['', None, 'null']:
                    payload['profile_referred_by_user_id'] = "110-000001"

                payload['profile_uid'] = new_profile_uid
                payload['profile_user_id'] = user_uid
                payload['profile_updated_at_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                processImage(key, payload)

                print(payload)
                response = db.insert('every_circle.profile', payload)
                print(response)
            
            response['profile_uid'] = new_profile_uid
            response['message'] = 'Profile created successfully'

            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500


    def put(self):
        print("In Profile PUT")
        response = {}

        try:
            payload = request.form.to_dict()

            if 'profile_uid' not in payload:
                    response['message'] = 'profile_uid is required'
                    response['code'] = 400
                    return response, 400

            profile_uid = payload.pop('profile_uid')
            key = {'profile_uid': profile_uid}

            with connect() as db:

                # Check if the profile exists
                profile_exists_query = db.select('every_circle.profile', where=key)
                print(profile_exists_query)
                if not profile_exists_query['result']:
                    response['message'] = 'Profile does not exist'
                    response['code'] = 404
                    return response, 404
                
                processImage(key, payload)

                payload['profile_updated_at_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                response = db.update('every_circle.profile', key, payload)
            
            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500