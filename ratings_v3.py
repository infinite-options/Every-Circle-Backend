# from flask import request
# from flask_restful import Resource
# import traceback
# from datetime import datetime
# import requests
# from data_ec import connect, processImage
# import json

# class Ratings_v3(Resource):
#     def __init__(self):
#         self.business_api_url = "https://ioEC2testsspm.infiniteoptions.com/api/v3/business_v3"  # Update with your actual API URL

#     def get(self, uid):
#         print("In Ratings GET")
#         response = {}
#         try:
#             print(uid, type(uid))
#             with connect() as db:
#                 key = {}
#                 if uid[:3] == "110":
#                     query = f'''
#                                 SELECT *
#                                 FROM every_circle.ratings
#                                 LEFT JOIN every_circle.business ON rating_business_id = business_uid
#                                 WHERE rating_profile_id = '{uid}';   
#                             '''
                    
#                     response = db.execute(query)
                    
#                 elif uid[:3] == "200":
#                     query = f'''
#                                 SELECT *
#                                 FROM every_circle.ratings
#                                 LEFT JOIN every_circle.business ON rating_business_id = business_uid
#                                 WHERE rating_business_id = '{uid}';
#                             '''
#                     response = db.execute(query)

#                 else:
#                     response['message'] = 'Invalid UID'
#                     response['code'] = 400
#                     return response, 400
            
#             if not response['result']:
#                 response.pop('result')
#                 response['message'] = f'No ratings found for {key}'
#                 response['code'] = 404
#                 return response, 404

#             return response, 200

#         except Exception as e:
#             print(f"Error in Ratings GET: {str(e)}")
#             traceback.print_exc()
#             response['message'] = 'Internal Server Error'
#             response['code'] = 500
#             return response, 500

#     def post(self):
#         print("In Rating POST")
#         response = {}

#         try:
#             payload = request.form.to_dict()
#             print(f"Received payload: {payload}")

#             if 'profile_uid' not in payload:
#                 response['message'] = 'profile_uid is required'
#                 response['code'] = 400
#                 return response, 400

#             profile_uid = payload.pop('profile_uid')

#             with connect() as db:
#                 # Check if the user exists
#                 user_exists_query = db.select('every_circle.profile', where={'profile_uid': profile_uid})
#                 if not user_exists_query['result']:
#                     response['message'] = 'User does not exist'
#                     response['code'] = 404
#                     return response, 404
                
#                 user_uid = user_exists_query['result'][0]['profile_user_id']
#                 business_google_id = payload.get('rating_business_google_id')
#                 business_name = payload.get('rating_business_name')

#                 if not business_google_id and not business_name:
#                     response['message'] = 'rating_business_google_id or rating_business_name is required'
#                     response['code'] = 400
#                     return response, 400

#                 # Check if business exists first
#                 business_query = None
#                 if business_google_id:
#                     business_query = db.select('every_circle.business', where={'business_google_id': business_google_id})
#                 elif business_name:
#                     business_query = db.select('every_circle.business', where={'business_name': business_name})

#                 business_uid = None
#                 if not business_query or not business_query['result']:
#                     # Business doesn't exist, create it using Business_v3 API
#                     print("Business not found, creating new business...")
                    
#                     # Prepare business payload
#                     business_payload = {
#                         'user_uid': user_uid,  # Using profile_uid as user_uid for business creation
#                         'business_google_id': business_google_id,
#                         'business_name': business_name,
#                         'business_owner_fn': payload.get('rating_business_owner_fn'),
#                         'business_owner_ln': payload.get('rating_business_owner_ln'),
#                         'business_phone_number': payload.get('rating_business_phone_number'),
#                         'business_email_id': payload.get('rating_business_email_id'),
#                         'business_address_line_1': payload.get('rating_business_address_line_1'),
#                         'business_address_line_2': payload.get('rating_business_address_line_2'),
#                         'business_city': payload.get('rating_business_city'),
#                         'business_state': payload.get('rating_business_state'),
#                         'business_country': payload.get('rating_business_country'),
#                         'business_zip_code': payload.get('rating_business_zip_code'),
#                         'business_latitude': payload.get('rating_business_latitude'),
#                         'business_longitude': payload.get('rating_business_longitude'),
#                         'business_yelp': payload.get('rating_business_yelp'),
#                         'business_website': payload.get('rating_business_website'),
#                         'business_price_level': payload.get('rating_business_price_level'),
#                         'business_google_rating': payload.get('rating_business_google_rating'),
#                         'business_google_photos': payload.get('rating_business_google_photos'),
#                     }
                    
#                     # Clean None values and empty strings
#                     business_payload = {k: v for k, v in business_payload.items() if v is not None and v != ''}

#                     # Make POST request to Business API
#                     print(f"Calling Business API with payload: {business_payload}")
#                     business_response = requests.post(
#                         self.business_api_url,
#                         data=business_payload,
#                         files=request.files  # Pass any files from the original request
#                     )
                    
#                     if business_response.status_code != 200:
#                         response['message'] = f'Failed to create business: {business_response.json().get("message")}'
#                         response['code'] = business_response.status_code
#                         return response, business_response.status_code
                    
#                     business_data = business_response.json()
#                     business_uid = business_data.get('business_uid')
#                     print(f"Business created successfully with UID: {business_uid}")
#                 else:
#                     business_uid = business_query['result'][0]['business_uid']
#                     print(f"Using existing business with UID: {business_uid}")

#                 # Create new rating
#                 rating_stored_procedure_response = db.call(procedure='new_rating_uid')
#                 new_rating_uid = rating_stored_procedure_response['result'][0]['new_id']
                
#                 # Clean payload of business-related fields
#                 for key in list(payload.keys()):
#                     if 'business' in key:
#                         payload.pop(key)

#                 # Prepare rating payload
#                 rating_payload = {
#                     'rating_uid': new_rating_uid,
#                     'rating_profile_id': profile_uid,
#                     'rating_business_id': business_uid,
#                     'rating_star': payload.get('rating_star'),
#                     'rating_description': payload.get('rating_description'),
#                     'rating_updated_at_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                 }

#                 # Clean None values
#                 rating_payload = {k: v for k, v in rating_payload.items() if v is not None}

#                 key = {'rating_uid': new_rating_uid}
#                 processImage(key, rating_payload)

#                 # Insert rating
#                 response = db.insert('every_circle.ratings', rating_payload)
                
#                 if response.get('code') == 200:
#                     response['rating_uid'] = new_rating_uid
#                     response['business_uid'] = business_uid

#             return response, 200
        
#         except Exception as e:
#             print(f"Error in Rating POST: {str(e)}")
#             traceback.print_exc()
#             response['message'] = 'Internal Server Error'
#             response['code'] = 500
#             return response, 500

#     def put(self):
#         print("In rating PUT")
#         response = {}

#         try:
#             payload = request.form.to_dict()

#             if 'rating_uid' not in payload:
#                 response['message'] = 'rating_uid is required'
#                 response['code'] = 400
#                 return response, 400

#             rating_uid = payload.pop('rating_uid')
#             key = {'rating_uid': rating_uid}

#             with connect() as db:
#                 # Check if the rating exists
#                 rating_exists_query = db.select('every_circle.ratings', where=key)
#                 if not rating_exists_query['result']:
#                     response['message'] = 'ratings does not exist'
#                     response['code'] = 404
#                     return response, 404
                
#                 processImage(key, payload)
                
#                 payload['rating_updated_at_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                 response = db.update('every_circle.ratings', key, payload)
            
#             return response, 200
        
#         except Exception as e:
#             print(f"Error in Rating PUT: {str(e)}")
#             traceback.print_exc()
#             response['message'] = 'Internal Server Error'
#             response['code'] = 500
#             return response, 500
        

from flask import request
from flask_restful import Resource
import traceback
from datetime import datetime
import requests
import json
from data_ec import connect, processImage

class Ratings_v3(Resource):
    def __init__(self):
        # Update these URLs based on your environment
        self.business_api_url = "https://ioEC2testsspm.infiniteoptions.com/api/v3/business_v3"
        self.tag_generator_url = "https://ioEC2testsspm.infiniteoptions.com/api/v1/taggenerator"

    def get_generated_tags(self, business_info: dict) -> list:
        """Get tags from Tag Generator API"""
        try:
            tag_payload = {
                "business_name": business_info.get('rating_business_name'),
                "business_description": business_info.get('rating_business_description'),
                "business_city": business_info.get('rating_business_city'),
                "business_state": business_info.get('rating_business_state'),
                "business_website": business_info.get('rating_business_website')
            }

            print(f"Calling Tag Generator API with payload: {tag_payload}")
            tag_response = requests.post(self.tag_generator_url, json=tag_payload)
            
            if tag_response.status_code != 200:
                print(f"Tag Generator API error: {tag_response.text}")
                return []
            
            generated_tags = tag_response.json().get('tags', [])
            print(f"Generated tags: {generated_tags}")
            return generated_tags

        except Exception as e:
            print(f"Error calling Tag Generator API: {str(e)}")
            traceback.print_exc()
            return []

    def get(self, uid):
        print("In Ratings GET")
        response = {}
        try:
            print(uid, type(uid))
            with connect() as db:
                key = {}
                if uid[:3] == "110":
                    query = f'''
                                SELECT *
                                FROM every_circle.ratings
                                LEFT JOIN every_circle.business ON rating_business_id = business_uid
                                WHERE rating_profile_id = '{uid}';   
                            '''
                    
                    response = db.execute(query)
                    
                elif uid[:3] == "200":
                    query = f'''
                                SELECT *
                                FROM every_circle.ratings
                                LEFT JOIN every_circle.business ON rating_business_id = business_uid
                                WHERE rating_business_id = '{uid}';
                            '''
                    response = db.execute(query)

                else:
                    response['message'] = 'Invalid UID'
                    response['code'] = 400
                    return response, 400
            
            if not response['result']:
                response.pop('result')
                response['message'] = f'No ratings found for {key}'
                response['code'] = 404
                return response, 404

            return response, 200

        except Exception as e:
            print(f"Error in Ratings GET: {str(e)}")
            traceback.print_exc()
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def post(self):
        print("In Rating POST")
        response = {}

        try:
            payload = request.form.to_dict()
            print(f"Received payload: {payload}")

            if 'profile_uid' not in payload:
                response['message'] = 'profile_uid is required'
                response['code'] = 400
                return response, 400

            profile_uid = payload.pop('profile_uid')

            with connect() as db:
                # Check if user exists
                user_exists_query = db.select('every_circle.profile', where={'profile_uid': profile_uid})
                if not user_exists_query['result']:
                    response['message'] = 'User does not exist'
                    response['code'] = 404
                    return response, 404
                
                user_uid = user_exists_query['result'][0]['profile_user_id']
                business_google_id = payload.get('rating_business_google_id')
                business_name = payload.get('rating_business_name')

                if not business_google_id and not business_name:
                    response['message'] = 'rating_business_google_id or rating_business_name is required'
                    response['code'] = 400
                    return response, 400

                # Check if business exists
                business_query = None
                if business_google_id:
                    business_query = db.select('every_circle.business', where={'business_google_id': business_google_id})
                elif business_name:
                    business_query = db.select('every_circle.business', where={'business_name': business_name})

                business_uid = None
                if not business_query or not business_query['result']:
                    # Business doesn't exist, get tags and create it
                    print("Business not found, generating tags and creating new business...")
                    
                    # Get tags from Tag Generator API
                    generated_tags = self.get_generated_tags(payload)
                    
                    # Add any custom tags if provided in the payload
                    custom_tags = []
                    if 'rating_business_custom_tags' in payload:
                        try:
                            custom_tags = json.loads(payload.pop('rating_business_custom_tags'))
                        except json.JSONDecodeError:
                            print("Failed to parse custom tags, ignoring them")
                    
                    # Combine generated and custom tags
                    all_tags = list(set(generated_tags + custom_tags))
                    
                    # Prepare business payload
                    business_payload = {
                        'user_uid': user_uid,  # Using profile_uid as user_uid for business creation
                        'business_google_id': business_google_id,
                        'business_name': business_name,
                        'business_owner_fn': payload.get('rating_business_owner_fn'),
                        'business_owner_ln': payload.get('rating_business_owner_ln'),
                        'business_phone_number': payload.get('rating_business_phone_number'),
                        'business_email_id': payload.get('rating_business_email_id'),
                        'business_address_line_1': payload.get('rating_business_address_line_1'),
                        'business_address_line_2': payload.get('rating_business_address_line_2'),
                        'business_city': payload.get('rating_business_city'),
                        'business_state': payload.get('rating_business_state'),
                        'business_country': payload.get('rating_business_country'),
                        'business_zip_code': payload.get('rating_business_zip_code'),
                        'business_latitude': payload.get('rating_business_latitude'),
                        'business_longitude': payload.get('rating_business_longitude'),
                        'business_yelp': payload.get('rating_business_yelp'),
                        'business_website': payload.get('rating_business_website'),
                        'business_price_level': payload.get('rating_business_price_level'),
                        'business_google_rating': payload.get('rating_business_google_rating'),
                        'business_google_photos': payload.get('rating_business_google_photos'),
                        'business_tags': json.dumps(all_tags)  # Add tags to business payload
                    }
                    
                    # Clean None values and empty strings
                    business_payload = {k: v for k, v in business_payload.items() if v is not None and v != ''}

                    # Call Business API
                    print(f"Calling Business API with payload: {business_payload}")
                    files = {}
                    if request.files:
                        files = request.files
                    business_response = requests.post(
                        self.business_api_url,
                        data=business_payload,
                        files=files
                    )
                    
                    if business_response.status_code != 200:
                        response['message'] = f'Failed to create business: {business_response.json().get("message")}'
                        response['code'] = business_response.status_code
                        return response, business_response.status_code
                    
                    business_data = business_response.json()
                    business_uid = business_data.get('business_uid')
                    print(f"Business created successfully with UID: {business_uid}")
                else:
                    business_uid = business_query['result'][0]['business_uid']
                    print(f"Using existing business with UID: {business_uid}")

                # Create new rating
                rating_stored_procedure_response = db.call(procedure='new_rating_uid')
                new_rating_uid = rating_stored_procedure_response['result'][0]['new_id']
                
                # Clean payload of business-related fields
                for key in list(payload.keys()):
                    if 'business' in key:
                        payload.pop(key)

                # Prepare rating payload
                rating_payload = {
                    'rating_uid': new_rating_uid,
                    'rating_profile_id': profile_uid,
                    'rating_business_id': business_uid,
                    'rating_star': payload.get('rating_star'),
                    'rating_description': payload.get('rating_description'),
                    'rating_receipt_date': payload.get('rating_receipt_date'),
                    'rating_updated_at_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                # Clean None values
                rating_payload = {k: v for k, v in rating_payload.items() if v is not None}

                key = {'rating_uid': new_rating_uid}
                processImage(key, rating_payload)

                # Insert rating
                response = db.insert('every_circle.ratings', rating_payload)
                
                if response.get('code') == 200:
                    response['rating_uid'] = new_rating_uid
                    response['business_uid'] = business_uid

            return response, 200
        
        except Exception as e:
            print(f"Error in Rating POST: {str(e)}")
            traceback.print_exc()
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
                if not rating_exists_query['result']:
                    response['message'] = 'ratings does not exist'
                    response['code'] = 404
                    return response, 404
                
                processImage(key, payload)
                
                payload['rating_updated_at_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                response = db.update('every_circle.ratings', key, payload)
            
            return response, 200
        
        except Exception as e:
            print(f"Error in Rating PUT: {str(e)}")
            traceback.print_exc()
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500