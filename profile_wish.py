from flask import request, abort , jsonify, Response
from flask_restful import Resource
from collections import defaultdict
from datetime import datetime
import json
from data_ec import connect

class ProfileWishInfo(Resource):
    # def get(self, query):

    #     print('In ProfileWishInfo GET with query', query)
        
    #     run_query = f"""
    #                     select distinct profile_wish_profile_personal_id  
    #                         FROM every_circle.profile_wish
    #                         WHERE lower(profile_wish_title) LIKE lower('%{query}%') OR 
    #                         lower(profile_wish_description) LIKE lower('%{query}%');
    #                     """

    #     try:
    #         with connect() as db:
    #             response = db.execute(run_query, cmd='get')

    #         if not response['result']:
    #             response['message'] = f"No item found"
    #             response['code'] = 404
    #             return response, 404

    #         return response, 200

    #     except Exception as e:
    #         print(f"Error Middle Layer: {str(e)}")
    #         response['message'] = f"Internal Server Error: {str(e)}"
    #         response['code'] = 500
    #         return response, 500
        
        # return store, 200

    def get(self, profile_id):
        # Determine which endpoint was called based on available parameters
        
        print(f"In ProfileWishInfo GET - Wish Responses for profile: {profile_id}")
        response = {}
        
        try:
            if not profile_id:
                response['message'] = 'profile_id is required'
                response['code'] = 400
                return response, 400
            
            with connect() as db:
                # Query to get wish responses with responder profile information
                wishes_responses_query = """
                    SELECT every_circle.wish_response.*,
                        profile_personal_uid, profile_personal_user_id, profile_personal_referred_by, 
                        profile_personal_first_name, profile_personal_last_name, profile_personal_email_is_public, 
                        profile_personal_phone_number, profile_personal_phone_number_is_public, 
                        profile_personal_city, profile_personal_state, profile_personal_country, 
                        profile_personal_location_is_public, profile_personal_latitude, profile_personal_longitude, 
                        profile_personal_experience_is_public, profile_personal_education_is_public, 
                        profile_personal_expertise_is_public, profile_personal_wishes_is_public, 
                        profile_personal_image, profile_personal_image_is_public, profile_personal_tag_line, 
                        profile_personal_tag_line_is_public, profile_personal_short_bio, 
                        profile_personal_short_bio_is_public, profile_personal_resume_is_public, 
                        profile_personal_banner_ads_bounty, profile_personal_allow_banner_ads, 
                        profile_personal_notification_preference, profile_personal_location_preference, 
                        profile_personal_last_updated_at, profile_personal_resume, profile_personal_path
                    FROM every_circle.profile_wish
                    LEFT JOIN every_circle.wish_response ON wr_profile_wish_id = profile_wish_uid
                    LEFT JOIN every_circle.profile_personal ON wr_responder_id = profile_personal_uid
                    WHERE profile_wish_profile_personal_id = %s
                """
                
                print(f"Executing query for profile_wish_profile_personal_id: {profile_id}")
                query_response = db.execute(wishes_responses_query, (profile_id,))
                print(f"Query response: {query_response}")
                
                if query_response.get('code') == 200:
                    response['message'] = 'Wish responses retrieved successfully'
                    response['code'] = 200
                    response['data'] = query_response.get('result', [])
                    response['count'] = len(query_response.get('result', []))
                else:
                    response['message'] = 'Query execution failed'
                    response['code'] = query_response.get('code', 500)
                    response['error'] = query_response.get('error', 'Unknown error')
                    return response, response['code']
                
                return response, 200
                
        except Exception as e:
            print(f"Error in ProfileWishInfo GET: {str(e)}")
            response['message'] = f'An error occurred: {str(e)}'
            response['code'] = 500
            return response, 500

    def post(self):
        print("In ProfileWishInfo POST - Create Wish Response")
        response = {}
        
        try:
            # Get JSON payload from request
            payload = request.get_json()
            print("Payload received:", payload)
            
            # Validate required fields
            required_fields = ['profile_wish_id', 'responder_id', 'responder_note']
            missing_fields = [field for field in required_fields if not payload.get(field)]
            
            if missing_fields:
                response['message'] = f"Missing required fields: {', '.join(missing_fields)}"
                response['code'] = 400
                return response, 400
            
            # Extract required fields
            profile_wish_id = payload.get('profile_wish_id')
            responder_id = payload.get('responder_id')
            responder_note = payload.get('responder_note')
            
            with connect() as db:
                # Generate new wish response UID
                wish_response_stored_procedure_response = db.call(procedure='new_wish_response_uid')
                
                if not wish_response_stored_procedure_response.get('result') or len(wish_response_stored_procedure_response['result']) == 0:
                    response['message'] = 'Failed to generate wish response UID'
                    response['code'] = 500
                    return response, 500
                
                new_wish_response_uid = wish_response_stored_procedure_response['result'][0]['new_id']
                print(f"Generated wish_response_uid: {new_wish_response_uid}")
                
                # Prepare wish response data
                wish_response_data = {
                    'wish_response_uid': new_wish_response_uid,
                    'wr_profile_wish_id': profile_wish_id,
                    'wr_responder_id': responder_id,
                    'wr_responder_note': responder_note
                }
                
                print(f"Inserting wish response data: {wish_response_data}")
                
                # Insert into wish_response table
                wish_response_insert = db.insert('every_circle.wish_response', wish_response_data)
                print(f"Wish response insert response: {wish_response_insert}")
                
                if wish_response_insert.get('code') != 200:
                    response['message'] = wish_response_insert.get('message', 'Failed to insert wish response')
                    response['code'] = wish_response_insert.get('code', 500)
                    return response, response['code']
                
                response['wish_response_uid'] = new_wish_response_uid
                response['message'] = 'Wish response created successfully'
                response['code'] = 200
                return response, 200
                
        except Exception as e:
            print(f"Error in ProfileWishInfo POST: {str(e)}")
            response['message'] = f'An error occurred: {str(e)}'
            response['code'] = 500
            return response, 500

