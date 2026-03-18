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

    def get(self, profile_wish_id):
        # Determine which endpoint was called based on available parameters
        
        print(f"In ProfileWishInfo GET - Wish Responses for profile: {profile_wish_id}")
        response = {}
        
        try:
            if not profile_wish_id:
                response['message'] = 'profile_wish_id is required'
                response['code'] = 400
                return response, 400
            
            with connect() as db:
                # Query to get wish responses with responder profile information
                wishes_responses_query = """
                    SELECT pw.*, wr.*,
                        responder.profile_personal_first_name AS responder_first_name, responder.profile_personal_last_name AS responder_last_name,
                        recommended.*,
                        if (recommended.profile_personal_email_is_public = 1, u.user_email_id, null) AS profile_personal_email
                    FROM every_circle.profile_wish pw
                    LEFT JOIN every_circle.wish_response wr ON wr_profile_wish_id = profile_wish_uid
                    LEFT JOIN every_circle.profile_personal AS responder ON wr_responder_id = profile_personal_uid
                    LEFT JOIN every_circle.profile_personal AS recommended ON (wr_type = 'refer' AND wr_recommended_id = recommended.profile_personal_uid) OR ((wr_type = 'help' OR ISNULL(wr_type)) AND wr_responder_id = recommended.profile_personal_uid)
                    LEFT JOIN every_circle.users u ON user_uid = recommended.profile_personal_user_id
                    -- WHERE wr_profile_wish_id = "160-000014"
                    WHERE wr_profile_wish_id = %s
                """
                
                print(f"Executing query for profile_wish_id: {profile_wish_id}")
                query_response = db.execute(wishes_responses_query, (profile_wish_id,))
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
                    'wr_responder_note': responder_note,
                    'wr_datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }
                if payload.get('referral_profile_uid'):
                    wish_response_data['wr_recommended_id'] = payload.get('referral_profile_uid')
                if payload.get('help_type'):
                    wish_response_data['wr_type'] = payload.get('help_type')
                
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

