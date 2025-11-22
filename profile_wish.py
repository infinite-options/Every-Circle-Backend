from flask import request, abort , jsonify, Response
from flask_restful import Resource
from collections import defaultdict
from datetime import datetime
import json
from data_ec import connect

class ProfileWishInfo(Resource):
    def get(self, query):

        print('query', query)
        
        run_query = f"""
                        select distinct profile_wish_profile_personal_id  
                            FROM every_circle.profile_wish
                            WHERE lower(profile_wish_title) LIKE lower('%{query}%') OR 
                            lower(profile_wish_description) LIKE lower('%{query}%');
                        """

        try:
            with connect() as db:
                response = db.execute(run_query, cmd='get')

            if not response['result']:
                response['message'] = f"No item found"
                response['code'] = 404
                return response, 404

            return response, 200

        except Exception as e:
            print(f"Error Middle Layer: {str(e)}")
            response['message'] = f"Internal Server Error: {str(e)}"
            response['code'] = 500
            return response, 500
        
        # return store, 200

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

