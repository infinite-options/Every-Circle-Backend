from flask_restful import Resource
from flask import request
from datetime import datetime
import traceback
import json

from data_ec import connect, processImage

class Transactions(Resource):

    def post(self):
        print("In Transactions POST")
        response = {}
        
        try:
            # Get JSON payload from request
            payload = request.get_json()
            print(payload)

            with connect() as db:
                # Extract required fields from payload
                buyer_id = payload.get('buyer_id', None)
                recommender_id = payload.get('recommender_id', None)
                bs_id = payload.get('bs_id', None)
                
                # Validate required fields
                if not buyer_id or not recommender_id or not bs_id:
                    response['message'] = 'Missing required fields'
                    response['code'] = 400
                    return response, 400
                
                # First, check if the buying service exists and get its details
                bs_query = f"""
                    SELECT bs_uid, bs_bounty, bs_business_id 
                    FROM every_circle.business_services 
                    WHERE bs_uid = '{bs_id}'
                """
                bs_result = db.execute(bs_query)
                
                if not bs_result['result'] or len(bs_result['result']) == 0:
                    response['message'] = 'Business service not found'
                    response['code'] = 404
                    return response, 404
                
                bs_details = bs_result['result'][0]
                bs_bounty = float(bs_details['bs_bounty'])
                
                # Find connection path between buyer and recommender using the profile_personal table
                find_connection_path_query = f"""
                    WITH RECURSIVE UserPaths AS (
                        -- Base case: Start from the buyer
                        SELECT 
                            profile_personal_uid AS user_id,
                            profile_personal_referred_by,
                            0 AS degree, 
                            CAST(profile_personal_uid AS CHAR(300)) AS connection_path
                        FROM every_circle.profile_personal
                        WHERE profile_personal_uid = '{buyer_id}'
                        
                        UNION ALL
                        
                        -- Forward expansion: Find users referred by the current user
                        SELECT 
                            p.profile_personal_uid AS user_id,
                            p.profile_personal_referred_by,
                            r.degree + 1 AS degree,
                            CONCAT(r.connection_path, ' -> ', p.profile_personal_uid) AS connection_path
                        FROM every_circle.profile_personal p
                        INNER JOIN UserPaths r ON p.profile_personal_referred_by = r.user_id
                        WHERE r.degree < 5  -- Maximum path length
                        AND NOT POSITION(p.profile_personal_uid IN r.connection_path) > 0  -- Prevent cycles
                        
                        UNION ALL
                        
                        -- Backward expansion: Find the user who referred the current user
                        SELECT 
                            p.profile_personal_referred_by AS user_id,
                            NULL,  -- Not needed for backward expansion
                            r.degree + 1 AS degree,
                            CONCAT(r.connection_path, ' -> ', p.profile_personal_referred_by) AS connection_path
                        FROM every_circle.profile_personal p
                        INNER JOIN UserPaths r ON p.profile_personal_uid = r.user_id
                        WHERE r.degree < 5  -- Maximum path length
                        AND p.profile_personal_referred_by IS NOT NULL
                        AND NOT POSITION(p.profile_personal_referred_by IN r.connection_path) > 0  -- Prevent cycles
                    )
                    
                    -- Find the path that ends with the recommender
                    SELECT 
                        connection_path AS path_from_buyer_to_recommender,
                        degree AS path_length
                    FROM UserPaths
                    WHERE user_id = '{recommender_id}'  -- Path to recommender
                    ORDER BY degree ASC  -- Shortest path first
                    LIMIT 1;
                """
                
                connection_path_result = db.execute(find_connection_path_query)
                
                if not connection_path_result['result'] or len(connection_path_result['result']) == 0:
                    response['message'] = 'No connection path found between buyer and recommender'
                    response['code'] = 404
                    return response, 404
                
                # Extract the connection path
                connection_data = connection_path_result['result'][0]
                connection_path = connection_data['path_from_buyer_to_recommender']
                path_length = connection_data['path_length']
                
                # Parse the connection path to get the list of users
                user_ids = connection_path.split(' -> ')
                
                # Generate a unique ID for this recommendation usage
                ru_stored_procedure_response = db.call(procedure='new_ru_uid')
                ru_id = ru_stored_procedure_response['result'][0]['new_id']
                
                # Create a record in the recommendation_used table
                ru_insert_data = {
                    'ru_uid': ru_id,
                    'ru_used_by_user_uid': buyer_id,
                    'ru_given_by_user_id': recommender_id,
                    'ru_bs_id': bs_id
                }
                
                ru_result = db.insert('every_circle.recommendation_used', ru_insert_data)
                
                # Calculate bounty distribution
                transactions = []
                
                if path_length > 0:
                    # Distribute bounty among users in the path
                    share_amount = bs_bounty / len(user_ids)
                    
                    for user_id in user_ids:
                        transaction_stored_procedure_response = db.call(procedure='new_transaction_uid')
                        transaction_id = transaction_stored_procedure_response['result'][0]['new_id']
                        
                        transaction_data = {
                            'transaction_uid': transaction_id,
                            'transaction_user_id': user_id,
                            'transaction_recommendation_used_id': ru_id,
                            'transaction_amount': str(share_amount)
                        }
                        
                        db.insert('every_circle.transactions', transaction_data)
                        
                        transactions.append({
                            'user_id': user_id,
                            'amount': share_amount
                        })
                
                # Prepare success response
                response['message'] = 'Transaction completed successfully'
                response['code'] = 200
                response['data'] = {
                    'recommendation_id': ru_id,
                    'connection_path': connection_path,
                    'path_length': path_length,
                    'bounty_amount': bs_bounty,
                    'transactions': transactions
                }
                
                return response, 200
                
        except Exception as e:
            print(f"Error in Transactions POST: {str(e)}")
            print(traceback.format_exc())
            response['message'] = f'An error occurred: {str(e)}'
            response['code'] = 500
            return response, 500