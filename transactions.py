from flask_restful import Resource
from flask import request
from datetime import datetime
import traceback
import json

from data_ec import connect, processImage
from user_path_connection import ConnectionsPath

class Transactions(Resource):

    # def post(self):
    #     print("In Transactions POST New")
    #     response = {}
        
    #     try:
    #         # Get JSON payload from request
    #         payload = request.get_json()
    #         print(payload)
    #         transaction = {}
    #         tx_item = {}
    #         tx_bounty = {}
    #         i = 0
    #         n = 0

    #         # Enter Data in Transactions Table
    #         # Validate required fields
    #         required_fields = ['user_profile_id', 'stripe_payment_intent', 'total_amount_paid', 'total_costs', 'items']
    #         missing_fields = [field for field in required_fields if not payload.get(field)]

    #         if missing_fields:
    #             response['message'] = f"Missing required fields: {', '.join(missing_fields)}"
    #             response['code'] = 400
    #             return response, 400
    #         print("No Missing Fields")

    #         # Extract required fields from payload          
    #         transaction['transaction_profile_id'] = payload.get('user_profile_id')
    #         transaction['transaction_business_id'] = payload.get('business_id')
    #         transaction['transaction_stripe_pi'] = payload.get('stripe_payment_intent')
    #         transaction['transaction_total'] = payload.get('total_amount_paid')
    #         transaction['transaction_amount'] = payload.get('total_costs')
    #         transaction['transaction_taxes'] = payload.get('total_taxes')

    #         with connect() as db:
    #             transaction_stored_procedure_response = db.call(procedure='new_transaction_uid')
    #             # print("transaction_stored_procedure_response: ", transaction_stored_procedure_response)
    #             new_transaction_uid = transaction_stored_procedure_response['result'][0]['new_id']
    #             # print("new_transaction_uid: ", new_transaction_uid, type(new_transaction_uid))
    #             transaction['transaction_uid']= new_transaction_uid
    #             # print("transaction payload: ", transaction)
    #             transactions_datetime = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #             transaction['transaction_datetime'] = transactions_datetime
    #             response['transaction'] = db.insert('every_circle.transactions', transaction)
    #             print("transaction post response: ", response['transaction'])

    #             # Enter Data in Transactions_ItemsTable
    #             print("items: ", payload.get('items'))
    #             for item in payload.get('items'):
    #                 print(item)
    #                 # {'bs_uid': '250-000021', 'quantity': 9, 'recommender_profile_id': '110-000231'}
    #                 transaction_item_stored_procedure_response = db.call(procedure='new_transaction_item_uid')
    #                 print("transaction_item_stored_procedure_response: ", transaction_item_stored_procedure_response)
    #                 new_transaction_item_uid = transaction_item_stored_procedure_response['result'][0]['new_id']
    #                 print("new_transaction_item_uid: ", new_transaction_item_uid, type(new_transaction_item_uid))
                    
    #                 tx_item['ti_uid']= new_transaction_item_uid
    #                 tx_item['ti_transaction_id']= new_transaction_uid
    #                 tx_item['ti_bs_id']= item.get('bs_uid')
    #                 tx_item['ti_bs_qty']= item.get('quantity')
    #                 print("tx_item: ", tx_item)

    #                 # Get other item details from business services table
    #                 bs_query = f"""
    #                     SELECT *
    #                     FROM every_circle.business_services
    #                     WHERE bs_uid = '{item.get('bs_uid')}'
    #                 """
    #                 bs_response = db.execute(bs_query)
    #                 print("bs_response: ", bs_response)
    #                 tx_item['ti_bs_cost'] = bs_response['result'][0]['bs_cost']
    #                 tx_item['ti_bs_cost_currency'] = bs_response['result'][0]['bs_cost_currency']
    #                 tx_item['ti_bs_sku'] = bs_response['result'][0]['bs_sku']
    #                 tx_item['ti_bs_is_taxable'] = bs_response['result'][0]['bs_is_taxable']
    #                 tx_item['ti_bs_tax_rate'] = bs_response['result'][0]['bs_tax_rate']
    #                 tx_item['ti_bs_refund_policy'] = bs_response['result'][0]['bs_refund_policy']
    #                 tx_item['ti_bs_return_window_days'] = bs_response['result'][0]['bs_return_window_days']
    #                 print("tx_item: ", tx_item)
    
    #                 response['transaction_item'] = db.insert('every_circle.transactions_items', tx_item)
    #                 print("transaction_item post response: ", response['transaction_item'])
    #                 if response['transaction_item']['code'] == 200:
    #                     i = i + 1

    #                 if item.get('bounty') > 0:
    #                     print("Processing bounty: ", item.get('bounty'))
                        

    #                     known_pariticpants = [payload.get('user_profile_id'), item.get('recommender_profile_id'), 'every-circle']
    #                     # Find connection path between buyer and recommender using the profile_personal table
    #                     # network = ['110-000232', '110-000233']
    #                     network = ConnectionsPath().get(payload.get('user_profile_id'), item.get('recommender_profile_id'))
    #                     print("network: ", network)
    #                     combined_path = network[0]['combined_path']
    #                     uids = combined_path.split(',')

    #                     # Extract middle elements (excluding the first and last)
    #                     network_result = uids[1:-1]
    #                     if len(network_result) < 2:
    #                         network_result.append('charity')
    #                     print("network_result: ", network_result)
    #                     network_percentage = .40 / len(network_result)
                        
    #                     all_pariticpants = known_pariticpants + network_result

    #                     for participant in known_pariticpants :
    #                         print(participant)

    #                         transaction_bounty_stored_procedure_response = db.call(procedure='new_transaction_bounty_uid')
    #                         print("transaction_bounty_stored_procedure_response: ", transaction_bounty_stored_procedure_response)
    #                         new_transaction_bounty_uid = transaction_bounty_stored_procedure_response['result'][0]['new_id']
    #                         print("new_transaction_bounty_uid: ", new_transaction_bounty_uid, type(new_transaction_bounty_uid))
    #                         tx_bounty['tb_uid']= new_transaction_bounty_uid

    #                         tx_bounty['tb_ti_id']= new_transaction_item_uid
    #                         tx_bounty['tb_profile_id']= participant
    #                         tx_bounty['tb_percentage']= "0.2"
    #                         tx_bounty['tb_amount']= .20 * item.get('bounty')
    #                         print("tx_bounty: ", tx_bounty)
    #                         response['tx_bounty'] = db.insert('every_circle.transactions_bounty', tx_bounty)
    #                         print("transaction_bounty post response: ", response['tx_bounty'])
    #                         if response['tx_bounty']['code'] == 200:
    #                             n = n + 1
                        
    #                     for participant in network_result :
    #                         print(participant)

    #                         transaction_bounty_stored_procedure_response = db.call(procedure='new_transaction_bounty_uid')
    #                         print("transaction_bounty_stored_procedure_response: ", transaction_bounty_stored_procedure_response)
    #                         new_transaction_bounty_uid = transaction_bounty_stored_procedure_response['result'][0]['new_id']
    #                         print("new_transaction_bounty_uid: ", new_transaction_bounty_uid, type(new_transaction_bounty_uid))
    #                         tx_bounty['tb_uid']= new_transaction_bounty_uid

    #                         tx_bounty['tb_ti_id']= new_transaction_item_uid
    #                         tx_bounty['tb_profile_id']= participant
    #                         tx_bounty['tb_percentage']= network_percentage
    #                         tx_bounty['tb_amount']= network_percentage * item.get('bounty')
    #                         print("tx_bounty: ", tx_bounty)
    #                         response['tx_bounty'] = db.insert('every_circle.transactions_bounty', tx_bounty)
    #                         print("transaction_bounty post response: ", response['tx_bounty'])
    #                         if response['tx_bounty']['code'] == 200:
    #                             n = n + 1
                                
    #                     response['transaction_items'] = i
    #                     response['transaction_bounty'] = n
                    
                    
    #             # for item in payload.get('items'):
    #             #     item['transaction_uid'] = payload.get('items)
    #             #     item['transaction_item_id'] = item.get('transaction_item_id')
    #             #     item['transaction_item_name'] = item.get('transaction_item_name')
    #             #     item['transaction_item_price'] = item.get('transaction_item_price')
    #             #     item['transaction_item_quantity'] = item.get('transaction_item_quantity')
    #             #     item['transaction_item_total'] = item.get('transaction_item_total')


            
                
    #             # # First, check if the buying service exists and get its details
    #             # bs_query = f"""
    #             #     SELECT bs_uid, bs_bounty, bs_business_id 
    #             #     FROM every_circle.business_services 
    #             #     WHERE bs_uid = '{bs_id}'
    #             # """
    #             # bs_result = db.execute(bs_query)
                
    #             # if not bs_result['result'] or len(bs_result['result']) == 0:
    #             #     response['message'] = 'Business service not found'
    #             #     response['code'] = 404
    #             #     return response, 404
                
    #             # bs_details = bs_result['result'][0]
    #             # bs_bounty = float(bs_details['bs_bounty'])
                
    #             # # Find connection path between buyer and recommender using the profile_personal table
    #             # find_connection_path_query = f"""
    #             #     WITH RECURSIVE UserPaths AS (
    #             #         -- Base case: Start from the buyer
    #             #         SELECT 
    #             #             profile_personal_uid AS user_id,
    #             #             profile_personal_referred_by,
    #             #             0 AS degree, 
    #             #             CAST(profile_personal_uid AS CHAR(300)) AS connection_path
    #             #         FROM every_circle.profile_personal
    #             #         WHERE profile_personal_uid = '{buyer_id}'
                        
    #             #         UNION ALL
                        
    #             #         -- Forward expansion: Find users referred by the current user
    #             #         SELECT 
    #             #             p.profile_personal_uid AS user_id,
    #             #             p.profile_personal_referred_by,
    #             #             r.degree + 1 AS degree,
    #             #             CONCAT(r.connection_path, ' -> ', p.profile_personal_uid) AS connection_path
    #             #         FROM every_circle.profile_personal p
    #             #         INNER JOIN UserPaths r ON p.profile_personal_referred_by = r.user_id
    #             #         WHERE r.degree < 5  -- Maximum path length
    #             #         AND NOT POSITION(p.profile_personal_uid IN r.connection_path) > 0  -- Prevent cycles
                        
    #             #         UNION ALL
                        
    #             #         -- Backward expansion: Find the user who referred the current user
    #             #         SELECT 
    #             #             p.profile_personal_referred_by AS user_id,
    #             #             NULL,  -- Not needed for backward expansion
    #             #             r.degree + 1 AS degree,
    #             #             CONCAT(r.connection_path, ' -> ', p.profile_personal_referred_by) AS connection_path
    #             #         FROM every_circle.profile_personal p
    #             #         INNER JOIN UserPaths r ON p.profile_personal_uid = r.user_id
    #             #         WHERE r.degree < 5  -- Maximum path length
    #             #         AND p.profile_personal_referred_by IS NOT NULL
    #             #         AND NOT POSITION(p.profile_personal_referred_by IN r.connection_path) > 0  -- Prevent cycles
    #             #     )
                    
    #             #     -- Find the path that ends with the recommender
    #             #     SELECT 
    #             #         connection_path AS path_from_buyer_to_recommender,
    #             #         degree AS path_length
    #             #     FROM UserPaths
    #             #     WHERE user_id = '{recommender_id}'  -- Path to recommender
    #             #     ORDER BY degree ASC  -- Shortest path first
    #             #     LIMIT 1;
    #             # """
                
    #             # connection_path_result = db.execute(find_connection_path_query)
                
    #             # if not connection_path_result['result'] or len(connection_path_result['result']) == 0:
    #             #     response['message'] = 'No connection path found between buyer and recommender'
    #             #     response['code'] = 404
    #             #     return response, 404
                
    #             # # Extract the connection path
    #             # connection_data = connection_path_result['result'][0]
    #             # connection_path = connection_data['path_from_buyer_to_recommender']
    #             # path_length = connection_data['path_length']
                
    #             # # Parse the connection path to get the list of users
    #             # user_ids = connection_path.split(' -> ')
                
    #             # # Generate a unique ID for this recommendation usage
    #             # ru_stored_procedure_response = db.call(procedure='new_ru_uid')
    #             # ru_id = ru_stored_procedure_response['result'][0]['new_id']
                
    #             # # Create a record in the recommendation_used table
    #             # ru_insert_data = {
    #             #     'ru_uid': ru_id,
    #             #     'ru_used_by_user_uid': buyer_id,
    #             #     'ru_given_by_user_id': recommender_id,
    #             #     'ru_bs_id': bs_id
    #             # }
                
    #             # ru_result = db.insert('every_circle.recommendation_used', ru_insert_data)
                
    #             # # Calculate bounty distribution
    #             # transactions = []
                
    #             # if path_length > 0:
    #             #     # Distribute bounty among users in the path
    #             #     share_amount = bs_bounty / len(user_ids)
                    
    #             #     for user_id in user_ids:
    #             #         transaction_stored_procedure_response = db.call(procedure='new_transaction_uid')
    #             #         transaction_id = transaction_stored_procedure_response['result'][0]['new_id']
                        
    #             #         transaction_data = {
    #             #             'transaction_uid': transaction_id,
    #             #             'transaction_user_id': user_id,
    #             #             'transaction_recommendation_used_id': ru_id,
    #             #             'transaction_amount': str(share_amount)
    #             #         }
                        
    #             #         db.insert('every_circle.transactions', transaction_data)
                        
    #             #         transactions.append({
    #             #             'user_id': user_id,
    #             #             'amount': share_amount
    #             #         })
                
    #             # # Prepare success response
    #             # response['message'] = 'Transaction completed successfully'
    #             # response['code'] = 200
    #             # response['data'] = {
    #             #     'recommendation_id': ru_id,
    #             #     'connection_path': connection_path,
    #             #     'path_length': path_length,
    #             #     'bounty_amount': bs_bounty,
    #             #     'transactions': transactions
    #             # }
                
    #             return response, 200
                
    #     except Exception as e:
    #         print(f"Error in Transactions POST: {str(e)}")
    #         print(traceback.format_exc())
    #         response['message'] = f'An error occurred: {str(e)}'
    #         response['code'] = 500
    #         return response, 500

    # def post(self):
    #     print("In Transactions POST")
    #     response = {}
        
    #     try:
    #         # Get JSON payload from request
    #         payload = request.get_json()
    #         print(payload)

    #         with connect() as db:
    #             # Extract required fields from payload
    #             buyer_id = payload.get('buyer_id', None)
    #             recommender_id = payload.get('recommender_id', None)
    #             bs_id = payload.get('bs_id', None)
                
    #             # Validate required fields
    #             if not buyer_id or not recommender_id or not bs_id:
    #                 response['message'] = 'Missing required fields'
    #                 response['code'] = 400
    #                 return response, 400
                
    #             # First, check if the buying service exists and get its details
    #             bs_query = f"""
    #                 SELECT bs_uid, bs_bounty, bs_business_id 
    #                 FROM every_circle.business_services 
    #                 WHERE bs_uid = '{bs_id}'
    #             """
    #             bs_result = db.execute(bs_query)
                
    #             if not bs_result['result'] or len(bs_result['result']) == 0:
    #                 response['message'] = 'Business service not found'
    #                 response['code'] = 404
    #                 return response, 404
                
    #             bs_details = bs_result['result'][0]
    #             bs_bounty = float(bs_details['bs_bounty'])
                
    #             # Find connection path between buyer and recommender using the profile_personal table
    #             find_connection_path_query = f"""
    #                 WITH RECURSIVE UserPaths AS (
    #                     -- Base case: Start from the buyer
    #                     SELECT 
    #                         profile_personal_uid AS user_id,
    #                         profile_personal_referred_by,
    #                         0 AS degree, 
    #                         CAST(profile_personal_uid AS CHAR(300)) AS connection_path
    #                     FROM every_circle.profile_personal
    #                     WHERE profile_personal_uid = '{buyer_id}'
                        
    #                     UNION ALL
                        
    #                     -- Forward expansion: Find users referred by the current user
    #                     SELECT 
    #                         p.profile_personal_uid AS user_id,
    #                         p.profile_personal_referred_by,
    #                         r.degree + 1 AS degree,
    #                         CONCAT(r.connection_path, ' -> ', p.profile_personal_uid) AS connection_path
    #                     FROM every_circle.profile_personal p
    #                     INNER JOIN UserPaths r ON p.profile_personal_referred_by = r.user_id
    #                     WHERE r.degree < 5  -- Maximum path length
    #                     AND NOT POSITION(p.profile_personal_uid IN r.connection_path) > 0  -- Prevent cycles
                        
    #                     UNION ALL
                        
    #                     -- Backward expansion: Find the user who referred the current user
    #                     SELECT 
    #                         p.profile_personal_referred_by AS user_id,
    #                         NULL,  -- Not needed for backward expansion
    #                         r.degree + 1 AS degree,
    #                         CONCAT(r.connection_path, ' -> ', p.profile_personal_referred_by) AS connection_path
    #                     FROM every_circle.profile_personal p
    #                     INNER JOIN UserPaths r ON p.profile_personal_uid = r.user_id
    #                     WHERE r.degree < 5  -- Maximum path length
    #                     AND p.profile_personal_referred_by IS NOT NULL
    #                     AND NOT POSITION(p.profile_personal_referred_by IN r.connection_path) > 0  -- Prevent cycles
    #                 )
                    
    #                 -- Find the path that ends with the recommender
    #                 SELECT 
    #                     connection_path AS path_from_buyer_to_recommender,
    #                     degree AS path_length
    #                 FROM UserPaths
    #                 WHERE user_id = '{recommender_id}'  -- Path to recommender
    #                 ORDER BY degree ASC  -- Shortest path first
    #                 LIMIT 1;
    #             """
                
    #             connection_path_result = db.execute(find_connection_path_query)
                
    #             if not connection_path_result['result'] or len(connection_path_result['result']) == 0:
    #                 response['message'] = 'No connection path found between buyer and recommender'
    #                 response['code'] = 404
    #                 return response, 404
                
    #             # Extract the connection path
    #             connection_data = connection_path_result['result'][0]
    #             connection_path = connection_data['path_from_buyer_to_recommender']
    #             path_length = connection_data['path_length']
                
    #             # Parse the connection path to get the list of users
    #             user_ids = connection_path.split(' -> ')
                
    #             # Generate a unique ID for this recommendation usage
    #             ru_stored_procedure_response = db.call(procedure='new_ru_uid')
    #             ru_id = ru_stored_procedure_response['result'][0]['new_id']
                
    #             # Create a record in the recommendation_used table
    #             ru_insert_data = {
    #                 'ru_uid': ru_id,
    #                 'ru_used_by_user_uid': buyer_id,
    #                 'ru_given_by_user_id': recommender_id,
    #                 'ru_bs_id': bs_id
    #             }
                
    #             ru_result = db.insert('every_circle.recommendation_used', ru_insert_data)
                
    #             # Calculate bounty distribution
    #             transactions = []
                
    #             if path_length > 0:
    #                 # Distribute bounty among users in the path
    #                 share_amount = bs_bounty / len(user_ids)
                    
    #                 for user_id in user_ids:
    #                     transaction_stored_procedure_response = db.call(procedure='new_transaction_uid')
    #                     transaction_id = transaction_stored_procedure_response['result'][0]['new_id']
                        
    #                     transaction_data = {
    #                         'transaction_uid': transaction_id,
    #                         'transaction_user_id': user_id,
    #                         'transaction_recommendation_used_id': ru_id,
    #                         'transaction_amount': str(share_amount)
    #                     }
                        
    #                     db.insert('every_circle.transactions', transaction_data)
                        
    #                     transactions.append({
    #                         'user_id': user_id,
    #                         'amount': share_amount
    #                     })
                
    #             # Prepare success response
    #             response['message'] = 'Transaction completed successfully'
    #             response['code'] = 200
    #             response['data'] = {
    #                 'recommendation_id': ru_id,
    #                 'connection_path': connection_path,
    #                 'path_length': path_length,
    #                 'bounty_amount': bs_bounty,
    #                 'transactions': transactions
    #             }
                
    #             return response, 200
                
    #     except Exception as e:
    #         print(f"Error in Transactions POST: {str(e)}")
    #         print(traceback.format_exc())
    #         response['message'] = f'An error occurred: {str(e)}'
    #         response['code'] = 500
    #         return response, 500

    def post(self):
        print("In Transactions POST - Profile Transactions Query")
        response = {}
        
        try:
            # Get JSON payload from request
            payload = request.get_json()
            print("Payload received:", payload)
            
            # Validate required fields
            if not payload or 'profile_id' not in payload:
                response['message'] = 'profile_id is required'
                response['code'] = 400
                return response, 400
            
            profile_id = payload.get('profile_id')
            
            with connect() as db:
                # Execute the complex query with parameterized profile_id
                query = """
                SELECT -- *,
                    transaction_uid, transaction_datetime, transaction_profile_id, transaction_business_id
                    , transaction_stripe_pi
                    , transaction_total, transaction_amount, transaction_taxes -- , transaction_user_id - DNU, transactions_business_service_id -DNU, transaction_recommendation_used_id - DNU, ti_uid, ti_transaction_id
                    , ti_bs_id, ti_bs_qty, ti_bs_cost -- , ti_bs_cost_currency, ti_bs_sku, ti_bs_is_taxable, ti_bs_tax_rate, ti_bs_refund_policy, ti_bs_return_window_days, tb_uid, tb_ti_id
                    , tb_profile_id -- , tb_percentage, tb_amount
                    , ROUND(SUM(tb_percentage), 4) AS tb_percentage_sum
                    , ROUND(SUM(tb_amount), 4) AS tb_amount_sum --  , bs_uid, bs_business_id, bs_is_visible, bs_status
                    , bs_service_name, bs_service_desc -- , bs_notes, bs_sku, bs_bounty, bs_bounty_currency, bs_bounty_limit, bs_is_taxable, bs_tax_rate, bs_discount_allowed, bs_refund_policy, bs_return_window_days, bs_image_url, bs_display_order, bs_tags, bs_created_at, bs_updated_at, bs_created_by, bs_updated_by, bs_duration_minutes, bs_cost, bs_cost_currency, business_uid, business_user_id, business_google_id
                    , business_name, business_phone_number, business_phone_number_is_public, business_email_id, business_email_id_is_public -- , business_ein_number, business_address_line_1, business_address_line_2, business_city, business_state, business_country, business_zip_code, business_latitude, business_longitude, business_tag_line, business_tag_line_is_public, business_images_url, business_images_is_public, business_favorite_image, business_banner_ads_is_public, business_joined_timestamp, business_price_level, business_google_photos, business_google_rating, business_reward_type, business_reward_amount, business_yelp, business_google, business_website, business_owner_fn, business_owner_ln, business_template, business_short_bio, business_short_bio_is_public, business_claim_approved, business_category_id, business_is_active, business_services_is_public, business_owners_is_public, business_organization_id, updated_at
                FROM every_circle.transactions
                LEFT JOIN every_circle.transactions_items ON transaction_uid=ti_transaction_id
                LEFT JOIN every_circle.transactions_bounty ON ti_uid=tb_ti_id
                LEFT JOIN every_circle.business_services ON ti_bs_id = bs_uid
                LEFT JOIN every_circle.business ON bs_business_id = business_uid
                -- WHERE tb_profile_id = '110-000019'
                WHERE tb_profile_id = %s
                GROUP BY transaction_uid, ti_bs_id
                ORDER BY transaction_uid;
                """
                
                result = db.execute(query, (profile_id,))
                
                if result['code'] == 200:
                    response['message'] = 'Query executed successfully'
                    response['code'] = 200
                    response['data'] = result['result']
                    response['count'] = len(result['result'])
                else:
                    response['message'] = 'Query execution failed'
                    response['code'] = result['code']
                    response['error'] = result.get('error', 'Unknown error')
                    return response, result['code']
                
                return response, 200
                
        except Exception as e:
            print(f"Error in Transactions POST: {str(e)}")
            print(traceback.format_exc())
            response['message'] = f'An error occurred: {str(e)}'
            response['code'] = 500
            return response, 500