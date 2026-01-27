from flask_restful import Resource
from flask import request
from datetime import datetime
import traceback

from data_ec import connect

class BountyResults(Resource):
    def get(self, profile_id):
        print(f"In BountyResults GET for profile_id: {profile_id}")
        response = {}
        
        try:
            with connect() as db:
                # Query to get bounty results for the specific profile_id
                bounty_query = f"""
                    SELECT 
                        transaction_uid,
                        transaction_datetime,
                        SUM(tb_amount) AS bounty_earned,
                        transaction_profile_id,
                        transaction_business_id
                    FROM (
                        SELECT *
                        FROM every_circle.transactions_bounty
                        LEFT JOIN every_circle.transactions_items ON tb_ti_id = ti_uid
                        LEFT JOIN every_circle.transactions ON ti_transaction_id = transaction_uid
                        LEFT JOIN every_circle.business ON ti_bs_id = business_uid
                        WHERE tb_profile_id = '{profile_id}'
                    ) AS t
                    GROUP BY t.transaction_uid
                    ORDER BY t.transaction_datetime DESC
                """
                
                bounty_response = db.execute(bounty_query)
                
                if bounty_response['code'] == 200:
                    response['code'] = 200
                    response['message'] = 'Bounty results retrieved successfully'
                    response['data'] = bounty_response['result']
                    response['total_bounties'] = len(bounty_response['result'])
                    
                    # Calculate total bounty earned
                    total_bounty = sum(float(bounty['bounty_earned']) for bounty in bounty_response['result'])
                    response['total_bounty_earned'] = total_bounty
                    
                    return response, 200
                else:
                    response['code'] = 500
                    response['message'] = 'Error retrieving bounty results'
                    return response, 500
                    
        except Exception as e:
            print(f"Error in BountyResults GET: {str(e)}")
            print(traceback.format_exc())
            response['code'] = 500
            response['message'] = f'An error occurred: {str(e)}'
            return response, 500 
        
class BusinessBountyResults(Resource):
    def get(self, business_id):
        print(f"In BusinessBountyResults GET for business_id: {business_id}")
        response = {}
        
        try:
            with connect() as db:
                # Query to get bounty results for transactions where this business was the seller
                bounty_query = """
                    SELECT 
                        transaction_uid,
                        transaction_datetime,
                        SUM(tb_amount) AS bounty_earned,
                        transaction_profile_id,
                        transaction_business_id
                    FROM (
                        SELECT *
                        FROM every_circle.transactions_bounty
                        LEFT JOIN every_circle.transactions_items ON tb_ti_id = ti_uid
                        LEFT JOIN every_circle.transactions ON ti_transaction_id = transaction_uid
                        WHERE transaction_business_id = %s
                    ) AS t
                    GROUP BY t.transaction_uid, t.transaction_datetime, t.transaction_profile_id, t.transaction_business_id
                    ORDER BY t.transaction_datetime DESC
                """
                
                bounty_response = db.execute(bounty_query, (business_id,))
                
                if bounty_response['code'] == 200:
                    response['code'] = 200
                    response['message'] = 'Business bounty results retrieved successfully'
                    response['data'] = bounty_response['result']
                    response['total_bounties'] = len(bounty_response['result'])
                    
                    # Calculate total bounty paid out by business
                    total_bounty = sum(float(bounty['bounty_earned']) for bounty in bounty_response['result'])
                    response['total_bounty_earned'] = total_bounty
                    
                    return response, 200
                else:
                    response['code'] = 500
                    response['message'] = 'Error retrieving business bounty results'
                    return response, 500
                    
        except Exception as e:
            print(f"Error in BusinessBountyResults GET: {str(e)}")
            print(traceback.format_exc())
            response['code'] = 500
            response['message'] = f'An error occurred: {str(e)}'
            return response, 500