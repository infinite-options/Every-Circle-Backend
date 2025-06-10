from flask import request, abort , jsonify
from flask_restful import Resource
from datetime import datetime
from user_path_connection import ConnectionsPath
from data_ec import connect

class TransactionCost(Resource):
    def get(self, user_uid, ts_uid):

        print('user_uid:', user_uid, ' ts_uid:', ts_uid)
        #get the ref details
        ref_query = f''' SELECT profile_personal_referred_by FROM every_circle.profile_personal
                                WHERE profile_personal_uid = '{user_uid}';
                    '''
        

        print('ref_query:', ref_query)
        with connect() as db:
            response = db.execute(ref_query)

        if not response or 'result' not in response or not response['result']:
                    return []


        ref_details = response['result']
        ref_uid = response['result'][0]['profile_personal_referred_by']
        print('ref_details: ', ref_details, 'ref_uid',ref_uid)



         # Get the transaction details
        bs_query = f"""
            SELECT bs_uid, bs_bounty, bs_business_id, transaction_amount
            FROM every_circle.transactions
            LEFT JOIN every_circle.business_services
            ON transactions_business_service_id = bs_uid
            WHERE transaction_uid = '{ts_uid}'
        """
        bs_result = db.execute(bs_query)
        
        if not bs_result['result'] or len(bs_result['result']) == 0:
            response['message'] = 'Business service not found'
            response['code'] = 404
            return response, 404
        
        bs_details = bs_result['result'][0]  
        bs_bounty = float(bs_details['bs_bounty'])

        print('bs_details', bs_details)
        print('bs_bounty', bs_bounty)

        ##get the list of ids between user_uid and ref_details[inclusive]
        
        cp = ConnectionsPath()
        resp, code = cp.get(user_uid, ref_uid)
        combined_path = resp.get('combined_path') if code == 200 else None


        print('combined_path', combined_path)
        

        

        #get the transaction details

