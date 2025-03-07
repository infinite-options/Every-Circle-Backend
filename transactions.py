from flask_restful import Resource
from flask import request
from datetime import datetime
import ast
import traceback
import json

from data_ec import connect, processImage

class Transactions(Resource):

    def post(self):
        print("In Transactions POST")
        response = {}
        
        try:
            payload = payload.get_json()
            print(payload)

            with connect() as db:
                buyer_id = payload.get('buyer_id', None)
                recommender_id = payload.get('recommender_id', None)
                coupon_id = payload.get('coupon_id', None)
                
                if not buyer_id or not recommender_id or not coupon_id:
                    response['message'] = 'Missing required fields'
                    response['code'] = 400
                    return response, 400
                
                find_connection_path_query = f'''
                                            
                                            '''

                connection_path_response = db.execute(find_connection_path_query)
                print(connection_path_response)

                # add the buyer_id as ru_used_by_user_id, recommender_id as ru_given_by_user_id, and coupon_id as ru_coupon_id


                # use the ru_id, all users in connection_path, and the amount by calculating it with the coupon amount

        except:
            pass