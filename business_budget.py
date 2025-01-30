from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage

class Business_Budget(Resource):
    def get(self, business_id):
        print("In Business Budget GET")
        response = {}

        try:
            print(business_id, type(business_id))
            with connect() as db:
                business_budget_click_query = f'''
                                            SELECT 
                                                SUM(charge_amount) AS total_charges,
                                                COUNT(charge_uid) AS total_clicks,
                                                SUM(CASE WHEN DATE_FORMAT(STR_TO_DATE(charge_timestamp, '%Y-%m-%d %H:%i:%s'), '%Y-%m') = DATE_FORMAT(CURRENT_DATE, '%Y-%m') THEN charge_amount ELSE 0 END) AS total_charges_current_month,
                                                COUNT(CASE WHEN DATE_FORMAT(STR_TO_DATE(charge_timestamp, '%Y-%m-%d %H:%i:%s'), '%Y-%m') = DATE_FORMAT(CURRENT_DATE, '%Y-%m') THEN charge_uid ELSE NULL END) AS total_clicks_current_month
                                            FROM every_circle.charges
                                            WHERE charge_business_id = '{business_id}' 
                                            AND charge_reason = 'click';
                                        ''' 
                
                business_budget_click_response = db.execute(business_budget_click_query)

                business_budget_impression_query = f'''
                                            SELECT 
                                                SUM(charge_amount) AS total_charges,
                                                COUNT(charge_uid) AS total_impressions,
                                                SUM(CASE WHEN DATE_FORMAT(STR_TO_DATE(charge_timestamp, '%Y-%m-%d %H:%i:%s'), '%Y-%m') = DATE_FORMAT(CURRENT_DATE, '%Y-%m') THEN charge_amount ELSE 0 END) AS total_charges_current_month,
                                                COUNT(CASE WHEN DATE_FORMAT(STR_TO_DATE(charge_timestamp, '%Y-%m-%d %H:%i:%s'), '%Y-%m') = DATE_FORMAT(CURRENT_DATE, '%Y-%m') THEN charge_uid ELSE NULL END) AS total_impressions_current_month
                                            FROM every_circle.charges
                                            WHERE charge_business_id = '{business_id}' 
                                            AND charge_reason = 'impression';
                                        ''' 
                
                business_budget_impression_response = db.execute(business_budget_impression_query)

                response['business_uid'] = business_id
                response['clicks'] = business_budget_click_response['result'][0]
                response['impressions'] = business_budget_impression_response['result'][0]
                
                return response, 200
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500