from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage

class BusinessRevenue(Resource):

    def get(self, business_id):

        response = {}

        with connect() as db:
            monthly_cap_query = f'''
                        SELECT 
                            bmc_impression_cap,
                            bmc_click_cap,
                            bmc_request_cap
                        FROM every_circle.business_monthly_cap
                        WHERE bmc_business_id = '{business_id}'
                    '''
            monthly_cap_response = db.execute(monthly_cap_query)

            first_day_of_month = datetime.now().replace(day=1).strftime('%Y-%m-%d')
            charges_query = f'''
                                SELECT 
                                    charge_reason,
                                    SUM(charge_amount) as total_amount
                                FROM charges
                                WHERE charge_business_id = '{business_id}'
                                AND charge_timestamp >= '{first_day_of_month}'
                                GROUP BY charge_reason;
                            '''

            charges_response = db.execute(charges_query)
            # Calculate current spend per type
            current_spend = {
                'impression': 0,
                'click': 0,
                'request': 0
            }
            for charge in charges_response['result']:
                reason = charge['charge_reason'].lower()
                if 'impression' in reason:
                    current_spend['impression'] = float(charge['total_amount'])
                elif 'click' in reason:
                    current_spend['click'] = float(charge['total_amount'])
                elif 'request' in reason:
                    current_spend['request'] = float(charge['total_amount'])
            
            transaction_query = f'''
                                    SELECT 
                                        c.charge_timestamp,
                                        b.business_name,
                                        c.charge_amount,
                                        c.charge_reason
                                    FROM charges c
                                    JOIN business b ON c.charge_business_id = b.business_uid
                                    WHERE c.charge_business_id = '{business_id}'
                                    ORDER BY c.charge_timestamp DESC
                                    LIMIT 10;
                                '''

            transaction_response = db.execute(transaction_query)

            total_charge_query = f'''
                                SELECT SUM(charge_amount) as total_balance
                                FROM charges
                                WHERE charge_business_id = '{business_id}';
                            '''
            
            total_charge_response = db.execute(total_charge_query)

            response = {
                #'total_charges': float(total_charge_response['result'][0]['total_balance']),
                'budget': {
                    'costs': {
                        'per_impression': 0.01,  # Fixed cost per impression
                        'per_click': 0.10,       # Fixed cost per click
                        'per_request': 1.00      # Fixed cost per request
                    },
                    'monthly_caps': {
                        'impression': float(monthly_cap_response['result'][0]['bmc_impression_cap']),
                        'click': float(monthly_cap_response['result'][0]['bmc_click_cap']),
                        'request': float(monthly_cap_response['result'][0]['bmc_request_cap'])
                    },
                    'current_spend': current_spend,
                    'max_monthly_spend': float(monthly_cap_response['result'][0]['bmc_impression_cap']) + float(monthly_cap_response['result'][0]['bmc_click_cap']) + float(monthly_cap_response['result'][0]['bmc_request_cap']),
                    'total_cur_spend': current_spend['click'] + current_spend['impression'] + current_spend['request']
                },
                'transactions': [{
                    'date': t['charge_timestamp'],
                    'business_name': t['business_name'],
                    'amount': float(t['charge_amount']),
                    'reason': t['charge_reason']
                } for t in transaction_response['result']]
                #'net_earnings': self.get_net_earnings(business_id, cursor)
            }
            
            return response

                
        #     finally:
        #         if cursor:
        #             cursor.close()
        #         if conn:
        #             conn.close()
        
        #     def get_net_earnings(self, business_id, cursor):
        #         # Get daily earnings for the last 30 days
        #         cursor.execute("""
        #             SELECT 
        #                 DATE(charge_timestamp) as date,
        #                 SUM(charge_amount) as daily_total
        #             FROM charges
        #             WHERE charge_business_id = %s
        #             AND charge_timestamp >= DATE_SUB(NOW(), INTERVAL 30 DAY)
        #             GROUP BY DATE(charge_timestamp)
        #             ORDER BY date
        #         """, (business_id,))
                
        #         earnings_data = cursor.fetchall()
        #         return [{
        #             'date': str(e['date']),
        #             'amount': float(e['daily_total'])
        #         } for e in earnings_data]