from flask_restful import Resource
from data_ec import connect


class TransactionReceipt(Resource):
    def get(self, profile_id, transaction_uid):
        print(f"In TransactionReceipt GET for profile_id: {profile_id}, transaction_uid: {transaction_uid}")
        response = {}

        try:
            with connect() as db:
                query = """
                    SELECT * FROM every_circle.transactions
                    LEFT JOIN every_circle.transactions_items ON ti_transaction_id = transaction_uid
                    LEFT JOIN every_circle.business_services ON bs_uid = ti_bs_id
                    WHERE transaction_profile_id = %s
                        AND transaction_uid = %s
                """
                result = db.execute(query, (profile_id, transaction_uid))

                if result.get('code') == 200:
                    response['message'] = 'Transaction receipt retrieved successfully'
                    response['code'] = 200
                    response['data'] = result.get('result', [])
                    return response, 200
                else:
                    response['message'] = result.get('message', 'Error retrieving transaction receipt')
                    response['code'] = result.get('code', 500)
                    return response, response['code']

        except Exception as e:
            print(f"Error in TransactionReceipt GET: {str(e)}")
            response['message'] = f'An error occurred: {str(e)}'
            response['code'] = 500
            return response, 500
