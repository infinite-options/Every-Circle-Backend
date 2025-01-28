from flask_restful import Resource
from data_ec import connect

class Charges(Resource):
    def get(self, business_id):
        print("In Charges GET")
        response = {}
        try:
            with connect() as db:
                # Query the charges table for the given business_id
                key = {'charge_business_id': business_id}
                response = db.select('every_circle.charges', where=key)

            if not response['result']:
                response['message'] = f"No charges found for Business ID {business_id}"
                response['code'] = 404
                return response, 404

            return response, 200

        except Exception as e:
            print(f"Error: {str(e)}")
            response['message'] = f"Internal Server Error: {str(e)}"
            response['code'] = 500
            return response, 500
