from flask_restful import Resource
from data_ec import connect

class Lists(Resource):
    def get(self, list_uid):
        print("In lists GET")
        response = {}
        try:
            with connect() as db:
                # Query the charges table for the given business_id
                key = {'list_uid': list_uid}
                response = db.select('every_circle.lists', where=key)

            if not response['result']:
                response['message'] = f"No item found for lists ID {list_uid}"
                response['code'] = 404
                return response, 404

            return response, 200

        except Exception as e:
            print(f"Error: {str(e)}")
            response['message'] = f"Internal Server Error: {str(e)}"
            response['code'] = 500
            return response, 500