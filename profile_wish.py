from flask import request, abort , jsonify, Response
from flask_restful import Resource
from collections import defaultdict
from datetime import datetime
import json
from data_ec import connect

class ProfileWishInfo(Resource):
    def get(self, query):

        print('query', query)
        
        run_query = f"""
                        select distinct profile_wish_profile_personal_id  
                            FROM every_circle.profile_wish
                            WHERE lower(profile_wish_title) LIKE lower('%{query}%') OR 
                            lower(profile_wish_description) LIKE lower('%{query}%');
                        """

        try:
            with connect() as db:
                response = db.execute(run_query, cmd='get')

            if not response['result']:
                response['message'] = f"No item found"
                response['code'] = 404
                return response, 404

            return response, 200

        except Exception as e:
            print(f"Error Middle Layer: {str(e)}")
            response['message'] = f"Internal Server Error: {str(e)}"
            response['code'] = 500
            return response, 500
        
        # return store, 200

