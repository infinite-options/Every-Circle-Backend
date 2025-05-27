from flask import request, abort , jsonify, Response
from flask_restful import Resource
from collections import defaultdict
from datetime import datetime
import json
from data_ec import connect

class ProfileDetails(Resource):
    def get(self, query):

        print('query', query)
        
        run_query = f"""
                        SELECT distinct profile_personal_uid
                        FROM every_circle.profile_personal
                        LEFT JOIN every_circle.profile_expertise exp
                        ON profile_personal_uid = profile_expertise_profile_personal_id
                        LEFT JOIN every_circle.profile_education edu
                        ON profile_expertise_profile_personal_id = profile_education_profile_personal_id
                        LEFT JOIN every_circle.profile_experience
                        ON profile_expertise_profile_personal_id = profile_experience_profile_personal_id
                        WHERE lower(profile_education_degree) LIKE lower('%{query}%') OR 
                        lower(profile_education_course) LIKE lower('%{query}%');
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

