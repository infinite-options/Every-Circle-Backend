from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime
import requests
import ast

from data_ec import connect, uploadImage, s3, processImage

# used category as main name
# show sub_category in suggestions

class Feed(Resource):
    def get(self, profile_id):
        print("In Feed GET")
        response = {}
        response['result'] = []
        try:
            
            with connect() as db:
                profile_query = db.select('every_circle.profile', where={'profile_uid': profile_id})
                profile_data = profile_query['result'][0]

                if not profile_data:
                    response['message'] = f'No profile found for {profile_id}'
                    response['code'] = 404
                    return response, 404
                
                if not profile_data['profile_how_can_we_help']:
                    response['message'] = f'No profile_how_can_we_help found for {profile_id}'
                    response['code'] = 404
                    return response, 404
                
                for i in ast.literal_eval(profile_data['profile_how_can_we_help']):
                    print(i)
                    search_response = requests.get(f"https://ioEC2testsspm.infiniteoptions.com/search/{profile_id}?category={i}")
                    # search_response = requests.get(f"http://127.0.0.1:4090/search/{profile_id}?category={i}")
                    if search_response.json()['result']:
                        response['result'].append({i: search_response.json()['result']})
                    # print(req_response.json())
            
            return response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500