from flask_restful import Resource
from flask import Flask, request, jsonify
import pandas as pd
from dotenv import load_dotenv
from network_connection import NetworkPath
import json
from data_ec import connect

load_dotenv()

class ExpertiseSearch(Resource):
    def get(self, target_uid):
        print("In Expertise Search")

        if not target_uid:
            return {"message": "Missing target_uid parameter"}, 400

        print('target_uid:', target_uid)
        degree = 3

        try:
            get_network = NetworkPath()
            network_res= get_network.get(target_uid, degree)
            print(type(network_res))

            print('get_network', network_res)
            data = json.loads(network_res.get_data(as_text=True))
            print('get_network parsed data:', data)

            network_ids = [ val['network_profile_personal_uid'] for val in data]
            network_ids_str = ",".join([f"'{uid}'" for uid in network_ids])
            print('network_ids_str', network_ids_str)

            #Get the expertise info for the network ids from every_circle.profile_expertise
            up_query = f'''
                SELECT profile_expertise_profile_personal_id, profile_expertise_title
                FROM profile_expertise
                WHERE profile_expertise_profile_personal_id in ({network_ids_str})
                ;
        '''
            print('up_query', up_query)
        
            with connect() as db:
                response = db.execute(up_query)
                print(response)

            if not response or 'result' not in response or not response['result']:
                # response['message'] = 'No connection found'
                # response['code'] = 404
                return []

            return response['result'], 200
        
        except Exception as e:
            
            return {"message": "ExpertiseSearch failed"}, 502