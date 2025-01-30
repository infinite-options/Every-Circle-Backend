from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage

class Charges(Resource):
    def get(self):
        pass

    def post(self):
        print("In Charges POST")
        response = {}

        try:
            payload = request.get_json()

            if 'charge_business_id' not in payload or 'charge_caused_by_user_id' not in payload:
                response['message'] = 'Both charge_business_id & charge_caused_by_user_id are required'
                response['code'] = 400
                return response, 400
            
            with connect() as db:
                charges_stored_procedure_response = db.call(procedure='new_charge_uid')
                new_charge_uid = charges_stored_procedure_response['result'][0]['new_id']

                payload['charge_uid'] = new_charge_uid
                payload['charge_reason'] = 'click'
                payload['charge_amount'] = 1.00
                payload['charge_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                response = db.insert('every_circle.charges', payload)
            
            response['charge_uid'] = new_charge_uid

            return response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500