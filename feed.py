from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage

class Feed(Resource):
    def get(self, profile_uid):
        print("In Feed GET")
        response = {}
        try:
            with connect() as db:
                response = db.select('every_circle.feed')
            return response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500