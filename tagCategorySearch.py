from flask import request, abort , jsonify, Response
from flask_restful import Resource
from collections import defaultdict
from datetime import datetime
import json
from data_ec import connect

class TagCategorySearch(Resource):
    def get(self, query):

        print('query', query)

        tag_query = f"""
                       SELECT DISTINCT business_uid, business_name
                        FROM every_circle.business b
                        LEFT JOIN every_circle.business_category bc
                        ON b.business_uid = bc.bc_business_id
                        LEFT JOIN every_circle.category c
                        ON bc.bc_category_id = c.category_uid
                        LEFT JOIN every_circle.business_tags bt
                        ON b.business_uid = bt.bt_business_id
                        LEFT JOIN every_circle.tags t
                        ON bt.bt_tag_id = t.tag_uid
                        WHERE lower(b.business_name) LIKE lower('%{query}%') 
                        OR lower(t.tag_name) LIKE lower('%{query}%')
                        OR lower(c.category_name) LIKE lower('%{query}%')
                        ;
                        """
        
        try:
            with connect() as db:
                response = db.execute(tag_query, cmd='get')

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

