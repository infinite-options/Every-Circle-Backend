from flask import request, abort , jsonify, Response
from flask_restful import Resource
from collections import defaultdict
from datetime import datetime
import json
from data_ec import connect

class SplitSearch(Resource):
    def get(self, query):

        print('query', query)

        word_list = [word.strip().lower() for word in query.split() if word.strip()]
        print('word_list', word_list)
        
        # like_conditions = []
        # for word in word_list:
        #     like_conditions.append(f"LOWER(t.tag_name) LIKE '%{word}%'")
        #     like_conditions.append(f"LOWER(c.category_name) LIKE '%{word}%'")
        #     like_conditions.append(f"LOWER(b.business_name) LIKE '%{word}%'")
        
        # print('like_conditions', like_conditions)

        # where_clause = " OR ".join(like_conditions)
        
        # tag_query = f"""
        #     SELECT 
        #         b.business_uid,
        #         b.business_name,
        #         COUNT(DISTINCT t.tag_uid) AS match_count
        #     FROM every_circle.business b
        #     LEFT JOIN every_circle.business_tags bt ON b.business_uid = bt.bt_business_id
        #     LEFT JOIN every_circle.tags t ON bt.bt_tag_id = t.tag_uid
        #     LEFT JOIN every_circle.business_category bc ON b.business_uid = bc.bc_business_id
        #     LEFT JOIN every_circle.category c ON bc.bc_category_id = c.category_uid
        #     WHERE {where_clause}
        #     GROUP BY b.business_uid, b.business_name
        #     HAVING match_count > 0
        #     ORDER BY match_count DESC;
        # """
        # print('Generated tag_query:\n', tag_query)

        match_cases = []
        for word in word_list:
            match_cases.append(f"CASE WHEN LOWER(t.tag_name) LIKE '%{word}%' THEN 1 ELSE 0 END")
            match_cases.append(f"CASE WHEN LOWER(c.category_name) LIKE '%{word}%' THEN 1 ELSE 0 END")
            match_cases.append(f"CASE WHEN LOWER(b.business_name) LIKE '%{word}%' THEN 1 ELSE 0 END")

        match_sum = " + ".join(match_cases)

        tag_query = f"""
            SELECT 
                b.business_uid,
                b.business_name,
                SUM({match_sum}) AS match_count
            FROM every_circle.business b
            LEFT JOIN every_circle.business_tags bt ON b.business_uid = bt.bt_business_id
            LEFT JOIN every_circle.tags t ON bt.bt_tag_id = t.tag_uid
            LEFT JOIN every_circle.business_category bc ON b.business_uid = bc.bc_business_id
            LEFT JOIN every_circle.category c ON bc.bc_category_id = c.category_uid
            GROUP BY b.business_uid, b.business_name
            HAVING match_count > 0
            ORDER BY match_count DESC;
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

