from flask import request, abort , jsonify, Response
from flask_restful import Resource
from collections import defaultdict
from datetime import datetime
import json
from data_ec import connect

class TagSplitSearch(Resource):
    def get(self, query):

        print('TagSplitSearch query', query)

        word_list = query.lower().split(' ')
        word_str = ','.join(word_list)

        print('word_list', word_list, ' word_str:', word_str)
        
        like_clauses = " OR ".join([f"lower(t.tag_name) LIKE '%{word}%'" for word in word_list])
        # params = [f"%{word}%" for word in word_list]

        print('like_clauses', like_clauses)
        
        # tag_query = f"""
        #                 SELECT DISTINCT business_uid, business_name
        #                 FROM every_circle.business b
        #                 LEFT JOIN every_circle.business_tags bt
        #                 ON b.business_uid = bt.bt_business_id
        #                 LEFT JOIN every_circle.tags t
        #                 ON bt.bt_tag_id = t.tag_uid
        #                 -- WHERE lower(t.tag_name) LIKE lower('%chinese food%')
        #                 WHERE lower(t.tag_name) LIKE lower('%{query}%');
        #                 """
        # tag_query = f"""
        #             SELECT result.business_uid, result.business_name
        #             FROM 
        #             (SELECT 
        #                 b.business_uid as business_uid,
        #                 b.business_name as business_name,
        #                 COUNT(DISTINCT t.tag_uid) AS match_count
        #             FROM every_circle.business b
        #             LEFT JOIN every_circle.business_tags bt ON b.business_uid = bt.bt_business_id
        #             LEFT JOIN every_circle.tags t ON bt.bt_tag_id = t.tag_uid
        #             WHERE {like_clauses}
        #             GROUP BY b.business_uid, b.business_name
        #             ORDER BY match_count DESC) result;
        #             """
        tag_query = f"""
                    SELECT 
                        b.business_uid as business_uid,
                        b.business_name as business_name,
                        COUNT(DISTINCT t.tag_uid) AS match_count
                    FROM every_circle.business b
                    LEFT JOIN every_circle.business_tags bt ON b.business_uid = bt.bt_business_id
                    LEFT JOIN every_circle.tags t ON bt.bt_tag_id = t.tag_uid
                    WHERE {like_clauses}
                    GROUP BY b.business_uid, b.business_name
                    ORDER BY match_count DESC;
                    """
        print('tag_query:', tag_query)
        try:
            with connect() as db:
                response = db.execute(tag_query, cmd='get')
                print("response from db", response)

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

