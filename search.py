from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage

# query -> category
# category -> business
# business -> ratings
# business -> charges
# add miles in the location

class Search(Resource):
    def get(self, user_id):
        print("In Search GET")
        search_category = request.args.get('category')
        
        if search_category is None:
            abort(400, description="category is required")
        
        response = {}
        
        try:
            with connect() as db:
                category_query_response = db.select('every_circle.category', where={'category_name': search_category})

                category_uid = category_query_response['result'][0]['category_uid']

                if not category_uid:
                    response['message'] = 'Category not found'
                    response['code'] = 404
                    return response, 404
                
                rating_query = f'''
                                    WITH UserConnections AS (
                                        WITH RECURSIVE Referrals AS (
                                            SELECT 
                                                profile_user_id AS user_id,
                                                profile_referred_by_user_id,
                                                1 AS degree,
                                                CAST(profile_referred_by_user_id AS CHAR(300)) AS connection_path
                                            FROM 
                                                profile
                                            WHERE 
                                                profile_referred_by_user_id = '{user_id}'
                                                
                                            UNION ALL

                                            SELECT 
                                                p.profile_user_id AS user_id,
                                                p.profile_referred_by_user_id,
                                                c.degree + 1 AS degree,
                                                CONCAT(c.connection_path, ' -> ', p.profile_referred_by_user_id) AS connection_path
                                            FROM 
                                                profile p
                                            INNER JOIN 
                                                Referrals c ON p.profile_referred_by_user_id = c.user_id
                                            WHERE 
                                                c.degree < 3
                                        )
                                        SELECT DISTINCT
                                            user_id,
                                            degree,
                                            connection_path
                                        FROM 
                                            Referrals
                                        ORDER BY 
                                            degree, connection_path
                                    ),
                                    RatingMatches AS (
                                        SELECT
                                            *
                                        FROM
                                            ratings r
                                        INNER JOIN
                                            UserConnections u ON r.rating_user_id = u.user_id
                                        WHERE
                                            r.rating_business_id IN (
                                                SELECT business_uid
                                                FROM every_circle.business
                                                WHERE business_category_id = '{category_uid}'
                                            )
                                    )
                                    SELECT * FROM RatingMatches;
                            '''
                
                rating_query_response = db.execute(rating_query)

                return rating_query_response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500