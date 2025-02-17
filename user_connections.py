from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage

class Connections(Resource):
    def get(self, profile_id):

        with connect() as db:

            connection_query = f'''
                                    WITH RECURSIVE Referrals AS (
                                        SELECT 
                                            profile_uid AS user_id,
                                            profile_referred_by_user_id,
                                            0 AS degree, 
                                            CAST(profile_uid AS CHAR(300)) AS connection_path
                                        FROM profile
                                        WHERE profile_uid = '{profile_id}'
                                        UNION ALL
                                        SELECT 
                                            p.profile_uid AS user_id,
                                            p.profile_referred_by_user_id,
                                            r.degree + 1 AS degree,
                                            CONCAT(r.connection_path, ' -> ', p.profile_uid) AS connection_path
                                        FROM profile p
                                        INNER JOIN Referrals r ON p.profile_referred_by_user_id = r.user_id
                                        WHERE r.degree < 3 
                                        AND NOT POSITION(p.profile_uid IN r.connection_path) > 0
                                        UNION ALL
                                        SELECT 
                                            p.profile_referred_by_user_id AS user_id,
                                            p.profile_uid AS profile_referred_by_user_id,
                                            r.degree + 1 AS degree,
                                            CONCAT(r.connection_path, ' -> ', p.profile_referred_by_user_id) AS connection_path
                                        FROM profile p
                                        INNER JOIN Referrals r ON p.profile_uid = r.user_id
                                        WHERE r.degree < 3
                                        AND NOT POSITION(p.profile_referred_by_user_id IN r.connection_path) > 0
                                    )
                                    SELECT 
                                        r.degree,
                                        r.connection_count,
                                        JSON_ARRAYAGG(d.user_id) as profile_id
                                    FROM (
                                        SELECT 
                                            degree,
                                            COUNT(DISTINCT user_id) as connection_count
                                        FROM Referrals
                                        GROUP BY degree
                                    ) r
                                    JOIN (
                                        SELECT DISTINCT degree, user_id
                                        FROM Referrals
                                    ) d ON r.degree = d.degree
                                    GROUP BY r.degree, r.connection_count
                                    ORDER BY r.degree;
                            '''
            
            response = db.execute(connection_query)
            print(response)

            if not response['result']:
                response['message'] = 'No connection found'
                response['code'] = 200
                return response, 200

            return response, 200