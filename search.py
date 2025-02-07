from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage
from charges import Charges
from rapidfuzz import process

# query -> type
# type -> business
# business -> ratings
# business -> charges
# add miles in the location

class Search(Resource):
    def get(self, profile_id):
        print("In Search GET")
        search_type = request.args.get('type', "").strip()
        # profile_id = request.args.get('profile_id', "").strip()
        
        if search_type is None:
            abort(400, description="type is required")
        
        response = {}
        
        try:
            with connect() as db:
                all_type_query = db.select('every_circle.types')
                all_types = {type['sub_type']: type['type_uid'] for type in all_type_query['result']}

                match = process.extractOne(search_type, all_types.keys(), score_cutoff=70)

                if match:
                    matched_type = match[0]
                    type_uid = all_types[matched_type]
                    print(matched_type, type_uid)
                else:
                    response['message'] = 'type not found'
                    response['code'] = 200
                    return response, 200

                # type_query_response = db.select('every_circle.type', where={'type_name': search_type})
                # if not type_query_response['result']:
                #     response['message'] = 'type not found'
                #     response['code'] = 200
                #     return response, 200
                # type_uid = type_query_response['result'][0]['type_uid']

                rating_query = f'''
                                    WITH UserConnections AS (
                                        WITH RECURSIVE Referrals AS (
                                            -- Base case: Start from the given user_id
                                            SELECT 
                                                profile_uid AS user_id,
                                                profile_referred_by_user_id,
                                                0 AS degree, 
                                                CAST(profile_uid AS CHAR(300)) AS connection_path
                                            FROM profile
                                            WHERE profile_uid = '{profile_id}'

                                            UNION ALL

                                            -- Forward expansion: Find users referred by the current user
                                            SELECT 
                                                p.profile_uid AS user_id,
                                                p.profile_referred_by_user_id,
                                                r.degree + 1 AS degree,
                                                CONCAT(r.connection_path, ' -> ', p.profile_uid) AS connection_path
                                            FROM profile p
                                            INNER JOIN Referrals r ON p.profile_referred_by_user_id = r.user_id
                                            WHERE r.degree < 3 
                                            AND NOT POSITION(p.profile_uid IN r.connection_path) > 0  -- Prevent revisiting users

                                            UNION ALL

                                            -- Backward expansion: Find the user who referred the current user
                                            SELECT 
                                                p.profile_referred_by_user_id AS user_id,
                                                p.profile_uid AS profile_referred_by_user_id,
                                                r.degree + 1 AS degree,
                                                CONCAT(r.connection_path, ' -> ', p.profile_referred_by_user_id) AS connection_path -- Append correctly
                                            FROM profile p
                                            INNER JOIN Referrals r ON p.profile_uid = r.user_id
                                            WHERE r.degree < 3
                                            AND NOT POSITION(p.profile_referred_by_user_id IN r.connection_path) > 0  -- Prevent revisiting users
                                        )
                                        -- Final selection of all users within 3 degrees of connection
                                        SELECT DISTINCT
                                            user_id,
                                            degree,
                                            connection_path
                                        FROM Referrals
                                        ORDER BY degree, connection_path
                                    ),
                                    RatingMatches AS (
                                        -- Match ratings based on UserConnections and business type
                                        SELECT
                                            r.*,
                                            u.degree,
                                            u.connection_path
                                        FROM
                                            ratings r
                                        INNER JOIN UserConnections u ON r.rating_profile_id = u.user_id
                                        WHERE
                                            r.rating_business_id IN (
                                                SELECT bt_business_id
                                                FROM every_circle.business_type
                                                -- INNER JOIN every_circle.business ON business_uid = bt_business_id
                                                WHERE bt_type_id = '{type_uid}'
                                            )
                                    )
                                    -- Final selection from RatingMatches to get the required output
                                    -- SELECT * 
                                    -- FROM RatingMatches;
                                    SELECT 
                                        rm.*, 
                                        b.*
                                    FROM RatingMatches rm
                                    LEFT JOIN every_circle.business b ON rm.rating_business_id = b.business_uid;
                            '''

                # rating_query = f'''
                #                     WITH UserConnections AS (
                #                         WITH RECURSIVE Referrals AS (
                #                             -- Base case: Start from the given user_id
                #                             SELECT 
                #                                 profile_user_id AS user_id,
                #                                 profile_referred_by_user_id,
                #                                 0 AS degree, 
                #                                 CAST(profile_user_id AS CHAR(300)) AS connection_path
                #                             FROM profile
                #                             WHERE profile_user_id = '{profile_id}'

                #                             UNION ALL

                #                             -- Forward expansion: Find users referred by the current user
                #                             SELECT 
                #                                 p.profile_user_id AS user_id,
                #                                 p.profile_referred_by_user_id,
                #                                 r.degree + 1 AS degree,
                #                                 CONCAT(r.connection_path, ' -> ', p.profile_user_id) AS connection_path
                #                             FROM profile p
                #                             INNER JOIN Referrals r ON p.profile_referred_by_user_id = r.user_id
                #                             WHERE r.degree < 3 
                #                             AND NOT POSITION(p.profile_user_id IN r.connection_path) > 0  -- Prevent revisiting users

                #                             UNION ALL

                #                             -- Backward expansion: Find the user who referred the current user
                #                             SELECT 
                #                                 p.profile_referred_by_user_id AS user_id,
                #                                 p.profile_user_id AS profile_referred_by_user_id,
                #                                 r.degree + 1 AS degree,
                #                                 CONCAT(r.connection_path, ' -> ', p.profile_referred_by_user_id) AS connection_path -- Append correctly
                #                             FROM profile p
                #                             INNER JOIN Referrals r ON p.profile_user_id = r.user_id
                #                             WHERE r.degree < 3
                #                             AND NOT POSITION(p.profile_referred_by_user_id IN r.connection_path) > 0  -- Prevent revisiting users
                #                         )
                #                         -- Final selection of all users within 3 degrees of connection
                #                         SELECT DISTINCT
                #                             user_id,
                #                             degree,
                #                             connection_path
                #                         FROM Referrals
                #                         ORDER BY degree, connection_path
                #                     ),
                #                     RatingMatches AS (
                #                         -- Match ratings based on UserConnections and business type
                #                         SELECT
                #                             r.*,
                #                             u.degree,
                #                             u.connection_path
                #                         FROM
                #                             ratings r
                #                         INNER JOIN UserConnections u ON r.rating_user_id = u.user_id
                #                         WHERE
                #                             r.rating_business_id IN (
                #                                 SELECT business_uid
                #                                 FROM every_circle.business
                #                                 WHERE business_type_id = '{type_uid}'
                #                             )
                #                     )
                #                     -- Final selection from RatingMatches to get the required output
                #                     -- SELECT * 
                #                     -- FROM RatingMatches;
                #                     SELECT 
                #                         rm.*, 
                #                         b.business_name,
                #                         b.business_phone_number,
                #                         b.business_address_line_1,
                #                         b.business_address_line_2,
                #                         b.business_city,
                #                         b.business_state,
                #                         b.business_country,
                #                         b.business_zip_code,
                #                         b.business_google_id
                #                     FROM RatingMatches rm
                #                     LEFT JOIN every_circle.business b ON rm.rating_business_id = b.business_uid;
                #             '''
                
                rating_query_response = db.execute(rating_query)

                business_uid_list = tuple(set([business['rating_business_id'] for business in rating_query_response['result']]))

                for business_uid in business_uid_list:

                    charges_stored_procedure_response = db.call(procedure='new_charge_uid')
                    new_charge_uid = charges_stored_procedure_response['result'][0]['new_id']

                    charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    charges_query = f'''
                                            INSERT INTO `every_circle`.`charges` (`charge_uid`, `charge_business_id`, `charge_caused_by_user_id`, `charge_reason`, `charge_amount`, `charge_timestamp`) 
                                            VALUES ('{new_charge_uid}', '{business_uid}', '{profile_id}', 'impression', '1.00', '{charge_timestamp}');
                                    '''

                    charges_query_response = db.execute(charges_query, cmd='post')

                return rating_query_response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500


    def post(self):
        print("In Search GET")
        # search_type = request.args.get('type', "").strip()
        # profile_id = request.args.get('profile_id', "").strip()
        payload = request.get_json()
        search_type = payload['type']
        profile_id = payload['profile_id']
        if search_type is None:
            abort(400, description="type is required")
        
        response = {}
        
        try:
            with connect() as db:
                all_type_query = db.select('every_circle.types')
                all_types = {type['sub_type']: type['type_uid'] for type in all_type_query['result']}

                match = process.extractOne(search_type, all_types.keys(), score_cutoff=70)

                if match:
                    matched_type = match[0]
                    type_uid = all_types[matched_type]
                    print(matched_type, type_uid)
                else:
                    response['message'] = 'type not found'
                    response['code'] = 200
                    return response, 200

                # type_query_response = db.select('every_circle.type', where={'type_name': search_type})
                # if not type_query_response['result']:
                #     response['message'] = 'type not found'
                #     response['code'] = 200
                #     return response, 200
                # type_uid = type_query_response['result'][0]['type_uid']

                rating_query = f'''
                                    WITH UserConnections AS (
                                        WITH RECURSIVE Referrals AS (
                                            -- Base case: Start from the given user_id
                                            SELECT 
                                                profile_uid AS user_id,
                                                profile_referred_by_user_id,
                                                0 AS degree, 
                                                CAST(profile_uid AS CHAR(300)) AS connection_path
                                            FROM profile
                                            WHERE profile_uid = '{profile_id}'

                                            UNION ALL

                                            -- Forward expansion: Find users referred by the current user
                                            SELECT 
                                                p.profile_uid AS user_id,
                                                p.profile_referred_by_user_id,
                                                r.degree + 1 AS degree,
                                                CONCAT(r.connection_path, ' -> ', p.profile_uid) AS connection_path
                                            FROM profile p
                                            INNER JOIN Referrals r ON p.profile_referred_by_user_id = r.user_id
                                            WHERE r.degree < 3 
                                            AND NOT POSITION(p.profile_uid IN r.connection_path) > 0  -- Prevent revisiting users

                                            UNION ALL

                                            -- Backward expansion: Find the user who referred the current user
                                            SELECT 
                                                p.profile_referred_by_user_id AS user_id,
                                                p.profile_uid AS profile_referred_by_user_id,
                                                r.degree + 1 AS degree,
                                                CONCAT(r.connection_path, ' -> ', p.profile_referred_by_user_id) AS connection_path -- Append correctly
                                            FROM profile p
                                            INNER JOIN Referrals r ON p.profile_uid = r.user_id
                                            WHERE r.degree < 3
                                            AND NOT POSITION(p.profile_referred_by_user_id IN r.connection_path) > 0  -- Prevent revisiting users
                                        )
                                        -- Final selection of all users within 3 degrees of connection
                                        SELECT DISTINCT
                                            user_id,
                                            degree,
                                            connection_path
                                        FROM Referrals
                                        ORDER BY degree, connection_path
                                    ),
                                    RatingMatches AS (
                                        -- Match ratings based on UserConnections and business type
                                        SELECT
                                            r.*,
                                            u.degree,
                                            u.connection_path
                                        FROM
                                            ratings r
                                        INNER JOIN UserConnections u ON r.rating_profile_id = u.user_id
                                        WHERE
                                            r.rating_business_id IN (
                                                SELECT business_uid
                                                FROM every_circle.business_type
                                                LEFT JOIN every_circle.business ON business_uid = bt_business_id
                                                WHERE bt_type_id = '{type_uid}'
                                            )
                                    )
                                    -- Final selection from RatingMatches to get the required output
                                    -- SELECT * 
                                    -- FROM RatingMatches;
                                    SELECT 
                                        rm.*, 
                                        b.business_name,
                                        b.business_phone_number,
                                        b.business_address_line_1,
                                        b.business_address_line_2,
                                        b.business_city,
                                        b.business_state,
                                        b.business_country,
                                        b.business_zip_code,
                                        b.business_google_id
                                    FROM RatingMatches rm
                                    LEFT JOIN every_circle.business b ON rm.rating_business_id = b.business_uid;
                            '''

                rating_query_response = db.execute(rating_query)

                business_uid_list = tuple(set([business['rating_business_id'] for business in rating_query_response['result']]))

                for business_uid in business_uid_list:

                    charges_stored_procedure_response = db.call(procedure='new_charge_uid')
                    new_charge_uid = charges_stored_procedure_response['result'][0]['new_id']

                    charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    charges_query = f'''
                                            INSERT INTO `every_circle`.`charges` (`charge_uid`, `charge_business_id`, `charge_caused_by_user_id`, `charge_reason`, `charge_amount`, `charge_timestamp`) 
                                            VALUES ('{new_charge_uid}', '{business_uid}', '{profile_id}', 'impression', '1.00', '{charge_timestamp}');
                                    '''

                    charges_query_response = db.execute(charges_query, cmd='post')

                return rating_query_response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500


'''
-- b.business_name,
-- b.business_phone_number,
-- b.business_address_line_1,
-- b.business_address_line_2,
-- b.business_city,
-- b.business_state,
-- b.business_country,
-- b.business_zip_code,
-- b.business_google_id
'''