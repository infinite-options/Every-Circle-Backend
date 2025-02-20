from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage
from charges import Charges
from rapidfuzz import process


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


# class Search_v2(Resource):
#     def get(self, profile_id):
#         print("In Search GET")
#         search_category = request.args.get('category', "").strip()
        
#         if search_category is None:
#             abort(400, description="category is required")
        
#         response = {}
        
#         try:
#             with connect() as db:
#                 all_category_query = db.select('every_circle.category')
#                 all_categories = {category['category_name']: category['category_uid'] for category in all_category_query['result']}

#                 match = process.extractOne(search_category, all_categories.keys(), score_cutoff=70)

#                 if match:
#                     matched_category = match[0]
#                     category_uid = all_categories[matched_category]
#                     print(matched_category, category_uid)
#                 else:
#                     response['message'] = 'category not found'
#                     response['code'] = 200
#                     return response, 200


#                 rating_query = f'''
#                                     WITH UserConnections AS (
#                                         WITH RECURSIVE Referrals AS (
#                                             -- Base case: Start from the given user_id
#                                             SELECT 
#                                                 profile_uid AS user_id,
#                                                 profile_referred_by_user_id,
#                                                 0 AS degree, 
#                                                 CAST(profile_uid AS CHAR(300)) AS connection_path
#                                             FROM profile
#                                             WHERE profile_uid = '{profile_id}'

#                                             UNION ALL

#                                             -- Forward expansion: Find users referred by the current user
#                                             SELECT 
#                                                 p.profile_uid AS user_id,
#                                                 p.profile_referred_by_user_id,
#                                                 r.degree + 1 AS degree,
#                                                 CONCAT(r.connection_path, ' -> ', p.profile_uid) AS connection_path
#                                             FROM profile p
#                                             INNER JOIN Referrals r ON p.profile_referred_by_user_id = r.user_id
#                                             WHERE r.degree < 3 
#                                             AND NOT POSITION(p.profile_uid IN r.connection_path) > 0  -- Prevent revisiting users

#                                             UNION ALL

#                                             -- Backward expansion: Find the user who referred the current user
#                                             SELECT 
#                                                 p.profile_referred_by_user_id AS user_id,
#                                                 p.profile_uid AS profile_referred_by_user_id,
#                                                 r.degree + 1 AS degree,
#                                                 CONCAT(r.connection_path, ' -> ', p.profile_referred_by_user_id) AS connection_path -- Append correctly
#                                             FROM profile p
#                                             INNER JOIN Referrals r ON p.profile_uid = r.user_id
#                                             WHERE r.degree < 3
#                                             AND NOT POSITION(p.profile_referred_by_user_id IN r.connection_path) > 0  -- Prevent revisiting users
#                                         )
#                                         -- Final selection of all users within 3 degrees of connection
#                                         SELECT DISTINCT
#                                             user_id,
#                                             degree,
#                                             connection_path
#                                         FROM Referrals
#                                         ORDER BY degree, connection_path
#                                     ),
#                                     RatingMatches AS (
#                                         -- Match ratings based on UserConnections and business type
#                                         SELECT
#                                             r.*,
#                                             u.degree,
#                                             u.connection_path
#                                         FROM
#                                             ratings r
#                                         INNER JOIN UserConnections u ON r.rating_profile_id = u.user_id
#                                         WHERE
#                                             r.rating_business_id IN (
#                                                 SELECT bc_business_id
#                                                 FROM every_circle.business_category
#                                                 WHERE bc_category_id = '{category_uid}'
#                                             )
#                                     )
#                                     -- Final selection from RatingMatches to get the required output
#                                     -- SELECT * 
#                                     -- FROM RatingMatches;
#                                     SELECT 
#                                         rm.*, 
#                                         b.*
#                                     FROM RatingMatches rm
#                                     LEFT JOIN every_circle.business b ON rm.rating_business_id = b.business_uid;
#                             '''

#                 rating_query_response = db.execute(rating_query)

#                 business_uid_list = tuple(set([business['rating_business_id'] for business in rating_query_response['result']]))

#                 for business_uid in business_uid_list:

#                     charges_stored_procedure_response = db.call(procedure='new_charge_uid')
#                     new_charge_uid = charges_stored_procedure_response['result'][0]['new_id']

#                     charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

#                     charges_query = f'''
#                                             INSERT INTO `every_circle`.`charges` (`charge_uid`, `charge_business_id`, `charge_caused_by_user_id`, `charge_reason`, `charge_amount`, `charge_timestamp`) 
#                                             VALUES ('{new_charge_uid}', '{business_uid}', '{profile_id}', 'impression', '1.00', '{charge_timestamp}');
#                                     '''

#                     charges_query_response = db.execute(charges_query, cmd='post')

#                 return rating_query_response, 200

#         except:
#             response['message'] = 'Internal Server Error'
#             response['code'] = 500
#             return response, 500
        
# this is working
# class Search_v2(Resource):
#     def get(self, profile_id):
#         print("In Search GET")
#         search_category = request.args.get('category', "").strip()
        
#         if search_category is None:
#             abort(400, description="category is required")
        
#         response = {}
        
#         try:
#             with connect() as db:
#                 print(f"Searching for category: {search_category}")
                
#                 # Step 1: Find matching category using fuzzy logic
#                 categories_query = "SELECT category_uid, category_name, category_parent_id FROM category"
#                 categories_result = db.execute(categories_query)
                
#                 if 'result' not in categories_result:
#                     response['message'] = 'No categories found'
#                     response['code'] = 200
#                     return response, 200
                
#                 categories = {cat['category_name']: cat for cat in categories_result['result']}
#                 match = process.extractOne(search_category, categories.keys(), score_cutoff=70)
                
#                 if not match:
#                     response['message'] = 'No matching category found'
#                     response['code'] = 200
#                     return response, 200
                
#                 matched_category = categories[match[0]]
#                 matched_uid = matched_category['category_uid']
#                 parent_id = matched_category['category_parent_id']
#                 print(f"Matched category: {match[0]} (ID: {matched_uid})")

#                 def get_businesses_for_category(category_id):
#                     """Helper function to get businesses for a specific category with ratings and connections"""
#                     rating_query = f"""
#                         WITH UserConnections AS (
#                             WITH RECURSIVE Referrals AS (
#                                 -- Base case: Start from the given user_id
#                                 SELECT 
#                                     profile_uid AS user_id,
#                                     profile_referred_by_user_id,
#                                     0 AS degree, 
#                                     CAST(profile_uid AS CHAR(300)) AS connection_path
#                                 FROM profile
#                                 WHERE profile_uid = '{profile_id}'

#                                 UNION ALL

#                                 -- Forward expansion: Find users referred by the current user
#                                 SELECT 
#                                     p.profile_uid AS user_id,
#                                     p.profile_referred_by_user_id,
#                                     r.degree + 1 AS degree,
#                                     CONCAT(r.connection_path, ' -> ', p.profile_uid) AS connection_path
#                                 FROM profile p
#                                 INNER JOIN Referrals r ON p.profile_referred_by_user_id = r.user_id
#                                 WHERE r.degree < 3 
#                                 AND NOT POSITION(p.profile_uid IN r.connection_path) > 0

#                                 UNION ALL

#                                 -- Backward expansion: Find the user who referred the current user
#                                 SELECT 
#                                     p.profile_referred_by_user_id AS user_id,
#                                     p.profile_uid AS profile_referred_by_user_id,
#                                     r.degree + 1 AS degree,
#                                     CONCAT(r.connection_path, ' -> ', p.profile_referred_by_user_id) AS connection_path
#                                 FROM profile p
#                                 INNER JOIN Referrals r ON p.profile_uid = r.user_id
#                                 WHERE r.degree < 3
#                                 AND NOT POSITION(p.profile_referred_by_user_id IN r.connection_path) > 0
#                             )
#                             -- Final selection of all users within 3 degrees of connection
#                             SELECT DISTINCT
#                                 user_id,
#                                 degree,
#                                 connection_path
#                             FROM Referrals
#                             ORDER BY degree, connection_path
#                         )
#                         SELECT DISTINCT
#                             r.*,
#                             b.*,
#                             uc.degree AS connection_degree,
#                             uc.connection_path
#                         FROM ratings r
#                         INNER JOIN business b ON r.rating_business_id = b.business_uid
#                         INNER JOIN business_category bc ON b.business_uid = bc.bc_business_id
#                         INNER JOIN UserConnections uc ON r.rating_profile_id = uc.user_id
#                         WHERE bc.bc_category_id = '{category_id}'
#                         ORDER BY uc.degree, r.rating_star DESC;
#                     """
#                     return db.execute(rating_query)

#                 # Step 2: Try to find businesses for the matched category
#                 print("Searching businesses for matched category")
#                 direct_results = get_businesses_for_category(matched_uid)
                
#                 if 'result' in direct_results and direct_results['result']:
#                     print(f"Found {len(direct_results['result'])} direct matches")
#                     direct_results['search_level'] = 'direct'
#                     direct_results['message'] = f"Found businesses matching '{match[0]}'"
                    
#                     # Process charges for direct matches
#                     business_uid_list = list(set([rating['rating_business_id'] for rating in direct_results['result']]))
#                     for business_uid in business_uid_list:
#                         new_charge_uid = db.call(procedure='new_charge_uid')['result'][0]['new_id']
#                         charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                         charges_query = f"""
#                             INSERT INTO charges (
#                                 charge_uid, charge_business_id, charge_caused_by_user_id,
#                                 charge_reason, charge_amount, charge_timestamp
#                             ) VALUES (
#                                 '{new_charge_uid}', '{business_uid}', '{profile_id}',
#                                 'impression', '1.00', '{charge_timestamp}'
#                             )
#                         """
#                         db.execute(charges_query, cmd='post')
                    
#                     return direct_results, 200
                
#                 # Step 3: If no direct matches, look for sibling categories
#                 print("Looking for sibling categories")
#                 if parent_id:
#                     sibling_query = f"""
#                         SELECT category_uid, category_name 
#                         FROM category 
#                         WHERE category_parent_id = '{parent_id}'
#                         AND category_uid != '{matched_uid}'
#                     """
#                     siblings = db.execute(sibling_query)
                    
#                     if 'result' in siblings and siblings['result']:
#                         print("Checking businesses in sibling categories")
#                         for sibling in siblings['result']:
#                             sibling_results = get_businesses_for_category(sibling['category_uid'])
#                             if 'result' in sibling_results and sibling_results['result']:
#                                 print(f"Found {len(sibling_results['result'])} matches in sibling category")
#                                 sibling_results['search_level'] = 'sibling'
#                                 sibling_results['message'] = f"Found businesses in related category '{sibling['category_name']}'"
                                
#                                 # Process charges for sibling matches
#                                 business_uid_list = list(set([rating['rating_business_id'] for rating in sibling_results['result']]))
#                                 for business_uid in business_uid_list:
#                                     new_charge_uid = db.call(procedure='new_charge_uid')['result'][0]['new_id']
#                                     charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                                     charges_query = f"""
#                                         INSERT INTO charges (
#                                             charge_uid, charge_business_id, charge_caused_by_user_id,
#                                             charge_reason, charge_amount, charge_timestamp
#                                         ) VALUES (
#                                             '{new_charge_uid}', '{business_uid}', '{profile_id}',
#                                             'impression', '1.00', '{charge_timestamp}'
#                                         )
#                                     """
#                                     db.execute(charges_query, cmd='post')
                                
#                                 return sibling_results, 200
                
#                 # Step 4: If no sibling matches, try parent category
#                 print("Looking for parent category businesses")
#                 if parent_id:
#                     parent_results = get_businesses_for_category(parent_id)
#                     if 'result' in parent_results and parent_results['result']:
#                         print(f"Found {len(parent_results['result'])} matches in parent category")
#                         parent_results['search_level'] = 'parent'
#                         parent_results['message'] = f"Found businesses in broader category"
                        
#                         # Process charges for parent matches
#                         business_uid_list = list(set([rating['rating_business_id'] for rating in parent_results['result']]))
#                         for business_uid in business_uid_list:
#                             new_charge_uid = db.call(procedure='new_charge_uid')['result'][0]['new_id']
#                             charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                             charges_query = f"""
#                                 INSERT INTO charges (
#                                     charge_uid, charge_business_id, charge_caused_by_user_id,
#                                     charge_reason, charge_amount, charge_timestamp
#                                 ) VALUES (
#                                     '{new_charge_uid}', '{business_uid}', '{profile_id}',
#                                     'impression', '1.00', '{charge_timestamp}'
#                                 )
#                             """
#                             db.execute(charges_query, cmd='post')
                        
#                         return parent_results, 200
                
#                 # No results found at any level
#                 response['message'] = 'No businesses found in this or related categories'
#                 response['code'] = 200
#                 return response, 200

#         except Exception as e:
#             print(f"Error in search: {str(e)}")
#             print(f"Error details: {type(e).__name__}")
#             import traceback
#             print(traceback.format_exc())
#             response['message'] = 'Internal Server Error'
#             response['code'] = 500
#             return response, 500


class Search_v2(Resource):
    def get(self, profile_id):
        print("In Search GET")
        search_category = request.args.get('category', "").strip().lower()
        
        if search_category is None:
            abort(400, description="category is required")
        
        response = {}
        
        try:
            with connect() as db:
                print(f"Searching for category: {search_category}")
                
                # Split search terms and remove common words
                search_words = set(search_category.split())
                
                # First try exact match
                exact_match_query = f"""
                    SELECT category_uid, category_name, category_parent_id 
                    FROM category 
                    WHERE LOWER(category_name) = '{search_category}'
                """
                exact_match = db.execute(exact_match_query)
                
                if 'result' in exact_match and exact_match['result']:
                    matched_category = exact_match['result'][0]
                    print(f"Found exact match: {matched_category['category_name']}")
                else:
                    # If no exact match, search for partial matches
                    search_conditions = " OR ".join([f"LOWER(category_name) LIKE '%{word}%'" for word in search_words])
                    partial_match_query = f"""
                        SELECT 
                            category_uid, 
                            category_name, 
                            category_parent_id,
                            (
                                {" + ".join([f"(CASE WHEN LOWER(category_name) LIKE '%{word}%' THEN 1 ELSE 0 END)" for word in search_words])}
                            ) as match_count
                        FROM category 
                        WHERE {search_conditions}
                        ORDER BY match_count DESC, LENGTH(category_name)
                        LIMIT 1
                    """
                    partial_matches = db.execute(partial_match_query)
                    
                    if 'result' not in partial_matches or not partial_matches['result']:
                        response['message'] = 'No matching category found'
                        response['code'] = 200
                        return response, 200
                        
                    matched_category = partial_matches['result'][0]
                    print(f"Found partial match: {matched_category['category_name']}")
                
                matched_uid = matched_category['category_uid']
                parent_id = matched_category['category_parent_id']

                # Rest of your existing code for getting businesses and user connections...
                def get_businesses_for_category(category_id):
                    """Helper function to get businesses for a specific category with ratings and connections"""
                    rating_query = f"""
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
                                AND NOT POSITION(p.profile_uid IN r.connection_path) > 0

                                UNION ALL

                                -- Backward expansion: Find the user who referred the current user
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
                            SELECT DISTINCT
                                user_id,
                                degree,
                                connection_path
                            FROM Referrals
                            ORDER BY degree, connection_path
                        )
                        SELECT DISTINCT
                            r.*,
                            b.*,
                            uc.degree AS connection_degree,
                            uc.connection_path
                        FROM ratings r
                        INNER JOIN business b ON r.rating_business_id = b.business_uid
                        INNER JOIN business_category bc ON b.business_uid = bc.bc_business_id
                        INNER JOIN UserConnections uc ON r.rating_profile_id = uc.user_id
                        WHERE bc.bc_category_id = '{category_id}'
                        ORDER BY uc.degree, r.rating_star DESC
                    """
                    return db.execute(rating_query)

                # Continue with the rest of your existing code for searching businesses, siblings, etc...
                # [Previous implementation for direct search, sibling search, and parent search remains the same]
                
                # The rest of your code remains the same...
                # Step 2: Try to find businesses for the matched category
                print("Searching businesses for matched category")
                direct_results = get_businesses_for_category(matched_uid)
                
                if 'result' in direct_results and direct_results['result']:
                    print(f"Found {len(direct_results['result'])} direct matches")
                    direct_results['search_level'] = 'direct'
                    # direct_results['message'] = f"Found businesses matching '{match[0]}'"
                    
                    # Process charges for direct matches
                    business_uid_list = list(set([rating['rating_business_id'] for rating in direct_results['result']]))
                    for business_uid in business_uid_list:
                        new_charge_uid = db.call(procedure='new_charge_uid')['result'][0]['new_id']
                        charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        charges_query = f"""
                            INSERT INTO charges (
                                charge_uid, charge_business_id, charge_caused_by_user_id,
                                charge_reason, charge_amount, charge_timestamp
                            ) VALUES (
                                '{new_charge_uid}', '{business_uid}', '{profile_id}',
                                'impression', '1.00', '{charge_timestamp}'
                            )
                        """
                        db.execute(charges_query, cmd='post')
                    
                    return direct_results, 200
                
                # Step 3: If no direct matches, look for sibling categories
                print("Looking for sibling categories")
                if parent_id:
                    sibling_query = f"""
                        SELECT category_uid, category_name 
                        FROM category 
                        WHERE category_parent_id = '{parent_id}'
                        AND category_uid != '{matched_uid}'
                    """
                    siblings = db.execute(sibling_query)
                    
                    if 'result' in siblings and siblings['result']:
                        print("Checking businesses in sibling categories")
                        for sibling in siblings['result']:
                            sibling_results = get_businesses_for_category(sibling['category_uid'])
                            if 'result' in sibling_results and sibling_results['result']:
                                print(f"Found {len(sibling_results['result'])} matches in sibling category")
                                sibling_results['search_level'] = 'sibling'
                                sibling_results['message'] = f"Found businesses in related category '{sibling['category_name']}'"
                                
                                # Process charges for sibling matches
                                business_uid_list = list(set([rating['rating_business_id'] for rating in sibling_results['result']]))
                                for business_uid in business_uid_list:
                                    new_charge_uid = db.call(procedure='new_charge_uid')['result'][0]['new_id']
                                    charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    charges_query = f"""
                                        INSERT INTO charges (
                                            charge_uid, charge_business_id, charge_caused_by_user_id,
                                            charge_reason, charge_amount, charge_timestamp
                                        ) VALUES (
                                            '{new_charge_uid}', '{business_uid}', '{profile_id}',
                                            'impression', '1.00', '{charge_timestamp}'
                                        )
                                    """
                                    db.execute(charges_query, cmd='post')
                                
                                return sibling_results, 200
                
                # Step 4: If no sibling matches, try parent category
                print("Looking for parent category businesses")
                if parent_id:
                    parent_results = get_businesses_for_category(parent_id)
                    if 'result' in parent_results and parent_results['result']:
                        print(f"Found {len(parent_results['result'])} matches in parent category")
                        parent_results['search_level'] = 'parent'
                        parent_results['message'] = f"Found businesses in broader category"
                        
                        # Process charges for parent matches
                        business_uid_list = list(set([rating['rating_business_id'] for rating in parent_results['result']]))
                        for business_uid in business_uid_list:
                            new_charge_uid = db.call(procedure='new_charge_uid')['result'][0]['new_id']
                            charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                            charges_query = f"""
                                INSERT INTO charges (
                                    charge_uid, charge_business_id, charge_caused_by_user_id,
                                    charge_reason, charge_amount, charge_timestamp
                                ) VALUES (
                                    '{new_charge_uid}', '{business_uid}', '{profile_id}',
                                    'impression', '1.00', '{charge_timestamp}'
                                )
                            """
                            db.execute(charges_query, cmd='post')
                        
                        return parent_results, 200
                
                # No results found at any level
                response['message'] = 'No businesses found in this or related categories'
                response['code'] = 200
                return response, 200
        
        except Exception as e:
            print(f"Error in search: {str(e)}")
            print(f"Error details: {type(e).__name__}")
            import traceback
            print(traceback.format_exc())
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500


# class Search_v2(Resource):
#     def get(self, profile_id):
#         print("In Search GET")
#         search_category = request.args.get('category', "").strip().lower()
        
#         if search_category is None:
#             abort(400, description="category is required")
        
#         response = {}
        
#         try:
#             with connect() as db:
#                 print(f"Searching for category: {search_category}")

#                 def is_parent_category(category_uid):
#                     """Check if the category is a parent category based on UID pattern"""
#                     parts = category_uid.split('-')
#                     if len(parts) != 2:
#                         return False
#                     return parts[1].endswith('0000') or parts[1].endswith('00')

#                 def get_category_level(category_uid):
#                     """
#                     Determine the level of the category based on its UID
#                     Returns: 'main_parent' (220-010000), 'sub_parent' (220-010100), or 'leaf' (220-010101)
#                     """
#                     if not category_uid:
#                         return None
#                     parts = category_uid.split('-')
#                     if len(parts) != 2:
#                         return None
                    
#                     num_part = parts[1]
#                     if num_part.endswith('0000'):
#                         return 'main_parent'
#                     elif num_part.endswith('00'):
#                         return 'sub_parent'
#                     return 'leaf'

#                 def get_parent_uid(uid):
#                     """Get parent UID based on the current UID level"""
#                     if not uid:
#                         return None
                    
#                     level = get_category_level(uid)
#                     if level == 'leaf':  # e.g., 220-010101 -> 220-010100
#                         return f"{uid[:7]}00"
#                     elif level == 'sub_parent':  # e.g., 220-010100 -> 220-010000
#                         return f"{uid[:4]}0000"
#                     return None

#                 def get_all_child_categories(parent_uid):
#                     """Get all child categories for a parent UID"""
#                     if not parent_uid:
#                         return []
                    
#                     level = get_category_level(parent_uid)
#                     pattern = None
                    
#                     if level == 'main_parent':  # e.g., 220-010000
#                         pattern = f"{parent_uid[:4]}%"
#                     elif level == 'sub_parent':  # e.g., 220-010100
#                         pattern = f"{parent_uid[:7]}%"
                    
#                     if not pattern:
#                         return []
                        
#                     child_query = f"""
#                         SELECT category_uid, category_name 
#                         FROM category 
#                         WHERE category_uid LIKE '{pattern}'
#                         AND category_uid != '{parent_uid}'
#                         ORDER BY category_uid
#                     """
#                     result = db.execute(child_query)
#                     return result.get('result', [])

#                 def find_matching_categories(search_term):
#                     """Find all categories that match the search term, including parents"""
#                     # First try exact match
#                     exact_match_query = f"""
#                         SELECT category_uid, category_name, 
#                                CASE 
#                                    WHEN category_uid LIKE '%-0000' THEN 'main_parent'
#                                    WHEN category_uid LIKE '%-00' THEN 'sub_parent'
#                                    ELSE 'leaf'
#                                END as category_type
#                         FROM category 
#                         WHERE LOWER(category_name) = '{search_term}'
#                     """
#                     exact_match = db.execute(exact_match_query)
                    
#                     if 'result' in exact_match and exact_match['result']:
#                         return exact_match['result']
                    
#                     # If no exact match, search for partial matches
#                     search_words = set(search_term.split())
#                     search_conditions = " OR ".join([f"LOWER(category_name) LIKE '%{word}%'" for word in search_words])
                    
#                     partial_match_query = f"""
#                         SELECT 
#                             category_uid, 
#                             category_name,
#                             CASE 
#                                 WHEN category_uid LIKE '%-0000' THEN 'main_parent'
#                                 WHEN category_uid LIKE '%-00' THEN 'sub_parent'
#                                 ELSE 'leaf'
#                             END as category_type,
#                             (
#                                 {" + ".join([f"(CASE WHEN LOWER(category_name) LIKE '%{word}%' THEN 1 ELSE 0 END)" for word in search_words])}
#                             ) as match_count
#                         FROM category 
#                         WHERE {search_conditions}
#                         ORDER BY match_count DESC, 
#                                 CASE 
#                                     WHEN category_uid LIKE '%-0000' THEN 1
#                                     WHEN category_uid LIKE '%-00' THEN 2
#                                     ELSE 3
#                                 END,
#                                 LENGTH(category_name)
#                     """
#                     partial_matches = db.execute(partial_match_query)
#                     return partial_matches.get('result', [])

#                 def get_businesses_for_category(category_id, relationship='direct'):
#                     """Get businesses for a category with ratings and connections"""
#                     rating_query = f"""
#                         WITH UserConnections AS (
#                             WITH RECURSIVE Referrals AS (
#                                 SELECT 
#                                     profile_uid AS user_id,
#                                     profile_referred_by_user_id,
#                                     0 AS degree, 
#                                     CAST(profile_uid AS CHAR(300)) AS connection_path
#                                 FROM profile
#                                 WHERE profile_uid = '{profile_id}'

#                                 UNION ALL

#                                 SELECT 
#                                     p.profile_uid AS user_id,
#                                     p.profile_referred_by_user_id,
#                                     r.degree + 1 AS degree,
#                                     CONCAT(r.connection_path, ' -> ', p.profile_uid) AS connection_path
#                                 FROM profile p
#                                 INNER JOIN Referrals r ON p.profile_referred_by_user_id = r.user_id
#                                 WHERE r.degree < 3 
#                                 AND NOT POSITION(p.profile_uid IN r.connection_path) > 0

#                                 UNION ALL

#                                 SELECT 
#                                     p.profile_referred_by_user_id AS user_id,
#                                     p.profile_uid AS profile_referred_by_user_id,
#                                     r.degree + 1 AS degree,
#                                     CONCAT(r.connection_path, ' -> ', p.profile_referred_by_user_id) AS connection_path
#                                 FROM profile p
#                                 INNER JOIN Referrals r ON p.profile_uid = r.user_id
#                                 WHERE r.degree < 3
#                                 AND NOT POSITION(p.profile_referred_by_user_id IN r.connection_path) > 0
#                             )
#                             SELECT DISTINCT
#                                 user_id,
#                                 degree,
#                                 connection_path
#                             FROM Referrals
#                             ORDER BY degree, connection_path
#                         )
#                         SELECT DISTINCT
#                             r.*,
#                             b.*,
#                             c.category_name,
#                             c.category_uid,
#                             uc.degree AS connection_degree,
#                             uc.connection_path,
#                             '{relationship}' as result_type
#                         FROM ratings r
#                         INNER JOIN business b ON r.rating_business_id = b.business_uid
#                         INNER JOIN business_category bc ON b.business_uid = bc.bc_business_id
#                         INNER JOIN category c ON bc.bc_category_id = c.category_uid
#                         INNER JOIN UserConnections uc ON r.rating_profile_id = uc.user_id
#                         WHERE bc.bc_category_id = '{category_id}'
#                         ORDER BY uc.degree, r.rating_star DESC
#                     """
#                     return db.execute(rating_query)

#                 def process_charges(business_results):
#                     """Process charges for business impressions"""
#                     if 'result' in business_results and business_results['result']:
#                         business_uid_list = list(set([rating['rating_business_id'] for rating in business_results['result']]))
#                         for business_uid in business_uid_list:
#                             new_charge_uid = db.call(procedure='new_charge_uid')['result'][0]['new_id']
#                             charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                             charges_query = f"""
#                                 INSERT INTO charges (
#                                     charge_uid, charge_business_id, charge_caused_by_user_id,
#                                     charge_reason, charge_amount, charge_timestamp
#                                 ) VALUES (
#                                     '{new_charge_uid}', '{business_uid}', '{profile_id}',
#                                     'impression', '1.00', '{charge_timestamp}'
#                                 )
#                             """
#                             db.execute(charges_query, cmd='post')

#                 # Find all matching categories
#                 matching_categories = find_matching_categories(search_category)
                
#                 if not matching_categories:
#                     response['message'] = 'No matching category found'
#                     response['code'] = 200
#                     return response, 200

#                 # Initialize results structure
#                 combined_results = {
#                     'exact_matches': [],
#                     'child_categories': [],
#                     'sibling_categories': [],
#                     'parent_categories': [],
#                     'related_categories': []
#                 }

#                 processed_categories = set()  # Track processed categories to avoid duplicates

#                 # Process each matching category
#                 for category in matching_categories:
#                     category_uid = category['category_uid']
#                     if category_uid in processed_categories:
#                         continue
                        
#                     category_level = get_category_level(category_uid)
                    
#                     # Get direct matches
#                     direct_results = get_businesses_for_category(category_uid, 'direct')
#                     if 'result' in direct_results and direct_results['result']:
#                         combined_results['exact_matches'].extend(direct_results['result'])
#                         process_charges(direct_results)
#                         processed_categories.add(category_uid)

#                     # If it's a parent category, get all child businesses
#                     if is_parent_category(category_uid):
#                         child_categories = get_all_child_categories(category_uid)
#                         for child in child_categories:
#                             if child['category_uid'] in processed_categories:
#                                 continue
#                             child_results = get_businesses_for_category(child['category_uid'], 'child')
#                             if 'result' in child_results and child_results['result']:
#                                 combined_results['child_categories'].extend(child_results['result'])
#                                 process_charges(child_results)
#                                 processed_categories.add(child['category_uid'])

#                     # Get sibling and parent categories
#                     parent_uid = get_parent_uid(category_uid)
#                     if parent_uid:
#                         # Get parent results
#                         if parent_uid not in processed_categories:
#                             parent_results = get_businesses_for_category(parent_uid, 'parent')
#                             if 'result' in parent_results and parent_results['result']:
#                                 combined_results['parent_categories'].extend(parent_results['result'])
#                                 process_charges(parent_results)
#                                 processed_categories.add(parent_uid)

#                         # Get sibling results
#                         siblings = get_all_child_categories(parent_uid)
#                         for sibling in siblings:
#                             if sibling['category_uid'] in processed_categories:
#                                 continue
#                             sibling_results = get_businesses_for_category(sibling['category_uid'], 'sibling')
#                             if 'result' in sibling_results and sibling_results['result']:
#                                 combined_results['sibling_categories'].extend(sibling_results['result'])
#                                 process_charges(sibling_results)
#                                 processed_categories.add(sibling['category_uid'])

#                         # Get parent's siblings (related categories)
#                         grandparent_uid = get_parent_uid(parent_uid)
#                         if grandparent_uid:
#                             parent_siblings = get_all_child_categories(grandparent_uid)
#                             for parent_sibling in parent_siblings:
#                                 if parent_sibling['category_uid'] in processed_categories:
#                                     continue
#                                 related_results = get_businesses_for_category(
#                                     parent_sibling['category_uid'], 
#                                     'related'
#                                 )
#                                 if 'result' in related_results and related_results['result']:
#                                     combined_results['related_categories'].extend(related_results['result'])
#                                     process_charges(related_results)
#                                     processed_categories.add(parent_sibling['category_uid'])

#                 # Prepare final response
#                 if any(results for results in combined_results.values()):
#                     response = {
#                         'code': 200,
#                         'message': 'Found businesses in various categories',
#                         'results': combined_results,
#                         'search_term': search_category
#                     }
#                     return response, 200
                
#                 response['message'] = 'No businesses found in this or related categories'
#                 response['code'] = 200
#                 return response, 200
        
#         except Exception as e:
#             print(f"Error in search: {str(e)}")
#             print(f"Error details: {type(e).__name__}")
#             import traceback
#             print(traceback.format_exc())
#             response['message'] = 'Internal Server Error'
#             response['code'] = 500
#             return response, 500





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