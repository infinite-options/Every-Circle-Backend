from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime
import ast

from data_ec import connect, uploadImage, s3, processImage

# add google social id in GET api
class Business(Resource):
    def get(self, uid):
        print("In Business GET")
        response = {}
        try:
            print(uid, type(uid))
            with connect() as db:
                key = {}
                if uid[:3] == "200":
                    key['business_uid'] = uid
                
                elif uid[:3] == "210":
                    key['business_type_id'] = uid
                
                elif uid[:3] == "100":
                    key['business_user_id'] = uid

                else:
                    key['business_google_id'] = uid
                    # response['message'] = 'Invalid UID'
                    # response['code'] = 400
                    # return response, 400
            
                response = db.select('every_circle.business', where=key)

            if not response['result']:
                response.pop('result')
                response['message'] = f'No business found for {key}'
                response['code'] = 404
                return response, 404

            return response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def post(self):
        print("In Business POST")
        response = {}

        def check_type(sub_type, business_uid):
            print("In Check Type")
            with connect() as db:
                type_query = db.select('every_circle.types', where={'sub_type': sub_type})
                if not type_query['result']:
                    type_stored_procedure_response = db.call(procedure='new_type_uid')
                    type_uid = type_stored_procedure_response['result'][0]['new_id']

                    type_payload = {}
                    type_payload['type_uid'] = type_uid
                    type_payload['sub_type'] = sub_type
                    type_insert_query = db.insert('every_circle.types', type_payload)
                
                else:
                    type_uid = type_query['result'][0]['type_uid']
                
                print(type_uid)
                business_type_stored_procedure_response = db.call(procedure='new_bt_uid')
                bt_uid = business_type_stored_procedure_response['result'][0]['new_id']
                business_type_payload = {}
                business_type_payload['bt_uid'] = bt_uid
                business_type_payload['bt_business_id'] = business_uid
                business_type_payload['bt_type_id'] = type_uid
                business_type_insert_query = db.insert('every_circle.business_type', business_type_payload)

        try:
            payload = request.form.to_dict()

            if 'user_uid' not in payload:
                    response['message'] = 'user_uid is required to register a business'
                    response['code'] = 400
                    return response, 400

            user_uid = payload.pop('user_uid')

            with connect() as db:

                # Check if the user exists
                user_exists_query = db.select('every_circle.users', where={'user_uid': user_uid})
                if not user_exists_query['result']:
                    response['message'] = 'User does not exist'
                    response['code'] = 404
                    return response, 404
                
                business_google_id = payload.get('business_google_id', None)
                business_name = payload.get('business_name', None)

                if not business_google_id and not business_name:
                    response['message'] = 'business_google_id or business_name is required'
                    response['code'] = 400
                    return response, 400

                if business_google_id:
                    business_query = db.select('every_circle.business', where={'business_google_id': business_google_id})
                elif business_name:
                    business_query = db.select('every_circle.business', where={'business_name': business_name})
                
                if not business_query['result']:
                    business_stored_procedure_response = db.call(procedure='new_business_uid')
                    new_business_uid = business_stored_procedure_response['result'][0]['new_id']
                    key = {'business_uid': new_business_uid}

                    if 'business_types' in payload:
                        business_types = payload.pop('business_types')
                        print('\n' + business_types)
                        business_types = ast.literal_eval(business_types)
                        print(business_types)
                        for business_type in business_types:
                            check_type(business_type, new_business_uid)

                    payload['business_uid'] = new_business_uid
                    payload['business_user_id'] = user_uid
                    payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    processImage(key, payload)

                    response = db.insert('every_circle.business', payload)
                
                else:
                    response['message'] = 'Business: Business already exists'
                    response['code'] = 409
                    return response, 409
            
            response['business_uid'] = new_business_uid

            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def put(self):
        print("In Business PUT")
        response = {}

        try:
            payload = request.form.to_dict()

            if 'business_uid' not in payload:
                    response['message'] = 'business_uid is required'
                    response['code'] = 400
                    return response, 400

            business_uid = payload.pop('business_uid')
            key = {'business_uid': business_uid}

            with connect() as db:

                # Check if the business exists
                business_exists_query = db.select('every_circle.business', where=key)
                print(business_exists_query)
                if not business_exists_query['result']:
                    response['message'] = 'Business does not exist'
                    response['code'] = 404
                    return response, 404
                
                processImage(key, payload)
                
                response = db.update('every_circle.business', key, payload)
            
            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

class Business_v2(Resource):
    def get(self, uid):
        print("In Business GET")
        response = {}
        try:
            print(uid, type(uid))
            with connect() as db:
                key = {}
                if uid[:3] == "200":
                    key['business_uid'] = uid
                
                elif uid[:3] == "100":
                    key['business_user_id'] = uid

                else:
                    key['business_google_id'] = uid

                response = db.select('every_circle.business', where=key)

            if not response['result']:
                response.pop('result')
                response['message'] = f'No business found for {key}'
                response['code'] = 404
                return response, 404

            # final_response = {}
            # for business in response['result']:
            #     print(business)
            #     query = f'''
            #                 SELECT *
            #                 FROM every_circle.business_category
            #                 WHERE bc_business_id = "{business['business_uid']}";
            #             '''

            #     category_response = db.execute(query)
            #     # category_response = db.select('every_circle.business_category', where={'bc_business_id': business['business_uid']})
            #     print(category_response, '\n\nCategory Response')
            #     final_response[business['business_uid']] = [business, category_response['result']]

            #     print(final_response)
            return response, 200

        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def post(self):
        print("In Business POST")
        response = {}

        def check_category(category_uid, business_uid):
            print("In Check Category")
            with connect() as db:
                
                business_category_stored_procedure_response = db.call(procedure='new_bc_uid')
                bc_uid = business_category_stored_procedure_response['result'][0]['new_id']
                business_category_payload = {}
                business_category_payload['bc_uid'] = bc_uid
                business_category_payload['bc_business_id'] = business_uid
                business_category_payload['bc_category_id'] = category_uid
                business_category_insert_query = db.insert('every_circle.business_category', business_category_payload)

        try:
            payload = request.form.to_dict()

            if 'user_uid' not in payload:
                    response['message'] = 'user_uid is required to register a business'
                    response['code'] = 400
                    return response, 400

            user_uid = payload.pop('user_uid')

            with connect() as db:

                # Check if the user exists
                user_exists_query = db.select('every_circle.users', where={'user_uid': user_uid})
                if not user_exists_query['result']:
                    response['message'] = 'User does not exist'
                    response['code'] = 404
                    return response, 404
                
                business_google_id = payload.get('business_google_id', None)
                business_name = payload.get('business_name', None)

                if not business_google_id and not business_name:
                    response['message'] = 'business_google_id or business_name is required'
                    response['code'] = 400
                    return response, 400

                if business_google_id:
                    business_look_up_query = db.select('every_circle.business', where={'business_google_id': business_google_id})
                elif business_name:
                    business_look_up_query = db.select('every_circle.business', where={'business_name': business_name})
                
                if not business_look_up_query['result']:
                    business_stored_procedure_response = db.call(procedure='new_business_uid')
                    new_business_uid = business_stored_procedure_response['result'][0]['new_id']
                    key = {'business_uid': new_business_uid}

                    if 'business_categories_uid' in payload:
                        business_categories_uid = payload.pop('business_categories_uid')
                        print('\n' + business_categories_uid)
                        business_categories_uid = ast.literal_eval(business_categories_uid)
                        print(business_categories_uid)
                        for business_category_uid in business_categories_uid:
                            check_category(business_category_uid, new_business_uid)

                    payload['business_uid'] = new_business_uid
                    payload['business_user_id'] = user_uid
                    payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    processImage(key, payload)

                    response = db.insert('every_circle.business', payload)
                
                else:
                    response['message'] = 'Business v2: Business already exists'
                    response['code'] = 409
                    return response, 409
            
            response['business_uid'] = new_business_uid

            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def put(self):
        print("In Business PUT")
        response = {}

        def check_category(category_uid, business_uid):
            print("In Check Category")
            with connect() as db:

                check_query = f'''
                                SELECT *
                                FROM every_circle.business_category
                                WHERE bc_business_id = "{business_uid}" AND bc_category_id = "{category_uid}";
                              '''
                check_query_response = db.execute(check_query)
                print('CHECK QUERY RESPOSNE', check_query_response)
                if len(check_query_response['result']) > 0:
                    return
                
                business_category_stored_procedure_response = db.call(procedure='new_bc_uid')
                bc_uid = business_category_stored_procedure_response['result'][0]['new_id']
                business_category_payload = {}
                business_category_payload['bc_uid'] = bc_uid
                business_category_payload['bc_business_id'] = business_uid
                business_category_payload['bc_category_id'] = category_uid
                business_category_insert_query = db.insert('every_circle.business_category', business_category_payload)
                print(business_category_insert_query)

        try:
            payload = request.form.to_dict()

            if 'business_uid' not in payload:
                    response['message'] = 'business_uid is required'
                    response['code'] = 400
                    return response, 400

            business_uid = payload.pop('business_uid')
            key = {'business_uid': business_uid}

            with connect() as db:

                # Check if the business exists
                business_exists_query = db.select('every_circle.business', where=key)
                print(business_exists_query)
                if not business_exists_query['result']:
                    response['message'] = 'Business does not exist'
                    response['code'] = 404
                    return response, 404
                
                if 'business_categories_uid' in payload:
                    print("in business categories uid")
                    business_categories_uid = payload.pop('business_categories_uid')
                    print('\n' + business_categories_uid)
                    business_categories_uid = ast.literal_eval(business_categories_uid)
                    print(business_categories_uid)
                    for business_category_uid in business_categories_uid:
                        check_category(business_category_uid, business_uid)

                processImage(key, payload)
                
                response = db.update('every_circle.business', key, payload)
            
            return response, 200
        
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
        

class Businesses(Resource):
    def get(self):
        print("In Businesses GET")
        response = {}
        try:
            with connect() as db:
                # Get list of businesses
                business_list = f"""
                        SELECT business_uid, business_name FROM every_circle.business
                        ORDER BY business_name
                """
                businesses = db.execute(business_list)
                response = businesses['result']
                return response, 200
        except:
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
        

class BusinessAvgRatings(Resource):
    def get(self):
        business_uids = request.args.get('uids', '').strip()
        viewer_uid = request.args.get('viewer_uid', '').strip()
        if not business_uids:
            return {'message': 'uids required', 'code': 400}, 400
        
        uid_list = [uid.strip() for uid in business_uids.split(',')]
        placeholders = ','.join([f"'{uid}'" for uid in uid_list])
        
        response = {}
        try:
            with connect() as db:
                # Initialize all requested businesses (even those with no ratings)
                ratings_map = {
                    uid: {
                        'avg_rating': None,
                        'rating_count': 0,
                        'nearest_connection': None,
                    }
                    for uid in uid_list
                }

                query = f"""
                    SELECT 
                        rating_business_id,
                        ROUND(AVG(rating_star), 1) AS avg_rating,
                        COUNT(rating_uid) AS rating_count
                    FROM every_circle.ratings
                    WHERE rating_business_id IN ({placeholders})
                    GROUP BY rating_business_id
                """
                result = db.execute(query)
                if result['result']:
                    for row in result['result']:
                        bid = row['rating_business_id']
                        ratings_map[bid]['avg_rating'] = row['avg_rating']
                        ratings_map[bid]['rating_count'] = row['rating_count']

                # If viewer_uid provided, BFS through referral tree to find
                # degree from viewer to any reviewer of each business
                if viewer_uid:
                    # Get all reviewer UIDs for these businesses
                    reviewer_query = f"""
                        SELECT rating_business_id, rating_profile_id
                        FROM every_circle.ratings
                        WHERE rating_business_id IN ({placeholders})
                    """
                    reviewer_result = db.execute(reviewer_query)
                    biz_reviewers = {}
                    if reviewer_result.get('result'):
                        for row in reviewer_result['result']:
                            biz_reviewers.setdefault(
                                row['rating_business_id'], []
                            ).append(row['rating_profile_id'])

                    all_reviewer_uids = set(
                        uid for uids in biz_reviewers.values() for uid in uids
                    )

                    if all_reviewer_uids:
                        # BFS: expand frontier in both directions (down=referred by me, up=who referred me)
                        reviewer_degrees = {}
                        seen = {viewer_uid}
                        frontier = [viewer_uid]

                        for degree in range(1, 6):
                            if not frontier:
                                break
                            if not (all_reviewer_uids - set(reviewer_degrees.keys())):
                                break
                            ph = ','.join(f"'{u}'" for u in frontier)
                            r_down = db.execute(
                                f"SELECT profile_personal_uid FROM every_circle.profile_personal WHERE profile_personal_referred_by IN ({ph})"
                            )
                            r_up = db.execute(
                                f"SELECT profile_personal_referred_by AS uid FROM every_circle.profile_personal WHERE profile_personal_uid IN ({ph}) AND profile_personal_referred_by IS NOT NULL"
                            )
                            next_frontier = []
                            for row in (r_down.get('result') or []) + (r_up.get('result') or []):
                                uid = row.get('profile_personal_uid') or row.get('uid')
                                if uid and uid not in seen:
                                    seen.add(uid)
                                    next_frontier.append(uid)
                                    if uid in all_reviewer_uids:
                                        reviewer_degrees[uid] = degree
                            frontier = next_frontier

                        # For each business, pick the min degree among its reviewers
                        for bid, reviewers in biz_reviewers.items():
                            degrees = [
                                reviewer_degrees[r]
                                for r in reviewers
                                if r in reviewer_degrees
                            ]
                            if degrees:
                                ratings_map[bid]['nearest_connection'] = min(degrees)

                response['result'] = ratings_map
                return response, 200
        except Exception as e:
            return {'message': 'Internal Server Error', 'code': 500}, 500


class BusinessMaxBounty(Resource):
    def get(self):
        business_uids = request.args.get('uids', '').strip()
        if not business_uids:
            return {'message': 'uids required', 'code': 400}, 400

        uid_list = [uid.strip() for uid in business_uids.split(',')]
        placeholders = ','.join([f"'{uid}'" for uid in uid_list])

        response = {}
        try:
            with connect() as db:
                query = f"""
                    SELECT
                        bs_business_id,
                        MAX(CASE WHEN bs_bounty_type = 'per_item' THEN bs_bounty END) AS max_per_item_bounty,
                        MAX(CASE WHEN bs_bounty_type = 'total'    THEN bs_bounty END) AS max_total_bounty,
                        MAX(bs_bounty) AS max_bounty
                    FROM every_circle.business_services
                    WHERE bs_business_id IN ({placeholders})
                      AND bs_bounty IS NOT NULL AND bs_bounty > 0
                    GROUP BY bs_business_id
                """
                result = db.execute(query)
                bounty_map = {}
                if result['result']:
                    for row in result['result']:
                        bounty_map[row['bs_business_id']] = {
                            'max_bounty': row['max_bounty'],
                            'max_per_item_bounty': row['max_per_item_bounty'],
                            'max_total_bounty': row['max_total_bounty'],
                        }
                response['result'] = bounty_map
                return response, 200
        except Exception as e:
            print(f"Error in BusinessMaxBounty GET: {str(e)}")
            return {'message': 'Internal Server Error', 'code': 500}, 500


class BusinessTagSearch(Resource):
    def get(self):
        query = request.args.get('q', '').strip().lower()
        if not query:
            return {'message': 'q parameter required', 'code': 400}, 400
        
        response = {}
        try:
            with connect() as db:
                tag_query = f"""
                    SELECT DISTINCT
                        b.business_uid,
                        b.business_name,
                        b.business_short_bio,
                        b.business_tag_line,
                        b.business_profile_img,
                        b.business_city,
                        b.business_state,
                        b.business_phone_number,
                        b.business_email_id,
                        b.business_website,
                        b.business_email_id_is_public,
                        b.business_phone_number_is_public,
                        b.business_tag_line_is_public,
                        b.business_short_bio_is_public,
                        b.business_profile_img_is_public,
                        pp.profile_personal_uid
                    FROM every_circle.business b
                    JOIN every_circle.business_tags bt ON bt.bt_business_id = b.business_uid
                    JOIN every_circle.tags t ON t.tag_uid = bt.bt_tag_id
                    LEFT JOIN every_circle.business_user bu ON bu.bu_business_id = b.business_uid
                    LEFT JOIN every_circle.profile_personal pp ON pp.profile_personal_user_id = bu.bu_user_id
                    WHERE LOWER(t.tag_name) LIKE '%{query}%'
                    AND b.business_is_active = 1
                """
                result = db.execute(tag_query)
                businesses = result['result'] if result['result'] else []

                # For each business, fetch all its tags
                for business in businesses:
                    uid = business['business_uid']
                    tags_query = f"""
                        SELECT t.tag_name
                        FROM every_circle.business_tags bt
                        JOIN every_circle.tags t ON t.tag_uid = bt.bt_tag_id
                        WHERE bt.bt_business_id = '{uid}'
                    """
                    tags_result = db.execute(tags_query)
                    business['tags'] = [row['tag_name'] for row in tags_result['result']] if tags_result['result'] else []

                response['result'] = businesses
                response['code'] = 200
                return response, 200
        except Exception as e:
            print(f"Error in BusinessTagSearch GET: {str(e)}")
            return {'message': 'Internal Server Error', 'code': 500}, 500