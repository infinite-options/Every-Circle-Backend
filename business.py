from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime
import ast
import traceback
import uuid

from data_ec import connect, uploadImage, s3, processImage, encrypt_data, decrypt_data


def _parse_bounty_amount(value):
    """Parse bs_bounty from DB (may include $, commas, or numeric types)."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            n = float(value)
        else:
            s = str(value).replace("$", "").replace(",", "").strip()
            if not s:
                return None
            n = float(s)
        return n if n > 0 else None
    except (TypeError, ValueError):
        return None


def _normalize_bounty_type(raw):
    t = str(raw or "per_item").strip().lower()
    if t == "none":
        return "none"
    if t == "total":
        return "total"
    return "per_item"


def _accumulate_business_bounties(services_rows):
    """Build per-business max bounty fields from raw service rows."""
    bounty_map = {}
    for row in services_rows or []:
        bid = str(row.get("bs_business_id") or "").strip()
        if not bid:
            continue
        if bid not in bounty_map:
            bounty_map[bid] = {
                "max_bounty": None,
                "max_per_item_bounty": None,
                "max_total_bounty": None,
                "product_count": 0,
            }
        entry = bounty_map[bid]
        entry["product_count"] += 1
        amount = _parse_bounty_amount(row.get("bs_bounty"))
        btype = _normalize_bounty_type(row.get("bs_bounty_type"))
        if amount is None or btype == "none":
            continue
        prev_max = entry["max_bounty"]
        entry["max_bounty"] = amount if prev_max is None else max(prev_max, amount)
        if btype == "total":
            prev = entry["max_total_bounty"]
            entry["max_total_bounty"] = amount if prev is None else max(prev, amount)
        else:
            prev = entry["max_per_item_bounty"]
            entry["max_per_item_bounty"] = amount if prev is None else max(prev, amount)
    return bounty_map


def compute_profile_degrees_from_viewer(db, viewer_uid, target_profile_uids, max_degree=15):
    """
    BFS over profile_personal_referred_by (up and down) from viewer_uid.
    Returns { profile_personal_uid: hop_count } for targets found within max_degree.
    """
    viewer_uid = str(viewer_uid).strip() if viewer_uid else ""
    target_set = {str(u).strip() for u in (target_profile_uids or []) if u is not None and str(u).strip()}
    if not viewer_uid or not target_set:
        return {}

    degrees = {}
    seen = {viewer_uid}
    frontier = [viewer_uid]

    for degree in range(1, max_degree + 1):
        if not frontier:
            break
        if len(degrees) >= len(target_set):
            break

        ph = ",".join(f"'{u}'" for u in frontier)

        r_down = db.execute(
            f"""
            SELECT profile_personal_uid
            FROM every_circle.profile_personal
            WHERE profile_personal_referred_by IN ({ph})
            """
        )

        r_up = db.execute(
            f"""
            SELECT profile_personal_referred_by AS uid
            FROM every_circle.profile_personal
            WHERE profile_personal_uid IN ({ph})
            AND profile_personal_referred_by IS NOT NULL
            """
        )

        next_frontier = []
        for row in (r_down.get("result") or []) + (r_up.get("result") or []):
            uid = row.get("profile_personal_uid") or row.get("uid")
            if uid and uid not in seen:
                seen.add(uid)
                next_frontier.append(uid)
                if uid in target_set:
                    degrees[uid] = degree

        frontier = next_frontier

    return degrees


def _min_degree_for_profiles(profile_uids, degree_map):
    vals = [degree_map[u] for u in profile_uids if u in degree_map]
    return min(vals) if vals else None


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
        print("In Business_v2 POST")
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
        print("In Business_v2 PUT")
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
        

class BusinessDetails(Resource):
    """
    POST JSON: { "uids": [...], "profile_uid": "..." }
    Returns per-business: avg_rating, rating_count, nearest_connection (legacy),
    review_connection_degree, owner_connection_degree, max_bounty fields,
    and product_count (COUNT(bs_uid) from bounty query).
    """

    def post(self):
        try:
            body = request.get_json(silent=True) or {}
            uids = body.get("uids")
            profile_uid = body.get("profile_uid")

            if not isinstance(uids, list):
                return {"message": "uids must be a JSON array", "code": 400}, 400

            uid_list = [str(u).strip() for u in uids if u is not None and str(u).strip()]
            if not uid_list:
                return {"message": "uids required", "code": 400}, 400

            viewer_uid = ""
            if profile_uid is not None:
                viewer_uid = str(profile_uid).strip()

            with connect() as db:
                placeholders = ",".join([f"'{uid}'" for uid in uid_list])

                ratings_map = {
                    uid: {
                        "avg_rating": None,
                        "rating_count": 0,
                        "nearest_connection": None,
                        "review_connection_degree": None,
                        "owner_connection_degree": None,
                        "max_bounty": None,
                        "max_per_item_bounty": None,
                        "max_total_bounty": None,
                        "product_count": 0,
                    }
                    for uid in uid_list
                }

                services_query = f"""
                    SELECT bs_business_id, bs_bounty, bs_bounty_type
                    FROM every_circle.business_services
                    WHERE bs_business_id IN ({placeholders})
                """
                services_result = db.execute(services_query)
                service_rows = services_result.get("result") or []
                bounty_map = _accumulate_business_bounties(service_rows)

                for bid_key, bounty_data in bounty_map.items():
                    if bid_key in ratings_map:
                        ratings_map[bid_key].update(bounty_data)

                ratings_sql = f"""
                    SELECT
                        rating_business_id,
                        ROUND(AVG(rating_star), 1) AS avg_rating,
                        COUNT(rating_uid) AS rating_count

                    FROM every_circle.ratings

                    WHERE rating_business_id IN ({placeholders})

                    GROUP BY rating_business_id
                """

                ratings_result = db.execute(ratings_sql)

                if ratings_result.get("result"):
                    for row in ratings_result["result"]:
                        bid = str(row["rating_business_id"]).strip()
                        if bid in ratings_map:
                            ratings_map[bid]["avg_rating"] = row["avg_rating"]
                            ratings_map[bid]["rating_count"] = row["rating_count"]

                if viewer_uid:
                    reviewer_query = f"""
                        SELECT
                            rating_business_id,
                            rating_profile_id
                        FROM every_circle.ratings
                        WHERE rating_business_id IN ({placeholders})
                    """
                    reviewer_result = db.execute(reviewer_query)
                    biz_reviewers = {}
                    if reviewer_result.get("result"):
                        for row in reviewer_result["result"]:
                            bid = str(row["rating_business_id"]).strip()
                            biz_reviewers.setdefault(bid, []).append(
                                row["rating_profile_id"]
                            )

                    owners_query = f"""
                        SELECT
                            bu.bu_business_id AS business_uid,
                            pp.profile_personal_uid
                        FROM every_circle.business_user bu
                        JOIN every_circle.profile_personal pp
                            ON pp.profile_personal_user_id = bu.bu_user_id
                        WHERE bu.bu_business_id IN ({placeholders})
                    """
                    owners_result = db.execute(owners_query)
                    biz_owners = {}
                    if owners_result.get("result"):
                        for row in owners_result["result"]:
                            bid = str(row.get("business_uid") or "").strip()
                            puid = row.get("profile_personal_uid")
                            if bid and puid:
                                biz_owners.setdefault(bid, []).append(puid)

                    all_target_uids = set(
                        uid for uids in biz_reviewers.values() for uid in uids
                    )
                    all_target_uids.update(
                        uid for uids in biz_owners.values() for uid in uids
                    )

                    degree_map = compute_profile_degrees_from_viewer(
                        db, viewer_uid, all_target_uids
                    )

                    for bid in uid_list:
                        review_deg = _min_degree_for_profiles(
                            biz_reviewers.get(bid, []), degree_map
                        )
                        owner_deg = _min_degree_for_profiles(
                            biz_owners.get(bid, []), degree_map
                        )
                        ratings_map[bid]["review_connection_degree"] = review_deg
                        ratings_map[bid]["owner_connection_degree"] = owner_deg
                        # Legacy field: closest path via review or owner
                        candidates = [d for d in (review_deg, owner_deg) if d is not None]
                        ratings_map[bid]["nearest_connection"] = (
                            min(candidates) if candidates else None
                        )

                return {"result": ratings_map}, 200

        except Exception as e:
            print("ERROR:", str(e))
            traceback.print_exc()
            return {
                "message": "Internal Server Error",
                "code": 500,
                "error": str(e),
            }, 500


class ProfileConnectionDegrees(Resource):
    """
    POST JSON: { "profile_uid": "<viewer>", "uids": ["<profile_personal_uid>", ...] }
    Returns { "result": { "<uid>": degree, ... } } for profiles within referral network.
    """

    def post(self):
        try:
            body = request.get_json(silent=True) or {}
            viewer_uid = str(body.get("profile_uid") or "").strip()
            uids = body.get("uids")

            if not viewer_uid:
                return {"message": "profile_uid required", "code": 400}, 400
            if not isinstance(uids, list):
                return {"message": "uids must be a JSON array", "code": 400}, 400

            uid_list = [str(u).strip() for u in uids if u is not None and str(u).strip()]
            if not uid_list:
                return {"result": {}}, 200

            with connect() as db:
                degree_map = compute_profile_degrees_from_viewer(db, viewer_uid, uid_list)
                return {"result": degree_map}, 200
        except Exception as e:
            print("ProfileConnectionDegrees ERROR:", str(e))
            traceback.print_exc()
            return {
                "message": "Internal Server Error",
                "code": 500,
                "error": str(e),
            }, 500


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
                        b.business_cc_fee_payer,
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
        

class BusinessServicePurchase(Resource):
    def post(self):
        """Decrement bs_quantity when a purchase is made. Atomic SQL to prevent race conditions."""
        print("In BusinessServicePurchase POST")
        response = {}

        try:
            #payload = request.get_json(force=True) or {}
            payload = getattr(request, '_decrypted_json', None) or request.get_json(force=True) or {}
            bs_uid = payload.get("bs_uid", "").strip()
            qty_purchased = payload.get("quantity", 1)

            if not bs_uid:
                response["message"] = "bs_uid is required"
                response["code"] = 400
                return response, 400

            try:
                qty_purchased = int(qty_purchased)
                if qty_purchased < 1:
                    raise ValueError
            except (TypeError, ValueError):
                response["message"] = "quantity must be a positive integer"
                response["code"] = 400
                return response, 400

            with connect() as db:
                # Fetch current service
                svc_query = db.select(
                    "every_circle.business_services", where={"bs_uid": bs_uid}
                )
                if not svc_query["result"]:
                    response["message"] = "Service not found"
                    response["code"] = 404
                    return response, 404

                row = svc_query["result"][0]
                current_qty = row.get("bs_quantity")

                # If null or "unlimited" — nothing to decrement
                if current_qty is None or str(current_qty).strip().lower() == "unlimited":
                    response["message"] = "Unlimited stock — no decrement needed"
                    response["remaining"] = None
                    response["code"] = 200
                    return response, 200

                current_qty_int = int(current_qty)
                print(f"DEBUG current_qty_int: {current_qty_int}, qty_purchased: {qty_purchased}")

                if current_qty_int < qty_purchased:
                    response["message"] = "Insufficient stock"
                    response["remaining"] = current_qty_int
                    response["code"] = 409
                    return response, 409

                # Simple direct update — skip rowcount check entirely
                new_qty = current_qty_int - qty_purchased
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                update_result = db.update(
                    "every_circle.business_services",
                    {"bs_uid": bs_uid},
                    {"bs_quantity": str(new_qty), "bs_updated_at": now},
                )
                print(f"DEBUG update_result: {update_result}")

                # Verify it actually changed
                verify = db.select("every_circle.business_services", where={"bs_uid": bs_uid})
                actual_qty = verify["result"][0].get("bs_quantity") if verify["result"] else None
                print(f"DEBUG actual_qty after update: {actual_qty}")

                remaining = int(actual_qty) if actual_qty is not None else new_qty

                # Auto-hide if sold out
                if remaining == 0:
                    db.update(
                        "every_circle.business_services",
                        {"bs_uid": bs_uid},
                        {"bs_is_visible": 0, "bs_status": "out_of_stock"},
                    )

                response["message"] = "Purchase recorded successfully"
                response["remaining"] = remaining
                response["bs_uid"] = bs_uid
                response["code"] = 200
                return response, 200

        except Exception as e:
            print(f"Error in BusinessServicePurchase POST: {str(e)}")
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500


class BusinessClaim(Resource):
    def post(self):
        print("In BusinessClaim POST")
        response = {}
        try:
            # Read files
            files = {}
            index = 0
            while True:
                key = f"document_{index}"
                if key in request.files:
                    files[key] = request.files[key]
                    index += 1
                else:
                    break

            print(f"Files captured: {list(files.keys())}")

            payload = request.form.to_dict()
            print("Form keys:", list(payload.keys()))

            profile_uid  = payload.get("profile_uid", "").strip()
            business_uid = payload.get("business_uid", "").strip()
            claim_role   = payload.get("claim_role", "").strip()
            claim_note   = payload.get("claim_note", "").strip()

            if not profile_uid or not business_uid or not claim_role:
                response["message"] = "profile_uid, business_uid, and claim_role are required"
                response["code"] = 400
                return response, 400

            with connect() as db:
                business_check = db.select("every_circle.business", where={"business_uid": business_uid})
                if not business_check["result"]:
                    response["message"] = "Business not found"
                    response["code"] = 404
                    return response, 404

                existing_query = f"""
                    SELECT claim_uid FROM every_circle.business_claims
                    WHERE claim_profile_id = '{profile_uid}'
                    AND claim_business_id = '{business_uid}'
                    AND claim_status IN ('pending', 'approved')
                    LIMIT 1
                """
                existing = db.execute(existing_query)
                if existing["result"]:
                    response["message"] = "A claim already exists for this business"
                    response["code"] = 409
                    return response, 409

                claim_uid_resp = db.call(procedure="new_claim_uid")
                claim_uid = claim_uid_resp["result"][0]["new_id"]
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                # Upload files
                uploaded_urls = []
                for i, (key, file) in enumerate(files.items()):
                    print(f"Processing file {i}: {file.filename}")
                    file.stream.seek(0)  # reset stream in case it was partially read
                    unique_filename = f"document_{i}_" + datetime.now().strftime('%Y%m%d%H%M%SZ')
                    image_key = f"business_claims/{claim_uid}/{unique_filename}"
                    print(f"Uploading to S3 key: {image_key}")
                    url = uploadImage(file, image_key, '')
                    print(f"Upload result URL: {url}")
                    if url:
                        uploaded_urls.append(url)

                print(f"Total uploaded: {len(uploaded_urls)}, doc_urls: {uploaded_urls}")
                doc_urls = ",".join(uploaded_urls)

                db.execute(f"""
                    INSERT INTO every_circle.business_claims
                        (claim_uid, claim_profile_id, claim_business_id,
                        claim_role, claim_note, claim_documents, claim_status, claim_created_at)
                    VALUES
                        ('{claim_uid}', '{profile_uid}', '{business_uid}',
                        '{claim_role}', '{claim_note}', '{doc_urls}', 'pending', '{now}')
                """, cmd='post')

            response["message"] = "Claim submitted successfully"
            response["claim_uid"] = claim_uid
            response["code"] = 200
            return response, 200

        except Exception as e:
            print(f"Error in BusinessClaim POST: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def put(self):
        """Admin resolves a claim — approve or reject."""
        print("In BusinessClaim PUT")
        response = {}
        try:
            payload = request.get_json(force=True) or {}
            claim_uid   = payload.get("claim_uid", "").strip()
            action      = payload.get("action", "").strip()       # 'approved' | 'rejected'
            admin_uid   = payload.get("admin_uid", "").strip()

            if not claim_uid or action not in ("approved", "rejected"):
                response["message"] = "claim_uid and action ('approved' or 'rejected') are required"
                response["code"] = 400
                return response, 400

            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with connect() as db:
                existing = db.execute(f"""
                    SELECT * FROM every_circle.business_claims
                    WHERE claim_uid = '{claim_uid}' LIMIT 1
                """)
                if not existing["result"]:
                    response["message"] = "Claim not found"
                    response["code"] = 404
                    return response, 404

                db.execute(f"""
                    UPDATE every_circle.business_claims
                    SET claim_status = '{action}',
                        claim_resolved_at = '{now}',
                        claim_resolved_by = '{admin_uid}'
                    WHERE claim_uid = '{claim_uid}'
                """, cmd='post')

                # If approved, add the claimant as a business_user
                if action == "approved":
                    claim = existing["result"][0]
                    bu_uid_resp = db.call(procedure="new_bu_uid")
                    bu_uid = bu_uid_resp["result"][0]["new_id"]
                    db.execute(f"""
                        INSERT INTO every_circle.business_user
                            (bu_uid, bu_business_id, bu_user_id, bu_role, bu_joined_at, bu_individual_business_is_public)
                        SELECT '{bu_uid}', '{claim["claim_business_id"]}',
                            u.user_uid, '{claim["claim_role"]}', '{now}', 1
                        FROM every_circle.profile_personal pp
                        JOIN every_circle.users u ON u.user_uid = pp.profile_personal_user_id
                        WHERE pp.profile_personal_uid = '{claim["claim_profile_id"]}'
                        LIMIT 1
                    """, cmd='post')

            response["message"] = f"Claim {action} successfully"
            response["code"] = 200
            return response, 200

        except Exception as e:
            print(f"Error in BusinessClaim PUT: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500

    def get(self):
        """Admin fetch — all pending claims, or filter by business_uid or profile_uid."""
        print("In BusinessClaim GET")
        response = {}
        try:
            business_uid = request.args.get("business_uid", "").strip()
            profile_uid  = request.args.get("profile_uid", "").strip()
            status       = request.args.get("status", "pending").strip()

            where_clause = f"claim_status = '{status}'"
            if business_uid:
                where_clause += f" AND claim_business_id = '{business_uid}'"
            if profile_uid:
                where_clause += f" AND claim_profile_id = '{profile_uid}'"

            with connect() as db:
                result = db.execute(f"""
                    SELECT bc.*,
                           b.business_name,
                           pp.profile_personal_first_name,
                           pp.profile_personal_last_name
                    FROM every_circle.business_claims bc
                    LEFT JOIN every_circle.business b
                           ON b.business_uid = bc.claim_business_id
                    LEFT JOIN every_circle.profile_personal pp
                           ON pp.profile_personal_uid = bc.claim_profile_id
                    WHERE {where_clause}
                    ORDER BY bc.claim_created_at DESC
                """)

            response["result"] = result["result"] or []
            response["code"] = 200
            return response, 200

        except Exception as e:
            print(f"Error in BusinessClaim GET: {str(e)}")
            traceback.print_exc()
            response["message"] = "Internal Server Error"
            response["code"] = 500
            return response, 500