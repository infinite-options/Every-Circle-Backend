from flask import Flask, request, jsonify
from flask_restful import Resource, Api, abort
from typing import Dict, List, Optional, Tuple
import os
import json
import traceback
from datetime import datetime
from openai import OpenAI
from data_ec import connect

class AISearchTag(Resource):
    def __init__(self):
        self.open_ai_client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

    def generate_search_tags(self, search_query: str) -> List[str]:
        """Generate search tags without storing them in database"""
        prompt = f"""
        Generate 5 most relevant search tags for the following query: "{search_query}"
        
        The tags should:
        1. Capture the main intent of the search
        2. Include related business categories
        3. Include common variations of the search terms
        4. Be ordered from most general to specific
        
        Return only a comma-separated list of 5 tags.
        """

        try:
            response = self.open_ai_client.chat.completions.create(
                model="gpt-4-0125-preview",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=100
            )
            
            # Split and clean tags
            tags = [tag.strip().lower() for tag in response.choices[0].message.content.split(',')]
            return tags[:5]  # Ensure we only get top 5 tags
            
        except Exception as e:
            print(f"Error generating search tags: {str(e)}")
            traceback.print_exc()
            return []

    # def find_matching_businesses(self, db, search_tags: List[str], profile_id: str) -> List[Dict]:
    #     """Find businesses based on tag matching"""
    #     try:
    #         # First find matching tags from tags table
    #         conditions = []
    #         params = []

    #         # Build the CASE statement parts for each search tag
    #         case_conditions = []
    #         for tag in search_tags:
    #             case_conditions.append(f"WHEN tag_name = %s THEN 100")
    #             case_conditions.append(f"WHEN tag_name LIKE CONCAT(%s, '%%') THEN 85")
    #             case_conditions.append(f"WHEN tag_name LIKE CONCAT('%%', %s) THEN 85")
    #             case_conditions.append(f"WHEN tag_name LIKE CONCAT('%%', %s, '%%') THEN 70")
    #             params.extend([tag, tag, tag, tag])

    #         # Build the WHERE conditions
    #         where_conditions = []
    #         for tag in search_tags:
    #             where_conditions.append("tag_name = %s")
    #             where_conditions.append("tag_name LIKE CONCAT(%s, '%%')")
    #             where_conditions.append("tag_name LIKE CONCAT('%%', %s)")
    #             where_conditions.append("tag_name LIKE CONCAT('%%', %s, '%%')")
    #             params.extend([tag, tag, tag, tag])

    #         matching_tags_query = f"""
    #             SELECT DISTINCT 
    #                 tag_uid,
    #                 tag_name,
    #                 CASE 
    #                     {' '.join(case_conditions)}
    #                     ELSE 0
    #                 END as match_percentage
    #             FROM every_circle.tags
    #             WHERE {' OR '.join(where_conditions)}
    #             HAVING match_percentage >= 70
    #         """

    #         print(f"Tag matching query: {matching_tags_query}")
    #         matching_tags_result = db.execute(matching_tags_query, params)
    #         print(f"Matching tags result: {matching_tags_result}")

    #         if not matching_tags_result.get('result'):
    #             return []

    #         # Get business_uids from business_tags table
    #         matching_tag_uids = [tag['tag_uid'] for tag in matching_tags_result['result']]
    #         tag_ids_str = "','".join(matching_tag_uids)

    #         # Main query using your previous structure
    #         business_query = f"""
    #             WITH UserConnections AS (
    #                 WITH RECURSIVE Referrals AS (
    #                     -- Base case: Start from the given user_id
    #                     SELECT 
    #                         profile_uid AS user_id,
    #                         profile_referred_by_user_id,
    #                         0 AS degree, 
    #                         CAST(profile_uid AS CHAR(300)) AS connection_path
    #                     FROM profile
    #                     WHERE profile_uid = '{profile_id}'

    #                     UNION ALL

    #                     -- Forward expansion: Find users referred by the current user
    #                     SELECT 
    #                         p.profile_uid AS user_id,
    #                         p.profile_referred_by_user_id,
    #                         r.degree + 1 AS degree,
    #                         CONCAT(r.connection_path, ' -> ', p.profile_uid) AS connection_path
    #                     FROM profile p
    #                     INNER JOIN Referrals r ON p.profile_referred_by_user_id = r.user_id
    #                     WHERE r.degree < 3 
    #                     AND NOT POSITION(p.profile_uid IN r.connection_path) > 0

    #                     UNION ALL

    #                     -- Backward expansion: Find the user who referred the current user
    #                     SELECT 
    #                         p.profile_referred_by_user_id AS user_id,
    #                         p.profile_uid AS profile_referred_by_user_id,
    #                         r.degree + 1 AS degree,
    #                         CONCAT(r.connection_path, ' -> ', p.profile_referred_by_user_id) AS connection_path
    #                     FROM profile p
    #                     INNER JOIN Referrals r ON p.profile_uid = r.user_id
    #                     WHERE r.degree < 3
    #                     AND NOT POSITION(p.profile_referred_by_user_id IN r.connection_path) > 0
    #                 )
    #                 SELECT DISTINCT
    #                     user_id,
    #                     degree,
    #                     connection_path
    #                 FROM Referrals
    #                 ORDER BY degree, connection_path
    #             )
    #             SELECT DISTINCT
    #                 r.*,
    #                 b.*,
    #                 uc.degree AS connection_degree,
    #                 uc.connection_path,
    #                 GROUP_CONCAT(DISTINCT t.tag_name) as matching_tags
    #             FROM ratings r
    #             INNER JOIN business b ON r.rating_business_id = b.business_uid
    #             INNER JOIN business_tags bt ON b.business_uid = bt.bt_business_id
    #             INNER JOIN tags t ON bt.bt_tag_id = t.tag_uid
    #             INNER JOIN UserConnections uc ON r.rating_profile_id = uc.user_id
    #             WHERE bt.bt_tag_id IN ('{tag_ids_str}')
    #             GROUP BY 
    #                 r.rating_uid,
    #                 b.business_uid,
    #                 uc.degree,
    #                 uc.connection_path
    #             ORDER BY uc.degree, r.rating_star DESC
    #         """

    #         print(f"Business query: {business_query}")
    #         result = db.execute(business_query)
    #         print(f"Business query result: {result}")

    #         return result.get('result', [])

    #     except Exception as e:
    #         print(f"Error in find_matching_businesses: {str(e)}")
    #         traceback.print_exc()
    #         return []

    def find_matching_businesses(self, db, search_tags: List[str], profile_id: str) -> List[Dict]:
        """Find businesses based on tag matching"""
        try:
            # First find matching tags from tags table
            conditions = []
            params = []

            # Build the CASE statement parts for each search tag
            case_conditions = []
            for tag in search_tags:
                case_conditions.append(f"WHEN tag_name = %s THEN 100")
                case_conditions.append(f"WHEN tag_name LIKE CONCAT(%s, '%%') THEN 85")
                case_conditions.append(f"WHEN tag_name LIKE CONCAT('%%', %s) THEN 85")
                case_conditions.append(f"WHEN tag_name LIKE CONCAT('%%', %s, '%%') THEN 70")
                params.extend([tag, tag, tag, tag])

            # Build the WHERE conditions
            where_conditions = []
            for tag in search_tags:
                where_conditions.append("tag_name = %s")
                where_conditions.append("tag_name LIKE CONCAT(%s, '%%')")
                where_conditions.append("tag_name LIKE CONCAT('%%', %s)")
                where_conditions.append("tag_name LIKE CONCAT('%%', %s, '%%')")
                params.extend([tag, tag, tag, tag])

            matching_tags_query = f"""
                SELECT DISTINCT 
                    tag_uid,
                    tag_name,
                    CASE 
                        {' '.join(case_conditions)}
                        ELSE 0
                    END as match_percentage
                FROM every_circle.tags
                WHERE {' OR '.join(where_conditions)}
                HAVING match_percentage >= 70
            """

            print(f"Tag matching query: {matching_tags_query}")
            matching_tags_result = db.execute(matching_tags_query, params)
            print(f"Matching tags result: {matching_tags_result}")

            if not matching_tags_result.get('result'):
                return []

            # Get business_uids from business_tags table
            matching_tag_uids = [tag['tag_uid'] for tag in matching_tags_result['result']]
            tag_ids_str = "','".join(matching_tag_uids)

            # Updated business query to include profile information
            business_query = f"""
                WITH UserConnections AS (
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
                    uc.connection_path,
                    GROUP_CONCAT(DISTINCT t.tag_name) as matching_tags,
                    p.profile_first_name,
                    p.profile_last_name,
                    p.profile_phone,
                    p.profile_tag_line,
                    p.profile_short_bio,
                    p.profile_facebook_link,
                    p.profile_twitter_link,
                    p.profile_linkedin_link,
                    p.profile_youtube_link,
                    p.profile_images_url,
                    p.profile_favorite_image,
                    p.profile_city,
                    p.profile_state,
                    p.profile_country
                FROM ratings r
                INNER JOIN business b ON r.rating_business_id = b.business_uid
                INNER JOIN business_tags bt ON b.business_uid = bt.bt_business_id
                INNER JOIN tags t ON bt.bt_tag_id = t.tag_uid
                INNER JOIN UserConnections uc ON r.rating_profile_id = uc.user_id
                INNER JOIN profile p ON r.rating_profile_id = p.profile_uid
                WHERE bt.bt_tag_id IN ('{tag_ids_str}')
                GROUP BY 
                    r.rating_uid,
                    b.business_uid,
                    uc.degree,
                    uc.connection_path,
                    p.profile_uid
                ORDER BY uc.degree, r.rating_star DESC
            """

            print(f"Business query: {business_query}")
            result = db.execute(business_query)
            print(f"Business query result: {result}")

            return result.get('result', [])

        except Exception as e:
            print(f"Error in find_matching_businesses: {str(e)}")
            traceback.print_exc()
            return []


    def process_charges(self, db, business_uid_list: List[str], profile_id: str):
        """Process impression charges for the businesses"""
        try:
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
        except Exception as e:
            print(f"Error processing charges: {str(e)}")
            traceback.print_exc()

    def get(self, profile_id):
        search_query = request.args.get('query', "").strip()
        
        if not search_query:
            abort(400, description="search query is required")
        
        try:
            # Generate search tags (but don't store them)
            search_tags = self.generate_search_tags(search_query)
            print(f"Generated search tags: {search_tags}")
            
            if not search_tags:
                return {
                    'message': 'Could not generate search tags',
                    'code': 400
                }, 400

            with connect() as db:
                # Find matching tags from database first
                matching_tags_query = """
                    SELECT DISTINCT 
                        tag_uid,
                        tag_name,
                        CASE 
                """
                conditions = []
                params = []

                # Build CASE conditions for each search tag
                for tag in search_tags:
                    conditions.extend([
                        f"WHEN tag_name = %s THEN 100",
                        f"WHEN tag_name LIKE CONCAT(%s, '%%') THEN 85",
                        f"WHEN tag_name LIKE CONCAT('%%', %s) THEN 85",
                        f"WHEN tag_name LIKE CONCAT('%%', %s, '%%') THEN 70"
                    ])
                    params.extend([tag, tag, tag, tag])

                matching_tags_query += " ".join(conditions)
                matching_tags_query += """
                        ELSE 0
                    END as match_percentage
                    FROM every_circle.tags
                    WHERE 
                """

                # Build WHERE conditions
                where_conditions = []
                for tag in search_tags:
                    where_conditions.extend([
                        "tag_name = %s",
                        "tag_name LIKE CONCAT(%s, '%%')",
                        "tag_name LIKE CONCAT('%%', %s)",
                        "tag_name LIKE CONCAT('%%', %s, '%%')"
                    ])
                    params.extend([tag, tag, tag, tag])

                matching_tags_query += " OR ".join(where_conditions)
                matching_tags_query += " HAVING match_percentage >= 70"

                # Get matching tags
                matching_tags_result = db.execute(matching_tags_query, params)
                matching_tags = matching_tags_result.get('result', [])

                # Find matching businesses with the found tags
                matching_businesses = self.find_matching_businesses(db, search_tags, profile_id)
                
                if matching_businesses:
                    # Process charges for matched businesses
                    self.process_charges(db, [b['business_uid'] for b in matching_businesses], profile_id)
                    
                response = {
                    'search_analysis': {
                        'search_query': search_query,
                        'generated_tags': search_tags,
                        'matching_database_tags': [
                            {
                                'tag_name': tag['tag_name'],
                                'match_percentage': tag['match_percentage']
                            }
                            for tag in matching_tags
                        ] if matching_tags else []
                    },
                    'business_results': {
                        'total_businesses': len(matching_businesses),
                        'businesses': matching_businesses,
                        'search_level': 'tag_match'
                    }
                }

                if not matching_businesses:
                    response['message'] = 'No matching businesses found'

                return response, 200

        except Exception as e:
            print(f"Error in get: {str(e)}")
            traceback.print_exc()
            return {
                'message': 'Internal Server Error',
                'code': 500
            }, 500
        

from flask import Flask, request, jsonify
from flask_restful import Resource, Api, abort
from typing import Dict, List, Optional, Tuple
import os
import json
import traceback
from datetime import datetime
from openai import OpenAI
from data_ec import connect

class AISearchTag_v3(Resource):
    def __init__(self):
        self.open_ai_client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

    def generate_search_tags(self, search_query: str) -> List[str]:
        """Generate search tags without storing them in database"""
        prompt = f"""
        Generate 5 most relevant search tags for the following query: "{search_query}"
        
        The tags should:
        1. Capture the main intent of the search
        2. Include related business categories
        3. Include common variations of the search terms
        4. Be ordered from most general to specific
        
        Return only a comma-separated list of 5 tags.
        """

        try:
            response = self.open_ai_client.chat.completions.create(
                model="gpt-4-0125-preview",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=100
            )
            
            # Split and clean tags
            tags = [tag.strip().lower() for tag in response.choices[0].message.content.split(',')]
            return tags[:5]  # Ensure we only get top 5 tags
            
        except Exception as e:
            print(f"Error generating search tags: {str(e)}")
            traceback.print_exc()
            return []

    def find_matching_tags(self, db, search_tags: List[str], min_match_percentage: float = 50.0) -> Dict[str, List]:
        """Find matching tags using full-text search and exact matching with refined scores"""
        try:
            # Track all matched tags to prevent duplicates
            matched_tag_uids = set()
            strict_matches = []
            loose_matches = []
            
            # First, try exact matching for high confidence matches
            for tag in search_tags:
                tag_query = f"""
                    SELECT 
                        tag_uid,
                        tag_name,
                        CASE 
                            WHEN tag_name = %s THEN 100
                            WHEN tag_name LIKE CONCAT(%s, '%%') THEN 85
                            WHEN tag_name LIKE CONCAT('%%', %s) THEN 85
                            WHEN tag_name LIKE CONCAT('%%', %s, '%%') THEN 70
                        END as match_percentage
                    FROM every_circle.tags
                    WHERE
                        tag_name = %s OR
                        tag_name LIKE CONCAT(%s, '%%') OR
                        tag_name LIKE CONCAT('%%', %s) OR
                        tag_name LIKE CONCAT('%%', %s, '%%')
                    HAVING match_percentage >= 70
                """
                
                params = [tag, tag, tag, tag, tag, tag, tag, tag]
                exact_match_result = db.execute(tag_query, params)
                
                if exact_match_result.get('result'):
                    for match in exact_match_result.get('result', []):
                        if match['tag_uid'] not in matched_tag_uids:
                            matched_tag_uids.add(match['tag_uid'])
                            strict_matches.append(match)
            
            # Now, use full-text search for broader matches
            search_string = ' '.join(search_tags)
            
            # Use MySQL's full-text search with natural language mode
            fulltext_query = """
                SELECT 
                    tag_uid,
                    tag_name,
                    MATCH(tag_name) AGAINST(%s IN NATURAL LANGUAGE MODE) * 100 as raw_score
                FROM every_circle.tags
                WHERE MATCH(tag_name) AGAINST(%s IN NATURAL LANGUAGE MODE)
                HAVING raw_score >= %s
                ORDER BY raw_score DESC
            """
            
            fulltext_params = [search_string, search_string, min_match_percentage]
            fulltext_result = db.execute(fulltext_query, fulltext_params)
            fulltext_matches = fulltext_result.get('result', [])
            
            # Normalize and refine full-text search scores
            if fulltext_matches:
                # Find maximum score to use for normalization
                max_score = max(match['raw_score'] for match in fulltext_matches)
                
                # Keep the top 20 matches to prevent too many irrelevant results
                top_matches = fulltext_matches[:20]
                
                for match in top_matches:
                    # Skip if we've already seen this tag
                    if match['tag_uid'] in matched_tag_uids:
                        continue
                        
                    # Normalize to 0-100 range
                    normalized_score = min(100, (match['raw_score'] / max_score) * 100)
                    
                    # Apply more conservative matching for tags without exact term matches
                    # First, check if any search tag appears in the tag name
                    direct_match = False
                    for tag in search_tags:
                        if tag.lower() in match['tag_name'].lower():
                            direct_match = True
                            break
                    
                    # Apply different scoring based on whether there's a direct match
                    if direct_match:
                        # Direct match gets higher score
                        if normalized_score > 80:
                            match_percentage = min(90, normalized_score)  # Cap at 90 for related terms
                        elif normalized_score > 60:
                            match_percentage = 80  # Strong match
                        elif normalized_score > 40:
                            match_percentage = 70  # Good match
                        else:
                            match_percentage = 60  # Moderate match
                    else:
                        # Indirect matches get more conservative scores
                        if normalized_score > 80:
                            match_percentage = 70  # Even high confidence indirect matches get 70 max
                        elif normalized_score > 60:
                            match_percentage = 60
                        else:
                            match_percentage = 55
                    
                    match['match_percentage'] = round(match_percentage)
                    matched_tag_uids.add(match['tag_uid'])
                    
                    # Add to appropriate list based on match percentage
                    if match['match_percentage'] >= 70:
                        strict_matches.append(match)
                    else:
                        loose_matches.append(match)
                    
                    # Debug print to see how scoring is working
                    print(f"Tag: {match['tag_name']}, Raw: {match['raw_score']}, Normalized: {match['match_percentage']}, Direct: {direct_match}")
            
            # If full-text search doesn't return enough results, fall back to LIKE-based search
            if len(strict_matches) + len(loose_matches) < 5:
                # Build a more complex combined query for multiple tags
                conditions = []
                params = []
                
                for tag in search_tags:
                    conditions.append("(tag_name LIKE CONCAT('%%', %s, '%%'))")
                    params.append(tag)
                
                like_query = f"""
                    SELECT 
                        tag_uid,
                        tag_name,
                        60 as match_percentage
                    FROM every_circle.tags
                    WHERE {' OR '.join(conditions)}
                """
                
                like_result = db.execute(like_query, params)
                
                # Add LIKE-based matches if they're not already included
                for match in like_result.get('result', []):
                    if match['tag_uid'] not in matched_tag_uids:
                        matched_tag_uids.add(match['tag_uid'])
                        loose_matches.append(match)
            
            # Sort matches by match percentage (highest first)
            strict_matches.sort(key=lambda x: x['match_percentage'], reverse=True)
            loose_matches.sort(key=lambda x: x['match_percentage'], reverse=True)
            
            print(f"Found {len(strict_matches)} strict matches and {len(loose_matches)} loose matches")
            
            return {
                'strict': strict_matches,
                'loose': loose_matches
            }
            
        except Exception as e:
            print(f"Error finding matching tags: {str(e)}")
            traceback.print_exc()
            return {'strict': [], 'loose': []}

    def find_matching_businesses(self, db, tag_uids: Dict[str, List], profile_id: str) -> Dict[str, List]:
        """Find businesses based on tag matching with different match levels"""
        try:
            # Extract tag UIDs for strict and loose matches
            strict_tag_uids = [tag['tag_uid'] for tag in tag_uids['strict']]
            loose_tag_uids = [tag['tag_uid'] for tag in tag_uids['loose']]
            
            if not strict_tag_uids and not loose_tag_uids:
                return {'strict': [], 'loose': []}
            
            # Get user connections
            user_connections_query = f"""
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
                SELECT DISTINCT
                    user_id,
                    degree,
                    connection_path
                FROM Referrals
                ORDER BY degree, connection_path
            """
            
            user_connections_result = db.execute(user_connections_query)
            user_connections = user_connections_result.get('result', [])
            
            # Get businesses matching strict tags
            strict_businesses = []
            if strict_tag_uids:
                strict_tag_ids_str = "','".join(strict_tag_uids)
                strict_business_query = f"""
                    SELECT DISTINCT
                        b.*,
                        GROUP_CONCAT(DISTINCT t.tag_name) as matching_tags,
                        'strict' as match_type
                    FROM every_circle.business b
                    INNER JOIN every_circle.business_tags bt ON b.business_uid = bt.bt_business_id
                    INNER JOIN every_circle.tags t ON bt.bt_tag_id = t.tag_uid
                    WHERE bt.bt_tag_id IN ('{strict_tag_ids_str}')
                    GROUP BY b.business_uid
                """
                
                strict_business_result = db.execute(strict_business_query)
                strict_businesses = strict_business_result.get('result', [])
            
            # Get businesses matching loose tags (that aren't already in strict matches)
            loose_businesses = []
            if loose_tag_uids:
                loose_tag_ids_str = "','".join(loose_tag_uids)
                exclude_clause = ""
                if strict_businesses:
                    exclude_business_ids = [b['business_uid'] for b in strict_businesses]
                    exclude_business_ids_str = "','".join(exclude_business_ids)
                    exclude_clause = f" AND b.business_uid NOT IN ('{exclude_business_ids_str}')"
                
                loose_business_query = f"""
                    SELECT DISTINCT
                        b.*,
                        GROUP_CONCAT(DISTINCT t.tag_name) as matching_tags,
                        'loose' as match_type
                    FROM every_circle.business b
                    INNER JOIN every_circle.business_tags bt ON b.business_uid = bt.bt_business_id
                    INNER JOIN every_circle.tags t ON bt.bt_tag_id = t.tag_uid
                    WHERE bt.bt_tag_id IN ('{loose_tag_ids_str}'){exclude_clause}
                    GROUP BY b.business_uid
                """
                
                loose_business_result = db.execute(loose_business_query)
                loose_businesses = loose_business_result.get('result', [])
            
            # Get all business UIDs
            all_business_uids = [b['business_uid'] for b in strict_businesses + loose_businesses]
            if not all_business_uids:
                return {'strict': [], 'loose': []}
            
            # Get ratings for these businesses
            business_uid_str = "','".join(all_business_uids)
            rating_query = f"""
                SELECT 
                    r.*,
                    p.profile_first_name,
                    p.profile_last_name,
                    p.profile_phone,
                    p.profile_tag_line,
                    p.profile_short_bio,
                    p.profile_facebook_link,
                    p.profile_twitter_link,
                    p.profile_linkedin_link,
                    p.profile_youtube_link,
                    p.profile_images_url,
                    p.profile_favorite_image,
                    p.profile_city,
                    p.profile_state,
                    p.profile_country
                FROM every_circle.ratings r
                INNER JOIN every_circle.profile p ON r.rating_profile_id = p.profile_uid
                WHERE r.rating_business_id IN ('{business_uid_str}')
                ORDER BY r.rating_star DESC
            """
            
            rating_result = db.execute(rating_query)
            ratings = rating_result.get('result', [])
            
            # Organize ratings by business
            business_ratings = {}
            for rating in ratings:
                business_id = rating['rating_business_id']
                if business_id not in business_ratings:
                    business_ratings[business_id] = []
                
                # Add connection information if this is a connected user
                for conn in user_connections:
                    if conn['user_id'] == rating['rating_profile_id']:
                        rating['connection_degree'] = conn['degree']
                        rating['connection_path'] = conn['connection_path']
                        break
                        
                business_ratings[business_id].append(rating)
            
            # Add ratings to businesses
            for business in strict_businesses + loose_businesses:
                business_id = business['business_uid']
                business['ratings'] = business_ratings.get(business_id, [])
            
            return {
                'strict': strict_businesses,
                'loose': loose_businesses
            }

        except Exception as e:
            print(f"Error in find_matching_businesses: {str(e)}")
            traceback.print_exc()
            return {'strict': [], 'loose': []}

    def process_charges(self, db, business_uid_list: List[str], profile_id: str):
        """Process impression charges for the businesses"""
        try:
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
        except Exception as e:
            print(f"Error processing charges: {str(e)}")
            traceback.print_exc()

    def get(self, profile_id):
        search_query = request.args.get('query', "").strip()
        
        if not search_query:
            abort(400, description="search query is required")
        
        try:
            # Generate search tags (but don't store them)
            search_tags = self.generate_search_tags(search_query)
            print(f"Generated search tags: {search_tags}")
            
            if not search_tags:
                return {
                    'message': 'Could not generate search tags',
                    'code': 400
                }, 400

            with connect() as db:
                # Find matching tags from database
                matching_tags = self.find_matching_tags(db, search_tags, 50.0)
                
                # Find matching businesses with different match levels
                matching_businesses = self.find_matching_businesses(db, matching_tags, profile_id)
                
                # Get all business UIDs for charge processing
                all_business_uids = [b['business_uid'] for b in matching_businesses['strict'] + matching_businesses['loose']]
                
                if all_business_uids:
                    # Process charges for matched businesses
                    self.process_charges(db, all_business_uids, profile_id)
                    
                response = {
                    'search_analysis': {
                        'search_query': search_query,
                        'generated_tags': search_tags,
                        'matching_database_tags': {
                            'strict': [
                                {
                                    'tag_name': tag['tag_name'],
                                    'match_percentage': tag['match_percentage']
                                }
                                for tag in matching_tags['strict']
                            ],
                            'loose': [
                                {
                                    'tag_name': tag['tag_name'],
                                    'match_percentage': tag['match_percentage']
                                }
                                for tag in matching_tags['loose']
                            ]
                        }
                    },
                    'business_results': {
                        'total_strict_matches': len(matching_businesses['strict']),
                        'total_loose_matches': len(matching_businesses['loose']),
                        'strict_matches': matching_businesses['strict'],
                        'loose_matches': matching_businesses['loose'],
                        'search_level': 'tag_match'
                    }
                }

                if not all_business_uids:
                    response['message'] = 'No matching businesses found'

                return response, 200

        except Exception as e:
            print(f"Error in get: {str(e)}")
            traceback.print_exc()
            return {
                'message': 'Internal Server Error',
                'code': 500
            }, 500