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

            # Main query using your previous structure
            business_query = f"""
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
                    uc.connection_path,
                    GROUP_CONCAT(DISTINCT t.tag_name) as matching_tags
                FROM ratings r
                INNER JOIN business b ON r.rating_business_id = b.business_uid
                INNER JOIN business_tags bt ON b.business_uid = bt.bt_business_id
                INNER JOIN tags t ON bt.bt_tag_id = t.tag_uid
                INNER JOIN UserConnections uc ON r.rating_profile_id = uc.user_id
                WHERE bt.bt_tag_id IN ('{tag_ids_str}')
                GROUP BY 
                    r.rating_uid,
                    b.business_uid,
                    uc.degree,
                    uc.connection_path
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