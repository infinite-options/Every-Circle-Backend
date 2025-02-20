# from flask import Flask, request, jsonify
# from flask_restful import Resource, Api, abort
# from typing import Dict, List, Optional
# from threading import Lock
# import openai
# from openai import OpenAI
# import os
# import re
# import json
# from collections import defaultdict
# from datetime import datetime
# from data_ec import connect

# class CategoryNavigator:
#     def __init__(self, db_connection, openai_client):
#         self.db = db_connection
#         self.openai_client = openai_client
#         self.categories_cache = self.load_all_categories()
#         print(f"Loaded {len(self.categories_cache)} categories from database")

#     def load_all_categories(self) -> Dict:
#         """Load all categories from database"""
#         query = """
#             SELECT 
#                 category_uid,
#                 category_name,
#                 category_description,
#                 category_parent_id
#             FROM every_circle.category
#             ORDER BY category_name
#         """
#         result = self.db.execute(query)
#         categories = {}
#         if result and result.get('result'):
#             for cat in result.get('result'):
#                 categories[cat['category_uid']] = cat
#                 print(f"Loaded category: {cat['category_name']} ({cat['category_uid']})")
#         return categories

#     def search_categories_by_terms(self, terms: List[str]) -> List[Dict]:
#         """Search categories by terms"""
#         matches = []
#         search_terms = [term.lower() for term in terms]
        
#         for uid, category in self.categories_cache.items():
#             category_name = category['category_name'].lower()
#             category_desc = (category['category_description'] or '').lower()
            
#             for term in search_terms:
#                 if term in category_name or term in category_desc:
#                     matches.append(category)
#                     break
        
#         return matches

#     def analyze_request(self, user_input: str) -> Dict:
#         """Analyze user request using OpenAI"""
#         # Prepare categories for the prompt
#         top_level_categories = []
#         for uid, cat in self.categories_cache.items():
#             if not cat['category_parent_id']:
#                 category_info = f"{cat['category_name']} ({cat['category_uid']})"
#                 if cat['category_description']:
#                     category_info += f": {cat['category_description']}"
#                 top_level_categories.append(category_info)
                
#                 # Add immediate subcategories
#                 subcats = self.get_subcategories(uid)
#                 for subcat in subcats:
#                     subcat_info = f"  - {subcat['category_name']} ({subcat['category_uid']})"
#                     if subcat['category_description']:
#                         subcat_info += f": {subcat['category_description']}"
#                     top_level_categories.append(subcat_info)

#         prompt = f"""
#         You are a service category assistant. Analyze this request: "{user_input}"

#         Available categories in our system:
#         {chr(10).join(top_level_categories)}

#         Return a JSON object that matches this request to our categories. Include:
#         1. The main service need
#         2. Keywords found in our categories that match the request
#         3. UIDs of relevant categories from the list above

#         Format:
#         {{
#             "primary_intent": "main service need",
#             "related_terms": ["matching terms from our categories"],
#             "relevant_category_uids": ["matching category uids"]
#         }}

#         IMPORTANT:
#         - Only use category UIDs that exist in the list above
#         - Include both parent and child categories when relevant
#         - Use exact category names and terms from our system
#         """

#         try:
#             print(f"\nAnalyzing request: {user_input}")
#             response = self.openai_client.chat.completions.create(
#                 model="gpt-4-0125-preview",
#                 messages=[{"role": "user", "content": prompt}],
#                 response_format={"type": "json_object"}
#             )
            
#             analysis = json.loads(response.choices[0].message.content)
#             print(f"Analysis result: {json.dumps(analysis, indent=2)}")
#             return analysis
            
#         except Exception as e:
#             print(f"Error in analyze_request: {str(e)}")
#             # Fallback to direct term matching
#             search_terms = user_input.lower().split()
#             matching_categories = self.search_categories_by_terms(search_terms)
            
#             return {
#                 "primary_intent": user_input,
#                 "related_terms": search_terms,
#                 "relevant_category_uids": [cat['category_uid'] for cat in matching_categories]
#             }

#     def get_category_hierarchy(self, category_uid: str) -> List[Dict]:
#         """Get full hierarchy path for a category"""
#         hierarchy = []
#         current_uid = category_uid
#         visited = set()  # Prevent infinite loops
        
#         while current_uid and current_uid in self.categories_cache and current_uid not in visited:
#             visited.add(current_uid)
#             category = self.categories_cache[current_uid]
#             hierarchy.insert(0, {
#                 'id': category['category_uid'],
#                 'name': category['category_name']
#             })
#             current_uid = category['category_parent_id']
            
#         return hierarchy

#     def get_subcategories(self, category_uid: str) -> List[Dict]:
#         """Get immediate subcategories for a category"""
#         subcategories = []
#         for uid, category in self.categories_cache.items():
#             if category['category_parent_id'] == category_uid:
#                 subcategories.append(category)
#         return sorted(subcategories, key=lambda x: x['category_name'])

#     def build_service_matches(self, analysis: Dict) -> List[Dict]:
#         """Build service matches based on analysis"""
#         matches = []
#         seen_categories = set()
        
#         # Get categories from analysis
#         category_uids = analysis.get('relevant_category_uids', [])
#         print(f"Building matches for categories: {category_uids}")
        
#         for category_uid in category_uids:
#             if category_uid not in self.categories_cache or category_uid in seen_categories:
#                 continue
                
#             seen_categories.add(category_uid)
#             category = self.categories_cache[category_uid]
            
#             # Get hierarchy and subcategories
#             hierarchy = self.get_category_hierarchy(category_uid)
#             subcategories = self.get_subcategories(category_uid)
            
#             # Calculate relevance score
#             relevance_score = 80  # Base score
#             if category['category_name'].lower() in analysis['primary_intent'].lower():
#                 relevance_score += 20
            
#             # Build match object
#             match = {
#                 'id': category_uid,
#                 'name': category['category_name'],
#                 'description': category['category_description'],
#                 'service_path': ' > '.join(cat['name'] for cat in hierarchy),
#                 'relevance_score': relevance_score,
#                 'parent_categories': hierarchy[:-1],  # Exclude self
#                 'sub_categories': [
#                     {
#                         'id': sub['category_uid'],
#                         'name': sub['category_name'],
#                         'description': sub['category_description']
#                     }
#                     for sub in subcategories
#                 ]
#             }
#             matches.append(match)
#             print(f"Added match: {match['name']} (Score: {match['relevance_score']})")
            
#         return sorted(matches, key=lambda x: x['relevance_score'], reverse=True)

# class ChatbotAPI(Resource):
#     def __init__(self):
#         self.open_ai_client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

#     def get_category_matches(self, message: str) -> Dict:
#         """Get category matches for a message"""
#         try:
#             with connect() as db:
#                 navigator = CategoryNavigator(db, self.open_ai_client)
                
#                 # Analyze request
#                 analysis = navigator.analyze_request(message)
#                 print(f"Analysis complete: {json.dumps(analysis, indent=2)}")
                
#                 # Build matches
#                 matches = navigator.build_service_matches(analysis)
#                 print(f"Found {len(matches)} matches")
                
#                 # Prepare response
#                 response = {
#                     'request_analysis': {
#                         'primary_intent': analysis['primary_intent'],
#                         'related_terms': analysis['related_terms']
#                     },
#                     'matched_services': matches,
#                     'service_summary': {
#                         'total_matches': len(matches),
#                         'primary_category': matches[0]['name'] if matches else None,
#                         'categories_found': len(set(m['name'] for m in matches))
#                     }
#                 }
                
#                 return response

#         except Exception as e:
#             print(f"Error in chatbot: {str(e)}")
#             import traceback
#             print(traceback.format_exc())
#             raise e

#     def post(self):
#         try:
#             data = request.get_json()
#             if not data or 'message' not in data:
#                 return {'error': 'Missing required fields'}, 400

#             user_input = data['message']
#             print(f"\nReceived request: {user_input}")
            
#             response = self.get_category_matches(user_input)
#             return jsonify(response)

#         except Exception as e:
#             print(f"Error in chatbot: {str(e)}")
#             import traceback
#             print(traceback.format_exc())
#             return {'error': 'Internal Server Error'}, 500

# class AISearch(Resource):
#     def __init__(self):
#         self.chatbot = ChatbotAPI()

#     def get_businesses_for_category(self, db, category_id: str, profile_id: str) -> Dict:
#         """Helper function to get businesses for a specific category with ratings and connections"""
#         rating_query = f"""
#             WITH UserConnections AS (
#                 WITH RECURSIVE Referrals AS (
#                     SELECT 
#                         profile_uid AS user_id,
#                         profile_referred_by_user_id,
#                         0 AS degree, 
#                         CAST(profile_uid AS CHAR(300)) AS connection_path
#                     FROM profile
#                     WHERE profile_uid = '{profile_id}'

#                     UNION ALL

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
#                 uc.connection_path
#             FROM ratings r
#             INNER JOIN business b ON r.rating_business_id = b.business_uid
#             INNER JOIN business_category bc ON b.business_uid = bc.bc_business_id
#             INNER JOIN UserConnections uc ON r.rating_profile_id = uc.user_id
#             WHERE bc.bc_category_id = '{category_id}'
#             ORDER BY uc.degree, r.rating_star DESC
#         """
#         return db.execute(rating_query)

#     def process_charges(self, db, business_uid_list: List[str], profile_id: str):
#         """Process impression charges for the businesses"""
#         for business_uid in business_uid_list:
#             new_charge_uid = db.call(procedure='new_charge_uid')['result'][0]['new_id']
#             charge_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#             charges_query = f"""
#                 INSERT INTO charges (
#                     charge_uid, charge_business_id, charge_caused_by_user_id,
#                     charge_reason, charge_amount, charge_timestamp
#                 ) VALUES (
#                     '{new_charge_uid}', '{business_uid}', '{profile_id}',
#                     'impression', '1.00', '{charge_timestamp}'
#                 )
#             """
#             db.execute(charges_query, cmd='post')

#     def get(self, profile_id):
#         search_category = request.args.get('category', "").strip()
        
#         if not search_category:
#             abort(400, description="category is required")
        
#         try:
#             # Get category matches from chatbot
#             chatbot_response = self.chatbot.get_category_matches(search_category)
            
#             if not chatbot_response.get('matched_services'):
#                 return {
#                     'message': 'No matching categories found',
#                     'code': 200,
#                     'chatbot_response': chatbot_response  # Include full chatbot response
#                 }, 200

#             with connect() as db:
#                 all_businesses = []
#                 processed_business_uids = set()
                
#                 # Try each matched category and its subcategories
#                 for category_match in chatbot_response['matched_services']:
#                     category_uid = category_match['id']
                    
#                     # Search in main category
#                     main_results = self.get_businesses_for_category(db, category_uid, profile_id)
#                     if main_results.get('result'):
#                         all_businesses.extend(main_results['result'])
#                         for result in main_results['result']:
#                             processed_business_uids.add(result['rating_business_id'])
                    
#                     # Search in subcategories
#                     for sub_category in category_match.get('sub_categories', []):
#                         sub_results = self.get_businesses_for_category(db, sub_category['id'], profile_id)
#                         if sub_results.get('result'):
#                             new_results = [
#                                 result for result in sub_results['result']
#                                 if result['rating_business_id'] not in processed_business_uids
#                             ]
#                             all_businesses.extend(new_results)
#                             for result in new_results:
#                                 processed_business_uids.add(result['rating_business_id'])
                
#                 if all_businesses:
#                     # Process charges for unique businesses
#                     self.process_charges(db, list(processed_business_uids), profile_id)
                    
#                     # Prepare final response maintaining original chatbot structure
#                     response = {
#                         'request_analysis': chatbot_response['request_analysis'],
#                         'matched_services': chatbot_response['matched_services'],
#                         'service_summary': chatbot_response['service_summary'],
#                         'business_results': {
#                             'total_businesses': len(all_businesses),
#                             'businesses': all_businesses,
#                             'search_level': 'ai_match'
#                         }
#                     }
                    
#                     return response, 200

#                 # If no businesses found
#                 return {
#                     'request_analysis': chatbot_response['request_analysis'],
#                     'matched_services': chatbot_response['matched_services'],
#                     'service_summary': chatbot_response['service_summary'],
#                     'business_results': {
#                         'total_businesses': 0,
#                         'businesses': [],
#                         'search_level': 'ai_match'
#                     },
#                     'message': 'No businesses found in matched categories or sub-categories'
#                 }, 200

#         except Exception as e:
#             print(f"Error in AI search: {str(e)}")
#             import traceback
#             print(traceback.format_exc())
#             return {
#                 'message': 'Internal Server Error',
#                 'code': 500
#             }, 500

from flask import Flask, request, jsonify
from flask_restful import Resource, Api, abort
from typing import Dict, List, Optional, Tuple
from threading import Lock
import openai
from openai import OpenAI
import os
import re
import json
from collections import defaultdict
from datetime import datetime
from data_ec import connect

class CategoryNavigator:
    def __init__(self, db_connection, openai_client):
        self.db = db_connection
        self.openai_client = openai_client
        self.categories_cache = self.load_all_categories()
        print(f"Loaded {len(self.categories_cache)} categories from database")

    def load_all_categories(self) -> Dict:
        """Load all categories from database"""
        query = """
            SELECT 
                category_uid,
                category_name,
                category_description,
                category_parent_id
            FROM every_circle.category
            ORDER BY category_name
        """
        result = self.db.execute(query)
        categories = {}
        if result and result.get('result'):
            for cat in result.get('result'):
                categories[cat['category_uid']] = cat
                print(f"Loaded category: {cat['category_name']} ({cat['category_uid']})")
        return categories

    def search_categories_by_terms(self, terms: List[str]) -> List[Dict]:
        """Search categories by terms"""
        matches = []
        search_terms = [term.lower() for term in terms]
        
        for uid, category in self.categories_cache.items():
            category_name = category['category_name'].lower()
            category_desc = (category['category_description'] or '').lower()
            
            for term in search_terms:
                if term in category_name or term in category_desc:
                    matches.append(category)
                    break
        
        return matches

    def analyze_request(self, user_input: str) -> Dict:
        """Analyze user request using OpenAI"""
        # Prepare categories for the prompt
        top_level_categories = []
        for uid, cat in self.categories_cache.items():
            if not cat['category_parent_id']:
                category_info = f"{cat['category_name']} ({cat['category_uid']})"
                if cat['category_description']:
                    category_info += f": {cat['category_description']}"
                top_level_categories.append(category_info)
                
                # Add immediate subcategories
                subcats = self.get_subcategories(uid)
                for subcat in subcats:
                    subcat_info = f"  - {subcat['category_name']} ({subcat['category_uid']})"
                    if subcat['category_description']:
                        subcat_info += f": {subcat['category_description']}"
                    top_level_categories.append(subcat_info)

        prompt = f"""
        You are a service category assistant. Analyze this request: "{user_input}"

        Available categories in our system:
        {chr(10).join(top_level_categories)}

        Return a JSON object that matches this request to our categories. Include:
        1. The main service need
        2. Keywords found in our categories that match the request
        3. UIDs of relevant categories from the list above

        Format:
        {{
            "primary_intent": "main service need",
            "related_terms": ["matching terms from our categories"],
            "relevant_category_uids": ["matching category uids"]
        }}

        IMPORTANT:
        - Only use category UIDs that exist in the list above
        - Include both parent and child categories when relevant
        - Use exact category names and terms from our system
        """

        try:
            print(f"\nAnalyzing request: {user_input}")
            response = self.openai_client.chat.completions.create(
                model="gpt-4-0125-preview",
                messages=[{"role": "user", "content": prompt}],
                response_format={"type": "json_object"}
            )
            
            analysis = json.loads(response.choices[0].message.content)
            print(f"Analysis result: {json.dumps(analysis, indent=2)}")
            return analysis
            
        except Exception as e:
            print(f"Error in analyze_request: {str(e)}")
            # Fallback to direct term matching
            search_terms = user_input.lower().split()
            matching_categories = self.search_categories_by_terms(search_terms)
            
            return {
                "primary_intent": user_input,
                "related_terms": search_terms,
                "relevant_category_uids": [cat['category_uid'] for cat in matching_categories]
            }

    def get_category_hierarchy(self, category_uid: str) -> List[Dict]:
        """Get full hierarchy path for a category"""
        hierarchy = []
        current_uid = category_uid
        visited = set()  # Prevent infinite loops
        
        while current_uid and current_uid in self.categories_cache and current_uid not in visited:
            visited.add(current_uid)
            category = self.categories_cache[current_uid]
            hierarchy.insert(0, {
                'id': category['category_uid'],
                'name': category['category_name']
            })
            current_uid = category['category_parent_id']
            
        return hierarchy

    def get_subcategories(self, category_uid: str) -> List[Dict]:
        """Get immediate subcategories for a category"""
        subcategories = []
        for uid, category in self.categories_cache.items():
            if category['category_parent_id'] == category_uid:
                subcategories.append(category)
        return sorted(subcategories, key=lambda x: x['category_name'])

    def build_service_matches(self, analysis: Dict) -> Tuple[List[Dict], Dict]:
        """Build service matches based on analysis and return hierarchical summary"""
        matches = []
        seen_categories = set()
        
        # Track categories by level and their relevance scores
        level_categories = defaultdict(dict)  # Using dict to store both category and its score
        highest_level_scores = {0: 0, 1: 0, 2: 0}  # Track highest scores for each level
        primary_level_ids = {0: None, 1: None, 2: None}  # Track primary IDs for each level
        
        # Get categories from analysis
        category_uids = analysis.get('relevant_category_uids', [])
        print(f"Building matches for categories: {category_uids}")
        
        for category_uid in category_uids:
            if category_uid not in self.categories_cache or category_uid in seen_categories:
                continue
                
            seen_categories.add(category_uid)
            category = self.categories_cache[category_uid]
            
            # Get hierarchy and subcategories
            hierarchy = self.get_category_hierarchy(category_uid)
            subcategories = self.get_subcategories(category_uid)
            
            # Calculate relevance score
            relevance_score = 80  # Base score
            if category['category_name'].lower() in analysis['primary_intent'].lower():
                relevance_score += 20
            
            # Track categories by their level in hierarchy and store with relevance score
            for level, cat in enumerate(hierarchy):
                cat_info = {
                    'id': cat['id'],
                    'name': cat['name'],
                    'relevance_score': relevance_score if cat['id'] == category_uid else 0
                }
                level_categories[level][cat['id']] = cat_info
                
                # Track highest scores and primary IDs for each level
                if cat_info['relevance_score'] > highest_level_scores[level]:
                    highest_level_scores[level] = cat_info['relevance_score']
                    primary_level_ids[level] = cat['id']
            
            # Add all subcategories to their respective levels
            current_level = len(hierarchy)
            if current_level == 1:  # If parent is level 1, subcats are level 2
                for subcat in subcategories:
                    level_categories[1][subcat['category_uid']] = {
                        'id': subcat['category_uid'],
                        'name': subcat['category_name'],
                        'relevance_score': 0
                    }
                    
                    # Add level 3 categories (subcategories of subcategories)
                    sub_subcategories = self.get_subcategories(subcat['category_uid'])
                    for sub_subcat in sub_subcategories:
                        level_categories[2][sub_subcat['category_uid']] = {
                            'id': sub_subcat['category_uid'],
                            'name': sub_subcat['category_name'],
                            'relevance_score': 0,
                            'parent_id': subcat['category_uid']  # Store parent ID for level 3
                        }
            
            # Build match object
            match = {
                'id': category_uid,
                'name': category['category_name'],
                'description': category['category_description'],
                'service_path': ' > '.join(cat['name'] for cat in hierarchy),
                'relevance_score': relevance_score,
                'parent_categories': hierarchy[:-1],  # Exclude self
                'sub_categories': [
                    {
                        'id': sub['category_uid'],
                        'name': sub['category_name'],
                        'description': sub['category_description']
                    }
                    for sub in subcategories
                ]
            }
            matches.append(match)
            print(f"Added match: {match['name']} (Score: {match['relevance_score']})")
        
        # Prepare categories for each level with summary counts
        level1_cats = [
            {
                'id': cat_info['id'],
                'name': cat_info['name'],
                'is_primary_match': cat_info['relevance_score'] == highest_level_scores[0] and cat_info['relevance_score'] > 0
            }
            for cat_info in level_categories[0].values()
        ]
        
        level2_cats = [
            {
                'id': cat_info['id'],
                'name': cat_info['name'],
                'is_primary_match': cat_info['relevance_score'] == highest_level_scores[1] and cat_info['relevance_score'] > 0
            }
            for cat_info in level_categories[1].values()
        ]
        
        level3_cats = [
            {
                'id': cat_info['id'],
                'name': cat_info['name'],
                'is_primary_match': cat_info.get('parent_id') == primary_level_ids[1]
            }
            for cat_info in level_categories[2].values()
        ]
        
        # Build hierarchical summary with counts
        hierarchy_summary = {
            'level1_categories': {
                'categories': level1_cats,
                'total_count': len(level1_cats),
                'primary_matches': sum(1 for cat in level1_cats if cat['is_primary_match'])
            },
            'level2_categories': {
                'categories': level2_cats,
                'total_count': len(level2_cats),
                'primary_matches': sum(1 for cat in level2_cats if cat['is_primary_match'])
            },
            'level3_categories': {
                'categories': level3_cats,
                'total_count': len(level3_cats),
                'primary_matches': sum(1 for cat in level3_cats if cat['is_primary_match'])
            }
        }
        
        return sorted(matches, key=lambda x: x['relevance_score'], reverse=True), hierarchy_summary

class ChatbotAPI(Resource):
    def __init__(self):
        self.open_ai_client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

    def get_category_matches(self, message: str) -> Dict:
        """Get category matches for a message"""
        try:
            with connect() as db:
                navigator = CategoryNavigator(db, self.open_ai_client)
                
                # Analyze request
                analysis = navigator.analyze_request(message)
                print(f"Analysis complete: {json.dumps(analysis, indent=2)}")
                
                # Build matches and get hierarchy summary
                matches, hierarchy_summary = navigator.build_service_matches(analysis)
                print(f"Found {len(matches)} matches")
                
                # Prepare response
                response = {
                    'request_analysis': {
                        'primary_intent': analysis['primary_intent'],
                        'related_terms': analysis['related_terms']
                    },
                    'matched_services': matches,
                    'service_summary': {
                        'total_matches': len(matches),
                        'primary_category': matches[0]['name'] if matches else None,
                        'categories_found': len(set(m['name'] for m in matches)),
                        'level1_categories': hierarchy_summary['level1_categories'],
                        'level2_categories': hierarchy_summary['level2_categories'],
                        'level3_categories': hierarchy_summary['level3_categories']
                    }
                }
                
                return response

        except Exception as e:
            print(f"Error in chatbot: {str(e)}")
            import traceback
            print(traceback.format_exc())
            raise e

    def post(self):
        try:
            data = request.get_json()
            if not data or 'message' not in data:
                return {'error': 'Missing required fields'}, 400

            user_input = data['message']
            print(f"\nReceived request: {user_input}")
            
            response = self.get_category_matches(user_input)
            return jsonify(response)

        except Exception as e:
            print(f"Error in chatbot: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return {'error': 'Internal Server Error'}, 500

class AISearch(Resource):
    def __init__(self):
        self.chatbot = ChatbotAPI()

    def get_businesses_for_category(self, db, category_id: str, profile_id: str) -> Dict:
        """Helper function to get businesses for a specific category with ratings and connections"""
        rating_query = f"""
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
                uc.connection_path
            FROM ratings r
            INNER JOIN business b ON r.rating_business_id = b.business_uid
            INNER JOIN business_category bc ON b.business_uid = bc.bc_business_id
            INNER JOIN UserConnections uc ON r.rating_profile_id = uc.user_id
            WHERE bc.bc_category_id = '{category_id}'
            ORDER BY uc.degree, r.rating_star DESC
        """
        return db.execute(rating_query)

    def process_charges(self, db, business_uid_list: List[str], profile_id: str):
        """Process impression charges for the businesses"""
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

    def get(self, profile_id):
        search_category = request.args.get('category', "").strip()
        
        if not search_category:
            abort(400, description="category is required")
        
        try:
            # Get category matches from chatbot
            chatbot_response = self.chatbot.get_category_matches(search_category)
            
            if not chatbot_response.get('matched_services'):
                return {
                    'message': 'No matching categories found',
                    'code': 200,
                    'chatbot_response': chatbot_response  # Include full chatbot response
                }, 200

            with connect() as db:
                all_businesses = []
                processed_business_uids = set()
                
                # Try each matched category and its subcategories
                for category_match in chatbot_response['matched_services']:
                    category_uid = category_match['id']
                    
                    # Search in main category
                    main_results = self.get_businesses_for_category(db, category_uid, profile_id)
                    if main_results.get('result'):
                        all_businesses.extend(main_results['result'])
                        for result in main_results['result']:
                            processed_business_uids.add(result['rating_business_id'])
                    
                    # Search in subcategories
                    for sub_category in category_match.get('sub_categories', []):
                        sub_results = self.get_businesses_for_category(db, sub_category['id'], profile_id)
                        if sub_results.get('result'):
                            new_results = [
                                result for result in sub_results['result']
                                if result['rating_business_id'] not in processed_business_uids
                            ]
                            all_businesses.extend(new_results)
                            for result in new_results:
                                processed_business_uids.add(result['rating_business_id'])
                
                if all_businesses:
                    # Process charges for unique businesses
                    self.process_charges(db, list(processed_business_uids), profile_id)
                    
                    # Prepare final response maintaining original chatbot structure
                    response = {
                        'request_analysis': chatbot_response['request_analysis'],
                        'matched_services': chatbot_response['matched_services'],
                        'service_summary': chatbot_response['service_summary'],
                        'business_results': {
                            'total_businesses': len(all_businesses),
                            'businesses': all_businesses,
                            'search_level': 'ai_match'
                        }
                    }
                    
                    return response, 200

                # If no businesses found
                return {
                    'request_analysis': chatbot_response['request_analysis'],
                    'matched_services': chatbot_response['matched_services'],
                    'service_summary': chatbot_response['service_summary'],
                    'business_results': {
                        'total_businesses': 0,
                        'businesses': [],
                        'search_level': 'ai_match'
                    },
                    'message': 'No businesses found in matched categories or sub-categories'
                }, 200

        except Exception as e:
            print(f"Error in AI search: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return {
                'message': 'Internal Server Error',
                'code': 500
            }, 500