from flask import Flask, request, jsonify
from flask_restful import Resource, Api
from typing import Dict, List, Optional
from threading import Lock
import openai
from openai import OpenAI
import os
import re
import json
from collections import defaultdict
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

    def build_service_matches(self, analysis: Dict) -> List[Dict]:
        """Build service matches based on analysis"""
        matches = []
        seen_categories = set()
        
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
            
        return sorted(matches, key=lambda x: x['relevance_score'], reverse=True)

class ChatbotAPI(Resource):
    def __init__(self):
        self.open_ai_client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

    def post(self):
        try:
            data = request.get_json()
            if not data or 'message' not in data:
                return {'error': 'Missing required fields'}, 400

            user_input = data['message']
            print(f"\nReceived request: {user_input}")
            
            with connect() as db:  # Assuming connect() is your database connection function
                navigator = CategoryNavigator(db, self.open_ai_client)
                
                # Analyze request
                analysis = navigator.analyze_request(user_input)
                print(f"Analysis complete: {json.dumps(analysis, indent=2)}")
                
                # Build matches
                matches = navigator.build_service_matches(analysis)
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
                        'categories_found': len(set(m['name'] for m in matches))
                    }
                }
                
                return jsonify(response)

        except Exception as e:
            print(f"Error in chatbot: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return {'error': 'Internal Server Error'}, 500





# it has search API and it's working
# from flask import request, abort
# from flask_restful import Resource
# from typing import Dict, List, Optional, Tuple
# from threading import Lock
# from data_ec import connect
# from openai import OpenAI
# import os
# import re
# import requests

# class SingletonMeta(type):
#     _instances = {}
#     _lock = Lock()

#     def __call__(cls, *args, **kwargs):
#         with cls._lock:
#             if cls not in cls._instances:
#                 instance = super().__call__(*args, **kwargs)
#                 cls._instances[cls] = instance
#             return cls._instances[cls]

# class ConversationManager(metaclass=SingletonMeta):
#     def __init__(self):
#         self._conversations = {}
#         self.lock = Lock()
#         print("[CONVERSATION MANAGER INITIALIZED]")
    
#     def get_conversation(self, conversation_id: str) -> Dict:
#         with self.lock:
#             if conversation_id in self._conversations:
#                 print(f"\n[RETRIEVING EXISTING CONVERSATION] ID: {conversation_id}")
#                 print(f"Current path: {self._conversations[conversation_id]['navigation_path']}")
#                 return self._conversations[conversation_id]
            
#             print(f"\n[CREATING NEW CONVERSATION] ID: {conversation_id}")
#             self._conversations[conversation_id] = {
#                 'messages': [],
#                 'navigation_path': [],
#                 'current_category_id': None,
#                 'last_shown_categories': []
#             }
#             return self._conversations[conversation_id]
    
#     def save_conversation(self, conversation_id: str, context: Dict):
#         with self.lock:
#             self._conversations[conversation_id] = {
#                 'messages': context['messages'].copy(),
#                 'navigation_path': context['navigation_path'].copy(),
#                 'current_category_id': context['current_category_id'],
#                 'last_shown_categories': context.get('last_shown_categories', []).copy()
#             }
#             print(f"\n[SAVED CONVERSATION] ID: {conversation_id}")
#             print(f"Updated path: {self._conversations[conversation_id]['navigation_path']}")

# class CategoryNavigator:
#     def __init__(self, db_connection, openai_client):
#         self.db = db_connection
#         self.openai_client = openai_client
#         self.search_api_url = os.getenv("SEARCH_API_URL")  # Add your Search API URL to environment variables

#     def get_categories(self, parent_id: Optional[str] = None) -> List[Dict]:
#         query = f"""
#             SELECT category_uid, category_name, category_description, category_parent_id
#             FROM every_circle.category
#             WHERE category_parent_id {'= \'' + parent_id + '\'' if parent_id else 'IS NULL'}
#             ORDER BY category_name
#         """
#         result = self.db.execute(query)
#         return result.get('result', []) if result else []

#     def get_category_info(self, category_uid: str) -> Optional[Dict]:
#         query = f"""
#             SELECT category_uid, category_name, category_description, category_parent_id
#             FROM every_circle.category
#             WHERE category_uid = '{category_uid}'
#         """
#         result = self.db.execute(query)
#         return result.get('result', [{}])[0] if result and result.get('result') else None

#     def has_subcategories(self, category_uid: str) -> bool:
#         query = f"""
#             SELECT COUNT(*) as count
#             FROM every_circle.category
#             WHERE category_parent_id = '{category_uid}'
#         """
#         result = self.db.execute(query)
#         return result.get('result', [{'count': 0}])[0]['count'] > 0 if result else False

#     def format_categories_list(self, categories: List[Dict]) -> str:
#         valid_categories = [cat for cat in categories if all(k in cat for k in ['category_name', 'category_uid', 'category_description'])]
#         sorted_categories = sorted(valid_categories, key=lambda x: x['category_name'])
#         return "\n".join([
#             f"{i+1}. {cat['category_name']} ({cat['category_uid']}): {cat['category_description']}"
#             for i, cat in enumerate(sorted_categories)
#         ])

#     def build_prompt(self, user_input: str, conversation_context: Dict) -> str:
#         current_id = conversation_context.get('current_category_id')
#         current_categories = self.get_categories(current_id)
#         conversation_context['last_shown_categories'] = current_categories
        
#         categories_list = self.format_categories_list(current_categories)
#         navigation_path = conversation_context.get('navigation_path', [])
#         path_str = " > ".join(navigation_path) if navigation_path else "Start"
        
#         instruction = """
#         You are an AI assistant helping users find the most specific service category for their needs.
        
#         CRITICAL RULES:
#         1. ONLY use categories that are shown in the 'Available categories' section below
#         2. NEVER make up or suggest categories that aren't listed
#         3. Based on user's input, directly identify and navigate to the most relevant main category
#         4. When showing subcategories:
#            - Only show DIRECT subcategories of the current category
#            - Do not show sub-subcategories until user navigates deeper
#         5. Use EXACT category names and UIDs as shown in the available categories
        
#         Response Format:
#         When you identify the relevant main category:
#         1. Use [NAVIGATE_TO: category_uid] for that category
#         2. In your response, list ONLY its direct subcategories
#         3. Ask user to choose from these specific options
        
#         Example Response:
#         "Based on your car repair needs, here are the available automotive services:
#         1. General Maintenance (220-050100): Routine vehicle maintenance
#         2. Engine Services (220-050200): Engine repair and maintenance
#         [NAVIGATE_TO: 220-050000]"
#         """
        
#         conversation_history = "\n".join([
#             f"{'User' if msg['role'] == 'user' else 'Assistant'}: {msg['content']}"
#             for msg in conversation_context.get('messages', [])
#         ])
        
#         return f"""
#         {instruction}

#         Current path: {path_str}
        
#         Available categories:
#         {categories_list}

#         Previous conversation:
#         {conversation_history}

#         User input: {user_input}
#         """

#     def update_navigation_path(self, context: Dict, category_uid: str) -> Tuple[List[str], str]:
#         category_info = self.get_category_info(category_uid)
#         if not category_info:
#             return context['navigation_path'], ""

#         path = []
#         current = category_info
#         while current:
#             path.insert(0, current['category_name'])
#             if current['category_parent_id']:
#                 current = self.get_category_info(current['category_parent_id'])
#             else:
#                 break

#         context['navigation_path'] = path
#         return path, category_info['category_name']

#     def extract_uids_and_names(self, message: str) -> List[Dict[str, str]]:
#         # Extract UIDs and their associated category names from numbered list format
#         pattern = r'\d+\.\s+([^():]+)\s*\((\d{3}-\d{6})\)'
#         matches = re.findall(pattern, message)
#         return [{'category_name': name.strip(), 'uid': uid} for name, uid in matches]

#     def search_categories(self, categories_info: List[Dict[str, str]], profile_id: str) -> Dict:
#         """
#         Call the Search API with the found categories using GET request
#         """
#         try:
#             search_results = []
#             # base_url = os.getenv("SEARCH_API_BASE_URL", "http://localhost:8000")
            
#             for category in categories_info:
#                 # Construct the URL with query parameters
#                 url = f"https://ioec2testsspm.infiniteoptions.com/api/v2/search/{profile_id}"
#                 params = {
#                     'category': category['category_name']
#                 }
                
#                 response = requests.get(url, params=params)
#                 if response.status_code == 200:
#                     search_results.append({
#                         'category_name': category['category_name'],
#                         'category_uid': category['uid'],
#                         'results': response.json()
#                     })
#                 else:
#                     print(f"Search API error for category {category['category_name']}: {response.status_code}")
                    
#             return {'search_results': search_results}
#         except Exception as e:
#             print(f"Error calling Search API: {str(e)}")
#             return {'error': f'Failed to fetch search results: {str(e)}'}

# class ChatbotAPI(Resource):
#     def __init__(self):
#         self.conversation_manager = ConversationManager()
#         self.open_ai_key = os.getenv("OPEN_AI_KEY")
#         self.open_ai_client = OpenAI(api_key=self.open_ai_key)

#     def post(self):
#         try:
#             data = request.get_json()
#             if not data:
#                 abort(400, description="Request body is required")

#             conversation_id = data.get('conversation_id')
#             user_input = data.get('message')
#             profile_id = data.get('profile_id')

#             if not all([conversation_id, user_input, profile_id]):
#                 abort(400, description="conversation_id, message, and profile_id are required")

#             if not all([conversation_id, user_input]):
#                 abort(400, description="conversation_id and message are required")

#             context = self.conversation_manager.get_conversation(conversation_id)
#             context['messages'].append({'role': 'user', 'content': user_input})

#             with connect() as db:
#                 category_navigator = CategoryNavigator(db, self.open_ai_client)
#                 prompt = category_navigator.build_prompt(user_input, context)

#                 response = self.open_ai_client.chat.completions.create(
#                     model="gpt-4-0125-preview",
#                     messages=[{"role": "user", "content": prompt}]
#                 )

#                 assistant_response = response.choices[0].message.content

#                 # Check if this is the first message and contains UIDs
#                 is_first_message = len(context['messages']) == 1
#                 if is_first_message:
#                     categories_info = category_navigator.extract_uids_and_names(assistant_response)
#                     if categories_info:
#                         # Call Search API with the found categories
#                         search_results = category_navigator.search_categories(categories_info, profile_id)
                        
#                         # Update navigation path for all found categories
#                         all_paths = []
#                         for category in categories_info:
#                             path, _ = category_navigator.update_navigation_path(context, category['uid'])
#                             all_paths.append(path)

#                         return {
#                             'message': assistant_response,
#                             'category_found': True,
#                             'found_categories': categories_info,
#                             'search_results': search_results,
#                             'stop_chat': True,
#                             'navigation_paths': all_paths,
#                             'conversation_id': conversation_id
#                         }

#                 # Handle category selection or navigation
#                 if "[CATEGORY_SELECTED:" in assistant_response:
#                     category_uid = assistant_response.split("[CATEGORY_SELECTED:")[1].split("]")[0].strip()
                    
#                     if category_navigator.has_subcategories(category_uid):
#                         path, category_name = category_navigator.update_navigation_path(context, category_uid)
#                         context['current_category_id'] = category_uid
#                         assistant_response = assistant_response.replace("[CATEGORY_SELECTED:", "[NAVIGATE_TO:")
                        
#                     else:
#                         path, category_name = category_navigator.update_navigation_path(context, category_uid)
#                         print(f"\n[CATEGORY SELECTED] UID: {category_uid}")
#                         print(f"Final Path: {' > '.join(path)}")

#                         context['messages'].append({'role': 'assistant', 'content': assistant_response})
#                         self.conversation_manager.save_conversation(conversation_id, context)

#                         return {
#                             'message': assistant_response.split("[CATEGORY_SELECTED:")[0].strip(),
#                             'category_found': True,
#                             'category_uid': category_uid,
#                             'navigation_path': path,
#                             'conversation_id': conversation_id
#                         }

#                 elif "[NAVIGATE_TO:" in assistant_response:
#                     category_uid = assistant_response.split("[NAVIGATE_TO:")[1].split("]")[0].strip()
#                     path, category_name = category_navigator.update_navigation_path(context, category_uid)
                    
#                     context['current_category_id'] = category_uid
#                     context['messages'].append({'role': 'assistant', 'content': assistant_response})
#                     self.conversation_manager.save_conversation(conversation_id, context)

#                     return {
#                         'message': assistant_response.split("[NAVIGATE_TO:")[0].strip(),
#                         'category_found': False,
#                         'navigation_path': path,
#                         'conversation_id': conversation_id
#                     }

#                 context['messages'].append({'role': 'assistant', 'content': assistant_response})
#                 self.conversation_manager.save_conversation(conversation_id, context)

#                 return {
#                     'message': assistant_response,
#                     'category_found': False,
#                     'navigation_path': context.get('navigation_path', []),
#                     'conversation_id': conversation_id
#                 }

#         except Exception as e:
#             print(f"Error in chatbot: {str(e)}")
#             import traceback
#             print(traceback.format_exc())
#             return {'message': 'Internal Server Error', 'code': 500}, 500






# working partially
# from flask import request, abort
# from flask_restful import Resource
# from typing import Dict, List, Optional, Tuple
# from threading import Lock
# from data_ec import connect
# from openai import OpenAI
# import os

# class SingletonMeta(type):
#     _instances = {}
#     _lock = Lock()

#     def __call__(cls, *args, **kwargs):
#         with cls._lock:
#             if cls not in cls._instances:
#                 instance = super().__call__(*args, **kwargs)
#                 cls._instances[cls] = instance
#             return cls._instances[cls]

# class ConversationManager(metaclass=SingletonMeta):
#     def __init__(self):
#         self._conversations = {}
#         self.lock = Lock()
#         print("[CONVERSATION MANAGER INITIALIZED]")
    
#     def get_conversation(self, conversation_id: str) -> Dict:
#         with self.lock:
#             if conversation_id in self._conversations:
#                 print(f"\n[RETRIEVING EXISTING CONVERSATION] ID: {conversation_id}")
#                 print(f"Current path: {self._conversations[conversation_id]['navigation_path']}")
#                 return self._conversations[conversation_id]
            
#             print(f"\n[CREATING NEW CONVERSATION] ID: {conversation_id}")
#             self._conversations[conversation_id] = {
#                 'messages': [],
#                 'navigation_path': [],
#                 'current_category_id': None,
#                 'last_shown_categories': []
#             }
#             return self._conversations[conversation_id]
    
#     def save_conversation(self, conversation_id: str, context: Dict):
#         with self.lock:
#             self._conversations[conversation_id] = {
#                 'messages': context['messages'].copy(),
#                 'navigation_path': context['navigation_path'].copy(),
#                 'current_category_id': context['current_category_id'],
#                 'last_shown_categories': context.get('last_shown_categories', []).copy()
#             }
#             print(f"\n[SAVED CONVERSATION] ID: {conversation_id}")
#             print(f"Updated path: {self._conversations[conversation_id]['navigation_path']}")

# class CategoryNavigator:
#     def __init__(self, db_connection, openai_client):
#         self.db = db_connection
#         self.openai_client = openai_client

#     def get_categories(self, parent_id: Optional[str] = None) -> List[Dict]:
#         query = f"""
#             SELECT category_uid, category_name, category_description, category_parent_id
#             FROM every_circle.category
#             WHERE category_parent_id {'= \'' + parent_id + '\'' if parent_id else 'IS NULL'}
#             ORDER BY category_name
#         """
#         result = self.db.execute(query)
#         return result.get('result', []) if result else []

#     def get_category_info(self, category_uid: str) -> Optional[Dict]:
#         query = f"""
#             SELECT category_uid, category_name, category_description, category_parent_id
#             FROM every_circle.category
#             WHERE category_uid = '{category_uid}'
#         """
#         result = self.db.execute(query)
#         return result.get('result', [{}])[0] if result and result.get('result') else None

#     def has_subcategories(self, category_uid: str) -> bool:
#         query = f"""
#             SELECT COUNT(*) as count
#             FROM every_circle.category
#             WHERE category_parent_id = '{category_uid}'
#         """
#         result = self.db.execute(query)
#         return result.get('result', [{'count': 0}])[0]['count'] > 0 if result else False

#     def format_categories_list(self, categories: List[Dict]) -> str:
#         # Filter out any categories that don't match the expected format
#         valid_categories = [cat for cat in categories if all(k in cat for k in ['category_name', 'category_uid', 'category_description'])]
        
#         # Sort categories by name for consistent presentation
#         sorted_categories = sorted(valid_categories, key=lambda x: x['category_name'])
        
#         return "\n".join([
#             f"{i+1}. {cat['category_name']} ({cat['category_uid']}): {cat['category_description']}"
#             for i, cat in enumerate(sorted_categories)
#         ])

#     def build_prompt(self, user_input: str, conversation_context: Dict) -> str:
#         current_id = conversation_context.get('current_category_id')
#         current_categories = self.get_categories(current_id)
#         conversation_context['last_shown_categories'] = current_categories
        
#         categories_list = self.format_categories_list(current_categories)
#         navigation_path = conversation_context.get('navigation_path', [])
#         path_str = " > ".join(navigation_path) if navigation_path else "Start"
        
#         instruction = """
#         You are an AI assistant helping users find the most specific service category for their needs.
        
#         CRITICAL RULES:
#         1. ONLY use categories that are shown in the 'Available categories' section below
#         2. NEVER make up or suggest categories that aren't listed
#         3. Based on user's input, directly identify and navigate to the most relevant main category
#         4. When showing subcategories:
#            - Only show DIRECT subcategories of the current category
#            - Do not show sub-subcategories until user navigates deeper
#         5. Use EXACT category names and UIDs as shown in the available categories
        
#         Response Format:
#         When you identify the relevant main category:
#         1. Use [NAVIGATE_TO: category_uid] for that category
#         2. In your response, list ONLY its direct subcategories
#         3. Ask user to choose from these specific options
        
#         Example Response:
#         "Based on your car repair needs, here are the available automotive services:
#         1. General Maintenance (220-050100): Routine vehicle maintenance
#         2. Engine Services (220-050200): Engine repair and maintenance
#         [NAVIGATE_TO: 220-050000]"
#         """
        
#         conversation_history = "\n".join([
#             f"{'User' if msg['role'] == 'user' else 'Assistant'}: {msg['content']}"
#             for msg in conversation_context.get('messages', [])
#         ])
        
#         return f"""
#         {instruction}

#         Current path: {path_str}
        
#         Available categories:
#         {categories_list}

#         Previous conversation:
#         {conversation_history}

#         User input: {user_input}
#         """

#     def update_navigation_path(self, context: Dict, category_uid: str) -> Tuple[List[str], str]:
#         category_info = self.get_category_info(category_uid)
#         if not category_info:
#             return context['navigation_path'], ""

#         path = []
#         current = category_info
#         while current:
#             path.insert(0, current['category_name'])
#             if current['category_parent_id']:
#                 current = self.get_category_info(current['category_parent_id'])
#             else:
#                 break

#         context['navigation_path'] = path
#         return path, category_info['category_name']

# class ChatbotAPI(Resource):
#     def __init__(self):
#         self.conversation_manager = ConversationManager()
#         self.open_ai_key = os.getenv("OPEN_AI_KEY")
#         self.open_ai_client = OpenAI(api_key=self.open_ai_key)

#     def post(self):
#         try:
#             data = request.get_json()
#             if not data:
#                 abort(400, description="Request body is required")

#             conversation_id = data.get('conversation_id')
#             user_input = data.get('message')

#             if not all([conversation_id, user_input]):
#                 abort(400, description="conversation_id and message are required")

#             context = self.conversation_manager.get_conversation(conversation_id)
#             context['messages'].append({'role': 'user', 'content': user_input})

#             with connect() as db:
#                 category_navigator = CategoryNavigator(db, self.open_ai_client)
#                 prompt = category_navigator.build_prompt(user_input, context)

#                 response = self.open_ai_client.chat.completions.create(
#                     model="gpt-4-0125-preview",
#                     messages=[{"role": "user", "content": prompt}]
#                 )

#                 assistant_response = response.choices[0].message.content

#                 # Handle category selection or navigation
#                 if "[CATEGORY_SELECTED:" in assistant_response:
#                     category_uid = assistant_response.split("[CATEGORY_SELECTED:")[1].split("]")[0].strip()
                    
#                     # Verify this is actually a leaf category
#                     if category_navigator.has_subcategories(category_uid):
#                         # If it has subcategories, force navigation instead
#                         path, category_name = category_navigator.update_navigation_path(context, category_uid)
#                         context['current_category_id'] = category_uid
                        
#                         # Modify response to show it's still navigating
#                         assistant_response = assistant_response.replace("[CATEGORY_SELECTED:", "[NAVIGATE_TO:")
                        
#                     else:
#                         # Actually a leaf category, proceed with selection
#                         path, category_name = category_navigator.update_navigation_path(context, category_uid)
#                         print(f"\n[CATEGORY SELECTED] UID: {category_uid}")
#                         print(f"Final Path: {' > '.join(path)}")

#                         context['messages'].append({'role': 'assistant', 'content': assistant_response})
#                         self.conversation_manager.save_conversation(conversation_id, context)

#                         return {
#                             'message': assistant_response.split("[CATEGORY_SELECTED:")[0].strip(),
#                             'category_found': True,
#                             'category_uid': category_uid,
#                             'navigation_path': path,
#                             'conversation_id': conversation_id
#                         }

#                 elif "[NAVIGATE_TO:" in assistant_response:
#                     category_uid = assistant_response.split("[NAVIGATE_TO:")[1].split("]")[0].strip()
#                     path, category_name = category_navigator.update_navigation_path(context, category_uid)
                    
#                     context['current_category_id'] = category_uid
#                     context['messages'].append({'role': 'assistant', 'content': assistant_response})
#                     self.conversation_manager.save_conversation(conversation_id, context)

#                     return {
#                         'message': assistant_response.split("[NAVIGATE_TO:")[0].strip(),
#                         'category_found': False,
#                         'navigation_path': path,
#                         'conversation_id': conversation_id
#                     }

#                 context['messages'].append({'role': 'assistant', 'content': assistant_response})
#                 self.conversation_manager.save_conversation(conversation_id, context)

#                 return {
#                     'message': assistant_response,
#                     'category_found': False,
#                     'navigation_path': path if 'path' in locals() else [],
#                     'conversation_id': conversation_id
#                 }

#         except Exception as e:
#             print(f"Error in chatbot: {str(e)}")
#             import traceback
#             print(traceback.format_exc())
#             return {'message': 'Internal Server Error', 'code': 500}, 500



















# working but not proper response
# from flask import request, abort
# from flask_restful import Resource
# from typing import Dict, List, Optional, Tuple
# from threading import Lock
# from data_ec import connect
# from openai import OpenAI
# import os

# class SingletonMeta(type):
#     _instances = {}
#     _lock = Lock()

#     def __call__(cls, *args, **kwargs):
#         with cls._lock:
#             if cls not in cls._instances:
#                 instance = super().__call__(*args, **kwargs)
#                 cls._instances[cls] = instance
#             return cls._instances[cls]

# class ConversationManager(metaclass=SingletonMeta):
#     def __init__(self):
#         self._conversations = {}
#         self.lock = Lock()
#         print("[CONVERSATION MANAGER INITIALIZED]")
    
#     def get_conversation(self, conversation_id: str) -> Dict:
#         with self.lock:
#             if conversation_id in self._conversations:
#                 print(f"\n[RETRIEVING EXISTING CONVERSATION] ID: {conversation_id}")
#                 print(f"Current path: {self._conversations[conversation_id]['navigation_path']}")
#                 return self._conversations[conversation_id]
            
#             print(f"\n[CREATING NEW CONVERSATION] ID: {conversation_id}")
#             self._conversations[conversation_id] = {
#                 'messages': [],
#                 'navigation_path': [],
#                 'current_category_id': None,
#                 'last_shown_categories': []  # Store last shown categories
#             }
#             return self._conversations[conversation_id]
    
#     def save_conversation(self, conversation_id: str, context: Dict):
#         with self.lock:
#             self._conversations[conversation_id] = {
#                 'messages': context['messages'].copy(),
#                 'navigation_path': context['navigation_path'].copy(),
#                 'current_category_id': context['current_category_id'],
#                 'last_shown_categories': context.get('last_shown_categories', []).copy()
#             }
#             print(f"\n[SAVED CONVERSATION] ID: {conversation_id}")
#             print(f"Updated path: {self._conversations[conversation_id]['navigation_path']}")

# class CategoryNavigator:
#     def __init__(self, db_connection, openai_client):
#         self.db = db_connection
#         self.openai_client = openai_client

#     def get_categories(self, parent_id: Optional[str] = None) -> List[Dict]:
#         query = f"""
#             SELECT category_uid, category_name, category_description, category_parent_id
#             FROM every_circle.category
#             WHERE category_parent_id {'= \'' + parent_id + '\'' if parent_id else 'IS NULL'}
#             ORDER BY category_name
#         """
#         result = self.db.execute(query)
#         return result.get('result', []) if result else []

#     def get_category_info(self, category_uid: str) -> Optional[Dict]:
#         query = f"""
#             SELECT category_uid, category_name, category_description, category_parent_id
#             FROM every_circle.category
#             WHERE category_uid = '{category_uid}'
#         """
#         result = self.db.execute(query)
#         return result.get('result', [{}])[0] if result and result.get('result') else None

#     def has_subcategories(self, category_uid: str) -> bool:
#         query = f"""
#             SELECT COUNT(*) as count
#             FROM every_circle.category
#             WHERE category_parent_id = '{category_uid}'
#         """
#         result = self.db.execute(query)
#         return result.get('result', [{'count': 0}])[0]['count'] > 0 if result else False

#     def format_categories_list(self, categories: List[Dict]) -> str:
#         #f"{i+1}. {cat['category_name']}: {cat['category_uid']} , {cat['category_description']}"
#         """Format categories into a numbered list for display"""
#         return "\n".join([
#             f"{i+1}. {cat['category_name']}: {cat['category_description']}"
#             for i, cat in enumerate(categories)
#         ])

#     def build_prompt(self, user_input: str, conversation_context: Dict) -> str:
#         current_id = conversation_context.get('current_category_id')
#         current_categories = self.get_categories(current_id)
#         conversation_context['last_shown_categories'] = current_categories
        
#         # Format categories as a numbered list
#         categories_list = self.format_categories_list(current_categories)

#         navigation_path = conversation_context.get('navigation_path', [])
#         path_str = " > ".join(navigation_path) if navigation_path else "Start"
        
#         has_subcats = current_id and self.has_subcategories(current_id)
        
#         instruction = """
#         Your role is to help users find the most specific category for their needs.
        
#         Guidelines:
#         1. ALWAYS guide users to the most specific (lowest level) category possible
#         2. NEVER select a category if it has subcategories - keep navigating deeper
#         3. Ask clarifying questions to help choose between available options
#         4. List ALL available options to the user in your response
#         5. Return categories only from the available categories
#         """
        
#         conversation_history = self._format_previous_messages(conversation_context.get('messages', []))
        
#         return f"""
#         {instruction}

#         Current navigation path: {path_str}
        
#         Available categories:
#         {categories_list}

#         Previous conversation:
#         {conversation_history}

#         User input: {user_input}

#         Response format:
#         1. If there are subcategories available:
#            - List ALL available options that appear in the available categories
#            - Ask clarifying questions
#            - Use [NAVIGATE_TO: category_uid] to move to subcategories
#         2. Only use [CATEGORY_SELECTED: category_uid] when you reach a category with NO subcategories
        
#         Always structure your response as:
#         1. Brief acknowledgment of user's need
#         2. List of ALL available options
#         3. Question about specific needs or [NAVIGATE_TO/CATEGORY_SELECTED] command
#         """

#     def update_navigation_path(self, context: Dict, category_uid: str) -> Tuple[List[str], str]:
#         category_info = self.get_category_info(category_uid)
#         if not category_info:
#             return context['navigation_path'], ""

#         # Build complete path to this category
#         path = []
#         current = category_info
#         while current:
#             path.insert(0, current['category_name'])
#             if current['category_parent_id']:
#                 current = self.get_category_info(current['category_parent_id'])
#             else:
#                 break

#         context['navigation_path'] = path
#         return path, category_info['category_name']

#     def _format_previous_messages(self, messages: List[Dict]) -> str:
#         return "\n".join([
#             f"{'User' if msg['role'] == 'user' else 'Assistant'}: {msg['content']}"
#             for msg in messages
#         ])

# conversation_manager = ConversationManager()

# class ChatbotAPI(Resource):
#     def __init__(self):
#         self.conversation_manager = conversation_manager
#         self.open_ai_key = os.getenv("OPEN_AI_KEY")
#         self.open_ai_client = OpenAI(api_key=self.open_ai_key)

#     def post(self):
#         try:
#             data = request.get_json()
#             if not data:
#                 abort(400, description="Request body is required")

#             conversation_id = data.get('conversation_id')
#             user_input = data.get('message')

#             if not all([conversation_id, user_input]):
#                 abort(400, description="conversation_id and message are required")

#             context = self.conversation_manager.get_conversation(conversation_id)
#             context['messages'].append({'role': 'user', 'content': user_input})

#             with connect() as db:
#                 category_navigator = CategoryNavigator(db, self.open_ai_client)
#                 prompt = category_navigator.build_prompt(user_input, context)

#                 response = self.open_ai_client.chat.completions.create(
#                     model="gpt-4o-mini",
#                     messages=[{"role": "user", "content": prompt}]
#                 )

#                 assistant_response = response.choices[0].message.content

#                 # Handle category selection or navigation
#                 if "[CATEGORY_SELECTED:" in assistant_response:
#                     category_uid = assistant_response.split("[CATEGORY_SELECTED:")[1].split("]")[0].strip()
                    
#                     # Verify this is actually a leaf category (no subcategories)
#                     if category_navigator.has_subcategories(category_uid):
#                         # If it has subcategories, force navigation instead
#                         path, category_name = category_navigator.update_navigation_path(context, category_uid)
#                         context['current_category_id'] = category_uid
                        
#                         # Modify response to show it's still navigating
#                         assistant_response = assistant_response.replace("[CATEGORY_SELECTED:", "[NAVIGATE_TO:")
#                         context['messages'].append({'role': 'assistant', 'content': assistant_response})
#                         self.conversation_manager.save_conversation(conversation_id, context)
                        
#                         return {
#                             'message': assistant_response.split("[NAVIGATE_TO:")[0].strip(),
#                             'category_found': False,
#                             'navigation_path': path,
#                             'conversation_id': conversation_id
#                         }
                    
#                     # Actually a leaf category, proceed with selection
#                     path, category_name = category_navigator.update_navigation_path(context, category_uid)
#                     print(f"\n[CATEGORY SELECTED] UID: {category_uid}")
#                     print(f"Final Path: {' > '.join(path)}")

#                     context['messages'].append({'role': 'assistant', 'content': assistant_response})
#                     self.conversation_manager.save_conversation(conversation_id, context)

#                     return {
#                         'message': assistant_response.split("[CATEGORY_SELECTED:")[0].strip(),
#                         'category_found': True,
#                         'category_uid': category_uid,
#                         'navigation_path': path,
#                         'conversation_id': conversation_id
#                     }

#                 elif "[NAVIGATE_TO:" in assistant_response:
#                     category_uid = assistant_response.split("[NAVIGATE_TO:")[1].split("]")[0].strip()
#                     path, category_name = category_navigator.update_navigation_path(context, category_uid)
                    
#                     context['current_category_id'] = category_uid
#                     context['messages'].append({'role': 'assistant', 'content': assistant_response})
#                     self.conversation_manager.save_conversation(conversation_id, context)

#                     return {
#                         'message': assistant_response.split("[NAVIGATE_TO:")[0].strip(),
#                         'category_found': False,
#                         'navigation_path': path,
#                         'conversation_id': conversation_id
#                     }

#                 context['messages'].append({'role': 'assistant', 'content': assistant_response})
#                 self.conversation_manager.save_conversation(conversation_id, context)

#                 return {
#                     'message': assistant_response,
#                     'category_found': False,
#                     'conversation_id': conversation_id
#                 }

#         except Exception as e:
#             print(f"Error in chatbot: {str(e)}")
#             import traceback
#             print(traceback.format_exc())
#             return {'message': 'Internal Server Error', 'code': 500}, 500



















# from flask import request, abort, jsonify
# from flask_restful import Resource
# from typing import Dict, List, Optional
# from threading import Lock
# from data_ec import connect
# from search import Search_v2
# from openai import OpenAI
# import os

# class ConversationManager:
#     def __init__(self):
#         self.conversations = {}
#         self.lock = Lock() 

#     def get_conversation(self, conversation_id: str) -> Optional[Dict]:
#         if conversation_id in self.conversations:
#             with self.lock:
#                 return self.conversations.get(conversation_id)
#         else:
#             return {
#                 'messages': [],
#                 'navigation_path': [],
#                 'current_category_id': None
#             }
        
#     def save_conversation(self, conversation_id: str, context: Dict):
#         with self.lock:
#             self.conversations[conversation_id] = context
#             print("\n\nSAVED CONVERSATION\n", self.conversations, '\n\n')

#     def update_conversation(self, conversation_id: str, new_context: Dict):
#         with self.lock:
#             current = self.conversations.get(conversation_id)
#             if current:
#                 current.update(new_context)
#                 self.conversations[conversation_id] = current

# class CategoryNavigator:
#     def __init__(self, db_connection, deepseek_client):
#         self.db = db_connection
#         self.deepseek_client = deepseek_client

#     def get_categories(self, parent_id: Optional[str] = None) -> List[Dict]:
#         query = """
#             SELECT category_uid, category_name, category_description
#             FROM every_circle.category
#             WHERE category_parent_id {}
#             ORDER BY category_name
#         """.format("= '" + parent_id + "'" if parent_id else "IS NULL")
        
#         result = self.db.execute(query)
#         return result.get('result', []) if result else []

#     def build_prompt(self, user_input: str, conversation_context: Dict) -> str:
#         current_categories = self.get_categories(
#             conversation_context.get('current_category_id')
#         )
        
#         categories_context = "\n".join([
#             f"- {cat['category_name']}: {cat['category_description']}"
#             for cat in current_categories
#         ])

#         navigation_path = conversation_context.get('navigation_path', [])
#         path_str = " > ".join(navigation_path) if navigation_path else "Start"

#         return f"""
#         You are a helpful assistant guiding users to find the right business category.
#         Current navigation path: {path_str}
        
#         Available categories:
#         {categories_context}
        
#         Previous messages:
#         {self._format_previous_messages(conversation_context.get('messages', []))}
        
#         User input: {user_input}
        
#         Based on the context and available categories:
#         1. Help user navigate to the most relevant category by understanding what they are currently looking and providing sub categories of it
#         2. Ask clarifying questions if needed
#         3. If confident about a category match, indicate with [CATEGORY_SELECTED: category_name]
        
#         Keep responses conversational and helpful. If suggesting a category, explain why.
#         """

#     def _format_previous_messages(self, messages: List[Dict]) -> str:
#         return "\n".join([
#             f"{'User' if msg['role'] == 'user' else 'Assistant'}: {msg['content']}"
#             for msg in messages[-5:]  # Keep last 5 messages for context
#         ])

# class ChatbotAPI(Resource):
#     def __init__(self):
#         self.conversation_manager = ConversationManager()
#         self.open_ai_key = os.getenv("OPEN_AI_KEY")
#         self.open_ai_client = OpenAI(api_key=self.open_ai_key)
        
#         with connect() as db:
#             self.category_navigator = CategoryNavigator(db, self.open_ai_client)

#     def post(self):
#         try:
#             data = request.get_json()
            
#             if not data:
#                 abort(400, description="Request body is required")
            
#             conversation_id = data.get('conversation_id')
#             user_input = data.get('message')
#             profile_id = data.get('profile_id')
            
#             if not all([conversation_id, user_input, profile_id]):
#                 abort(400, description="conversation_id, message, and profile_id are required")

#             # Get or initialize conversation context
#             # context = self.conversation_manager.get_conversation(conversation_id) or {
#             #     'messages': [],
#             #     'navigation_path': [],
#             #     'current_category_id': None
#             # }

#             context = self.conversation_manager.get_conversation(conversation_id)

#             print("\n\nCONTEXT\n", context, '\n\n')

#             # Add user message to context
#             context['messages'].append({
#                 'role': 'user',
#                 'content': user_input
#             })

#             # Generate Deepseek prompt
#             prompt = self.category_navigator.build_prompt(user_input, context)

#             response = self.open_ai_client.chat.completions.create(
#                 model="gpt-4o-mini",
#                 store=True,
#                 messages=[
#                     {"role": "user", "content": prompt}
#                 ]
#             )

#             assistant_response = response.choices[0].message.content

#             # Check if a category was selected
#             if "[CATEGORY_SELECTED:" in assistant_response:
#                 # Extract category name and search
#                 category_name = assistant_response.split("[CATEGORY_SELECTED:")[1].split("]")[0].strip()
                
#                 # Use existing search functionality
#                 with connect() as db:
#                     search_query = f"""
#                         SELECT category_uid, category_name
#                         FROM category
#                         WHERE LOWER(category_name) = LOWER('{category_name}')
#                     """
#                     category_result = db.execute(search_query)
                    
#                     if category_result and 'result' in category_result:
#                         category_uid = category_result['result'][0]['category_uid']
#                         # Call your existing search API with the category
#                         search_response = self._perform_search(profile_id, category_uid)
                        
#                         return {
#                             'message': assistant_response.split("[CATEGORY_SELECTED:")[0].strip(),
#                             'category_found': True,
#                             'search_results': search_response,
#                             'conversation_id': conversation_id
#                         }

#             # Add assistant response to context
#             context['messages'].append({
#                 'role': 'assistant',
#                 'content': assistant_response
#             })

#             # Save updated context
#             self.conversation_manager.save_conversation(conversation_id, context)

#             return {
#                 'message': assistant_response,
#                 'category_found': False,
#                 'conversation_id': conversation_id
#             }

#         except Exception as e:
#             print(f"Error in chatbot: {str(e)}")
#             import traceback
#             print(traceback.format_exc())
#             return {'message': 'Internal Server Error', 'code': 500}, 500

#     def _perform_search(self, profile_id: str, category: str):
#         """Wrapper for your existing search functionality"""
#         try:
#             with connect() as db:
#                 # Create instance of your existing Search_v2 class
#                 search = Search_v2()
#                 # Call the get method with necessary parameters
#                 return search.get(profile_id=profile_id, category=category)
#         except Exception as e:
#             print(f"Error in search: {str(e)}")
#             return {'message': 'Search error', 'code': 500}, 500