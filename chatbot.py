from flask import request, abort, jsonify
from flask_restful import Resource
from typing import Dict, List, Optional
from threading import Lock
from data_ec import connect
from search import Search_v2
from openai import OpenAI
import os

class ConversationManager:
    def __init__(self):
        self.conversations = {}
        self.lock = Lock()  # Thread-safe operations for dictionary access

    def get_conversation(self, conversation_id: str) -> Optional[Dict]:
        """Retrieve conversation context from memory"""
        with self.lock:
            return self.conversations.get(conversation_id)

    def save_conversation(self, conversation_id: str, context: Dict):
        """Save conversation context to memory"""
        with self.lock:
            self.conversations[conversation_id] = context

    def update_conversation(self, conversation_id: str, new_context: Dict):
        """Update existing conversation context"""
        with self.lock:
            current = self.conversations.get(conversation_id)
            if current:
                current.update(new_context)
                self.conversations[conversation_id] = current

class CategoryNavigator:
    def __init__(self, db_connection, deepseek_client):
        self.db = db_connection
        self.deepseek_client = deepseek_client

    def get_categories(self, parent_id: Optional[str] = None) -> List[Dict]:
        """Get categories for a given parent ID"""
        query = """
            SELECT category_uid, category_name, category_description
            FROM every_circle.category
            WHERE category_parent_id {}
            ORDER BY category_name
        """.format("= '" + parent_id + "'" if parent_id else "IS NULL")
        
        result = self.db.execute(query)
        return result.get('result', []) if result else []

    def build_prompt(self, user_input: str, conversation_context: Dict) -> str:
        """Build context-aware prompt for Deepseek"""
        current_categories = self.get_categories(
            conversation_context.get('current_category_id')
        )
        
        categories_context = "\n".join([
            f"- {cat['category_name']}: {cat['category_description']}"
            for cat in current_categories
        ])

        navigation_path = conversation_context.get('navigation_path', [])
        path_str = " > ".join(navigation_path) if navigation_path else "Start"

        return f"""
        You are a helpful assistant guiding users to find the right business category.
        Current navigation path: {path_str}
        
        Available categories:
        {categories_context}
        
        Previous messages:
        {self._format_previous_messages(conversation_context.get('messages', []))}
        
        User input: {user_input}
        
        Based on the context and available categories:
        1. Help user navigate to the most relevant category
        2. Ask clarifying questions if needed
        3. If confident about a category match, indicate with [CATEGORY_SELECTED: category_name]
        
        Keep responses conversational and helpful. If suggesting a category, explain why.
        """

    def _format_previous_messages(self, messages: List[Dict]) -> str:
        return "\n".join([
            f"{'User' if msg['role'] == 'user' else 'Assistant'}: {msg['content']}"
            for msg in messages[-5:]  # Keep last 5 messages for context
        ])

class ChatbotAPI(Resource):
    def __init__(self):
        self.conversation_manager = ConversationManager()
        self.open_ai_api_key = os.getenv("OPEN_AI_API_KEY")
        self.open_ai_client = OpenAI(api_key=self.open_ai_api_key)
        
        with connect() as db:
            self.category_navigator = CategoryNavigator(db, self.open_ai_client)

    def post(self):
        try:
            data = request.get_json()
            
            if not data:
                abort(400, description="Request body is required")
            
            conversation_id = data.get('conversation_id')
            user_input = data.get('message')
            profile_id = data.get('profile_id')
            
            if not all([conversation_id, user_input, profile_id]):
                abort(400, description="conversation_id, message, and profile_id are required")

            # Get or initialize conversation context
            context = self.conversation_manager.get_conversation(conversation_id) or {
                'messages': [],
                'navigation_path': [],
                'current_category_id': None
            }

            # Add user message to context
            context['messages'].append({
                'role': 'user',
                'content': user_input
            })

            # Generate Deepseek prompt
            prompt = self.category_navigator.build_prompt(user_input, context)

            response = self.open_ai_client.chat.completions.create(
                model="gpt-4o-mini",
                store=True,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )

            assistant_response = response.choices[0].message.content

            # Check if a category was selected
            if "[CATEGORY_SELECTED:" in assistant_response:
                # Extract category name and search
                category_name = assistant_response.split("[CATEGORY_SELECTED:")[1].split("]")[0].strip()
                
                # Use existing search functionality
                with connect() as db:
                    search_query = f"""
                        SELECT category_uid, category_name
                        FROM category
                        WHERE LOWER(category_name) = LOWER('{category_name}')
                    """
                    category_result = db.execute(search_query)
                    
                    if category_result and 'result' in category_result:
                        category_uid = category_result['result'][0]['category_uid']
                        # Call your existing search API with the category
                        search_response = self._perform_search(profile_id, category_uid)
                        
                        return {
                            'message': assistant_response.split("[CATEGORY_SELECTED:")[0].strip(),
                            'category_found': True,
                            'search_results': search_response,
                            'conversation_id': conversation_id
                        }

            # Add assistant response to context
            context['messages'].append({
                'role': 'assistant',
                'content': assistant_response
            })

            # Save updated context
            self.conversation_manager.save_conversation(conversation_id, context)

            return {
                'message': assistant_response,
                'category_found': False,
                'conversation_id': conversation_id
            }

        except Exception as e:
            print(f"Error in chatbot: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return {'message': 'Internal Server Error', 'code': 500}, 500

    def _perform_search(self, profile_id: str, category: str):
        """Wrapper for your existing search functionality"""
        try:
            with connect() as db:
                # Create instance of your existing Search_v2 class
                search = Search_v2()
                # Call the get method with necessary parameters
                return search.get(profile_id=profile_id, category=category)
        except Exception as e:
            print(f"Error in search: {str(e)}")
            return {'message': 'Search error', 'code': 500}, 500

