from flask import request
from flask_restful import Resource
from data_ec import connect

class SearchReferral(Resource):
    def get(self):
        """
        Search for users by name, city, or state
        Query params:
        - query: search term (searches name, city, state)
        """
        try:
            query = request.args.get('query', '').strip()
            
            if not query or len(query) < 2:
                return {
                    'message': 'Search query must be at least 2 characters',
                    'code': 400,
                    'results': []
                }, 400
            
            # Search in first name, last name, city, and state
            search_query = """
                SELECT 
                    profile_personal_uid,
                    profile_personal_user_id,
                    profile_personal_first_name,
                    profile_personal_last_name,
                    CASE WHEN profile_personal_location_is_public = 1 
                        THEN profile_personal_city 
                        ELSE NULL 
                    END as profile_personal_city,
                    CASE WHEN profile_personal_location_is_public = 1 
                        THEN profile_personal_state 
                        ELSE NULL 
                    END as profile_personal_state,
                    CASE WHEN profile_personal_image_is_public = 1 
                        THEN profile_personal_image 
                        ELSE NULL 
                    END as profile_personal_image
                FROM every_circle.profile_personal
                WHERE 
                    LOWER(COALESCE(profile_personal_first_name, '')) LIKE LOWER(%s) OR
                    LOWER(COALESCE(profile_personal_last_name, '')) LIKE LOWER(%s) OR
                    LOWER(COALESCE(profile_personal_city, '')) LIKE LOWER(%s) OR
                    LOWER(COALESCE(profile_personal_state, '')) LIKE LOWER(%s)
                LIMIT 50
            """
            
            search_term = f"%{query}%"
            params = [search_term, search_term, search_term, search_term]
            
            print(f"Searching for: '{query}' with pattern: '{search_term}'")  # DEBUG
            
            with connect() as db:
                response = db.execute(search_query, params)
            
            print(f"Search response: {response}")  # DEBUG
            
            if response and 'result' in response:
                results = response['result']
                print(f"Found {len(results)} results")  # DEBUG
                return {
                    'message': 'Search results retrieved',
                    'code': 200,
                    'results': results,
                    'count': len(results)
                }, 200
            else:
                return {
                    'message': 'No results found',
                    'code': 200,
                    'results': [],
                    'count': 0
                }, 200
                
        except Exception as e:
            print(f"Error in SearchReferral: {str(e)}")
            import traceback
            traceback.print_exc()  # Print full error trace
            return {
                'message': f'An error occurred: {str(e)}',
                'code': 500,
                'results': []
            }, 500
    