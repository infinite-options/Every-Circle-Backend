from flask import Flask, request, jsonify
from flask_restful import Resource, Api, abort
from typing import Dict, List, Optional, Tuple, Any
import os
import json
import traceback
from datetime import datetime
from openai import OpenAI
from data_ec import connect


class AIDirectBusinessSearch(Resource):
    def __init__(self):
        self.open_ai_client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))
        self.prompt_strategies = {
            "conversational": self._get_conversational_matcher_prompt,
            "persona": self._get_persona_based_recommender_prompt,
            "multi_criteria": self._get_multi_criteria_evaluator_prompt,
            "scenario": self._get_scenario_simulator_prompt,
            "semantic": self._get_semantic_matcher_prompt
        }
        self.default_strategy = "conversational"

    def _get_conversational_matcher_prompt(self, search_query: str, businesses: List[Dict]) -> str:
        """Generate a conversational matcher prompt for business search"""
        businesses_json = json.dumps([{
            "id": b["business_uid"],
            "name": b.get("business_name", ""),
            "description": b.get("business_short_bio", ""),
            "tagline": b.get("business_tag_line", ""),
            "city": b.get("business_city", ""),
            "state": b.get("business_state", ""),
            "phone": b.get("business_phone_number", ""),
            "email": b.get("business_email_id", ""),
            "address": f"{b.get('business_address_line_1', '')} {b.get('business_address_line_2', '')}".strip(),
            "website": b.get("business_website", ""),
            "ratings_count": len(b.get("ratings", [])),
            "avg_rating": b.get("avg_rating", 0),
            "connected_ratings_count": b.get("connected_ratings_count", 0)
        } for b in businesses], indent=2)
        
        return f"""
        Act as an expert business finder that understands natural language search queries.
        
        USER SEARCH QUERY: "{search_query}"
        
        AVAILABLE BUSINESSES:
        {businesses_json}
        
        INSTRUCTIONS:
        1. Analyze the user's search query to understand their intent, needs, and preferences
        2. Evaluate all businesses against the query, considering:
           - Relevance to the user's needs
           - Service/product alignment based on business description
           - Location if mentioned
           - Any specific requirements stated or implied
        3. Score each business on a scale of 0-100 based on how well it matches the query
        4. Return ONLY JSON with businesses that score 50 or higher
        
        Return format:
        {{"matches": [
          {{"id": "business_uid", "score": 85, "reasoning": "Brief explanation of match"}}
        ]}}
        """

    def _get_persona_based_recommender_prompt(self, search_query: str, businesses: List[Dict]) -> str:
        """Generate a persona-based recommender prompt for business search"""
        businesses_json = json.dumps([{
            "id": b["business_uid"],
            "name": b.get("business_name", ""),
            "description": b.get("business_short_bio", ""),
            "tagline": b.get("business_tag_line", ""),
            "city": b.get("business_city", ""),
            "state": b.get("business_state", ""),
            "phone": b.get("business_phone_number", ""),
            "email": b.get("business_email_id", ""),
            "address": f"{b.get('business_address_line_1', '')} {b.get('business_address_line_2', '')}".strip(),
            "website": b.get("business_website", ""),
            "ratings_count": len(b.get("ratings", [])),
            "avg_rating": b.get("avg_rating", 0),
            "connected_ratings_count": b.get("connected_ratings_count", 0)
        } for b in businesses], indent=2)
        
        return f"""
        Act as a personal recommendation agent that understands user personas.
        
        USER SEARCH QUERY: "{search_query}"
        
        AVAILABLE BUSINESSES:
        {businesses_json}
        
        INSTRUCTIONS:
        1. First, infer a likely persona based on the search query (e.g., busy parent, professional, student)
        2. Identify the underlying needs, pain points, and preferences of this persona
        3. Evaluate businesses based on how well they address these specific needs
        4. Score each business on a scale of 0-100 based on persona fit
        5. Return ONLY JSON with businesses that score 50 or higher
        
        Return format:
        {{"persona": "Briefly described persona", 
          "matches": [
            {{"id": "business_uid", "score": 85, "reasoning": "Brief explanation of match"}}
          ]
        }}
        """

    def _get_multi_criteria_evaluator_prompt(self, search_query: str, businesses: List[Dict]) -> str:
        """Generate a multi-criteria evaluator prompt for business search"""
        businesses_json = json.dumps([{
            "id": b["business_uid"],
            "name": b.get("business_name", ""),
            "description": b.get("business_short_bio", ""),
            "tagline": b.get("business_tag_line", ""),
            "city": b.get("business_city", ""),
            "state": b.get("business_state", ""),
            "phone": b.get("business_phone_number", ""),
            "email": b.get("business_email_id", ""),
            "address": f"{b.get('business_address_line_1', '')} {b.get('business_address_line_2', '')}".strip(),
            "website": b.get("business_website", ""),
            "ratings_count": len(b.get("ratings", [])),
            "avg_rating": b.get("avg_rating", 0),
            "connected_ratings_count": b.get("connected_ratings_count", 0)
        } for b in businesses], indent=2)
        
        return f"""
        Act as a multi-criteria business evaluator.
        
        USER SEARCH QUERY: "{search_query}"
        
        AVAILABLE BUSINESSES:
        {businesses_json}
        
        INSTRUCTIONS:
        1. Extract multiple evaluation criteria from the query (e.g., price, quality, expertise)
        2. If criteria aren't explicit, infer reasonable criteria based on the query
        3. Assign weights to each criterion (summing to 100%)
        4. Score each business against each weighted criterion
        5. Calculate a final weighted score (0-100) for each business
        6. Return ONLY JSON with businesses that score 50 or higher
        
        Return format:
        {{"criteria": [{{"name": "criterion", "weight": 0.4}}], 
          "matches": [
            {{"id": "business_uid", "score": 85, "criteria_scores": {{"criterion": 90}}, "reasoning": "Brief explanation"}}
          ]
        }}
        """

    def _get_scenario_simulator_prompt(self, search_query: str, businesses: List[Dict]) -> str:
        """Generate a scenario simulator prompt for business search"""
        businesses_json = json.dumps([{
            "id": b["business_uid"],
            "name": b.get("business_name", ""),
            "description": b.get("business_short_bio", ""),
            "tagline": b.get("business_tag_line", ""),
            "city": b.get("business_city", ""),
            "state": b.get("business_state", ""),
            "phone": b.get("business_phone_number", ""),
            "email": b.get("business_email_id", ""),
            "address": f"{b.get('business_address_line_1', '')} {b.get('business_address_line_2', '')}".strip(),
            "website": b.get("business_website", ""),
            "ratings_count": len(b.get("ratings", [])),
            "avg_rating": b.get("avg_rating", 0),
            "connected_ratings_count": b.get("connected_ratings_count", 0)
        } for b in businesses], indent=2)
        
        return f"""
        Act as a scenario simulation expert for matching businesses to user needs.
        
        USER SEARCH QUERY: "{search_query}"
        
        AVAILABLE BUSINESSES:
        {businesses_json}
        
        INSTRUCTIONS:
        1. Analyze the search query to identify the user's core problem or need
        2. Create a specific scenario that the user might be facing based on the query
        3. For each business, simulate how effectively it would solve the problem in this scenario
        4. Score each business on a scale of 0-100 based on scenario effectiveness
        5. Return ONLY JSON with businesses that score 50 or higher
        
        Return format:
        {{"scenario": "Detailed scenario description", 
          "matches": [
            {{"id": "business_uid", "score": 85, "reasoning": "How business addresses the scenario"}}
          ]
        }}
        """

    def _get_semantic_matcher_prompt(self, search_query: str, businesses: List[Dict]) -> str:
        """Generate a semantic matcher prompt for business search"""
        businesses_json = json.dumps([{
            "id": b["business_uid"],
            "name": b.get("business_name", ""),
            "description": b.get("business_short_bio", ""),
            "tagline": b.get("business_tag_line", ""),
            "city": b.get("business_city", ""),
            "state": b.get("business_state", ""),
            "phone": b.get("business_phone_number", ""),
            "email": b.get("business_email_id", ""),
            "address": f"{b.get('business_address_line_1', '')} {b.get('business_address_line_2', '')}".strip(),
            "website": b.get("business_website", ""),
            "ratings_count": len(b.get("ratings", [])),
            "avg_rating": b.get("avg_rating", 0),
            "connected_ratings_count": b.get("connected_ratings_count", 0)
        } for b in businesses], indent=2)
        
        return f"""
        Act as a semantic search engine with deep understanding of business services and customer needs.
        
        USER SEARCH QUERY: "{search_query}"
        
        AVAILABLE BUSINESSES:
        {businesses_json}
        
        INSTRUCTIONS:
        1. Parse the search query to understand explicit terms and implicit intent
        2. Expand the query with related concepts, synonyms, and service variations
        3. Analyze each business for semantic similarity to the expanded query, focusing on business descriptions
        4. Consider both direct matches and contextual relevance
        5. Score each business on a scale of 0-100 based on semantic relevance
        6. Return ONLY JSON with businesses that score 50 or higher
        
        Return format:
        {{"expanded_concepts": ["concept1", "concept2"], 
          "matches": [
            {{"id": "business_uid", "score": 85, "semantic_relevance": "Explanation of semantic match"}}
          ]
        }}
        """

    def get_user_connections(self, db, profile_id: str) -> List[Dict]:
        """Get user connections up to 3 degrees of separation"""
        try:
            print(f"[INFO] Getting user connections for {profile_id}")
            start_time = datetime.now()
            
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
            connections = user_connections_result.get('result', [])
            
            # Count connections by degree
            degree_counts = {}
            for conn in connections:
                degree = conn['degree']
                degree_counts[degree] = degree_counts.get(degree, 0) + 1
            
            # Log connection statistics
            for degree, count in sorted(degree_counts.items()):
                print(f"[INFO] Found {count} connections at degree {degree}")
                
            print(f"[INFO] Connection retrieval completed in {(datetime.now() - start_time).total_seconds():.2f} seconds")
            return connections
            
        except Exception as e:
            print(f"[ERROR] Error getting user connections: {str(e)}")
            traceback.print_exc()
            return []

    def get_all_businesses(self, db, profile_id: str) -> List[Dict]:
        """Fetch all businesses from the database with their ratings"""
        try:
            print(f"[INFO] Fetching businesses for user {profile_id}")
            start_time = datetime.now()
            
            # Get all businesses
            business_query = """
                SELECT b.*
                FROM every_circle.business b
            """
            
            business_result = db.execute(business_query)
            businesses = business_result.get('result', [])
            
            print(f"[INFO] Retrieved {len(businesses)} businesses from database")
            
            if not businesses:
                print("[WARN] No businesses found in database")
                return []
            
            # Get user connections
            user_connections = self.get_user_connections(db, profile_id)
            print(f"[INFO] Found {len(user_connections)} connections for user {profile_id}")
            
            # Debug log for connections
            for conn in user_connections[:5]:  # Log first 5 connections for debugging
                print(f"[DEBUG] Connection: User ID: {conn['user_id']}, Degree: {conn['degree']}")
            if len(user_connections) > 5:
                print(f"[DEBUG] ... and {len(user_connections) - 5} more connections")
            
            # Get all business UIDs
            business_uids = [b['business_uid'] for b in businesses]
            business_uid_str = "','".join(business_uids)
            
            # Get ALL ratings for businesses (not just from connected users)
            ratings_query = f"""
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
            
            ratings_result = db.execute(ratings_query)
            all_ratings = ratings_result.get('result', [])
            print(f"[INFO] Retrieved {len(all_ratings)} total ratings for all businesses")
            
            # Organize ratings by business and mark connected ratings
            ratings_by_business = {}
            connected_ratings_count = 0
            
            # Create a lookup dictionary for faster connection checks
            connection_lookup = {conn['user_id']: conn for conn in user_connections}
            
            for rating in all_ratings:
                business_id = rating['rating_business_id']
                if business_id not in ratings_by_business:
                    ratings_by_business[business_id] = []
                
                # Check if this rating is from a connected user
                rater_id = rating['rating_profile_id']
                if rater_id in connection_lookup:
                    conn = connection_lookup[rater_id]
                    rating['connection_degree'] = conn['degree']
                    rating['connection_path'] = conn['connection_path']
                    connected_ratings_count += 1
                
                ratings_by_business[business_id].append(rating)
            
            print(f"[INFO] Found {connected_ratings_count} ratings from connected users")
            
            # Add ratings to businesses
            businesses_with_ratings = 0
            businesses_with_connected_ratings = 0
            
            for business in businesses:
                business_id = business['business_uid']
                business['ratings'] = ratings_by_business.get(business_id, [])
                
                # Calculate average rating
                if business['ratings']:
                    businesses_with_ratings += 1
                    business['avg_rating'] = sum(r.get('rating_star', 0) for r in business['ratings']) / len(business['ratings'])
                    # Count ratings from connected users
                    connected_count = sum(1 for r in business['ratings'] if 'connection_degree' in r)
                    business['connected_ratings_count'] = connected_count
                    if connected_count > 0:
                        businesses_with_connected_ratings += 1
                else:
                    business['avg_rating'] = 0
                    business['connected_ratings_count'] = 0
            
            print(f"[INFO] {businesses_with_ratings} businesses have at least one rating")
            print(f"[INFO] {businesses_with_connected_ratings} businesses have at least one rating from a connected user")
            print(f"[INFO] Business data retrieval completed in {(datetime.now() - start_time).total_seconds():.2f} seconds")
            
            return businesses
            
        except Exception as e:
            print(f"[ERROR] Error fetching businesses: {str(e)}")
            traceback.print_exc()
            return []

    def match_businesses_with_ai(self, search_query: str, businesses: List[Dict], strategy: str = None) -> Dict:
        """Match businesses to search query using AI with specified strategy"""
        try:
            print(f"[INFO] Starting AI matching with strategy: {strategy}")
            start_time = datetime.now()
            
            if not strategy or strategy not in self.prompt_strategies:
                print(f"[INFO] Invalid strategy: {strategy}, using default: {self.default_strategy}")
                strategy = self.default_strategy
                
            # Handle case with no valid businesses to match
            if not businesses:
                print("[WARN] No businesses available to match")
                return {"matches": [], "error": "No businesses available to match"}
                
            print(f"[INFO] Matching {len(businesses)} businesses with search query: '{search_query}'")
            
            prompt_function = self.prompt_strategies[strategy]
            
            prompt = prompt_function(search_query, businesses)
            
            # Always add connection and rating instructions to the prompt
            connection_instructions = """
            ADDITIONAL SCORING FACTORS:
            - Give a 10-point bonus to businesses with ratings from connected users
            - Consider the average rating as a factor in your scoring
            - Businesses with more ratings from connected users should be preferred when relevant to the query
            """
            
            prompt += connection_instructions
            
            # Log sample of the prompt (first 500 chars)
            print(f"[DEBUG] Prompt sample: {prompt[:500]}...")
            
            print(f"[INFO] Sending request to OpenAI API")
            api_start_time = datetime.now()
            
            response = self.open_ai_client.chat.completions.create(
                model="gpt-4-0125-preview",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2,
                response_format={"type": "json_object"},
                max_tokens=2000
            )
            
            api_duration = (datetime.now() - api_start_time).total_seconds()
            print(f"[INFO] OpenAI API response received in {api_duration:.2f} seconds")
            
            result = json.loads(response.choices[0].message.content)
            
            # Log matching results
            matches = result.get('matches', [])
            print(f"[INFO] AI returned {len(matches)} matching businesses")
            
            # Log top 5 matches for debugging
            if matches:
                print("[DEBUG] Top matches:")
                for i, match in enumerate(sorted(matches, key=lambda x: x.get('score', 0), reverse=True)[:5]):
                    print(f"[DEBUG] {i+1}. Business ID: {match['id']}, Score: {match.get('score', 0)}")
                    
            total_duration = (datetime.now() - start_time).total_seconds()
            print(f"[INFO] AI matching completed in {total_duration:.2f} seconds")
            
            return result
            
        except Exception as e:
            print(f"[ERROR] Error in AI matching: {str(e)}")
            traceback.print_exc()
            return {"matches": [], "error": str(e)}

    def process_charges(self, db, business_uid_list: List[str], profile_id: str):
        """Process impression charges for the businesses"""
        try:
            print(f"[INFO] Processing charges for {len(business_uid_list)} businesses")
            start_time = datetime.now()
            
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
                print(f"[DEBUG] Charge created: ID={new_charge_uid}, Business={business_uid}, Amount=1.00")
                
            print(f"[INFO] Charge processing completed in {(datetime.now() - start_time).total_seconds():.2f} seconds")
        except Exception as e:
            print(f"[ERROR] Error processing charges: {str(e)}")
            traceback.print_exc()

    def get(self, profile_id):
        search_query = request.args.get('query', "").strip()
        strategy = request.args.get('strategy', self.default_strategy).strip()
        
        if not search_query:
            abort(400, description="search query is required")
        
        try:
            print(f"[INFO] === Starting search for user {profile_id} ===")
            print(f"[INFO] Query: '{search_query}', Strategy: '{strategy}'")
            start_time = datetime.now()
            
            with connect() as db:
                print(f"[INFO] Database connection established")
                
                # Get all businesses with ratings, including connection info
                all_businesses = self.get_all_businesses(db, profile_id)
                
                if not all_businesses:
                    print("[WARN] No businesses found in database, returning 404")
                    return {
                        'message': 'No businesses found in database',
                        'code': 404
                    }, 404
                
                # Match businesses with AI using the specified strategy
                matching_result = self.match_businesses_with_ai(search_query, all_businesses, strategy)
                
                if 'error' in matching_result:
                    error_msg = f"Error in AI matching: {matching_result['error']}"
                    print(f"[ERROR] {error_msg}")
                    return {
                        'message': error_msg,
                        'search_query': search_query,
                        'strategy': strategy,
                        'code': 500
                    }, 500
                
                if not matching_result.get('matches', []):
                    print(f"[INFO] No matching businesses found for query: '{search_query}'")
                    return {
                        'message': 'No matching businesses found',
                        'search_query': search_query,
                        'strategy': strategy,
                        'code': 200
                    }, 200
                
                # Extract the matched business UIDs
                matched_business_uids = [match['id'] for match in matching_result.get('matches', [])]
                print(f"[INFO] Found {len(matched_business_uids)} matched business IDs")
                
                # Find the matched businesses from all businesses
                strict_matches = []
                business_lookup = {b['business_uid']: b for b in all_businesses}
                
                for match_info in matching_result.get('matches', []):
                    business_id = match_info['id']
                    if business_id in business_lookup:
                        business = business_lookup[business_id]
                        # Add match information to business object
                        business['match_score'] = match_info.get('score', 0)
                        business['match_reasoning'] = match_info.get('reasoning', '')
                        
                        # Add other match details from the specific strategy
                        if 'criteria_scores' in match_info:
                            business['criteria_scores'] = match_info['criteria_scores']
                        if 'semantic_relevance' in match_info:
                            business['semantic_relevance'] = match_info['semantic_relevance']
                            
                        strict_matches.append(business)
                    else:
                        print(f"[WARN] Matched business ID {business_id} not found in database")
                
                # Sort matches by score, highest first
                strict_matches.sort(key=lambda x: x.get('match_score', 0), reverse=True)
                
                # Log top 5 matched businesses with their scores
                print("[INFO] Top matched businesses:")
                for i, b in enumerate(strict_matches[:5]):
                    print(f"[INFO] {i+1}. {b.get('business_name', 'Unknown')} (ID: {b['business_uid']}) - Score: {b.get('match_score', 0)}")
                    print(f"[INFO]    Reason: {b.get('match_reasoning', 'No reasoning provided')[:100]}...")
                    print(f"[INFO]    Ratings: {len(b.get('ratings', []))}, Connected Ratings: {b.get('connected_ratings_count', 0)}")
                
                # Process charges for matched businesses
                if matched_business_uids:
                    print(f"[INFO] Processing charges for {len(matched_business_uids)} businesses")
                    self.process_charges(db, matched_business_uids, profile_id)
                
                # Count businesses with connected ratings
                connected_businesses = sum(1 for b in strict_matches if b.get('connected_ratings_count', 0) > 0)
                print(f"[INFO] {connected_businesses} of {len(strict_matches)} matched businesses have ratings from connected users")
                
                # Prepare the response with strategy-specific data
                response = {
                    'search_analysis': {
                        'search_query': search_query,
                        'strategy': strategy
                    },
                    'business_results': {
                        'total_matches': len(strict_matches),
                        'matches': strict_matches,
                        'search_level': 'ai_direct_match'
                    },
                    'connection_info': {
                        'user_id': profile_id,
                        'total_connected_businesses': connected_businesses
                    }
                }
                
                # Add strategy-specific data to the response
                if strategy == 'persona' and 'persona' in matching_result:
                    response['search_analysis']['persona'] = matching_result['persona']
                    print(f"[INFO] Persona: {matching_result['persona']}")
                elif strategy == 'multi_criteria' and 'criteria' in matching_result:
                    response['search_analysis']['criteria'] = matching_result['criteria']
                    print(f"[INFO] Criteria: {matching_result['criteria']}")
                elif strategy == 'scenario' and 'scenario' in matching_result:
                    response['search_analysis']['scenario'] = matching_result['scenario']
                    print(f"[INFO] Scenario: {matching_result['scenario']}")
                elif strategy == 'semantic' and 'expanded_concepts' in matching_result:
                    response['search_analysis']['expanded_concepts'] = matching_result['expanded_concepts']
                    print(f"[INFO] Expanded concepts: {matching_result['expanded_concepts']}")

                total_duration = (datetime.now() - start_time).total_seconds()
                print(f"[INFO] Search completed in {total_duration:.2f} seconds")
                print(f"[INFO] === End search for user {profile_id} ===")
                
                return response, 200

        except Exception as e:
            print(f"[ERROR] Error in get: {str(e)}")
            traceback.print_exc()
            return {
                'message': f'Internal Server Error: {str(e)}',
                'code': 500
            }, 500