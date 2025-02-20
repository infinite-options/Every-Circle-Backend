from flask import request
from flask_restful import Resource
from typing import Dict, List
from openai import OpenAI
import os
import traceback

class TagGeneratorAPI(Resource):
    def __init__(self):
        self.open_ai_client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

    def generate_tags_only(self, business_info: Dict[str, str]) -> List[str]:
        """Generate tags without database interaction"""
        prompt = f"""
        You are a business categorization expert. Generate the 10 most relevant tags based on the following criteria:
        1. Primary Category Tags:
           - Identify the main business category
           - Include relevant parent and child categories
        
        2. Service/Product Tags:
           - List key services or products offered
           - Include common industry-specific terms
           - Use both general and specific descriptors
        
        3. Business Type Tags:
           - Indicate the type of establishment (retail, service, professional, etc.)
           - Include relevant business model descriptors (B2C, B2B, etc.)
        
        4. Common Search Terms:
           - Include frequently used search terms related to the business
           - Add common variations and related terms
        
        Business Information:
        Name: {business_info.get('business_name', '')}
        Description: {business_info.get('business_description', '')}
        Address: {business_info.get('business_city', '')}, {business_info.get('business_state', '')}
        Website: {business_info.get('business_website', '')}

        Return only a comma-separated list of tags, ordered from most general to most specific.
        Make sure each tag is unique and not repeated.
        """

        try:
            response = self.open_ai_client.chat.completions.create(
                model="gpt-4-0125-preview",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=150
            )
            
            # Split and clean tags
            raw_tags = [tag.strip().lower() for tag in response.choices[0].message.content.split(',')]
            
            # Remove duplicates while maintaining order
            seen = set()
            unique_tags = [x for x in raw_tags if not (x in seen or seen.add(x))]
            
            return unique_tags[:10]  # Return top 10 unique tags
            
        except Exception as e:
            print(f"Error generating tags: {str(e)}")
            traceback.print_exc()
            return []

    def post(self):
        print("In TagGenerator POST")
        response = {}

        try:
            # Get payload data
            payload = request.get_json()
            print(f"Received payload: {payload}")

            if not payload or 'business_name' not in payload:
                response['message'] = 'business_name is required in the payload'
                response['code'] = 400
                return response, 400

            # Generate tags without database interaction
            print("Generating tags...")
            generated_tags = self.generate_tags_only(payload)
            print(f"Generated tags: {generated_tags}")

            if not generated_tags:
                response['message'] = 'Failed to generate tags'
                response['code'] = 400
                return response, 400

            # Format response
            response = {
                'tags': generated_tags,
                'total_tags': len(generated_tags),
                'business_name': payload['business_name'],
                'code': 200
            }

            return response, 200

        except Exception as e:
            print(f"Error in TagGenerator POST: {str(e)}")
            traceback.print_exc()
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500