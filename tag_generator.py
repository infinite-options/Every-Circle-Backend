from typing import Dict, List
from openai import OpenAI
from data_ec import connect
import os
from datetime import datetime
import traceback

class TagGenerator:
    def __init__(self, openai_client=None):
        self.openai_client = openai_client or OpenAI(api_key=os.getenv("OPEN_AI_KEY"))

    def _get_or_create_tag(self, db, tag_name: str) -> str:
        """Get existing tag UID or create new tag and return its UID"""
        try:
            print(f"\nProcessing tag: {tag_name}")
            # Check if tag exists - exact match only
            tag_query = db.select('every_circle.tags', where={'tag_name': tag_name.lower()})
            print(f"Tag query result: {tag_query}")
            
            if tag_query['result']:
                print(f"Found existing tag: {tag_query['result'][0]}")
                return tag_query['result'][0]['tag_uid']
                
            # Create new tag if it doesn't exist
            tag_uid_response = db.call(procedure='new_tag_uid')
            new_tag_uid = tag_uid_response['result'][0]['new_id']
            print(f"Generated new tag UID: {new_tag_uid}")
            
            # Insert new tag
            tag_payload = {
                'tag_uid': new_tag_uid,
                'tag_name': tag_name.lower()
            }
            insert_response = db.insert('every_circle.tags', tag_payload)
            print(f"Tag insert response: {insert_response}")
            
            if insert_response.get('code') != 200:
                raise Exception(f"Failed to insert tag: {insert_response.get('message')}")
            
            return new_tag_uid
            
        except Exception as e:
            print(f"Error in _get_or_create_tag: {str(e)}")
            traceback.print_exc()
            return None  # or raise the exception depending on how you want to handle errors

    def generate_tags(self, business_info: Dict[str, str], db) -> List[Dict[str, str]]:
        """Generate tags for a business using OpenAI"""
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
            response = self.openai_client.chat.completions.create(
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
            tags = unique_tags[:10]  # Ensure we only get top 10 unique tags
            
            print(f"\nGenerated raw tags: {tags}")
            
            # Get or create tag UIDs
            tag_info = []
            for tag in tags:
                tag_uid = self._get_or_create_tag(db, tag)
                if tag_uid:  # Only add if we successfully got/created a tag
                    tag_info.append({
                        'tag_uid': tag_uid,
                        'tag_name': tag
                    })
                    print(f"Added tag to info: {tag} with UID: {tag_uid}")
            
            print(f"\nFinal tag info: {tag_info}")
            return tag_info
            
        except Exception as e:
            print(f"Error generating tags: {str(e)}")
            traceback.print_exc()
            return []

    def generate_search_tags(self, search_query: str, db) -> List[Dict[str, str]]:
        """Generate search tags from user query"""
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
            response = self.openai_client.chat.completions.create(
                model="gpt-4-0125-preview",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.7,
                max_tokens=100
            )
            
            # Split and clean tags
            raw_tags = [tag.strip().lower() for tag in response.choices[0].message.content.split(',')]
            tags = raw_tags[:5]  # Ensure we only get top 5 tags
            
            # Get existing tag UIDs or create new ones if necessary
            tag_info = []
            for tag in tags:
                # First check if tag exists
                tag_query = db.select('every_circle.tags', where={'tag_name': tag.lower()})
                
                if tag_query['result']:
                    # Use existing tag
                    tag_info.append({
                        'tag_uid': tag_query['result'][0]['tag_uid'],
                        'tag_name': tag_query['result'][0]['tag_name']
                    })
                else:
                    # Create new tag only if it doesn't exist
                    tag_uid = self._get_or_create_tag(db, tag)
                    tag_info.append({
                        'tag_uid': tag_uid,
                        'tag_name': tag
                    })
            
            print(f"Generated tag info: {tag_info}")  # Debug print
            return tag_info
            
        except Exception as e:
            print(f"Error generating search tags: {str(e)}")
            return []