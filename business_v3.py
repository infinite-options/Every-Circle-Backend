# from flask import request
# from flask_restful import Resource
# import traceback
# from typing import Dict
# from openai import OpenAI
# import os
# from datetime import datetime
# from data_ec import connect, processImage
# from tag_generator import TagGenerator

# class Business_v3(Resource):
#     def __init__(self):
#         self.open_ai_client = OpenAI(api_key=os.getenv("OPEN_AI_KEY"))
#         self.tag_generator = TagGenerator(self.open_ai_client)

#     def get(self, uid):
#         print("In Business GET")
#         response = {}
#         try:
#             with connect() as db:
#                 key = {}
#                 if uid[:3] == "200":
#                     key['business_uid'] = uid
#                 elif uid[:3] == "100":
#                     key['business_user_id'] = uid
#                 else:
#                     key['business_google_id'] = uid

#                 # Get business details
#                 business_response = db.select('every_circle.business', where=key)
                
#                 if not business_response['result']:
#                     response['message'] = f'No business found for {key}'
#                     response['code'] = 404
#                     return response, 404

#                 # Get associated tags
#                 tags_query = f"""
#                     SELECT t.tag_name 
#                     FROM every_circle.business_tags bt
#                     JOIN every_circle.tags t ON bt.bt_tag_id = t.tag_uid
#                     WHERE bt.bt_business_id = '{business_response['result'][0]['business_uid']}'
#                 """
#                 tags_response = db.execute(tags_query)
                
#                 # Add tags to response
#                 response = business_response
#                 response['result'][0]['tags'] = [tag['tag_name'] for tag in tags_response.get('result', [])]
                
#                 return response, 200

#         except Exception as e:
#             print(f"Error: {str(e)}")
#             response['message'] = 'Internal Server Error'
#             response['code'] = 500
#             return response, 500

#     def post(self):
#         print("In Business POST")
#         response = {}

#         try:
#             payload = request.form.to_dict()
#             print(f"Received payload: {payload}")

#             if 'user_uid' not in payload:
#                 response['message'] = 'user_uid is required to register a business'
#                 response['code'] = 400
#                 return response, 400

#             user_uid = payload.pop('user_uid')

#             with connect() as db:
#                 # Check if user exists
#                 user_exists_query = db.select('every_circle.users', where={'user_uid': user_uid})
#                 if not user_exists_query['result']:
#                     response['message'] = 'User does not exist'
#                     response['code'] = 404
#                     return response, 404
                
#                 business_google_id = payload.get('business_google_id', None)
#                 business_name = payload.get('business_name', None)

#                 if not business_google_id and not business_name:
#                     response['message'] = 'business_google_id or business_name is required'
#                     response['code'] = 400
#                     return response, 400

#                 # Check if business exists
#                 if business_google_id:
#                     business_look_up_query = db.select('every_circle.business', where={'business_google_id': business_google_id})
#                 elif business_name:
#                     business_look_up_query = db.select('every_circle.business', where={'business_name': business_name})
                
#                 if not business_look_up_query['result']:
#                     try:
#                         # Generate new business UID
#                         business_stored_procedure_response = db.call(procedure='new_business_uid')
#                         new_business_uid = business_stored_procedure_response['result'][0]['new_id']
#                         print(f"Generated new business UID: {new_business_uid}")
                        
#                         # Generate tags for the business
#                         print("Generating tags...")
#                         generated_tags = self.tag_generator.generate_tags(payload, db)
#                         print(f"Generated tags: {generated_tags}")
                        
#                         if not generated_tags:
#                             raise Exception("Failed to generate tags for business")
                        
#                         # Store business information
#                         payload['business_uid'] = new_business_uid
#                         payload['business_user_id'] = user_uid
#                         payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
#                         key = {'business_uid': new_business_uid}
#                         processImage(key, payload)
                        
#                         # Insert business
#                         print("Inserting business...")
#                         business_insert_response = db.insert('every_circle.business', payload)
#                         print(f"Business insert response: {business_insert_response}")
                        
#                         if not business_insert_response.get('code') == 200:
#                             raise Exception(f"Failed to insert business: {business_insert_response.get('message')}")
                        
#                         # Store business-tag associations
#                         print("Storing tags...")
#                         stored_tags = []
#                         for tag_info in generated_tags:
#                             try:
#                                 # Generate new business_tag UID
#                                 bt_uid_response = db.call(procedure='new_bt_uid')
#                                 bt_uid = bt_uid_response['result'][0]['new_id']
#                                 print(f"Generated new bt_uid: {bt_uid} for tag: {tag_info}")
                                
#                                 # Create business-tag association with only the required fields
#                                 bt_payload = {
#                                     'bt_uid': bt_uid,
#                                     'bt_tag_id': tag_info['tag_uid'],
#                                     'bt_business_id': new_business_uid
#                                 }
#                                 bt_response = db.insert('every_circle.business_tags', bt_payload)
#                                 print(f"Business-tag insert response: {bt_response}")
                                
#                                 if bt_response.get('code') == 200:
#                                     stored_tags.append(tag_info['tag_name'])
#                                 else:
#                                     print(f"Failed to store tag {tag_info['tag_name']}: {bt_response.get('message')}")
                            
#                             except Exception as tag_error:
#                                 print(f"Error storing tag {tag_info.get('tag_name')}: {str(tag_error)}")
#                                 continue
                        
#                         response['business_uid'] = new_business_uid
#                         response['tags'] = stored_tags
#                         print(f"Successfully created business with tags: {stored_tags}")
                        
#                     except Exception as inner_error:
#                         print(f"Error during business creation: {str(inner_error)}")
#                         traceback.print_exc()
#                         response['message'] = f'Error during business creation: {str(inner_error)}'
#                         response['code'] = 500
#                         return response, 500
                    
#                 else:
#                     response['message'] = 'Business already exists'
#                     response['code'] = 409
#                     return response, 409

#             return response, 200
        
#         except Exception as e:
#             print(f"Error in Business POST: {str(e)}")
#             traceback.print_exc()
#             response['message'] = 'Internal Server Error'
#             response['code'] = 500
#             return response, 500

from flask import request
from flask_restful import Resource
from typing import Dict, List
import os
import json
import traceback
from datetime import datetime
from data_ec import connect, processImage

class Business_v3(Resource):
    def _process_tags(self, db, business_uid: str, tags: List[str]) -> List[Dict]:
        """Process tags for a business - check if they exist, create if needed"""
        print(f"Processing tags for business {business_uid}: {tags}")
        stored_tags = []
        
        for tag in tags:
            try:
                tag_name = tag.strip().lower()
                
                # Check if tag exists
                tag_query = db.select('every_circle.tags', where={'tag_name': tag_name})
                
                if tag_query['result']:
                    # Tag exists, get its UID
                    tag_uid = tag_query['result'][0]['tag_uid']
                    print(f"Found existing tag: {tag_name} with UID: {tag_uid}")
                else:
                    # Create new tag
                    tag_uid_response = db.call(procedure='new_tag_uid')
                    tag_uid = tag_uid_response['result'][0]['new_id']
                    
                    tag_payload = {
                        'tag_uid': tag_uid,
                        'tag_name': tag_name
                    }
                    tag_insert = db.insert('every_circle.tags', tag_payload)
                    print(f"Created new tag: {tag_name} with UID: {tag_uid}")
                    
                    if tag_insert.get('code') != 200:
                        print(f"Failed to create tag {tag_name}: {tag_insert.get('message')}")
                        continue
                
                # Create business-tag association
                bt_uid_response = db.call(procedure='new_bt_uid')
                bt_uid = bt_uid_response['result'][0]['new_id']
                
                bt_payload = {
                    'bt_uid': bt_uid,
                    'bt_tag_id': tag_uid,
                    'bt_business_id': business_uid
                }
                bt_response = db.insert('every_circle.business_tags', bt_payload)
                
                if bt_response.get('code') == 200:
                    stored_tags.append({
                        'tag_name': tag_name,
                        'tag_uid': tag_uid
                    })
                    print(f"Created business-tag association: {bt_uid}")
                else:
                    print(f"Failed to create business-tag association: {bt_response.get('message')}")
                
            except Exception as e:
                print(f"Error processing tag {tag}: {str(e)}")
                traceback.print_exc()
                continue
        
        return stored_tags

    def get(self, uid):
        print("In Business GET")
        response = {}
        try:
            with connect() as db:
                key = {}
                if uid[:3] == "200":
                    key['business_uid'] = uid
                elif uid[:3] == "100":
                    key['business_user_id'] = uid
                else:
                    key['business_google_id'] = uid

                # Get business details
                business_response = db.select('every_circle.business', where=key)
                
                if not business_response['result']:
                    response['message'] = f'No business found for {key}'
                    response['code'] = 404
                    return response, 404

                # Get associated tags
                tags_query = f"""
                    SELECT t.tag_name 
                    FROM every_circle.business_tags bt
                    JOIN every_circle.tags t ON bt.bt_tag_id = t.tag_uid
                    WHERE bt.bt_business_id = '{business_response['result'][0]['business_uid']}'
                """
                tags_response = db.execute(tags_query)
                
                # Add tags to response
                response = business_response
                response['result'][0]['tags'] = [tag['tag_name'] for tag in tags_response.get('result', [])]
                
                return response, 200

        except Exception as e:
            print(f"Error in GET: {str(e)}")
            traceback.print_exc()
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def post(self):
        print("In Business POST")
        response = {}

        try:
            payload = request.form.to_dict()
            print(f"Received payload: {payload}")

            if 'user_uid' not in payload:
                response['message'] = 'user_uid is required'
                response['code'] = 400
                return response, 400

            if 'business_tags' not in payload:
                response['message'] = 'business_tags list is required'
                response['code'] = 400
                return response, 400

            try:
                # Parse tags from the payload
                business_tags = json.loads(payload.pop('business_tags'))
                if not isinstance(business_tags, list):
                    response['message'] = 'business_tags must be a list'
                    response['code'] = 400
                    return response, 400
            except json.JSONDecodeError:
                response['message'] = 'invalid business_tags format'
                response['code'] = 400
                return response, 400

            user_uid = payload.pop('user_uid')

            with connect() as db:
                # Check if user exists
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

                # Check if business exists
                if business_google_id:
                    business_look_up_query = db.select('every_circle.business', where={'business_google_id': business_google_id})
                elif business_name:
                    business_look_up_query = db.select('every_circle.business', where={'business_name': business_name})
                
                if not business_look_up_query['result']:
                    # Generate new business UID
                    business_stored_procedure_response = db.call(procedure='new_business_uid')
                    new_business_uid = business_stored_procedure_response['result'][0]['new_id']
                    
                    # Store business information
                    payload['business_uid'] = new_business_uid
                    payload['business_user_id'] = user_uid
                    payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    key = {'business_uid': new_business_uid}
                    processImage(key, payload)

                    if 'business_template' not in payload:
                        payload['business_template'] = "5"
                    elif payload['business_template'].strip() in ['', None, 'null']:
                        payload['business_template'] = "5"
                    
                    # Insert business
                    business_response = db.insert('every_circle.business', payload)
                    
                    if business_response.get('code') != 200:
                        response['message'] = f"Failed to create business: {business_response.get('message')}"
                        response['code'] = 500
                        return response, 500
                    
                    # Process tags
                    stored_tags = self._process_tags(db, new_business_uid, business_tags)
                    
                    response['business_uid'] = new_business_uid
                    response['tags'] = stored_tags
                    
                else:
                    response['message'] = 'Business already exists'
                    response['code'] = 409
                    return response, 409

            return response, 200
        
        except Exception as e:
            print(f"Error in POST: {str(e)}")
            traceback.print_exc()
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

            # Check for tags update
            tags = None
            if 'business_tags' in payload:
                try:
                    tags = json.loads(payload.pop('business_tags'))
                    if not isinstance(tags, list):
                        response['message'] = 'business_tags must be a list'
                        response['code'] = 400
                        return response, 400
                except json.JSONDecodeError:
                    response['message'] = 'invalid business_tags format'
                    response['code'] = 400
                    return response, 400

            with connect() as db:
                # Check if business exists
                business_exists_query = db.select('every_circle.business', where=key)
                if not business_exists_query['result']:
                    response['message'] = 'Business does not exist'
                    response['code'] = 404
                    return response, 404

                # Update tags if provided
                if tags is not None:
                    # Remove existing tags
                    db.execute(f"DELETE FROM every_circle.business_tags WHERE bt_business_id = '{business_uid}'")
                    
                    # Add new tags
                    stored_tags = self._process_tags(db, business_uid, tags)
                    response['updated_tags'] = stored_tags
                
                processImage(key, payload)
                response.update(db.update('every_circle.business', key, payload))
            
            return response, 200
        
        except Exception as e:
            print(f"Error in PUT: {str(e)}")
            traceback.print_exc()
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500