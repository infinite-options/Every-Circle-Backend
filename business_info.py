from flask_restful import Resource
from flask import request
from datetime import datetime
import ast
import traceback
import json
import googlemaps
import os

from data_ec import connect, processImage, uploadImage, deleteImage


class BusinessInfo(Resource):
    def get(self, uid):
        print(f"In Business GET with uid: {uid}")
        response = {}
        
        try:
            with connect() as db:
                # First, find the business_uid based on the input uid
                business_uid = None
                if uid.startswith("200"):
                    # Direct business_uid lookup
                    business_query = db.select('every_circle.business', where={'business_uid': uid})
                    if business_query['result']:
                        business_uid = uid
                elif uid.startswith("100"):
                    # Look up business_uid from business_user table
                    business_user_query = db.select('every_circle.business_user', where={'bu_user_id': uid})
                    if business_user_query['result']:
                        business_uid = business_user_query['result'][0]['bu_business_id']
                else:
                    # Google ID lookup
                    business_query = db.select('every_circle.business', where={'business_google_id': uid})
                    if business_query['result']:
                        business_uid = business_query['result'][0]['business_uid']
                
                if not business_uid:
                    response['message'] = f'No business found for {uid}'
                    response['code'] = 404
                    return response, 404
                
                # Get business data
                business_query = db.select('every_circle.business', where={'business_uid': business_uid})
                if not business_query['result']:
                    response['message'] = f'No business found for {uid}'
                    response['code'] = 404
                    return response, 404
                
                business_data = business_query['result'][0]
                
                # Get all business_user records for this business with user email and profile information
                business_users_query = f"""
                    SELECT bu.*, 
                           u.user_email_id,
                           pp.profile_personal_uid as profile_id,
                           pp.profile_personal_first_name as first_name,
                           pp.profile_personal_last_name as last_name,
                           pp.profile_personal_image as profile_photo
                    FROM every_circle.business_user bu
                    LEFT JOIN every_circle.users u ON bu.bu_user_id = u.user_uid
                    LEFT JOIN every_circle.profile_personal pp ON bu.bu_user_id = pp.profile_personal_user_id
                    WHERE bu.bu_business_id = "{business_uid}"
                """
                business_users_response = db.execute(business_users_query)
                
                # Format business_users data
                business_users = []
                if business_users_response.get('result'):
                    for bu_record in business_users_response['result']:
                        business_users.append({
                            'business_user_id': bu_record.get('bu_user_id'),
                            'business_role': bu_record.get('bu_role'),
                            'business_uid': bu_record.get('bu_uid'),
                            'user_email': bu_record.get('user_email_id'),
                            'profile_id': bu_record.get('profile_id'),
                            'first_name': bu_record.get('first_name'),
                            'last_name': bu_record.get('last_name'),
                            'profile_photo': bu_record.get('profile_photo')
                        })
                
                category_query = f"""
                    SELECT c.*
                    FROM every_circle.business_category bc
                    JOIN every_circle.category c ON bc.bc_category_id = c.category_uid
                    WHERE bc.bc_business_id = "{business_uid}"
                """
                category_response = db.execute(category_query)
                
                links_query = f"""
                    SELECT sl.social_link_name, bl.business_link_url
                    FROM every_circle.business_link bl
                    JOIN every_circle.social_link sl ON bl.business_link_social_link_id = sl.social_link_uid
                    WHERE bl.business_link_business_id = "{business_uid}"
                """
                links_response = db.execute(links_query)
                
                # Added query for business services
                services_query = f"""
                    SELECT *
                    FROM every_circle.business_services
                    WHERE bs_business_id = "{business_uid}"
                """
                services_response = db.execute(services_query)

                # Added query for business ratings
                ratings_query = f"""
                    SELECT *
                    FROM every_circle.ratings
                    LEFT JOIN every_circle.profile_personal ON profile_personal_uid = rating_profile_id
                    -- WHERE rating_business_id = "200-000056"
                    WHERE rating_business_id = "{business_uid}"
                """
                ratings_response = db.execute(ratings_query)
                
                response = {
                    'business': business_data,
                    'business_users': business_users,
                    'categories': category_response['result'] if 'result' in category_response else [],
                    'social_links': links_response['result'] if 'result' in links_response else [],
                    'services': services_response['result'] if 'result' in services_response else [],
                    'ratings': ratings_response['result'] if 'result' in ratings_response else [],
                    'code': 200,
                    'message': 'Business data retrieved successfully'
                }
                
                return response, 200

        except Exception as e:
            print(f"Error in Business GET: {str(e)}")
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500

    def post(self):
        print("In BusinessInfo POST")
        response = {}

        try:
            payload = request.form.to_dict()

            if 'user_uid' not in payload:
                response['message'] = 'user_uid is required to register a business'
                response['code'] = 400
                return response, 400

            user_uid = payload.pop('user_uid')

            with connect() as db:
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

                business_look_up_query = None
                if business_google_id:
                    business_look_up_query = db.select('every_circle.business', where={'business_google_id': business_google_id})
                elif business_name:
                    business_look_up_query = db.select('every_circle.business', where={'business_name': business_name})
                
                if business_look_up_query and business_look_up_query['result']:
                    response['message'] = 'BusinessInfo: Business already exists'
                    response['code'] = 409
                    return response, 409
                
                business_stored_procedure_response = db.call(procedure='new_business_uid')
                new_business_uid = business_stored_procedure_response['result'][0]['new_id']
                
                categories_uid_str = None
                social_links_str = None
                services_str = None
                
                if 'business_categories_uid' in payload:
                    categories_uid_str = payload.pop('business_categories_uid')
                
                if 'social_links' in payload:
                    social_links_str = payload.pop('social_links')
                
                # Extract services data
                if 'business_services' in payload:
                    services_str = payload.pop('business_services')
                
                # Extract business_user fields from payload
                business_user_id = payload.pop('business_user_id', user_uid)  # Default to user_uid if not provided
                business_role = payload.pop('business_role', None)
                business_uid_param = payload.pop('business_uid', None)  # This should be None for POST, but handle if provided
                
                payload['business_uid'] = new_business_uid
                payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if 'business_img_0' in request.files or 'delete_business_images' in payload:
                    key = {'business_personal_uid': new_business_uid}
                    images = processImage(key, payload)
                    payload['business_images_url'] = json.dumps(images)

                # print("Insert Payload: ", payload)
                insert_response = db.insert('every_circle.business', payload)
                # print("insert_response: ", insert_response)
                
                # Insert into business_user table
                # Always create a business_user record when creating a business
                try:
                    bu_uid_response = db.call(procedure='new_bu_uid')
                    print(f"bu_uid_response: {bu_uid_response}")
                    
                    if 'result' not in bu_uid_response or not bu_uid_response['result']:
                        error_msg = bu_uid_response.get("message", "Stored procedure 'new_bu_uid' may not exist or failed")
                        print(f"Error: {error_msg}")
                        print(f"Full response: {bu_uid_response}")
                        response['message'] = f'Failed to generate bu_uid: {error_msg}'
                        response['code'] = 500
                        return response, 500
                except Exception as e:
                    print(f"Exception calling new_bu_uid: {str(e)}")
                    traceback.print_exc()
                    response['message'] = f'Error calling new_bu_uid stored procedure: {str(e)}'
                    response['code'] = 500
                    return response, 500
                
                new_bu_uid = bu_uid_response['result'][0]['new_id']
                
                business_user_payload = {
                    'bu_uid': new_bu_uid,
                    'bu_business_id': new_business_uid,
                    'bu_user_id': business_user_id,
                    'bu_role': business_role
                }
                db.insert('every_circle.business_user', business_user_payload)
                
                if categories_uid_str:
                    self._add_categories(db, categories_uid_str, new_business_uid)
                
                if social_links_str:
                    self._add_social_links(db, social_links_str, new_business_uid)
                
                # Add services if provided
                if services_str:
                    self._add_services(db, services_str, new_business_uid, business_user_id, request.files)
                
                # Handle additional business users if provided
                additional_business_user = payload.pop('additional_business_user', None)
                additional_business_role = payload.pop('additional_business_role', None)
                if additional_business_user and additional_business_role:
                    self._handle_additional_business_users(db, new_business_uid, additional_business_user, additional_business_role, exclude_user_id=business_user_id)

                
                # print(insert_response["code"])
                if(insert_response["code"] == 200):
                    response = {
                        'business_uid': new_business_uid,
                        'message': 'Business created successfully',
                        'code': insert_response["code"]
                    }
                else:
                    response = {
                        'Error': f"{new_business_uid} - Not Created",
                        'message': insert_response["message"],
                        'code': insert_response["code"]
                    }
                
                return response, 200
        
        except Exception as e:
            print(f"Error in BusinessInfo POST: {str(e)}")
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

            with connect() as db:
                business_exists_query = db.select('every_circle.business', where=key)
                if not business_exists_query['result']:
                    response['message'] = 'Business does not exist'
                    response['code'] = 404
                    return response, 404
                
                # Extract business_user fields from payload if provided
                business_user_id = payload.pop('business_user_id', None)
                business_role = payload.pop('business_role', None)
                
                # If business_user_id is not provided, try using user_uid as fallback
                if business_user_id is None and 'user_uid' in payload:
                    business_user_id = payload.get('user_uid')
                    print(f"Using user_uid '{business_user_id}' as business_user_id")
                
                # Handle business_user table update/insert
                if business_role is not None and business_user_id is not None:
                    # Check if a record exists with BOTH business_id AND user_id matching
                    business_user_query = db.select('every_circle.business_user', 
                                                   where={'bu_business_id': business_uid, 'bu_user_id': business_user_id})
                    
                    if business_user_query['result']:
                        # Record exists - check if role is different and update if needed
                        existing_record = business_user_query['result'][0]
                        existing_role = existing_record.get('bu_role')
                        
                        if existing_role != business_role:
                            # Role is different, update it
                            bu_uid = existing_record['bu_uid']
                            db.update('every_circle.business_user', {'bu_uid': bu_uid}, {'bu_role': business_role})
                            print(f"Updated business_user role from '{existing_role}' to '{business_role}' for business {business_uid}, user {business_user_id}")
                        else:
                            print(f"Business_user role unchanged: '{business_role}' for business {business_uid}, user {business_user_id}")
                    else:
                        # No matching record exists - insert new one
                        try:
                            bu_uid_response = db.call(procedure='new_bu_uid')
                            if 'result' not in bu_uid_response or not bu_uid_response['result']:
                                response['message'] = f'Failed to generate bu_uid: {bu_uid_response.get("message", "Unknown error")}'
                                response['code'] = 500
                                return response, 500
                            
                            new_bu_uid = bu_uid_response['result'][0]['new_id']
                            business_user_payload = {
                                'bu_uid': new_bu_uid,
                                'bu_business_id': business_uid,
                                'bu_user_id': business_user_id,
                                'bu_role': business_role
                            }
                            db.insert('every_circle.business_user', business_user_payload)
                            print(f"Inserted new business_user record for business {business_uid}, user {business_user_id}, role {business_role}")
                        except Exception as e:
                            print(f"Exception creating new business_user record: {str(e)}")
                            traceback.print_exc()
                
                # Get current business_user_id for service updates (fallback to first record if not provided)
                if business_user_id:
                    service_user_id = business_user_id
                else:
                    # Fallback: get the first business_user record for this business
                    fallback_query = db.select('every_circle.business_user', where={'bu_business_id': business_uid})
                    if fallback_query['result']:
                        service_user_id = fallback_query['result'][0].get('bu_user_id')
                    else:
                        service_user_id = None
                
                # Handle additional business users if provided
                additional_business_user = payload.pop('additional_business_user', None)
                additional_business_role = payload.pop('additional_business_role', None)
                if additional_business_user and additional_business_role:
                    # Exclude the primary business_user_id from deletion
                    exclude_id = business_user_id if business_user_id else service_user_id
                    self._handle_additional_business_users(db, business_uid, additional_business_user, additional_business_role, exclude_user_id=exclude_id)
                
                categories_uid_str = None
                social_links_str = None
                delete_services_str = None
                services_str = None
                custom_tags = None
                
                # Handle custom_tags field
                if 'custom_tags' in payload:
                    try:
                        custom_tags_str = payload.pop('custom_tags')
                        # Try to parse as JSON if it's a string
                        if isinstance(custom_tags_str, str):
                            custom_tags = json.loads(custom_tags_str)
                        elif isinstance(custom_tags_str, list):
                            custom_tags = custom_tags_str
                        else:
                            print(f"Warning: custom_tags is not a valid format: {type(custom_tags_str)}")
                            custom_tags = None
                        
                        if custom_tags is not None:
                            if not isinstance(custom_tags, list):
                                print(f"Warning: custom_tags must be a list, got {type(custom_tags)}")
                                custom_tags = None
                            else:
                                # Remove existing business_tags for this business
                                delete_query = f"DELETE FROM every_circle.business_tags WHERE bt_business_id = '{business_uid}'"
                                db.execute(delete_query)
                                print(f"Deleted existing business_tags for business {business_uid}")
                                
                                # Process and add new tags
                                stored_tags = self._process_tags(db, business_uid, custom_tags)
                                response['updated_tags'] = stored_tags
                                print(f"Processed {len(stored_tags)} tags for business {business_uid}")
                    except json.JSONDecodeError as e:
                        print(f"Error parsing custom_tags JSON: {str(e)}")
                    except Exception as e:
                        print(f"Error processing custom_tags: {str(e)}")
                        traceback.print_exc()
                
                if 'business_categories_uid' in payload:
                    categories_uid_str = payload.pop('business_categories_uid')
                    self._add_categories(db, categories_uid_str, business_uid)
                
                if 'social_links' in payload:
                    social_links_str = payload.pop('social_links')
                    self._update_social_links(db, social_links_str, business_uid)

                if 'delete_business_services' in payload:
                    delete_services_str = payload.pop('delete_business_services')
                    delete_services = ast.literal_eval(delete_services_str)
                    for service_uid in delete_services:
                        db.delete(f"""DELETE FROM every_circle.business_services 
                                  WHERE bs_uid = "{service_uid}";""")
                
                # Handle services update
                if 'business_services' in payload:
                    try:
                        services_data = json.loads(payload.pop('business_services'))
                        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        
                        # Process each service entry
                        for service_data in services_data:
                            print("Processing service data:", service_data)
                            
                            # Check if this is an existing service (has UID)
                            if 'bs_uid' in service_data:
                                # Update existing service
                                service_uid = service_data.pop('bs_uid')
                                print(f"Updating existing service with UID: {service_uid}")
                                
                                # Check if service exists
                                service_exists_query = db.select('every_circle.business_services', 
                                                               where={'bs_uid': service_uid})
                                
                                if not service_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Service with UID {service_uid} not found")
                                    continue
                                
                                # Add updated timestamp and user
                                service_data['bs_updated_at'] = current_time
                                if service_user_id:
                                    service_data['bs_updated_by'] = service_user_id
                                
                                # Handle service images if present
                                if 'bs_image_key' in service_data:
                                    image_key = service_data.pop('bs_image_key')
                                    service_images = []
                                    
                                    # Get existing images if any
                                    if service_exists_query['result'][0].get('bs_image_url'):
                                        try:
                                            service_images = json.loads(service_exists_query['result'][0]['bs_image_url'])
                                        except:
                                            service_images = []
                                    
                                    # Handle image deletion if specified
                                    if 'delete_images' in service_data:
                                        delete_images = json.loads(service_data.pop('delete_images'))
                                        for image_url in delete_images:
                                            if image_url in service_images:
                                                service_images.remove(image_url)
                                                # Delete from S3
                                                try:
                                                    delete_key = image_url.split(f"{os.getenv('BUCKET_NAME')}/", 1)[1]
                                                    deleteImage(delete_key)
                                                except Exception as e:
                                                    print(f"Error deleting image: {str(e)}")
                                    
                                    # Add new images
                                    for key in request.files:
                                        if key.startswith(f"{image_key}_img_"):
                                            file = request.files[key]
                                            if file and file.filename:
                                                unique_filename = f"service_{business_uid}_{service_uid}_{file.filename}"
                                                image_key = f'services/{business_uid}/{unique_filename}'
                                                image_url = uploadImage(file, image_key, '')
                                                if image_url:
                                                    service_images.append(image_url)
                                    
                                    # Update the service with new image URLs
                                    if service_images:
                                        service_data['bs_image_url'] = json.dumps(service_images)
                                    else:
                                        service_data['bs_image_url'] = None
                                
                                print(f"Updating service with data: {service_data}")
                                if service_data:
                                    update_response = db.update('every_circle.business_services', 
                                           {'bs_uid': service_uid}, service_data)
                                    print(f"Update response: {update_response}")
                            else:
                                # Add new service
                                print("Adding new service")
                                if service_user_id:
                                    self._add_services(db, json.dumps([service_data]), business_uid, 
                                                     service_user_id, request.files)
                                else:
                                    print("Warning: Cannot add service - service_user_id is None")
                    
                    except Exception as e:
                        print(f"Error processing business_services JSON in PUT: {str(e)}")
                        traceback.print_exc()  # Add this for better error tracking

                if 'business_img_0' in request.files or 'delete_business_images' in payload:
                    key_personal = {'business_personal_uid': business_uid}
                    images = processImage(key_personal, payload)
                    print("OUTSIDE IMAGEs", images)
                    payload['business_images_url'] = (json.dumps(images) if images else None)
                
                # List of valid business table columns
                valid_columns = [
                    'business_name', 'business_address_line_1', 'business_address_line_2',
                    'business_city', 'business_state', 'business_country', 'business_zip_code',
                    'business_phone_number', 'business_email_id', 'business_category_id',
                    'business_short_bio', 'business_tag_line', 'business_ein_number',
                    'business_website', 'business_images_url', 'business_email_id_is_public',
                    'business_phone_number_is_public', 'business_tag_line_is_public',
                    'business_short_bio_is_public', 'business_google_id', 'business_latitude',
                    'business_longitude', 'business_price_level', 'business_google_rating',
                    'business_joined_timestamp', 'business_is_active'
                ]
                
                # Remove any fields that don't exist in the business table
                invalid_fields = [field for field in payload.keys() if field not in valid_columns]
                for field in invalid_fields:
                    print(f"Removing invalid field: {field}")
                    payload.pop(field)
                
                print("Final payload:", payload)
                if payload:
                    update_response = db.update('every_circle.business', key, payload)
                    print(update_response)
                response = {
                    'message': 'Business updated successfully',
                    'code': 200
                }
                
                return response, 200
        
        except Exception as e:
            print(f"Error in Business PUT: {str(e)}")
            traceback.print_exc()  # Add this for better error tracking
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
    
    def delete(self, uid):
        print(f"In Business DELETE with uid: {uid}")
        response = {}
        
        try:
            with connect() as db:
                business_exists_query = db.select('every_circle.business', where={'business_uid': uid})
                if not business_exists_query['result']:
                    response['message'] = 'Business does not exist'
                    response['code'] = 404
                    return response, 404
                
                # Delete business services
                db.delete(f"""DELETE FROM every_circle.business_services 
                              WHERE bs_business_id = "{uid}";""")
                
                db.delete(f"""DELETE FROM every_circle.business_link 
                              WHERE business_link_business_id = "{uid}";""")
                
                db.delete(f"""DELETE FROM every_circle.business_category 
                              WHERE bc_business_id = "{uid}";""")
                
                db.delete(f"""DELETE FROM every_circle.business 
                              WHERE business_uid = "{uid}";""")
                
                response = {
                    'message': 'Business and associated data deleted successfully',
                    'code': 200
                }
                
                return response, 200
                
        except Exception as e:
            print(f"Error in Business DELETE: {str(e)}")
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
    
    def _add_categories(self, db, categories_uid_str, business_uid):
        try:
            categories_uid = ast.literal_eval(categories_uid_str)
            for category_uid in categories_uid:
                self._add_category_if_not_exists(db, category_uid, business_uid)
        except Exception as e:
            print(f"Error processing categories: {str(e)}")
            raise
    
    def _add_category_if_not_exists(self, db, category_uid, business_uid):
        check_query = f"""
            SELECT * FROM every_circle.business_category
            WHERE bc_business_id = "{business_uid}" AND bc_category_id = "{category_uid}";
        """
        check_response = db.execute(check_query)
        
        if check_response['result']:
            return
        
        bc_uid_response = db.call(procedure='new_bc_uid')
        bc_uid = bc_uid_response['result'][0]['new_id']
        
        category_payload = {
            'bc_uid': bc_uid,
            'bc_business_id': business_uid,
            'bc_category_id': category_uid
        }
        db.insert('every_circle.business_category', category_payload)
    
    def _add_social_links(self, db, social_links_str, business_uid):
        try:
            social_links = ast.literal_eval(social_links_str)
            for social_platform, url in social_links.items():
                social_link_query = db.select('every_circle.social_link', 
                                             where={'social_link_name': social_platform})
                
                if not social_link_query['result']:
                    continue
                
                social_link_uid = social_link_query['result'][0]['social_link_uid']
                
                bl_uid_response = db.call(procedure='new_business_link_uid')
                bl_uid = bl_uid_response['result'][0]['new_id']
                
                link_payload = {
                    'business_link_uid': bl_uid,
                    'business_link_business_id': business_uid,
                    'business_link_social_link_id': social_link_uid,
                    'business_link_url': url
                }
                db.insert('every_circle.business_link', link_payload)
        except Exception as e:
            print(f"Error processing social links: {str(e)}")
            raise
            
    def _update_social_links(self, db, social_links_str, business_uid):
        try:
            social_links = ast.literal_eval(social_links_str)
            
            existing_links_query = f"""
                SELECT sl.social_link_name, bl.business_link_url, bl.business_link_uid
                FROM every_circle.business_link bl
                JOIN every_circle.social_link sl ON bl.business_link_social_link_id = sl.social_link_uid
                WHERE bl.business_link_business_id = "{business_uid}";
            """
            existing_links_response = db.execute(existing_links_query)
            
            existing_links = {}
            if 'result' in existing_links_response and existing_links_response['result']:
                for link in existing_links_response['result']:
                    existing_links[link['social_link_name']] = {
                        'url': link['business_link_url'],
                        'uid': link['business_link_uid']
                    }
            
            for social_platform, url in social_links.items():
                social_link_query = db.select('every_circle.social_link', 
                                             where={'social_link_name': social_platform})
                
                if not social_link_query['result']:
                    continue
                
                social_link_uid = social_link_query['result'][0]['social_link_uid']
                
                if social_platform in existing_links:
                    update_link_uid = existing_links[social_platform]['uid']
                    update_link_key = {'business_link_uid': update_link_uid}
                    update_link_data = {'business_link_url': url}
                    
                    db.update('every_circle.business_link', update_link_key, update_link_data)
                    existing_links.pop(social_platform)
                else:
                    bl_uid_response = db.call(procedure='new_business_link_uid')
                    bl_uid = bl_uid_response['result'][0]['new_id']
                    
                    link_payload = {
                        'business_link_uid': bl_uid,
                        'business_link_business_id': business_uid,
                        'business_link_social_link_id': social_link_uid,
                        'business_link_url': url
                    }
                    
                    db.insert('every_circle.business_link', link_payload)
        except Exception as e:
            print(f"Error updating social links: {str(e)}")
            raise
    
    # New method to add services
    def _handle_additional_business_users(self, db, business_uid, additional_business_user, additional_business_role, exclude_user_id=None):
        """
        Handle additional business users from email arrays.
        - Find user_id for each email
        - Update existing records if role changed
        - Insert new records if they don't exist
        - Delete records that exist in DB but not in the new arrays (excluding exclude_user_id)
        
        Args:
            db: Database connection
            business_uid: Business UID
            additional_business_user: List of email addresses
            additional_business_role: List of roles (must match length of additional_business_user)
            exclude_user_id: User ID to exclude from deletion (typically the primary business_user_id)
        """
        try:
            # Parse the arrays if they're strings
            if isinstance(additional_business_user, str):
                additional_business_user = ast.literal_eval(additional_business_user)
            if isinstance(additional_business_role, str):
                additional_business_role = ast.literal_eval(additional_business_role)
            
            # Validate arrays have same length
            if len(additional_business_user) != len(additional_business_role):
                print(f"Error: additional_business_user and additional_business_role arrays must have same length")
                return
            
            # Get all existing business_user records for this business
            existing_records_query = db.select('every_circle.business_user', 
                                              where={'bu_business_id': business_uid})
            existing_records = {}
            if existing_records_query['result']:
                for record in existing_records_query['result']:
                    existing_records[record['bu_user_id']] = record
            
            # Track which user_ids we're processing (to identify deletions later)
            processed_user_ids = set()
            
            # Process each email/role pair
            for email, role in zip(additional_business_user, additional_business_role):
                # Find user_id from email
                user_query = db.select('every_circle.users', where={'user_email_id': email})
                if not user_query['result']:
                    print(f"Warning: User with email '{email}' not found, skipping")
                    continue
                
                user_id = user_query['result'][0]['user_uid']
                processed_user_ids.add(user_id)
                
                # Check if record exists for this user_id and business_id
                business_user_query = db.select('every_circle.business_user',
                                               where={'bu_business_id': business_uid, 'bu_user_id': user_id})
                
                if business_user_query['result']:
                    # Record exists - check if role changed
                    existing_record = business_user_query['result'][0]
                    existing_role = existing_record.get('bu_role')
                    
                    if existing_role != role:
                        # Role changed, update it
                        bu_uid = existing_record['bu_uid']
                        db.update('every_circle.business_user', {'bu_uid': bu_uid}, {'bu_role': role})
                        print(f"Updated business_user role from '{existing_role}' to '{role}' for business {business_uid}, user {user_id} (email: {email})")
                    else:
                        print(f"Business_user role unchanged: '{role}' for business {business_uid}, user {user_id} (email: {email})")
                else:
                    # Record doesn't exist - insert new one
                    try:
                        bu_uid_response = db.call(procedure='new_bu_uid')
                        if 'result' not in bu_uid_response or not bu_uid_response['result']:
                            print(f"Error: Failed to generate bu_uid for email {email}")
                            continue
                        
                        new_bu_uid = bu_uid_response['result'][0]['new_id']
                        business_user_payload = {
                            'bu_uid': new_bu_uid,
                            'bu_business_id': business_uid,
                            'bu_user_id': user_id,
                            'bu_role': role
                        }
                        db.insert('every_circle.business_user', business_user_payload)
                        print(f"Inserted new business_user record for business {business_uid}, user {user_id} (email: {email}), role {role}")
                    except Exception as e:
                        print(f"Exception creating new business_user record for email {email}: {str(e)}")
                        traceback.print_exc()
            
            # Delete records that exist in DB but not in the new arrays (excluding exclude_user_id)
            for existing_user_id, existing_record in existing_records.items():
                if existing_user_id not in processed_user_ids and existing_user_id != exclude_user_id:
                    # This record exists in DB but wasn't in the new arrays - delete it (unless it's the excluded user)
                    bu_uid = existing_record['bu_uid']
                    db.delete(f"""DELETE FROM every_circle.business_user WHERE bu_uid = "{bu_uid}";""")
                    print(f"Deleted business_user record for business {business_uid}, user {existing_user_id} (not in new arrays)")
                    
        except Exception as e:
            print(f"Error processing additional business users: {str(e)}")
            traceback.print_exc()
            raise
    
    def _process_tags(self, db, business_uid: str, tags: list) -> list:
        """
        Process tags for a business:
        - Check if tags exist in every_circle.tags table
        - Create new tags if they don't exist
        - Create business-tag associations in every_circle.business_tags table
        
        Args:
            db: Database connection
            business_uid: Business UID
            tags: List of tag names (strings)
            
        Returns:
            List of dictionaries with tag_name and tag_uid for successfully processed tags
        """
        print(f"Processing tags for business {business_uid}: {tags}")
        stored_tags = []
        
        for tag in tags:
            try:
                if not tag or not isinstance(tag, str):
                    print(f"Warning: Skipping invalid tag: {tag}")
                    continue
                    
                tag_name = tag.strip().lower()
                
                if not tag_name:
                    print(f"Warning: Skipping empty tag after processing")
                    continue
                
                # Check if tag exists in every_circle.tags
                tag_query = db.select('every_circle.tags', where={'tag_name': tag_name})
                
                if tag_query.get('result') and len(tag_query['result']) > 0:
                    # Tag exists, get its UID
                    tag_uid = tag_query['result'][0]['tag_uid']
                    print(f"Found existing tag: {tag_name} with UID: {tag_uid}")
                else:
                    # Create new tag in every_circle.tags
                    tag_uid_response = db.call(procedure='new_tag_uid')
                    if not tag_uid_response.get('result') or len(tag_uid_response['result']) == 0:
                        print(f"Warning: Failed to generate tag UID for tag: {tag_name}")
                        continue
                    
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
                
                # Check if business-tag association already exists
                existing_bt_query = db.select('every_circle.business_tags', 
                                             where={'bt_business_id': business_uid, 'bt_tag_id': tag_uid})
                
                if existing_bt_query.get('result') and len(existing_bt_query['result']) > 0:
                    print(f"Business-tag association already exists for tag: {tag_name}")
                    stored_tags.append({
                        'tag_name': tag_name,
                        'tag_uid': tag_uid
                    })
                    continue
                
                # Create business-tag association in every_circle.business_tags
                bt_uid_response = db.call(procedure='new_bt_uid')
                if not bt_uid_response.get('result') or len(bt_uid_response['result']) == 0:
                    print(f"Warning: Failed to generate business_tag UID for tag: {tag_name}")
                    continue
                
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
                    print(f"Created business-tag association: {bt_uid} for tag {tag_name}")
                else:
                    print(f"Failed to create business-tag association: {bt_response.get('message')}")
                
            except Exception as e:
                print(f"Error processing tag {tag}: {str(e)}")
                traceback.print_exc()
                continue
        
        return stored_tags

    def _add_services(self, db, services_str, business_uid, user_uid, request_files=None):
        try:
            import json
            services = json.loads(services_str)
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            for service in services:
                # Generate a new bs_uid
                bs_uid_response = db.call(procedure='new_bs_uid')
                bs_uid = bs_uid_response['result'][0]['new_id']
                
                # Handle service images if present
                service_images = []
                if request_files and 'bs_image_key' in service:
                    image_key = service.pop('bs_image_key')
                    # Look for images with the matching key prefix
                    for key in request_files:
                        if key.startswith(f"{image_key}_img_"):
                            file = request_files[key]
                            if file and file.filename:
                                # Use the existing uploadImage function
                                unique_filename = f"service_{business_uid}_{bs_uid}_{file.filename}"
                                image_key = f'services/{business_uid}/{unique_filename}'
                                image_url = uploadImage(file, image_key, '')
                                if image_url:
                                    service_images.append(image_url)
                
                # Set default values for required fields
                service_data = {
                    'bs_uid': bs_uid,
                    'bs_business_id': business_uid,
                    'bs_is_visible': service.get('bs_is_visible', 1),
                    'bs_status': service.get('bs_status', 'active'),
                    'bs_service_name': service.get('bs_service_name'),
                    'bs_service_desc': service.get('bs_service_desc'),
                    'bs_notes': service.get('bs_notes'),
                    'bs_sku': service.get('bs_sku'),
                    'bs_bounty': service.get('bs_bounty'),
                    'bs_bounty_currency': service.get('bs_bounty_currency'),
                    'bs_is_taxable': service.get('bs_is_taxable', 0),
                    'bs_tax_rate': service.get('bs_tax_rate'),
                    'bs_discount_allowed': service.get('bs_discount_allowed', 0),
                    'bs_refund_policy': service.get('bs_refund_policy'),
                    'bs_return_window_days': service.get('bs_return_window_days'),
                    'bs_image_url': json.dumps(service_images) if service_images else None,
                    'bs_display_order': service.get('bs_display_order'),
                    'bs_tags': service.get('bs_tags'),
                    'bs_created_at': current_time,
                    'bs_updated_at': current_time,
                    'bs_created_by': user_uid,
                    'bs_updated_by': user_uid,
                    'bs_duration_minutes': service.get('bs_duration_minutes'),
                    'bs_cost': service.get('bs_cost'),
                    'bs_cost_currency': service.get('bs_cost_currency')
                }
                
                # Remove None values to use database defaults
                service_data = {k: v for k, v in service_data.items() if v is not None}
                
                db.insert('every_circle.business_services', service_data)
        except Exception as e:
            print(f"Error processing services: {str(e)}")
            raise

    def get_google_places_info(self, place_id, user_uid):
        """Get business information from Google Places API and save to database"""
        try:
            if not place_id:
                return {'error': 'place_id is required'}, 400
                
            if not user_uid:
                return {'error': 'user_uid is required'}, 400
            
            # Initialize Google Maps client
            api_key = os.getenv('GOOGLE_MAPS_API_KEY')
            gmaps = googlemaps.Client(key=api_key)
            
            # Get place details
            place_details = gmaps.place(place_id, fields=[
                'name', 'formatted_address', 'formatted_phone_number',
                'website', 'rating', 'user_ratings_total', 'opening_hours',
                'geometry', 'type', 'price_level', 'business_status'
            ])
            
            if not place_details or 'result' not in place_details:
                return {'error': 'Place not found'}, 404
                
            place_data = place_details['result']
            print("Google Places Info: ", place_data)
            
            with connect() as db:
                # Verify user exists
                user_exists_query = db.select('every_circle.users', where={'user_uid': user_uid})
                if not user_exists_query['result']:
                    return {'error': 'User does not exist'}, 404
                
                # Start transaction
                db.execute("START TRANSACTION")
                
                # Get new business_uid
                business_stored_procedure_response = db.call(procedure='new_business_uid')
                business_uid = business_stored_procedure_response['result'][0]['new_id']
                print(f"Generated business_uid: {business_uid}")
                
                # Parse address components
                address_parts = place_data.get('formatted_address', '').split(',')
                address_line_1 = address_parts[0].strip() if address_parts else ''
                city = address_parts[1].strip() if len(address_parts) > 1 else ''
                state_zip = address_parts[2].strip() if len(address_parts) > 2 else ''
                country = address_parts[3].strip() if len(address_parts) > 3 else ''
                
                # Split state and zip code
                state_zip_parts = state_zip.split()
                state = ' '.join(state_zip_parts[:-1]) if len(state_zip_parts) > 1 else state_zip
                zip_code = state_zip_parts[-1] if len(state_zip_parts) > 1 else ''
                
                # Prepare data for database insertion
                business_data = {
                    'business_uid': business_uid,
                    'business_google_id': place_id,
                    'business_name': place_data.get('name'),
                    'business_phone_number': place_data.get('formatted_phone_number'),
                    'business_address_line_1': address_line_1,
                    'business_city': city,
                    'business_state': state,
                    'business_country': country,
                    'business_zip_code': zip_code,
                    'business_latitude': place_data.get('geometry', {}).get('location', {}).get('lat'),
                    'business_longitude': place_data.get('geometry', {}).get('location', {}).get('lng'),
                    'business_joined_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'business_price_level': place_data.get('price_level'),
                    'business_google_rating': str(place_data.get('rating')),
                    'business_website': place_data.get('website')
                }
                
                # Insert into database
                query = """
                    INSERT INTO every_circle.business 
                        (business_uid, business_google_id, business_name, 
                        business_phone_number, business_phone_number_is_public,
                        business_address_line_1, business_city, business_state, 
                        business_country, business_zip_code, business_latitude, 
                        business_longitude, business_joined_timestamp, 
                        business_price_level, business_google_rating, 
                        business_website, business_is_active)
                    VALUES 
                        (%(business_uid)s, %(business_google_id)s, %(business_name)s,
                        %(business_phone_number)s, 1,
                        %(business_address_line_1)s, %(business_city)s, %(business_state)s,
                        %(business_country)s, %(business_zip_code)s, %(business_latitude)s,
                        %(business_longitude)s, %(business_joined_timestamp)s,
                        %(business_price_level)s, %(business_google_rating)s,
                        %(business_website)s, 1)
                    ON DUPLICATE KEY UPDATE
                        business_name = %(business_name)s,
                        business_phone_number = %(business_phone_number)s,
                        business_address_line_1 = %(business_address_line_1)s,
                        business_city = %(business_city)s,
                        business_state = %(business_state)s,
                        business_country = %(business_country)s,
                        business_zip_code = %(business_zip_code)s,
                        business_latitude = %(business_latitude)s,
                        business_longitude = %(business_longitude)s,
                        business_price_level = %(business_price_level)s,
                        business_google_rating = %(business_google_rating)s,
                        business_website = %(business_website)s;
                """
                
                result = db.execute(query, business_data)
                print("Database insert result:", result)
                
                # Insert into business_user table
                bu_uid_response = db.call(procedure='new_bu_uid')
                if 'result' not in bu_uid_response or not bu_uid_response['result']:
                    db.execute("ROLLBACK")
                    return {'error': f'Failed to generate bu_uid: {bu_uid_response.get("message", "Unknown error")}'}, 500
                
                new_bu_uid = bu_uid_response['result'][0]['new_id']
                
                business_user_payload = {
                    'bu_uid': new_bu_uid,
                    'bu_business_id': business_uid,
                    'bu_user_id': user_uid,
                    'bu_role': None  # Default role, can be updated later
                }
                db.insert('every_circle.business_user', business_user_payload)
                
                # Verify the insert
                verify_query = "SELECT * FROM every_circle.business WHERE business_uid = %s"
                verify_result = db.execute(verify_query, (business_uid,))
                
                if not verify_result['result']:
                    print("Warning: Record not found after insert!")
                    db.execute("ROLLBACK")
                    return {'error': 'Failed to save business data'}, 500
                
                # Commit the transaction
                db.execute("COMMIT")
                
                return {
                    'message': 'Business information saved successfully',
                    'business_id': business_uid,
                    'place_details': place_data
                }, 200
                
        except Exception as e:
            print(f"Error in get_google_places_info: {str(e)}")
            return {'error': str(e)}, 500