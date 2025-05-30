from flask_restful import Resource
from flask import request
from datetime import datetime
import ast
import traceback
import json

from data_ec import connect, processImage


class BusinessInfo(Resource):
    def get(self, uid):
        print(f"In Business GET with uid: {uid}")
        response = {}
        
        try:
            with connect() as db:
                key = {}
                if uid.startswith("200"):
                    key['business_uid'] = uid
                elif uid.startswith("100"):
                    key['business_user_id'] = uid
                else:
                    key['business_google_id'] = uid

                business_query = db.select('every_circle.business', where=key)
                
                if not business_query['result']:
                    response['message'] = f'No business found for {key}'
                    response['code'] = 404
                    return response, 404
                
                business_data = business_query['result'][0]
                business_uid = business_data['business_uid']
                
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
                
                response = {
                    'business': business_data,
                    'categories': category_response['result'] if 'result' in category_response else [],
                    'social_links': links_response['result'] if 'result' in links_response else [],
                    'services': services_response['result'] if 'result' in services_response else [],
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
        print("In Business POST")
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
                    response['message'] = 'Business already exists'
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
                
                payload['business_uid'] = new_business_uid
                # payload['business_user_id'] = user_uid
                payload['business_joined_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                if 'business_img_0' in request.files or 'delete_business_images' in payload:
                    key = {'business_personal_uid': new_business_uid}
                    images = processImage(key, payload)
                    payload['business_images_url'] = json.dumps(images)

                # print("Insert Payload: ", payload)
                insert_response = db.insert('every_circle.business', payload)
                # print("insert_response: ", insert_response)
                
                if categories_uid_str:
                    self._add_categories(db, categories_uid_str, new_business_uid)
                
                if social_links_str:
                    self._add_social_links(db, social_links_str, new_business_uid)
                
                # Add services if provided
                if services_str:
                    self._add_services(db, services_str, new_business_uid)
                

                
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
            print(f"Error in Business POST: {str(e)}")
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
                
                categories_uid_str = None
                social_links_str = None
                delete_services_str = None
                
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
                        import json
                        services_data = json.loads(payload.pop('business_services'))
                        service_uids = []
                        
                        # Process each service entry
                        for service_data in services_data:
                            print(service_data)
                            
                            # Check if this is an existing service (has UID)
                            if 'bs_uid' in service_data:
                                # Get the existing service UID
                                service_uid = service_data.pop('bs_uid')
                                
                                # Check if service exists
                                service_exists_query = db.select('every_circle.business_services', 
                                                               where={'bs_uid': service_uid})
                                
                                if not service_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Service with UID {service_uid} not found")
                                    continue
                                
                                # Update the existing service
                                if service_data:
                                    db.update('every_circle.business_services', 
                                           {'bs_uid': service_uid}, service_data)
                                    
                                service_uids.append(service_uid)
                            else:
                                # This is a new service entry
                                service_stored_procedure_response = db.call(procedure='new_bs_uid')
                                new_service_uid = service_stored_procedure_response['result'][0]['new_id']
                                service_data['bs_uid'] = new_service_uid
                                service_data['bs_business_id'] = business_uid
                                
                                # Insert the service record
                                db.insert('every_circle.business_services', service_data)
                                service_uids.append(new_service_uid)
                    
                    except Exception as e:
                        print(f"Error processing business_services JSON in PUT: {str(e)}")

                if 'business_img_0' in request.files or 'delete_business_images' in payload:
                    import json
                    key_personal = {'business_personal_uid': business_uid}
                    images = processImage(key_personal, payload)
                    print("OUTSIDE IMAGEs", images)
                    payload['business_images_url'] = (json.dumps(images) if images else None)
                
                print(payload)
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
    def _add_services(self, db, services_str, business_uid):
        try:
            import json
            services = json.loads(services_str)
            for service in services:
                # Generate a new bs_uid
                bs_uid_response = db.call(procedure='new_bs_uid')
                bs_uid = bs_uid_response['result'][0]['new_id']
                
                service['bs_uid'] = bs_uid
                service['bs_business_id'] = business_uid
                
                db.insert('every_circle.business_services', service)
        except Exception as e:
            print(f"Error processing services: {str(e)}")
            raise