from flask import request
from flask_restful import Resource
from datetime import datetime


from data_ec import connect, processImage, processDocument

class UserProfileInfo(Resource):
    def get(self, uid):
        print("In UserProfileInfo GET")
        response = {}
        try:
            print(uid, type(uid))
            with connect() as db:
                if uid[:3] == "100":
                    # This is a user UID
                    user_response = db.select('every_circle.users', where={'user_uid': uid})
                    if not user_response['result']:
                        response['message'] = f'No user found for {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    user_data = user_response['result'][0]
                    
                    # Get personal info
                    personal_info = db.select('every_circle.profile_personal', where={'profile_personal_user_id': uid})
                    
                    if not personal_info['result']:
                        response['message'] = 'Profile not found for this user'
                        response['code'] = 404
                        return response, 404
                    
                    profile_id = personal_info['result'][0]['profile_personal_uid']
                    
                    # Get all associated profile data
                    response['personal_info'] = personal_info['result'][0]
                    
                    # Get social media links
                    social_links_query = f"""
                        SELECT pl.profile_link_uid, sl.social_link_name, pl.profile_link_url
                        FROM every_circle.profile_link pl
                        JOIN every_circle.social_link sl ON pl.profile_link_social_link_id = sl.social_link_uid
                        WHERE pl.profile_link_profile_personal_id = '{profile_id}'
                    """
                    social_links_response = db.execute(social_links_query)
                    response['links_info'] = social_links_response['result'] if social_links_response['result'] else []
                    
                    # Get expertise info - returning all expertise entries for this profile
                    expertise_info = db.select('every_circle.profile_expertise', 
                                            where={'profile_expertise_profile_personal_id': profile_id})
                    response['expertise_info'] = expertise_info['result'] if expertise_info['result'] else []
                    
                    # Get wishes info - returning all wishes entries for this profile
                    wishes_info = db.select('every_circle.profile_wish', 
                                         where={'profile_wish_profile_personal_id': profile_id})
                    response['wishes_info'] = wishes_info['result'] if wishes_info['result'] else []
                    
                    # Get experience info - returning all experiences for this profile
                    experience_info = db.select('every_circle.profile_experience', 
                                             where={'profile_experience_profile_personal_id': profile_id})
                    response['experience_info'] = experience_info['result'] if experience_info['result'] else []
                    
                    # Get education info - returning all education entries for this profile
                    education_info = db.select('every_circle.profile_education', 
                                            where={'profile_education_profile_personal_id': profile_id})
                    response['education_info'] = education_info['result'] if education_info['result'] else []

                    business_info = db.select('every_circle.business',
                                             where={'business_user_id': uid})
                    response['business_info'] = business_info['result'] if business_info['result'] else []
                    
                    # Add user_role from users table
                    response['user_role'] = user_data['user_role']
                    response['user_email'] = user_data['user_email_id']
                    
                    return response, 200
                    
                elif uid[:3] == "110":
                    # This is a profile UID (profile_personal)
                    personal_info = db.select('every_circle.profile_personal', where={'profile_personal_uid': uid})
                    
                    if not personal_info['result']:
                        response['message'] = f'No profile found for {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    profile_id = uid
                    
                    # Get all associated profile data
                    response['personal_info'] = personal_info['result'][0]
                    
                    # Get user info
                    user_id = personal_info['result'][0]['profile_personal_user_id']
                    user_info = db.select('every_circle.users', where={'user_uid': user_id})
                    response['user_role'] = user_info['result'][0]['user_role'] if user_info['result'] else "unknown"
                    response['user_email'] = user_info['result'][0]['user_email_id']
                    
                    # Get social media links
                    social_links_query = f"""
                        SELECT pl.profile_link_uid, sl.social_link_name, pl.profile_link_url
                        FROM every_circle.profile_link pl
                        JOIN every_circle.social_link sl ON pl.profile_link_social_link_id = sl.social_link_uid
                        WHERE pl.profile_link_profile_personal_id = '{profile_id}'
                    """
                    social_links_response = db.execute(social_links_query)
                    response['links_info'] = social_links_response['result'] if social_links_response['result'] else []
                    
                    # Get expertise info - returning all expertise entries
                    expertise_info = db.select('every_circle.profile_expertise', 
                                            where={'profile_expertise_profile_personal_id': profile_id})
                    response['expertise_info'] = expertise_info['result'] if expertise_info['result'] else []
                    
                    # Get wishes info - returning all wishes entries
                    wishes_info = db.select('every_circle.profile_wish', 
                                         where={'profile_wish_profile_personal_id': profile_id})
                    response['wishes_info'] = wishes_info['result'] if wishes_info['result'] else []
                    
                    # Get experience info - returning all experiences
                    experience_info = db.select('every_circle.profile_experience', 
                                             where={'profile_experience_profile_personal_id': profile_id})
                    response['experience_info'] = experience_info['result'] if experience_info['result'] else []
                    
                    # Get education info - returning all education entries
                    education_info = db.select('every_circle.profile_education', 
                                            where={'profile_education_profile_personal_id': profile_id})
                    response['education_info'] = education_info['result'] if education_info['result'] else []

                    business_info = db.select('every_circle.business',
                                             where={'business_user_id': user_id})
                    response['business_info'] = business_info['result'] if business_info['result'] else []
                    
                    return response, 200
                
                else:
                    response['message'] = 'Invalid UID'
                    response['code'] = 400
                    return response, 400

        except Exception as e:
            print(f"Error in UserProfileInfo GET: {str(e)}")
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
        
    def post(self):
        print("In UserProfileInfo POST")
        response = {}

        try:
            payload = request.form.to_dict()

            if 'user_uid' not in payload:
                response['message'] = 'user_uid is required'
                response['code'] = 400
                return response, 400

            user_uid = payload.pop('user_uid')

            with connect() as db:
                # Check if the user exists
                user_exists_query = db.select('every_circle.users', where={'user_uid': user_uid})
                if not user_exists_query['result']:
                    response['message'] = 'User does not exist'
                    response['code'] = 404
                    return response, 404
                
                # Check if the user already has a profile
                profile_exists_query = db.select('every_circle.profile_personal', where={'profile_personal_user_id': user_uid})
                if profile_exists_query['result']:
                    response['message'] = 'Profile already exists for this user'
                    response['code'] = 400
                    return response, 400

                # Generate new profile UID
                profile_stored_procedure_response = db.call(procedure='new_profile_personal_uid')
                new_profile_uid = profile_stored_procedure_response['result'][0]['new_id']
                
                # Create personal info record
                personal_info = {}
                personal_info['profile_personal_uid'] = new_profile_uid
                personal_info['profile_personal_user_id'] = user_uid
                
                # Set default referred by if not provided
                if 'profile_personal_referred_by' not in payload:
                    personal_info['profile_personal_referred_by'] = "110-000001"
                elif payload.get('profile_personal_referred_by', '').strip() in ['', 'null']:
                    personal_info['profile_personal_referred_by'] = "110-000001"
                else:
                    personal_info['profile_personal_referred_by'] = payload.pop('profile_personal_referred_by')
                
                # Extract personal info fields from payload
                personal_info_fields = [
                    'profile_personal_first_name', 'profile_personal_last_name', 'profile_personal_email_is_public', 
                    'profile_personal_phone_number', 'profile_personal_phone_number_is_public', 
                    'profile_personal_city', 'profile_personal_state', 'profile_personal_country',
                    'profile_personal_location_is_public', 'profile_personal_latitude', 'profile_personal_longitude', 
                    'profile_personal_image', 'profile_personal_image_is_public', 'profile_personal_tag_line', 
                    'profile_personal_tag_line_is_public', 'profile_personal_short_bio', 
                    'profile_personal_short_bio_is_public', 'profile_personal_resume', 
                    'profile_personal_resume_is_public', 'profile_personal_notification_preference', 
                    'profile_personal_location_preference', 'profile_personal_allow_banner_ads', 'profile_personal_banner_ads_bounty',
                    'profile_personal_experience_is_public', 'profile_personal_education_is_public',
                    'profile_personal_expertise_is_public', 'profile_personal_wishes_is_public'
                ]
                
                for field in personal_info_fields:
                    if field in payload:
                        personal_info[field] = payload.pop(field)
                
                # Process profile image if provided
                if 'profile_image' in request.files:
                    payload_images = {}
                    payload_images['profile_image'] = request.files['profile_image']
                    if 'delete_profile_image' in request.files:
                        payload_images['delete_profile_image'] = request.files['delete_profile_image']
                    key = {'profile_personal_uid': new_profile_uid}
                    personal_info['profile_personal_image'] = processImage(key, payload_images)
                
                # Set last updated timestamp
                personal_info['profile_personal_last_updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Insert personal info
                db.insert('every_circle.profile_personal', personal_info)
                
                # Create social media links if provided
                # First, get all social media platforms from the social_link table
                social_links_query = db.select('every_circle.social_link')
                social_links = {}
                
                if social_links_query['result']:
                    for link in social_links_query['result']:
                        social_links[link['social_link_name'].lower()] = link['social_link_uid']
                
                # Check for social media links in the payload
                social_media_links = {}
                if 'social_links' in payload:
                    try:
                        import json
                        social_media_links = json.loads(payload.pop('social_links'))
                    except Exception as e:
                        print(f"Error parsing social_links JSON: {str(e)}")
                
                # Process each social media link
                link_uids = []
                for platform, url in social_media_links.items():
                    platform_lower = platform.lower()
                    if platform_lower in social_links and url:
                        # Generate new profile link UID
                        link_stored_procedure_response = db.call(procedure='new_profile_link_uid')
                        new_link_uid = link_stored_procedure_response['result'][0]['new_id']
                        
                        link_info = {
                            'profile_link_uid': new_link_uid,
                            'profile_link_profile_personal_id': new_profile_uid,
                            'profile_link_social_link_id': social_links[platform_lower],
                            'profile_link_url': url
                        }
                        
                        db.insert('every_circle.profile_link', link_info)
                        link_uids.append(new_link_uid)
                
                # For expertise (handling multiple entries)
                expertise_entries = []
                expertise_uids = []
                
                # Check if expertise data is provided in JSON array format
                if 'expertises' in payload:
                    try:
                        import json
                        expertises_data = json.loads(payload.pop('expertises'))
                        
                        # Process each expertise entry
                        for exp_data in expertises_data:
                            expertise_info = {}
                            expertise_stored_procedure_response = db.call(procedure='new_profile_expertise_uid')
                            new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                            expertise_info['profile_expertise_uid'] = new_expertise_uid
                            expertise_info['profile_expertise_profile_personal_id'] = new_profile_uid
                            
                            # Map fields from the expertise data
                            if 'title' in exp_data:
                                expertise_info['profile_expertise_title'] = exp_data['title']
                            if 'description' in exp_data:
                                expertise_info['profile_expertise_description'] = exp_data['description']
                            if 'cost' in exp_data:
                                expertise_info['profile_expertise_cost'] = exp_data['cost']
                            if 'bounty' in exp_data:
                                expertise_info['profile_expertise_bounty'] = exp_data['bounty']
                            
                            # Insert the expertise record
                            db.insert('every_circle.profile_expertise', expertise_info)
                            expertise_entries.append(expertise_info)
                            expertise_uids.append(new_expertise_uid)
                    except Exception as e:
                        print(f"Error processing expertises JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('profile_expertise_') for key in payload):
                    expertise_info = {k: v for k, v in payload.items() if k.startswith('profile_expertise_')}
                    if expertise_info:
                        expertise_stored_procedure_response = db.call(procedure='new_profile_expertise_uid')
                        new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                        expertise_info['profile_expertise_uid'] = new_expertise_uid
                        expertise_info['profile_expertise_profile_personal_id'] = new_profile_uid
                        db.insert('every_circle.profile_expertise', expertise_info)
                        expertise_entries.append(expertise_info)
                        expertise_uids.append(new_expertise_uid)
                        # Remove used items
                        for k in list(expertise_info.keys()):
                            if k in payload:
                                payload.pop(k)
                
                # For wishes (handling multiple entries)
                wishes_entries = []
                wishes_uids = []
                
                # Check if wishes data is provided in JSON array format
                if 'wishes' in payload:
                    try:
                        import json
                        wishes_data = json.loads(payload.pop('wishes'))
                        
                        # Process each wish entry
                        for wish_data in wishes_data:
                            wish_info = {}
                            wishes_stored_procedure_response = db.call(procedure='new_profile_wish_uid')
                            new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                            wish_info['profile_wish_uid'] = new_wish_uid
                            wish_info['profile_wish_profile_personal_id'] = new_profile_uid
                            
                            # Map fields from the wish data
                            if 'title' in wish_data:
                                wish_info['profile_wish_title'] = wish_data['title']
                            if 'description' in wish_data:
                                wish_info['profile_wish_description'] = wish_data['description']
                            if 'bounty' in wish_data:
                                wish_info['profile_wish_bounty'] = wish_data['bounty']
                            
                            # Insert the wish record
                            db.insert('every_circle.profile_wish', wish_info)
                            wishes_entries.append(wish_info)
                            wishes_uids.append(new_wish_uid)
                    except Exception as e:
                        print(f"Error processing wishes JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('profile_wish_') for key in payload):
                    wish_info = {k: v for k, v in payload.items() if k.startswith('profile_wish_')}
                    if wish_info:
                        wishes_stored_procedure_response = db.call(procedure='new_profile_wish_uid')
                        new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                        wish_info['profile_wish_uid'] = new_wish_uid
                        wish_info['profile_wish_profile_personal_id'] = new_profile_uid
                        db.insert('every_circle.profile_wish', wish_info)
                        wishes_entries.append(wish_info)
                        wishes_uids.append(new_wish_uid)
                        # Remove used items
                        for k in list(wish_info.keys()):
                            if k in payload:
                                payload.pop(k)
                
                # For experience (handling multiple experiences)
                experience_entries = []
                experience_uids = []
                
                # Check if experience data is provided in JSON array format
                if 'experiences' in payload:
                    try:
                        import json
                        experiences_data = json.loads(payload.pop('experiences'))
                        
                        # Process each experience entry
                        for exp_data in experiences_data:
                            experience_info = {}
                            experience_stored_procedure_response = db.call(procedure='new_profile_experience_uid')
                            new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                            experience_info['profile_experience_uid'] = new_experience_uid
                            experience_info['profile_experience_profile_personal_id'] = new_profile_uid
                            
                            # Map fields from the experience data
                            if 'company_name' in exp_data:
                                experience_info['profile_experience_company_name'] = exp_data['company_name']
                            if 'position' in exp_data:
                                experience_info['profile_experience_position'] = exp_data['position']
                            if 'start_date' in exp_data:
                                experience_info['profile_experience_start_date'] = exp_data['start_date']
                            if 'end_date' in exp_data:
                                experience_info['profile_experience_end_date'] = exp_data['end_date']
                            
                            # Insert the experience record
                            db.insert('every_circle.profile_experience', experience_info)
                            experience_entries.append(experience_info)
                            experience_uids.append(new_experience_uid)
                    except Exception as e:
                        print(f"Error processing experiences JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('profile_experience_') for key in payload):
                    experience_info = {k: v for k, v in payload.items() if k.startswith('profile_experience_')}
                    if experience_info:
                        experience_stored_procedure_response = db.call(procedure='new_profile_experience_uid')
                        new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                        experience_info['profile_experience_uid'] = new_experience_uid
                        experience_info['profile_experience_profile_personal_id'] = new_profile_uid
                        db.insert('every_circle.profile_experience', experience_info)
                        experience_entries.append(experience_info)
                        experience_uids.append(new_experience_uid)
                        # Remove used items
                        for k in list(experience_info.keys()):
                            if k in payload:
                                payload.pop(k)
                
                # For education (handling multiple education entries)
                education_entries = []
                education_uids = []
                
                # Check if education data is provided in JSON array format
                if 'educations' in payload:
                    try:
                        import json
                        educations_data = json.loads(payload.pop('educations'))
                        
                        # Process each education entry
                        for edu_data in educations_data:
                            education_info = {}
                            education_stored_procedure_response = db.call(procedure='new_profile_education_uid')
                            new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                            education_info['profile_education_uid'] = new_education_uid
                            education_info['profile_education_profile_personal_id'] = new_profile_uid
                            
                            # Map fields from the education data
                            if 'school_name' in edu_data:
                                education_info['profile_education_school_name'] = edu_data['school_name']
                            if 'degree' in edu_data:
                                education_info['profile_education_degree'] = edu_data['degree']
                            if 'course' in edu_data:
                                education_info['profile_education_course'] = edu_data['course']
                            if 'start_date' in edu_data:
                                education_info['profile_education_start_date'] = edu_data['start_date']
                            if 'end_date' in edu_data:
                                education_info['profile_education_end_date'] = edu_data['end_date']
                            
                            # Insert the education record
                            db.insert('every_circle.profile_education', education_info)
                            education_entries.append(education_info)
                            education_uids.append(new_education_uid)
                    except Exception as e:
                        print(f"Error processing educations JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('profile_education_') for key in payload):
                    education_info = {k: v for k, v in payload.items() if k.startswith('profile_education_')}
                    if education_info:
                        education_stored_procedure_response = db.call(procedure='new_profile_education_uid')
                        new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                        education_info['profile_education_uid'] = new_education_uid
                        education_info['profile_education_profile_personal_id'] = new_profile_uid
                        db.insert('every_circle.profile_education', education_info)
                        education_entries.append(education_info)
                        education_uids.append(new_education_uid)
                        # Remove used items
                        for k in list(education_info.keys()):
                            if k in payload:
                                payload.pop(k)
            
            # Include all created UIDs in the response
            response['uids'] = {
                'profile_personal_uid': new_profile_uid,  # Main profile UID
            }
            
            # Add social media link UIDs
            if link_uids:
                response['uids']['profile_link_uids'] = link_uids
            
            # Add expertise UIDs if created
            if expertise_uids:
                response['uids']['profile_expertise_uids'] = expertise_uids
                
            # Add wishes UIDs if created
            if wishes_uids:
                response['uids']['profile_wish_uids'] = wishes_uids
            
            # Add experience UIDs if created
            if experience_uids:
                response['uids']['profile_experience_uids'] = experience_uids
            
            # Add education UIDs if created
            if education_uids:
                response['uids']['profile_education_uids'] = education_uids
            
            response['message'] = 'Profile created successfully'
            return response, 200
        
        except Exception as e:
            print(f"Error in UserProfileInfo POST: {str(e)}")
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
        
    def put(self):
        print("In UserProfileInfo PUT")
        response = {}
        import json

        try:
            payload = request.form.to_dict()
            print("PUT Payload: ", payload)

            if 'profile_uid' not in payload:
                response['message'] = 'profile_uid is required'
                response['code'] = 400
                return response, 400

            profile_uid = payload.pop('profile_uid')
            key = {'profile_personal_uid': profile_uid}
            print("UPDATED Payload: ", payload)
            updated_uids = {}
            deleted_uids = {}

            with connect() as db:
                # Check if the profile exists
                profile_exists_query = db.select('every_circle.profile_personal', where={'profile_personal_uid': profile_uid})
                print("Current Profile: ", profile_exists_query)
                if not profile_exists_query['result']:
                    response['message'] = 'Profile does not exist'
                    response['code'] = 404
                    return response, 404
                
                # Handle deletion requests first
                # Format: delete_experiences=["120-000001", "120-000002"]
                
                # Delete experiences if requested
                if 'delete_experiences' in payload:
                    try:
                        import json
                        experience_uids_to_delete = json.loads(payload.pop('delete_experiences'))
                        deleted_experience_uids = []
                        
                        for exp_uid in experience_uids_to_delete:
                            # Verify the experience exists and belongs to this profile
                            exp_exists_query = db.select('every_circle.profile_experience', 
                                                      where={'profile_experience_uid': exp_uid, 
                                                             'profile_experience_profile_personal_id': profile_uid})
                            
                            if exp_exists_query['result']:
                                delete_query = f"DELETE FROM every_circle.profile_experience WHERE profile_experience_uid = '{exp_uid}'"
                                delete_result = db.delete(delete_query)
                                deleted_experience_uids.append(exp_uid)
                        
                        if deleted_experience_uids:
                            deleted_uids['experiences'] = deleted_experience_uids
                    except Exception as e:
                        print(f"Error deleting experiences: {str(e)}")
                
                # Delete educations if requested
                if 'delete_educations' in payload:
                    try:
                        import json
                        education_uids_to_delete = json.loads(payload.pop('delete_educations'))
                        deleted_education_uids = []
                        
                        for edu_uid in education_uids_to_delete:
                            # Verify the education exists and belongs to this profile
                            edu_exists_query = db.select('every_circle.profile_education', 
                                                      where={'profile_education_uid': edu_uid, 
                                                             'profile_education_profile_personal_id': profile_uid})
                            
                            if edu_exists_query['result']:
                                delete_query = f"DELETE FROM every_circle.profile_education WHERE profile_education_uid = '{edu_uid}'"
                                delete_result = db.delete(delete_query)
                                deleted_education_uids.append(edu_uid)
                        
                        if deleted_education_uids:
                            deleted_uids['educations'] = deleted_education_uids
                    except Exception as e:
                        print(f"Error deleting educations: {str(e)}")
                
                # Delete expertises if requested
                if 'delete_expertises' in payload:
                    try:
                        import json
                        expertise_uids_to_delete = json.loads(payload.pop('delete_expertises'))
                        deleted_expertise_uids = []
                        
                        for exp_uid in expertise_uids_to_delete:
                            # Verify the expertise exists and belongs to this profile
                            exp_exists_query = db.select('every_circle.profile_expertise', 
                                                      where={'profile_expertise_uid': exp_uid, 
                                                             'profile_expertise_profile_personal_id': profile_uid})
                            
                            if exp_exists_query['result']:
                                delete_query = f"DELETE FROM every_circle.profile_expertise WHERE profile_expertise_uid = '{exp_uid}'"
                                delete_result = db.delete(delete_query)
                                deleted_expertise_uids.append(exp_uid)
                        
                        if deleted_expertise_uids:
                            deleted_uids['expertises'] = deleted_expertise_uids
                    except Exception as e:
                        print(f"Error deleting expertises: {str(e)}")
                
                # Delete wishes if requested
                if 'delete_wishes' in payload:
                    try:
                        import json
                        wish_uids_to_delete = json.loads(payload.pop('delete_wishes'))
                        deleted_wish_uids = []
                        
                        for wish_uid in wish_uids_to_delete:
                            # Verify the wish exists and belongs to this profile
                            wish_exists_query = db.select('every_circle.profile_wish', 
                                                       where={'profile_wish_uid': wish_uid, 
                                                              'profile_wish_profile_personal_id': profile_uid})
                            
                            if wish_exists_query['result']:
                                delete_query = f"DELETE FROM every_circle.profile_wish WHERE profile_wish_uid = '{wish_uid}'"
                                delete_result = db.delete(delete_query)
                                deleted_wish_uids.append(wish_uid)
                        
                        if deleted_wish_uids:
                            deleted_uids['wishes'] = deleted_wish_uids
                    except Exception as e:
                        print(f"Error deleting wishes: {str(e)}")
                
                # Delete social links if requested
                if 'delete_social_links' in payload:
                    try:
                        import json
                        social_link_uids_to_delete = json.loads(payload.pop('delete_social_links'))
                        deleted_social_link_uids = []
                        
                        for link_uid in social_link_uids_to_delete:
                            # Verify the link exists and belongs to this profile
                            link_exists_query = db.select('every_circle.profile_link', 
                                                       where={'profile_link_uid': link_uid, 
                                                              'profile_link_profile_personal_id': profile_uid})
                            
                            if link_exists_query['result']:
                                delete_query = f"DELETE FROM every_circle.profile_link WHERE profile_link_uid = '{link_uid}'"
                                delete_result = db.delete(delete_query)
                                deleted_social_link_uids.append(link_uid)
                        
                        if deleted_social_link_uids:
                            deleted_uids['social_links'] = deleted_social_link_uids
                    except Exception as e:
                        print(f"Error deleting social links: {str(e)}")
                
                # Now proceed with the regular update logic
                
                # Update personal info fields
                personal_info = {}
                personal_info_fields = [
                    'profile_personal_first_name', 'profile_personal_last_name', 'profile_personal_email_is_public', 
                    'profile_personal_phone_number', 'profile_personal_phone_number_is_public', 
                    'profile_personal_city', 'profile_personal_state', 'profile_personal_country',
                    'profile_personal_location_is_public', 'profile_personal_latitude', 'profile_personal_longitude', 
                    'profile_personal_image', 'profile_personal_image_is_public', 'profile_personal_tag_line', 
                    'profile_personal_tag_line_is_public', 'profile_personal_short_bio', 
                    'profile_personal_short_bio_is_public', 'profile_personal_resume', 
                    'profile_personal_resume_is_public', 'profile_personal_notification_preference', 
                    'profile_personal_location_preference', 'profile_personal_allow_banner_ads', 'profile_personal_banner_ads_bounty',
                    'profile_personal_experience_is_public', 'profile_personal_education_is_public',
                    'profile_personal_expertise_is_public', 'profile_personal_wishes_is_public'
                ]
                
                for field in personal_info_fields:
                    if field in payload:
                        personal_info[field] = payload.pop(field)
                
                print("Remaining payload fields: ", payload)
                
                if 'profile_image' in request.files or 'delete_profile_image' in payload:
                    payload_images = {}
                    if 'profile_image' in request.files:
                        payload_images['profile_image'] = request.files['profile_image']
                    if 'delete_profile_image' in payload:
                        payload_images['delete_profile_image'] = payload['delete_profile_image']
                    # key = {'profile_personal_uid': profile_uid}
                    personal_info['profile_personal_image'] = processImage(key, payload_images)

                if ('profile_resume_details' in payload and 'file_0' in request.files) or 'delete_profile_resume' in payload:
                    print("In Profile Personal Resume")
                    # if new resume is added check if there is an existing resume.  Delete existing resume and add new resume
                    # --------------- PROCESS DOCUMENTS ------------------
        
                    processDocument(key, payload)
                    print("Payload after processDocument function: ", payload, type(payload))

                    # Convert JSON string to a Python list
                    resumes = json.loads(payload['profile_personal_resume'])

                    # Access the first link
                    first_link = resumes[0]['link']

                    print(first_link)

                    personal_info['profile_personal_resume'] = first_link
                    print("Data to update: ", personal_info)

                    
                    # --------------- PROCESS DOCUMENTS ------------------


                    # if just deleting, then delete resume


                    # if just adding, then add resume


                    # payload_images = {}
                    # if 'profile_resume' in request.files:
                    #     payload_images['profile_resume'] = request.files['profile_resume']
                    # if 'delete_profile__resume' in payload:
                    #     payload_images['delete_profile_resume'] = payload['delete_profile_resume']
                    # key = {'profile_personal_uid': profile_uid}
                    # personal_info['profile_personal_resume'] = processDocument(key, payload)
                    # print(personal_info['profile_personal_resume'], type(personal_info['profile_personal_resume']))

                if personal_info:
                    # Process profile image if provided
                    # if 'profile_image' in personal_info:
                    
                    
                    # Set last updated timestamp
                    personal_info['profile_personal_last_updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Update personal info
                    db.update('every_circle.profile_personal', {'profile_personal_uid': profile_uid}, personal_info)

                    updated_uids['profile_personal_uid'] = profile_uid
                
                # Update social media links if provided
                # First, get all social media platforms from the social_link table
                social_links_query = db.select('every_circle.social_link')
                social_links = {}
                
                if social_links_query['result']:
                    for link in social_links_query['result']:
                        social_links[link['social_link_name'].lower()] = link['social_link_uid']
                
                # Check for social media links in the payload
                if 'social_links' in payload:
                    try:
                        import json
                        social_media_links = json.loads(payload.pop('social_links'))
                        updated_link_uids = []
                        
                        # Get existing links for this profile
                        existing_links_query = f"""
                            SELECT pl.profile_link_uid, sl.social_link_name, pl.profile_link_url
                            FROM every_circle.profile_link pl
                            JOIN every_circle.social_link sl ON pl.profile_link_social_link_id = sl.social_link_uid
                            WHERE pl.profile_link_profile_personal_id = '{profile_uid}'
                        """
                        existing_links_response = db.execute(existing_links_query)
                        
                        # Create a map of platform name to existing link data
                        existing_links = {}
                        if existing_links_response['result']:
                            for link in existing_links_response['result']:
                                existing_links[link['social_link_name'].lower()] = {
                                    'uid': link['profile_link_uid'],
                                    'url': link['profile_link_url']
                                }
                        
                        # Process each social media link
                        for platform, url in social_media_links.items():
                            platform_lower = platform.lower()
                            
                            if platform_lower in social_links:
                                if platform_lower in existing_links:
                                    # Update existing link
                                    link_uid = existing_links[platform_lower]['uid']
                                    
                                    # Only update if URL is different
                                    if url != existing_links[platform_lower]['url']:
                                        db.update('every_circle.profile_link', 
                                                 {'profile_link_uid': link_uid}, 
                                                 {'profile_link_url': url})
                                    
                                    updated_link_uids.append(link_uid)
                                else:
                                    # Create new link
                                    if url:  # Only create if URL is provided
                                        link_stored_procedure_response = db.call(procedure='new_profile_link_uid')
                                        new_link_uid = link_stored_procedure_response['result'][0]['new_id']
                                        
                                        link_info = {
                                            'profile_link_uid': new_link_uid,
                                            'profile_link_profile_personal_id': profile_uid,
                                            'profile_link_social_link_id': social_links[platform_lower],
                                            'profile_link_url': url
                                        }
                                        
                                        db.insert('every_circle.profile_link', link_info)
                                        updated_link_uids.append(new_link_uid)
                        
                        # Add updated link UIDs to response
                        if updated_link_uids:
                            updated_uids['profile_link_uids'] = updated_link_uids
                    except Exception as e:
                        print(f"Error processing social_links JSON: {str(e)}")
                
                # Handle multiple expertise entries
                if 'expertises' in payload:
                    try:
                        import json
                        expertises_data = json.loads(payload.pop('expertises'))
                        expertise_uids = []
                        
                        # Process each expertise entry
                        for exp_data in expertises_data:
                            expertise_info = {}
                            
                            # Check if this is an existing expertise (has UID)
                            if 'uid' in exp_data:
                                # Get the existing expertise UID
                                expertise_uid = exp_data.pop('uid')
                                
                                # Check if expertise exists
                                expertise_exists_query = db.select('every_circle.profile_expertise', 
                                                                 where={'profile_expertise_uid': expertise_uid})
                                
                                if not expertise_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Expertise with UID {expertise_uid} not found")
                                    continue
                                
                                # Map fields from the expertise data
                                if 'title' in exp_data:
                                    expertise_info['profile_expertise_title'] = exp_data['title']
                                if 'description' in exp_data:
                                    expertise_info['profile_expertise_description'] = exp_data['description']
                                if 'cost' in exp_data:
                                    expertise_info['profile_expertise_cost'] = exp_data['cost']
                                if 'bounty' in exp_data:
                                    expertise_info['profile_expertise_bounty'] = exp_data['bounty']
                                
                                # Update the existing expertise
                                if expertise_info:
                                    db.update('every_circle.profile_expertise', 
                                             {'profile_expertise_uid': expertise_uid}, expertise_info)
                                    
                                expertise_uids.append(expertise_uid)
                            else:
                                # This is a new expertise entry
                                expertise_stored_procedure_response = db.call(procedure='new_profile_expertise_uid')
                                new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                                expertise_info['profile_expertise_uid'] = new_expertise_uid
                                expertise_info['profile_expertise_profile_personal_id'] = profile_uid
                                
                                # Map fields from the expertise data
                                if 'title' in exp_data:
                                    expertise_info['profile_expertise_title'] = exp_data['title']
                                if 'description' in exp_data:
                                    expertise_info['profile_expertise_description'] = exp_data['description']
                                if 'cost' in exp_data:
                                    expertise_info['profile_expertise_cost'] = exp_data['cost']
                                if 'bounty' in exp_data:
                                    expertise_info['profile_expertise_bounty'] = exp_data['bounty']
                                
                                # Insert the expertise record
                                db.insert('every_circle.profile_expertise', expertise_info)
                                expertise_uids.append(new_expertise_uid)
                        
                        updated_uids['profile_expertise_uids'] = expertise_uids
                    except Exception as e:
                        print(f"Error processing expertises JSON in PUT: {str(e)}")
                
                # Handle individual expertise update (legacy format)
                elif any(key.startswith('profile_expertise_') and key != 'profile_expertise_profile_personal_id' for key in payload):
                    expertise_info = {k: v for k, v in payload.items() if k.startswith('profile_expertise_')}
                    
                    if 'profile_expertise_uid' in expertise_info:
                        # Update specific expertise entry
                        expertise_uid = expertise_info.pop('profile_expertise_uid')
                        
                        # Check if expertise exists
                        expertise_exists_query = db.select('every_circle.profile_expertise', 
                                                         where={'profile_expertise_uid': expertise_uid})
                        
                        if not expertise_exists_query['result']:
                            response['message'] = f'Expertise with UID {expertise_uid} not found'
                            response['code'] = 404
                            return response, 404
                        
                        # Remove profile_id if present
                        if 'profile_expertise_profile_personal_id' in expertise_info:
                            expertise_info.pop('profile_expertise_profile_personal_id')
                        
                        # Update existing expertise
                        if expertise_info:
                            db.update('every_circle.profile_expertise', 
                                     {'profile_expertise_uid': expertise_uid}, expertise_info)
                            updated_uids['profile_expertise_uid'] = expertise_uid
                    else:
                        # Add new expertise
                        expertise_stored_procedure_response = db.call(procedure='new_profile_expertise_uid')
                        new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                        expertise_info['profile_expertise_uid'] = new_expertise_uid
                        expertise_info['profile_expertise_profile_personal_id'] = profile_uid
                        db.insert('every_circle.profile_expertise', expertise_info)
                        updated_uids['profile_expertise_uid'] = new_expertise_uid
                    
                    # Remove used items
                    for k in list(expertise_info.keys()):
                        if k in payload:
                            payload.pop(k)
                
                # Handle multiple wishes entries
                if 'wishes' in payload:
                    try:
                        import json
                        wishes_data = json.loads(payload.pop('wishes'))
                        wishes_uids = []
                        
                        # Process each wish entry
                        for wish_data in wishes_data:
                            wish_info = {}
                            
                            # Check if this is an existing wish (has UID)
                            if 'uid' in wish_data:
                                # Get the existing wish UID
                                wish_uid = wish_data.pop('uid')
                                
                                # Check if wish exists
                                wish_exists_query = db.select('every_circle.profile_wish', 
                                                            where={'profile_wish_uid': wish_uid})
                                
                                if not wish_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Wish with UID {wish_uid} not found")
                                    continue
                                
                                # Map fields from the wish data
                                if 'title' in wish_data:
                                    wish_info['profile_wish_title'] = wish_data['title']
                                if 'description' in wish_data:
                                    wish_info['profile_wish_description'] = wish_data['description']
                                if 'bounty' in wish_data:
                                    wish_info['profile_wish_bounty'] = wish_data['bounty']
                                
                                # Update the existing wish
                                if wish_info:
                                    db.update('every_circle.profile_wish', 
                                             {'profile_wish_uid': wish_uid}, wish_info)
                                    
                                wishes_uids.append(wish_uid)
                            else:
                                # This is a new wish entry
                                wishes_stored_procedure_response = db.call(procedure='new_profile_wish_uid')
                                new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                                wish_info['profile_wish_uid'] = new_wish_uid
                                wish_info['profile_wish_profile_personal_id'] = profile_uid
                                
                                # Map fields from the wish data
                                if 'title' in wish_data:
                                    wish_info['profile_wish_title'] = wish_data['title']
                                if 'description' in wish_data:
                                    wish_info['profile_wish_description'] = wish_data['description']
                                if 'bounty' in wish_data:
                                    wish_info['profile_wish_bounty'] = wish_data['bounty']
                                
                                # Insert the wish record
                                db.insert('every_circle.profile_wish', wish_info)
                                wishes_uids.append(new_wish_uid)
                        
                        updated_uids['profile_wish_uids'] = wishes_uids
                    except Exception as e:
                        print(f"Error processing wishes JSON in PUT: {str(e)}")
                
                # Handle individual wish update (legacy format)
                elif any(key.startswith('profile_wish_') and key != 'profile_wish_profile_personal_id' for key in payload):
                    wish_info = {k: v for k, v in payload.items() if k.startswith('profile_wish_')}
                    
                    if 'profile_wish_uid' in wish_info:
                        # Update specific wish entry
                        wish_uid = wish_info.pop('profile_wish_uid')
                        
                        # Check if wish exists
                        wish_exists_query = db.select('every_circle.profile_wish', 
                                                     where={'profile_wish_uid': wish_uid})
                        
                        if not wish_exists_query['result']:
                            response['message'] = f'Wish with UID {wish_uid} not found'
                            response['code'] = 404
                            return response, 404
                        
                        # Remove profile_id if present
                        if 'profile_wish_profile_personal_id' in wish_info:
                            wish_info.pop('profile_wish_profile_personal_id')
                        
                        # Update existing wish
                        if wish_info:
                            db.update('every_circle.profile_wish', 
                                     {'profile_wish_uid': wish_uid}, wish_info)
                            updated_uids['profile_wish_uid'] = wish_uid
                    else:
                        # Add new wish
                        wishes_stored_procedure_response = db.call(procedure='new_profile_wish_uid')
                        new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                        wish_info['profile_wish_uid'] = new_wish_uid
                        wish_info['profile_wish_profile_personal_id'] = profile_uid
                        db.insert('every_circle.profile_wish', wish_info)
                        updated_uids['profile_wish_uid'] = new_wish_uid
                    
                    # Remove used items
                    for k in list(wish_info.keys()):
                        if k in payload:
                            payload.pop(k)
                
                # Handle multiple experiences
                if 'experiences' in payload:
                    try:
                        import json
                        experiences_data = json.loads(payload.pop('experiences'))
                        experience_uids = []
                        
                        # Process each experience entry
                        for exp_data in experiences_data:
                            experience_info = {}
                            
                            # Check if this is an existing experience (has UID)
                            if 'uid' in exp_data:
                                # Get the existing experience UID
                                experience_uid = exp_data.pop('uid')
                                
                                # Check if experience exists
                                experience_exists_query = db.select('every_circle.profile_experience', 
                                                                  where={'profile_experience_uid': experience_uid})
                                
                                if not experience_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Experience with UID {experience_uid} not found")
                                    continue
                                
                                # Map fields from the experience data
                                if 'company_name' in exp_data:
                                    experience_info['profile_experience_company_name'] = exp_data['company_name']
                                if 'position' in exp_data:
                                    experience_info['profile_experience_position'] = exp_data['position']
                                if 'start_date' in exp_data:
                                    experience_info['profile_experience_start_date'] = exp_data['start_date']
                                if 'end_date' in exp_data:
                                    experience_info['profile_experience_end_date'] = exp_data['end_date']
                                
                                # Update the existing experience
                                if experience_info:
                                    db.update('every_circle.profile_experience', 
                                             {'profile_experience_uid': experience_uid}, experience_info)
                                    
                                experience_uids.append(experience_uid)
                            else:
                                # This is a new experience entry
                                experience_stored_procedure_response = db.call(procedure='new_profile_experience_uid')
                                new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                                experience_info['profile_experience_uid'] = new_experience_uid
                                experience_info['profile_experience_profile_personal_id'] = profile_uid
                                
                                # Map fields from the experience data
                                if 'company_name' in exp_data:
                                    experience_info['profile_experience_company_name'] = exp_data['company_name']
                                if 'position' in exp_data:
                                    experience_info['profile_experience_position'] = exp_data['position']
                                if 'start_date' in exp_data:
                                    experience_info['profile_experience_start_date'] = exp_data['start_date']
                                if 'end_date' in exp_data:
                                    experience_info['profile_experience_end_date'] = exp_data['end_date']
                                
                                # Insert the experience record
                                db.insert('every_circle.profile_experience', experience_info)
                                experience_uids.append(new_experience_uid)
                        
                        updated_uids['profile_experience_uids'] = experience_uids
                    except Exception as e:
                        print(f"Error processing experiences JSON in PUT: {str(e)}")
                
                # Handle individual experience update (legacy format)
                elif any(key.startswith('profile_experience_') and key != 'profile_experience_profile_personal_id' for key in payload):
                    experience_info = {k: v for k, v in payload.items() if k.startswith('profile_experience_')}
                    
                    if 'profile_experience_uid' in experience_info:
                        # Update specific experience entry
                        experience_uid = experience_info.pop('profile_experience_uid')
                        
                        # Check if experience exists
                        experience_exists_query = db.select('every_circle.profile_experience', 
                                                          where={'profile_experience_uid': experience_uid})
                        
                        if not experience_exists_query['result']:
                            response['message'] = f'Experience with UID {experience_uid} not found'
                            response['code'] = 404
                            return response, 404
                        
                        # Remove profile_id if present
                        if 'profile_experience_profile_personal_id' in experience_info:
                            experience_info.pop('profile_experience_profile_personal_id')
                        
                        # Update existing experience
                        if experience_info:
                            db.update('every_circle.profile_experience', 
                                     {'profile_experience_uid': experience_uid}, experience_info)
                            updated_uids['profile_experience_uid'] = experience_uid
                    else:
                        # Add new experience
                        experience_stored_procedure_response = db.call(procedure='new_profile_experience_uid')
                        new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                        experience_info['profile_experience_uid'] = new_experience_uid
                        experience_info['profile_experience_profile_personal_id'] = profile_uid
                        db.insert('every_circle.profile_experience', experience_info)
                        updated_uids['profile_experience_uid'] = new_experience_uid
                    
                    # Remove used items
                    for k in list(experience_info.keys()):
                        if k in payload:
                            payload.pop(k)
                
                # Handle multiple education entries
                if 'educations' in payload:
                    try:
                        import json
                        educations_data = json.loads(payload.pop('educations'))
                        education_uids = []
                        
                        # Process each education entry
                        for edu_data in educations_data:
                            education_info = {}
                            
                            # Check if this is an existing education (has UID)
                            if 'uid' in edu_data:
                                # Get the existing education UID
                                education_uid = edu_data.pop('uid')
                                
                                # Check if education exists
                                education_exists_query = db.select('every_circle.profile_education', 
                                                                 where={'profile_education_uid': education_uid})
                                
                                if not education_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Education with UID {education_uid} not found")
                                    continue
                                
                                # Map fields from the education data
                                if 'school_name' in edu_data:
                                    education_info['profile_education_school_name'] = edu_data['school_name']
                                if 'degree' in edu_data:
                                    education_info['profile_education_degree'] = edu_data['degree']
                                if 'course' in edu_data:
                                    education_info['profile_education_course'] = edu_data['course']
                                if 'start_date' in edu_data:
                                    education_info['profile_education_start_date'] = edu_data['start_date']
                                if 'end_date' in edu_data:
                                    education_info['profile_education_end_date'] = edu_data['end_date']
                                
                                # Update the existing education
                                if education_info:
                                    db.update('every_circle.profile_education', 
                                             {'profile_education_uid': education_uid}, education_info)
                                    
                                education_uids.append(education_uid)
                            else:
                                # This is a new education entry
                                education_stored_procedure_response = db.call(procedure='new_profile_education_uid')
                                new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                                education_info['profile_education_uid'] = new_education_uid
                                education_info['profile_education_profile_personal_id'] = profile_uid
                                
                                # Map fields from the education data
                                if 'school_name' in edu_data:
                                    education_info['profile_education_school_name'] = edu_data['school_name']
                                if 'degree' in edu_data:
                                    education_info['profile_education_degree'] = edu_data['degree']
                                if 'course' in edu_data:
                                    education_info['profile_education_course'] = edu_data['course']
                                if 'start_date' in edu_data:
                                    education_info['profile_education_start_date'] = edu_data['start_date']
                                if 'end_date' in edu_data:
                                    education_info['profile_education_end_date'] = edu_data['end_date']
                                
                                # Insert the education record
                                db.insert('every_circle.profile_education', education_info)
                                education_uids.append(new_education_uid)
                        
                        updated_uids['profile_education_uids'] = education_uids
                    except Exception as e:
                        print(f"Error processing educations JSON in PUT: {str(e)}")
                
                # Handle individual education update (legacy format)
                elif any(key.startswith('profile_education_') and key != 'profile_education_profile_personal_id' for key in payload):
                    education_info = {k: v for k, v in payload.items() if k.startswith('profile_education_')}
                    
                    if 'profile_education_uid' in education_info:
                        # Update specific education entry
                        education_uid = education_info.pop('profile_education_uid')
                        
                        # Check if education exists
                        education_exists_query = db.select('every_circle.profile_education', 
                                                         where={'profile_education_uid': education_uid})
                        
                        if not education_exists_query['result']:
                            response['message'] = f'Education with UID {education_uid} not found'
                            response['code'] = 404
                            return response, 404
                        
                        # Remove profile_id if present
                        if 'profile_education_profile_personal_id' in education_info:
                            education_info.pop('profile_education_profile_personal_id')
                        
                        # Update existing education
                        if education_info:
                            db.update('every_circle.profile_education', 
                                     {'profile_education_uid': education_uid}, education_info)
                            updated_uids['profile_education_uid'] = education_uid
                    else:
                        # Add new education
                        education_stored_procedure_response = db.call(procedure='new_profile_education_uid')
                        new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                        education_info['profile_education_uid'] = new_education_uid
                        education_info['profile_education_profile_personal_id'] = profile_uid
                        db.insert('every_circle.profile_education', education_info)
                        updated_uids['profile_education_uid'] = new_education_uid
                    
                    # Remove used items
                    for k in list(education_info.keys()):
                        if k in payload:
                            payload.pop(k)
            
            # Prepare the response with both updated and deleted UIDs
            response['updated_uids'] = updated_uids
            if deleted_uids:
                response['deleted_uids'] = deleted_uids
            response['message'] = 'Profile updated successfully'
            return response, 200
        
        except Exception as e:
            print(f"Error in UserProfileInfo PUT: {str(e)}")
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
                                          
    def delete(self, uid):
        print("In UserProfileInfo DELETE")
        response = {}
        
        try:
            with connect() as db:
                # Handle different types of UIDs based on prefix
                prefix = uid[:3]
                
                # Case 1: User UID (100) or Profile UID (110) - Delete all profile data
                if prefix in ["100", "110"]:
                    if prefix == "100":
                        # This is a user UID, need to find their profile first
                        profile_exists_query = db.select('every_circle.profile_personal', where={'profile_personal_user_id': uid})
                        
                        if not profile_exists_query['result']:
                            response['message'] = 'Profile not found for this user'
                            response['code'] = 404
                            return response, 404
                        
                        profile_uid = profile_exists_query['result'][0]['profile_personal_uid']
                    else:
                        # This is already a profile UID
                        profile_exists_query = db.select('every_circle.profile_personal', where={'profile_personal_uid': uid})
                        
                        if not profile_exists_query['result']:
                            response['message'] = f'No profile found for {uid}'
                            response['code'] = 404
                            return response, 404
                        
                        profile_uid = uid
                    
                    # Delete all profile-related records
                    delete_results = {}
                    
                    # Delete social media links
                    links_query = f"DELETE FROM every_circle.profile_link WHERE profile_link_profile_personal_id = '{profile_uid}'"
                    delete_results['links'] = db.delete(links_query)
                    
                    # Delete expertise
                    expertise_query = f"DELETE FROM every_circle.profile_expertise WHERE profile_expertise_profile_personal_id = '{profile_uid}'"
                    delete_results['expertise'] = db.delete(expertise_query)
                    
                    # Delete wishes
                    wishes_query = f"DELETE FROM every_circle.profile_wish WHERE profile_wish_profile_personal_id = '{profile_uid}'"
                    delete_results['wishes'] = db.delete(wishes_query)
                    
                    # Delete experiences
                    experiences_query = f"DELETE FROM every_circle.profile_experience WHERE profile_experience_profile_personal_id = '{profile_uid}'"
                    delete_results['experiences'] = db.delete(experiences_query)
                    
                    # Delete education
                    education_query = f"DELETE FROM every_circle.profile_education WHERE profile_education_profile_personal_id = '{profile_uid}'"
                    delete_results['education'] = db.delete(education_query)
                    
                    # Finally delete the personal info (main profile)
                    personal_info_query = f"DELETE FROM every_circle.profile_personal WHERE profile_personal_uid = '{profile_uid}'"
                    delete_results['personal_info'] = db.delete(personal_info_query)
                    
                    response['results'] = delete_results
                    response['message'] = 'Profile information deleted successfully'
                
                # Case 2: Experience UID (120) - Delete a specific experience entry
                elif prefix == "120":
                    # First verify the experience exists
                    experience_exists_query = db.select('every_circle.profile_experience', where={'profile_experience_uid': uid})
                    
                    if not experience_exists_query['result']:
                        response['message'] = f'No experience found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific experience
                    experience_query = f"DELETE FROM every_circle.profile_experience WHERE profile_experience_uid = '{uid}'"
                    delete_result = db.delete(experience_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Experience with UID {uid} deleted successfully'
                
                # Case 3: Education UID (130) - Delete a specific education entry
                elif prefix == "130":
                    # First verify the education exists
                    education_exists_query = db.select('every_circle.profile_education', where={'profile_education_uid': uid})
                    
                    if not education_exists_query['result']:
                        response['message'] = f'No education found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific education
                    education_query = f"DELETE FROM every_circle.profile_education WHERE profile_education_uid = '{uid}'"
                    delete_result = db.delete(education_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Education with UID {uid} deleted successfully'
                
                # Case 4: Links UID (140) - Delete a specific links entry
                elif prefix == "140":
                    # First verify the link exists
                    link_exists_query = db.select('every_circle.profile_link', where={'profile_link_uid': uid})
                    
                    if not link_exists_query['result']:
                        response['message'] = f'No link found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific link
                    link_query = f"DELETE FROM every_circle.profile_link WHERE profile_link_uid = '{uid}'"
                    delete_result = db.delete(link_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Link with UID {uid} deleted successfully'
                
                # Case 5: Expertise UID (150) - Delete a specific expertise entry
                elif prefix == "150":
                    # First verify the expertise exists
                    expertise_exists_query = db.select('every_circle.profile_expertise', where={'profile_expertise_uid': uid})
                    
                    if not expertise_exists_query['result']:
                        response['message'] = f'No expertise found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific expertise
                    expertise_query = f"DELETE FROM every_circle.profile_expertise WHERE profile_expertise_uid = '{uid}'"
                    delete_result = db.delete(expertise_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Expertise with UID {uid} deleted successfully'
                
                # Case 6: Wishes UID (160) - Delete a specific wishes entry
                elif prefix == "160":
                    # First verify the wish exists
                    wish_exists_query = db.select('every_circle.profile_wish', where={'profile_wish_uid': uid})
                    
                    if not wish_exists_query['result']:
                        response['message'] = f'No wish found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific wish
                    wish_query = f"DELETE FROM every_circle.profile_wish WHERE profile_wish_uid = '{uid}'"
                    delete_result = db.delete(wish_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Wish with UID {uid} deleted successfully'
                
                else:
                    response['message'] = 'Invalid UID prefix'
                    response['code'] = 400
                    return response, 400
                
                return response, 200
        
        except Exception as e:
            print(f"Error in UserProfileInfo DELETE: {str(e)}")
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
        