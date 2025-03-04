from flask import request, abort, jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect, uploadImage, s3, processImage

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
                    personal_info = db.select('every_circle.user_profile_personal_info', where={'uppi_user_id': uid})
                    
                    if not personal_info['result']:
                        response['message'] = 'Profile not found for this user'
                        response['code'] = 404
                        return response, 404
                    
                    profile_id = personal_info['result'][0]['uppi_uid']
                    
                    # Get all associated profile data
                    response['personal_info'] = personal_info['result'][0]
                    
                    # Get links info
                    links_info = db.select('every_circle.user_profile_links_info', where={'upli_profile_id': profile_id})
                    response['links_info'] = links_info['result'] if links_info['result'] else []
                    
                    # Get expertise info - returning all expertise entries for this profile
                    expertise_info = db.select('every_circle.user_profile_expertise_info', where={'upei_profile_id': profile_id})
                    response['expertise_info'] = expertise_info['result'] if expertise_info['result'] else []
                    
                    # Get wishes info - returning all wishes entries for this profile
                    wishes_info = db.select('every_circle.user_profile_wishes_info', where={'upwi_profile_id': profile_id})
                    response['wishes_info'] = wishes_info['result'] if wishes_info['result'] else []
                    
                    # Get experience info - returning all experiences for this profile
                    experience_info = db.select('every_circle.user_profile_experience_info', where={'upexi_profile_id': profile_id})
                    response['experience_info'] = experience_info['result'] if experience_info['result'] else []
                    
                    # Get education info - returning all education entries for this profile
                    education_info = db.select('every_circle.user_profile_education_info', where={'upedi_profile_id': profile_id})
                    response['education_info'] = education_info['result'] if education_info['result'] else []
                    
                    # Add user_role from users table
                    response['user_role'] = user_data['user_role']
                    
                    return response, 200
                    
                elif uid[:3] == "110":
                    # This is a profile UID
                    personal_info = db.select('every_circle.user_profile_personal_info', where={'uppi_uid': uid})
                    
                    if not personal_info['result']:
                        response['message'] = f'No profile found for {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    profile_id = uid
                    
                    # Get all associated profile data
                    response['personal_info'] = personal_info['result'][0]
                    
                    # Get user info
                    user_id = personal_info['result'][0]['uppi_user_id']
                    user_info = db.select('every_circle.users', where={'user_uid': user_id})
                    response['user_role'] = user_info['result'][0]['user_role'] if user_info['result'] else "unknown"
                    
                    # Get links info
                    links_info = db.select('every_circle.user_profile_links_info', where={'upli_profile_id': profile_id})
                    response['links_info'] = links_info['result'] if links_info['result'] else []
                    
                    # Get expertise info - returning all expertise entries
                    expertise_info = db.select('every_circle.user_profile_expertise_info', where={'upei_profile_id': profile_id})
                    response['expertise_info'] = expertise_info['result'] if expertise_info['result'] else []
                    
                    # Get wishes info - returning all wishes entries
                    wishes_info = db.select('every_circle.user_profile_wishes_info', where={'upwi_profile_id': profile_id})
                    response['wishes_info'] = wishes_info['result'] if wishes_info['result'] else []
                    
                    # Get experience info - returning all experiences
                    experience_info = db.select('every_circle.user_profile_experience_info', where={'upexi_profile_id': profile_id})
                    response['experience_info'] = experience_info['result'] if experience_info['result'] else []
                    
                    # Get education info - returning all education entries
                    education_info = db.select('every_circle.user_profile_education_info', where={'upedi_profile_id': profile_id})
                    response['education_info'] = education_info['result'] if education_info['result'] else []
                    
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
                profile_exists_query = db.select('every_circle.user_profile_personal_info', where={'uppi_user_id': user_uid})
                if profile_exists_query['result']:
                    response['message'] = 'Profile already exists for this user'
                    response['code'] = 400
                    return response, 400

                # Generate new profile UID
                profile_stored_procedure_response = db.call(procedure='new_uppi_uid')
                new_profile_uid = profile_stored_procedure_response['result'][0]['new_id']
                
                # Create personal info record
                personal_info = {}
                personal_info['uppi_uid'] = new_profile_uid
                personal_info['uppi_user_id'] = user_uid
                
                # Set default referred by if not provided
                if 'uppi_referred_by' not in payload:
                    personal_info['uppi_referred_by'] = "110-000001"
                elif payload.get('uppi_referred_by', '').strip() in ['', 'null']:
                    personal_info['uppi_referred_by'] = "110-000001"
                else:
                    personal_info['uppi_referred_by'] = payload.pop('uppi_referred_by')
                
                # Extract personal info fields from payload
                personal_info_fields = [
                    'uppi_first_name', 'uppi_last_name', 'uppi_email_is_public', 'uppi_phone_number',
                    'uppi_phone_number_is_public', 'uppi_city', 'uppi_state', 'uppi_country',
                    'uppi_location_is_public', 'uppi_latitude', 'uppi_longitude', 'uppi_profile_image',
                    'uppi_profile_image_is_public', 'uppi_tag_line', 'uppi_tag_line_is_public',
                    'uppi_short_bio', 'uppi_short_bio_is_public', 'uppi_resume', 'uppi_resume_is_public',
                    'uppi_notification_preference', 'uppi_location_preference'
                ]
                
                for field in personal_info_fields:
                    if field in payload:
                        personal_info[field] = payload.pop(field)
                
                # Process profile image if provided
                if 'uppi_profile_image' in personal_info:
                    key = {'uppi_uid': new_profile_uid}
                    processImage(key, personal_info)
                
                # Set last updated timestamp
                personal_info['uppi_last_updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Insert personal info
                db.insert('every_circle.user_profile_personal_info', personal_info)
                
                # Create links info if provided
                links_info = {}
                links_fields = ['upli_facebook_link', 'upli_twitter_link', 'upli_linkedin_link', 'upli_youtube_link']
                has_links = False
                
                for field in links_fields:
                    if field in payload:
                        links_info[field] = payload.pop(field)
                        has_links = True
                
                if has_links:
                    links_stored_procedure_response = db.call(procedure='new_upli_uid')
                    new_links_uid = links_stored_procedure_response['result'][0]['new_id']
                    links_info['upli_uid'] = new_links_uid
                    links_info['upli_profile_id'] = new_profile_uid
                    db.insert('every_circle.user_profile_links_info', links_info)
                
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
                            expertise_stored_procedure_response = db.call(procedure='new_upei_uid')
                            new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                            expertise_info['upei_uid'] = new_expertise_uid
                            expertise_info['upei_profile_id'] = new_profile_uid
                            
                            # Map fields from the expertise data
                            if 'title' in exp_data:
                                expertise_info['upei_title'] = exp_data['title']
                            if 'description' in exp_data:
                                expertise_info['upei_description'] = exp_data['description']
                            if 'cost' in exp_data:
                                expertise_info['upei_cost'] = exp_data['cost']
                            if 'bounty' in exp_data:
                                expertise_info['upei_bounty'] = exp_data['bounty']
                            
                            # Insert the expertise record
                            db.insert('every_circle.user_profile_expertise_info', expertise_info)
                            expertise_entries.append(expertise_info)
                            expertise_uids.append(new_expertise_uid)
                    except Exception as e:
                        print(f"Error processing expertises JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('upei_') for key in payload):
                    expertise_info = {k: v for k, v in payload.items() if k.startswith('upei_')}
                    if expertise_info:
                        expertise_stored_procedure_response = db.call(procedure='new_upei_uid')
                        new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                        expertise_info['upei_uid'] = new_expertise_uid
                        expertise_info['upei_profile_id'] = new_profile_uid
                        db.insert('every_circle.user_profile_expertise_info', expertise_info)
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
                            wishes_stored_procedure_response = db.call(procedure='new_upwi_uid')
                            new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                            wish_info['upwi_uid'] = new_wish_uid
                            wish_info['upwi_profile_id'] = new_profile_uid
                            
                            # Map fields from the wish data
                            if 'title' in wish_data:
                                wish_info['upwi_title'] = wish_data['title']
                            if 'description' in wish_data:
                                wish_info['upwi_description'] = wish_data['description']
                            if 'bounty' in wish_data:
                                wish_info['upwi_bounty'] = wish_data['bounty']
                            
                            # Insert the wish record
                            db.insert('every_circle.user_profile_wishes_info', wish_info)
                            wishes_entries.append(wish_info)
                            wishes_uids.append(new_wish_uid)
                    except Exception as e:
                        print(f"Error processing wishes JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('upwi_') for key in payload):
                    wish_info = {k: v for k, v in payload.items() if k.startswith('upwi_')}
                    if wish_info:
                        wishes_stored_procedure_response = db.call(procedure='new_upwi_uid')
                        new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                        wish_info['upwi_uid'] = new_wish_uid
                        wish_info['upwi_profile_id'] = new_profile_uid
                        db.insert('every_circle.user_profile_wishes_info', wish_info)
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
                            experience_stored_procedure_response = db.call(procedure='new_upexi_uid')
                            new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                            experience_info['upexi_uid'] = new_experience_uid
                            experience_info['upexi_profile_id'] = new_profile_uid
                            
                            # Map fields from the experience data
                            if 'company_name' in exp_data:
                                experience_info['upexi_company_name'] = exp_data['company_name']
                            if 'position' in exp_data:
                                experience_info['upexi_position'] = exp_data['position']
                            if 'start_date' in exp_data:
                                experience_info['upexi_start_date'] = exp_data['start_date']
                            if 'end_date' in exp_data:
                                experience_info['upexi_end_date'] = exp_data['end_date']
                            
                            # Insert the experience record
                            db.insert('every_circle.user_profile_experience_info', experience_info)
                            experience_entries.append(experience_info)
                            experience_uids.append(new_experience_uid)
                    except Exception as e:
                        print(f"Error processing experiences JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('upexi_') for key in payload):
                    experience_info = {k: v for k, v in payload.items() if k.startswith('upexi_')}
                    if experience_info:
                        experience_stored_procedure_response = db.call(procedure='new_upexi_uid')
                        new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                        experience_info['upexi_uid'] = new_experience_uid
                        experience_info['upexi_profile_id'] = new_profile_uid
                        db.insert('every_circle.user_profile_experience_info', experience_info)
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
                            education_stored_procedure_response = db.call(procedure='new_upedi_uid')
                            new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                            education_info['upedi_uid'] = new_education_uid
                            education_info['upedi_profile_id'] = new_profile_uid
                            
                            # Map fields from the education data
                            if 'school_name' in edu_data:
                                education_info['upedi_school_name'] = edu_data['school_name']
                            if 'degree' in edu_data:
                                education_info['upedi_degree'] = edu_data['degree']
                            if 'course' in edu_data:
                                education_info['upedi_course'] = edu_data['course']
                            if 'start_date' in edu_data:
                                education_info['upedi_start_date'] = edu_data['start_date']
                            if 'end_date' in edu_data:
                                education_info['upedi_end_date'] = edu_data['end_date']
                            
                            # Insert the education record
                            db.insert('every_circle.user_profile_education_info', education_info)
                            education_entries.append(education_info)
                            education_uids.append(new_education_uid)
                    except Exception as e:
                        print(f"Error processing educations JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('upedi_') for key in payload):
                    education_info = {k: v for k, v in payload.items() if k.startswith('upedi_')}
                    if education_info:
                        education_stored_procedure_response = db.call(procedure='new_upedi_uid')
                        new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                        education_info['upedi_uid'] = new_education_uid
                        education_info['upedi_profile_id'] = new_profile_uid
                        db.insert('every_circle.user_profile_education_info', education_info)
                        education_entries.append(education_info)
                        education_uids.append(new_education_uid)
                        # Remove used items
                        for k in list(education_info.keys()):
                            if k in payload:
                                payload.pop(k)
            
            # Include all created UIDs in the response
            response['uids'] = {
                'uppi_uid': new_profile_uid,  # Main profile UID
            }
            
            # Add other UIDs if they were created
            if 'new_links_uid' in locals():
                response['uids']['upli_uid'] = new_links_uid
            
            # Add expertise UIDs if created
            if 'expertise_uids' in locals() and expertise_uids:
                response['uids']['upei_uids'] = expertise_uids
            elif 'new_expertise_uid' in locals():
                response['uids']['upei_uid'] = new_expertise_uid
                
            # Add wishes UIDs if created
            if 'wishes_uids' in locals() and wishes_uids:
                response['uids']['upwi_uids'] = wishes_uids
            elif 'new_wish_uid' in locals():
                response['uids']['upwi_uid'] = new_wish_uid
            
            # Add multiple experience UIDs if created
            if 'experience_uids' in locals() and experience_uids:
                response['uids']['upexi_uids'] = experience_uids
            elif 'new_experience_uid' in locals():
                response['uids']['upexi_uid'] = new_experience_uid
            
            # Add multiple education UIDs if created
            if 'education_uids' in locals() and education_uids:
                response['uids']['upedi_uids'] = education_uids
            elif 'new_education_uid' in locals():
                response['uids']['upedi_uid'] = new_education_uid
            
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

        try:
            payload = request.form.to_dict()

            if 'profile_uid' not in payload:
                response['message'] = 'profile_uid is required'
                response['code'] = 400
                return response, 400

            profile_uid = payload.pop('profile_uid')
            updated_uids = {}

            with connect() as db:
                # Check if the profile exists
                profile_exists_query = db.select('every_circle.user_profile_personal_info', where={'uppi_uid': profile_uid})
                if not profile_exists_query['result']:
                    response['message'] = 'Profile does not exist'
                    response['code'] = 404
                    return response, 404
                
                # Update personal info fields
                personal_info = {}
                personal_info_fields = [
                    'uppi_first_name', 'uppi_last_name', 'uppi_email_is_public', 'uppi_phone_number',
                    'uppi_phone_number_is_public', 'uppi_city', 'uppi_state', 'uppi_country',
                    'uppi_location_is_public', 'uppi_latitude', 'uppi_longitude', 'uppi_profile_image',
                    'uppi_profile_image_is_public', 'uppi_tag_line', 'uppi_tag_line_is_public',
                    'uppi_short_bio', 'uppi_short_bio_is_public', 'uppi_resume', 'uppi_resume_is_public',
                    'uppi_notification_preference', 'uppi_location_preference'
                ]
                
                for field in personal_info_fields:
                    if field in payload:
                        personal_info[field] = payload.pop(field)
                
                if personal_info:
                    # Process profile image if provided
                    # if 'uppi_profile_image' in personal_info:
                    #     key = {'uppi_uid': profile_uid}
                    #     processImage(key, personal_info)
                    
                    # Set last updated timestamp
                    personal_info['uppi_last_updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Update personal info
                    db.update('every_circle.user_profile_personal_info', {'uppi_uid': profile_uid}, personal_info)
                
                # Update links info if provided
                links_info = {}
                links_fields = ['upli_facebook_link', 'upli_twitter_link', 'upli_linkedin_link', 'upli_youtube_link']
                has_links = False
                
                for field in links_fields:
                    if field in payload:
                        links_info[field] = payload.pop(field)
                        has_links = True
                
                if has_links:
                    # Check if links info exists
                    links_exists_query = db.select('every_circle.user_profile_links_info', where={'upli_profile_id': profile_uid})
                    
                    if links_exists_query['result']:
                        # Update existing links info
                        db.update('every_circle.user_profile_links_info', 
                                 {'upli_profile_id': profile_uid}, links_info)
                        updated_uids['upli_uid'] = links_exists_query['result'][0]['upli_uid']
                    else:
                        # Create new links info
                        links_stored_procedure_response = db.call(procedure='new_upli_uid')
                        new_links_uid = links_stored_procedure_response['result'][0]['new_id']
                        links_info['upli_uid'] = new_links_uid
                        links_info['upli_profile_id'] = profile_uid
                        db.insert('every_circle.user_profile_links_info', links_info)
                        updated_uids['upli_uid'] = new_links_uid
                
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
                                upei_uid = exp_data.pop('uid')
                                
                                # Check if expertise exists
                                expertise_exists_query = db.select('every_circle.user_profile_expertise_info', 
                                                                 where={'upei_uid': upei_uid})
                                
                                if not expertise_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Expertise with UID {upei_uid} not found")
                                    continue
                                
                                # Map fields from the expertise data
                                if 'title' in exp_data:
                                    expertise_info['upei_title'] = exp_data['title']
                                if 'description' in exp_data:
                                    expertise_info['upei_description'] = exp_data['description']
                                if 'cost' in exp_data:
                                    expertise_info['upei_cost'] = exp_data['cost']
                                if 'bounty' in exp_data:
                                    expertise_info['upei_bounty'] = exp_data['bounty']
                                
                                # Update the existing expertise
                                if expertise_info:
                                    db.update('every_circle.user_profile_expertise_info', 
                                             {'upei_uid': upei_uid}, expertise_info)
                                    
                                expertise_uids.append(upei_uid)
                            else:
                                # This is a new expertise entry
                                expertise_stored_procedure_response = db.call(procedure='new_upei_uid')
                                new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                                expertise_info['upei_uid'] = new_expertise_uid
                                expertise_info['upei_profile_id'] = profile_uid
                                
                                # Map fields from the expertise data
                                if 'title' in exp_data:
                                    expertise_info['upei_title'] = exp_data['title']
                                if 'description' in exp_data:
                                    expertise_info['upei_description'] = exp_data['description']
                                if 'cost' in exp_data:
                                    expertise_info['upei_cost'] = exp_data['cost']
                                if 'bounty' in exp_data:
                                    expertise_info['upei_bounty'] = exp_data['bounty']
                                
                                # Insert the expertise record
                                db.insert('every_circle.user_profile_expertise_info', expertise_info)
                                expertise_uids.append(new_expertise_uid)
                        
                        updated_uids['upei_uids'] = expertise_uids
                    except Exception as e:
                        print(f"Error processing expertises JSON in PUT: {str(e)}")
                
                # Handle individual expertise update (legacy format)
                elif any(key.startswith('upei_') for key in payload):
                    expertise_info = {k: v for k, v in payload.items() if k.startswith('upei_')}
                    
                    if 'upei_uid' in expertise_info:
                        # Update specific expertise entry
                        upei_uid = expertise_info.pop('upei_uid')
                        
                        # Check if expertise exists
                        expertise_exists_query = db.select('every_circle.user_profile_expertise_info', 
                                                         where={'upei_uid': upei_uid})
                        
                        if expertise_exists_query['result']:
                            # Update existing expertise
                            db.update('every_circle.user_profile_expertise_info', 
                                     {'upei_uid': upei_uid}, expertise_info)
                            updated_uids['upei_uid'] = upei_uid
                        else:
                            response['message'] = f'Expertise with UID {upei_uid} not found'
                            response['code'] = 404
                            return response, 404
                    else:
                        # Add new expertise
                        expertise_stored_procedure_response = db.call(procedure='new_upei_uid')
                        new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                        expertise_info['upei_uid'] = new_expertise_uid
                        expertise_info['upei_profile_id'] = profile_uid
                        db.insert('every_circle.user_profile_expertise_info', expertise_info)
                        updated_uids['upei_uid'] = new_expertise_uid
                    
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
                                upwi_uid = wish_data.pop('uid')
                                
                                # Check if wish exists
                                wish_exists_query = db.select('every_circle.user_profile_wishes_info', 
                                                            where={'upwi_uid': upwi_uid})
                                
                                if not wish_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Wish with UID {upwi_uid} not found")
                                    continue
                                
                                # Map fields from the wish data
                                if 'title' in wish_data:
                                    wish_info['upwi_title'] = wish_data['title']
                                if 'description' in wish_data:
                                    wish_info['upwi_description'] = wish_data['description']
                                if 'bounty' in wish_data:
                                    wish_info['upwi_bounty'] = wish_data['bounty']
                                
                                # Update the existing wish
                                if wish_info:
                                    db.update('every_circle.user_profile_wishes_info', 
                                             {'upwi_uid': upwi_uid}, wish_info)
                                    
                                wishes_uids.append(upwi_uid)
                            else:
                                # This is a new wish entry
                                wishes_stored_procedure_response = db.call(procedure='new_upwi_uid')
                                new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                                wish_info['upwi_uid'] = new_wish_uid
                                wish_info['upwi_profile_id'] = profile_uid
                                
                                # Map fields from the wish data
                                if 'title' in wish_data:
                                    wish_info['upwi_title'] = wish_data['title']
                                if 'description' in wish_data:
                                    wish_info['upwi_description'] = wish_data['description']
                                if 'bounty' in wish_data:
                                    wish_info['upwi_bounty'] = wish_data['bounty']
                                
                                # Insert the wish record
                                db.insert('every_circle.user_profile_wishes_info', wish_info)
                                wishes_uids.append(new_wish_uid)
                        
                        updated_uids['upwi_uids'] = wishes_uids
                    except Exception as e:
                        print(f"Error processing wishes JSON in PUT: {str(e)}")
                
                # Handle individual wish update (legacy format)
                elif any(key.startswith('upwi_') for key in payload):
                    wish_info = {k: v for k, v in payload.items() if k.startswith('upwi_')}
                    
                    if 'upwi_uid' in wish_info:
                        # Update specific wish entry
                        upwi_uid = wish_info.pop('upwi_uid')
                        
                        # Check if wish exists
                        wish_exists_query = db.select('every_circle.user_profile_wishes_info', 
                                                     where={'upwi_uid': upwi_uid})
                        
                        if wish_exists_query['result']:
                            # Update existing wish
                            db.update('every_circle.user_profile_wishes_info', 
                                     {'upwi_uid': upwi_uid}, wish_info)
                            updated_uids['upwi_uid'] = upwi_uid
                        else:
                            response['message'] = f'Wish with UID {upwi_uid} not found'
                            response['code'] = 404
                            return response, 404
                    else:
                        # Add new wish
                        wishes_stored_procedure_response = db.call(procedure='new_upwi_uid')
                        new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                        wish_info['upwi_uid'] = new_wish_uid
                        wish_info['upwi_profile_id'] = profile_uid
                        db.insert('every_circle.user_profile_wishes_info', wish_info)
                        updated_uids['upwi_uid'] = new_wish_uid
                    
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
                                upexi_uid = exp_data.pop('uid')
                                
                                # Check if experience exists
                                experience_exists_query = db.select('every_circle.user_profile_experience_info', 
                                                                  where={'upexi_uid': upexi_uid})
                                
                                if not experience_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Experience with UID {upexi_uid} not found")
                                    continue
                                
                                # Map fields from the experience data
                                if 'company_name' in exp_data:
                                    experience_info['upexi_company_name'] = exp_data['company_name']
                                if 'position' in exp_data:
                                    experience_info['upexi_position'] = exp_data['position']
                                if 'start_date' in exp_data:
                                    experience_info['upexi_start_date'] = exp_data['start_date']
                                if 'end_date' in exp_data:
                                    experience_info['upexi_end_date'] = exp_data['end_date']
                                
                                # Update the existing experience
                                if experience_info:
                                    db.update('every_circle.user_profile_experience_info', 
                                             {'upexi_uid': upexi_uid}, experience_info)
                                    
                                experience_uids.append(upexi_uid)
                            else:
                                # This is a new experience entry
                                experience_stored_procedure_response = db.call(procedure='new_upexi_uid')
                                new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                                experience_info['upexi_uid'] = new_experience_uid
                                experience_info['upexi_profile_id'] = profile_uid
                                
                                # Map fields from the experience data
                                if 'company_name' in exp_data:
                                    experience_info['upexi_company_name'] = exp_data['company_name']
                                if 'position' in exp_data:
                                    experience_info['upexi_position'] = exp_data['position']
                                if 'start_date' in exp_data:
                                    experience_info['upexi_start_date'] = exp_data['start_date']
                                if 'end_date' in exp_data:
                                    experience_info['upexi_end_date'] = exp_data['end_date']
                                
                                # Insert the experience record
                                db.insert('every_circle.user_profile_experience_info', experience_info)
                                experience_uids.append(new_experience_uid)
                        
                        updated_uids['upexi_uids'] = experience_uids
                    except Exception as e:
                        print(f"Error processing experiences JSON in PUT: {str(e)}")
                
                # Handle individual experience update (legacy format)
                elif any(key.startswith('upexi_') for key in payload):
                    experience_info = {k: v for k, v in payload.items() if k.startswith('upexi_')}
                    
                    if 'upexi_uid' in experience_info:
                        # Update specific experience entry
                        upexi_uid = experience_info.pop('upexi_uid')
                        
                        # Check if experience exists
                        experience_exists_query = db.select('every_circle.user_profile_experience_info', 
                                                          where={'upexi_uid': upexi_uid})
                        
                        if experience_exists_query['result']:
                            # Update existing experience
                            db.update('every_circle.user_profile_experience_info', 
                                     {'upexi_uid': upexi_uid}, experience_info)
                            updated_uids['upexi_uid'] = upexi_uid
                        else:
                            response['message'] = f'Experience with UID {upexi_uid} not found'
                            response['code'] = 404
                            return response, 404
                    else:
                        # Add new experience
                        experience_stored_procedure_response = db.call(procedure='new_upexi_uid')
                        new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                        experience_info['upexi_uid'] = new_experience_uid
                        experience_info['upexi_profile_id'] = profile_uid
                        db.insert('every_circle.user_profile_experience_info', experience_info)
                        updated_uids['upexi_uid'] = new_experience_uid
                    
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
                                upedi_uid = edu_data.pop('uid')
                                
                                # Check if education exists
                                education_exists_query = db.select('every_circle.user_profile_education_info', 
                                                                 where={'upedi_uid': upedi_uid})
                                
                                if not education_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Education with UID {upedi_uid} not found")
                                    continue
                                
                                # Map fields from the education data
                                if 'school_name' in edu_data:
                                    education_info['upedi_school_name'] = edu_data['school_name']
                                if 'degree' in edu_data:
                                    education_info['upedi_degree'] = edu_data['degree']
                                if 'course' in edu_data:
                                    education_info['upedi_course'] = edu_data['course']
                                if 'start_date' in edu_data:
                                    education_info['upedi_start_date'] = edu_data['start_date']
                                if 'end_date' in edu_data:
                                    education_info['upedi_end_date'] = edu_data['end_date']
                                
                                # Update the existing education
                                if education_info:
                                    db.update('every_circle.user_profile_education_info', 
                                             {'upedi_uid': upedi_uid}, education_info)
                                    
                                education_uids.append(upedi_uid)
                            else:
                                # This is a new education entry
                                education_stored_procedure_response = db.call(procedure='new_upedi_uid')
                                new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                                education_info['upedi_uid'] = new_education_uid
                                education_info['upedi_profile_id'] = profile_uid
                                
                                # Map fields from the education data
                                if 'school_name' in edu_data:
                                    education_info['upedi_school_name'] = edu_data['school_name']
                                if 'degree' in edu_data:
                                    education_info['upedi_degree'] = edu_data['degree']
                                if 'course' in edu_data:
                                    education_info['upedi_course'] = edu_data['course']
                                if 'start_date' in edu_data:
                                    education_info['upedi_start_date'] = edu_data['start_date']
                                if 'end_date' in edu_data:
                                    education_info['upedi_end_date'] = edu_data['end_date']
                                
                                # Insert the education record
                                db.insert('every_circle.user_profile_education_info', education_info)
                                education_uids.append(new_education_uid)
                        
                        updated_uids['upedi_uids'] = education_uids
                    except Exception as e:
                        print(f"Error processing educations JSON in PUT: {str(e)}")
                
                # Handle individual education update (legacy format)
                elif any(key.startswith('upedi_') for key in payload):
                    education_info = {k: v for k, v in payload.items() if k.startswith('upedi_')}
                    
                    if 'upedi_uid' in education_info:
                        # Update specific education entry
                        upedi_uid = education_info.pop('upedi_uid')
                        
                        # Check if education exists
                        education_exists_query = db.select('every_circle.user_profile_education_info', 
                                                         where={'upedi_uid': upedi_uid})
                        
                        if education_exists_query['result']:
                            # Update existing education
                            db.update('every_circle.user_profile_education_info', 
                                     {'upedi_uid': upedi_uid}, education_info)
                            updated_uids['upedi_uid'] = upedi_uid
                        else:
                            response['message'] = f'Education with UID {upedi_uid} not found'
                            response['code'] = 404
                            return response, 404
                    else:
                        # Add new education
                        education_stored_procedure_response = db.call(procedure='new_upedi_uid')
                        new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                        education_info['upedi_uid'] = new_education_uid
                        education_info['upedi_profile_id'] = profile_uid
                        db.insert('every_circle.user_profile_education_info', education_info)
                        updated_uids['upedi_uid'] = new_education_uid
                    
                    # Remove used items
                    for k in list(education_info.keys()):
                        if k in payload:
                            payload.pop(k)
            
            response['updated_uids'] = updated_uids
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
                        profile_exists_query = db.select('every_circle.user_profile_personal_info', where={'uppi_user_id': uid})
                        
                        if not profile_exists_query['result']:
                            response['message'] = 'Profile not found for this user'
                            response['code'] = 404
                            return response, 404
                        
                        profile_uid = profile_exists_query['result'][0]['uppi_uid']
                    else:
                        # This is already a profile UID
                        profile_exists_query = db.select('every_circle.user_profile_personal_info', where={'uppi_uid': uid})
                        
                        if not profile_exists_query['result']:
                            response['message'] = f'No profile found for {uid}'
                            response['code'] = 404
                            return response, 404
                        
                        profile_uid = uid
                    
                    # Delete all profile-related records
                    delete_results = {}
                    
                    # Delete links
                    links_query = f"DELETE FROM every_circle.user_profile_links_info WHERE upli_profile_id = '{profile_uid}'"
                    delete_results['links'] = db.delete(links_query)
                    
                    # Delete expertise
                    expertise_query = f"DELETE FROM every_circle.user_profile_expertise_info WHERE upei_profile_id = '{profile_uid}'"
                    delete_results['expertise'] = db.delete(expertise_query)
                    
                    # Delete wishes
                    wishes_query = f"DELETE FROM every_circle.user_profile_wishes_info WHERE upwi_profile_id = '{profile_uid}'"
                    delete_results['wishes'] = db.delete(wishes_query)
                    
                    # Delete experiences
                    experiences_query = f"DELETE FROM every_circle.user_profile_experience_info WHERE upexi_profile_id = '{profile_uid}'"
                    delete_results['experiences'] = db.delete(experiences_query)
                    
                    # Delete education
                    education_query = f"DELETE FROM every_circle.user_profile_education_info WHERE upedi_profile_id = '{profile_uid}'"
                    delete_results['education'] = db.delete(education_query)
                    
                    # Finally delete the personal info (main profile)
                    personal_info_query = f"DELETE FROM every_circle.user_profile_personal_info WHERE uppi_uid = '{profile_uid}'"
                    delete_results['personal_info'] = db.delete(personal_info_query)
                    
                    response['results'] = delete_results
                    response['message'] = 'Profile information deleted successfully'
                
                # Case 2: Experience UID (120) - Delete a specific experience entry
                elif prefix == "120":
                    # First verify the experience exists
                    experience_exists_query = db.select('every_circle.user_profile_experience_info', where={'upexi_uid': uid})
                    
                    if not experience_exists_query['result']:
                        response['message'] = f'No experience found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific experience
                    experience_query = f"DELETE FROM every_circle.user_profile_experience_info WHERE upexi_uid = '{uid}'"
                    delete_result = db.delete(experience_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Experience with UID {uid} deleted successfully'
                
                # Case 3: Education UID (130) - Delete a specific education entry
                elif prefix == "130":
                    # First verify the education exists
                    education_exists_query = db.select('every_circle.user_profile_education_info', where={'upedi_uid': uid})
                    
                    if not education_exists_query['result']:
                        response['message'] = f'No education found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific education
                    education_query = f"DELETE FROM every_circle.user_profile_education_info WHERE upedi_uid = '{uid}'"
                    delete_result = db.delete(education_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Education with UID {uid} deleted successfully'
                
                # Case 4: Links UID (140) - Delete a specific links entry
                elif prefix == "140":
                    # First verify the links exists
                    links_exists_query = db.select('every_circle.user_profile_links_info', where={'upli_uid': uid})
                    
                    if not links_exists_query['result']:
                        response['message'] = f'No links found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific links
                    links_query = f"DELETE FROM every_circle.user_profile_links_info WHERE upli_uid = '{uid}'"
                    delete_result = db.delete(links_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Links with UID {uid} deleted successfully'
                
                # Case 5: Expertise UID (150) - Delete a specific expertise entry
                elif prefix == "150":
                    # First verify the expertise exists
                    expertise_exists_query = db.select('every_circle.user_profile_expertise_info', where={'upei_uid': uid})
                    
                    if not expertise_exists_query['result']:
                        response['message'] = f'No expertise found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific expertise
                    expertise_query = f"DELETE FROM every_circle.user_profile_expertise_info WHERE upei_uid = '{uid}'"
                    delete_result = db.delete(expertise_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Expertise with UID {uid} deleted successfully'
                
                # Case 6: Wishes UID (160) - Delete a specific wishes entry
                elif prefix == "160":
                    # First verify the wish exists
                    wish_exists_query = db.select('every_circle.user_profile_wishes_info', where={'upwi_uid': uid})
                    
                    if not wish_exists_query['result']:
                        response['message'] = f'No wish found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific wish
                    wish_query = f"DELETE FROM every_circle.user_profile_wishes_info WHERE upwi_uid = '{uid}'"
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