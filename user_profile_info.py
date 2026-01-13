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
                    print("User UID Passed - Consider Changing")
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
                    
                    # Get user info
                    user_id = uid
                    
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
                    
                    
                    
                    
                    
                    # Get experience info - returning all experiences for this profile
                    experience_info = db.select('every_circle.profile_experience', 
                                             where={'profile_experience_profile_personal_id': profile_id})
                    response['experience_info'] = experience_info['result'] if experience_info['result'] else []
                    
                    # Get expertise info - returning all expertise entries for this profile
                    expertise_info = db.select('every_circle.profile_expertise', 
                                            where={'profile_expertise_profile_personal_id': profile_id})
                    response['expertise_info'] = expertise_info['result'] if expertise_info['result'] else []

                    # Get education info - returning all education entries for this profile
                    education_info = db.select('every_circle.profile_education', 
                                            where={'profile_education_profile_personal_id': profile_id})
                    response['education_info'] = education_info['result'] if education_info['result'] else []

                    # Get wishes info - returning all wishes entries for this profile with response counts
                    wishes_query = """
                        SELECT profile_wish.*, COUNT(wr_profile_wish_id) AS wish_responses
                        FROM every_circle.profile_wish
                        LEFT JOIN every_circle.wish_response ON wr_profile_wish_id = profile_wish_uid
                        WHERE profile_wish_profile_personal_id = %s
                        GROUP BY profile_wish_uid
                    """
                    # print("wishes_query", wishes_query)
                    wishes_info = db.execute(wishes_query, (profile_id,))
                    response['wishes_info'] = wishes_info['result'] if wishes_info.get('result') else []

                    # Get ratings info - returning all ratings entries for this profile
                    ratings_info = db.select('every_circle.ratings', 
                                         where={'rating_profile_id': profile_id})
                    response['ratings_info'] = ratings_info['result'] if ratings_info['result'] else []

                    # Get business info - returning all business entries for this profile
                    # business_info = db.select('every_circle.profile_has_business',
                    #                          where={'profile_business_profile_personal_id': profile_id})
                    business_info = f"""
                                        SELECT 
                                            b.business_uid, 
                                            b.business_name,
                                            bu.bu_uid,
                                            bu.bu_role,
                                            bu.bu_individual_business_is_public
                                        FROM every_circle.business b
                                        LEFT JOIN every_circle.business_user bu ON b.business_uid = bu.bu_business_id
                                        WHERE bu.bu_user_id = '{user_id}';
                                    """
                    # print("business_info", business_info)
                    business_result = db.execute(business_info)
                    response['business_info'] = business_result['result'] if business_result['result'] else []
                    
                    # Add user_role from users table
                    response['user_role'] = user_data['user_role']
                    response['user_email'] = user_data['user_email_id']
                    
                    return response, 200
                    
                elif uid[:3] == "110":
                    print("Profile UID Passed", uid)
                    # This is a profile UID (profile_personal)
                    personal_info = db.select('every_circle.profile_personal', where={'profile_personal_uid': uid})
                    
                    if not personal_info['result']:
                        response['message'] = f'No profile found for {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    print("1")
                    profile_id = uid
                    
                    # Get all associated profile data
                    response['personal_info'] = personal_info['result'][0]
                    
                    # Get user info
                    user_id = personal_info['result'][0]['profile_personal_user_id']
                    user_info = db.select('every_circle.users', where={'user_uid': user_id})
                    response['user_role'] = user_info['result'][0]['user_role'] if user_info['result'] else "unknown"
                    response['user_email'] = user_info['result'][0]['user_email_id']
                    print("2")
                    # Get social media links
                    social_links_query = f"""
                        SELECT pl.profile_link_uid, sl.social_link_name, pl.profile_link_url
                        FROM every_circle.profile_link pl
                        JOIN every_circle.social_link sl ON pl.profile_link_social_link_id = sl.social_link_uid
                        WHERE pl.profile_link_profile_personal_id = '{profile_id}'
                    """
                    social_links_response = db.execute(social_links_query)
                    response['links_info'] = social_links_response['result'] if social_links_response['result'] else []
                    print("3")
                    
                     # Get experience info - returning all experiences for this profile
                    experience_info = db.select('every_circle.profile_experience', 
                                             where={'profile_experience_profile_personal_id': profile_id})
                    response['experience_info'] = experience_info['result'] if experience_info['result'] else []
                    
                    # Get expertise info - returning all expertise entries for this profile
                    expertise_info = db.select('every_circle.profile_expertise', 
                                            where={'profile_expertise_profile_personal_id': profile_id})
                    response['expertise_info'] = expertise_info['result'] if expertise_info['result'] else []

                    # Get education info - returning all education entries for this profile
                    education_info = db.select('every_circle.profile_education', 
                                            where={'profile_education_profile_personal_id': profile_id})
                    response['education_info'] = education_info['result'] if education_info['result'] else []

                    # Get wishes info - returning all wishes entries for this profile with response counts
                    wishes_query = """
                        SELECT profile_wish.*, COUNT(wr_profile_wish_id) AS wish_responses
                        FROM every_circle.profile_wish
                        LEFT JOIN every_circle.wish_response ON wr_profile_wish_id = profile_wish_uid
                        WHERE profile_wish_profile_personal_id = %s
                        GROUP BY profile_wish_uid
                    """
                    wishes_info = db.execute(wishes_query, (profile_id,))
                    response['wishes_info'] = wishes_info['result'] if wishes_info.get('result') else []

                    # Get ratings info - returning all ratings entries for this profile
                    ratings_info = db.select('every_circle.ratings', 
                                         where={'rating_profile_id': profile_id})
                    response['ratings_info'] = ratings_info['result'] if ratings_info['result'] else []

                    # Get business info - returning all business entries for this profile
                    # business_info = db.select('every_circle.profile_has_business',
                    #                          where={'profile_business_profile_personal_id': profile_id})
                    business_info = f"""
                                    SELECT 
                                        b.business_uid,
                                        b.business_name,
                                        bu.bu_uid,
                                        bu.bu_role,
                                        bu.bu_individual_business_is_public
                                    FROM every_circle.business b
                                    LEFT JOIN every_circle.business_user bu ON b.business_uid = bu.bu_business_id
                                    WHERE bu.bu_user_id = '{user_id}';
                                    """
                    print("business_info", business_info)
                    business_result = db.execute(business_info)
                    response['business_info'] = business_result['result'] if business_result['result'] else []

                    print("4")
                    
                    return response, 200
                
                elif "@" in uid:
                    print("Email UID Passed")
                    # This is an email UID
                    user_info = db.select('every_circle.users', where={'user_email_id': uid})
                    response['user_uid'] = user_info['result'][0]['user_uid'] if user_info['result'] else "unknown"
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
            print("payload", payload)

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
                print("processing referred by",new_profile_uid, user_uid)
                if 'profile_personal_referred_by' not in payload:
                    personal_info['profile_personal_referred_by'] = "110-000001"
                elif payload.get('profile_personal_referred_by', '').strip() in ['', 'null']:
                    personal_info['profile_personal_referred_by'] = "110-000001"
                else:
                    referred_by_value = payload.pop('profile_personal_referred_by')
                    
                    if referred_by_value [0:3] == "110":
                        print("referred by is a profile uid")
                        personal_info['profile_personal_referred_by'] = referred_by_value
                    elif referred_by_value [0:3] == "100":
                        print("referred by is a user uid", 'referred_by_value:',referred_by_value)
                        uid_query = f"""
                            SELECT profile_personal_uid
                            FROM every_circle.profile_personal
                            WHERE profile_personal_user_id = "{referred_by_value}"
                        """
                        uid_result = db.execute(uid_query)
                        print("uid_result", uid_result)
                        personal_info['profile_personal_referred_by'] = uid_result['result'][0]['profile_personal_uid']

                    # Check if the value is an email address (contains @ symbol)
                    elif '@' in referred_by_value:
                        print("processing referred by email")
                        # Query to find profile_personal_uid by email
                        email_query = f"""
                            SELECT profile_personal_uid
                            FROM every_circle.users
                            LEFT JOIN every_circle.profile_personal ON user_uid = profile_personal_user_id
                            WHERE user_email_id = "{referred_by_value}"
                        """
                        email_result = db.execute(email_query)
                        print("email_result", email_result)
                        
                        if email_result['result'] and len(email_result['result']) > 0:
                            personal_info['profile_personal_referred_by'] = email_result['result'][0]['profile_personal_uid']
                        else:
                            personal_info['profile_personal_referred_by'] = "110-000001"
                    else:
                        # Use the original value if it's not an email
                        personal_info['profile_personal_referred_by'] = '110-000001'
                        

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
                    'profile_personal_expertise_is_public', 'profile_personal_wishes_is_public', 'profile_personal_business_is_public'
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


                # Determine Path to Main Node
                personal_path_query = f'''
                                            WITH RECURSIVE ReferralPath AS (
                                SELECT 
                                    profile_personal_uid AS user_id,
                                    profile_personal_referred_by,
                                    CAST(CONCAT("'", profile_personal_uid, "'") AS CHAR(255)) AS path
                                FROM profile_personal
                                WHERE profile_personal_uid = '110-000001'

                                UNION ALL

                                SELECT 
                                    p.profile_personal_uid,
                                    p.profile_personal_referred_by,
                                    CONCAT(r.path, ',', "'", p.profile_personal_uid, "'")
                                FROM profile_personal p
                                JOIN ReferralPath r ON p.profile_personal_referred_by = r.user_id
                                WHERE LOCATE(p.profile_personal_uid, r.path) = 0 
                            )

                            SELECT path
                            FROM ReferralPath
                            WHERE user_id = '{new_profile_uid}';
                        '''
                print(personal_path_query)
                response = db.execute(personal_path_query)
                print(response)

                if not response['result']:
                    response['message'] = 'No connection found'
                    response['code'] = 404
                    return response, 404

                personal_path = response['result'][0]['path']
                print('personal_path_query: ', personal_path)

                # print('personal_path: ', personal_path['path'])

                db.update('every_circle.profile_personal', {'profile_personal_uid': new_profile_uid}, {'profile_personal_path': personal_path})
                
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
                        print("expertise data: ", expertises_data)
                        
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
                        print("wishes data: ", wishes_data)
                        
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
                        print("experience data: ", experiences_data)
                        
                        # Process each experience entry
                        for exp_data in experiences_data:
                            experience_info = {}
                            experience_stored_procedure_response = db.call(procedure='new_profile_experience_uid')
                            new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                            experience_info['profile_experience_uid'] = new_experience_uid
                            experience_info['profile_experience_profile_personal_id'] = new_profile_uid
                            
                            # Map fields from the experience data
                            if 'company' in exp_data:
                                experience_info['profile_experience_company_name'] = exp_data['company']
                            if 'title' in exp_data:
                                experience_info['profile_experience_position'] = exp_data['title']
                            if 'description' in exp_data:
                                experience_info['profile_experience_description'] = exp_data['description']
                            if 'startDate' in exp_data:
                                experience_info['profile_experience_start_date'] = exp_data['startDate']
                            if 'endDate' in exp_data:
                                experience_info['profile_experience_end_date'] = exp_data['endDate']
                            
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
                        print("education data: ", educations_data)
                        
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
                            if 'startDate' in edu_data:
                                education_info['profile_education_start_date'] = edu_data['startDate']
                            if 'endDate' in edu_data:
                                education_info['profile_education_end_date'] = edu_data['endDate']
                            
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
                    'profile_personal_city', 'profile_personal_state', 'profile_personal_country','profile_personal_location_is_public', 
                    'profile_personal_latitude', 'profile_personal_longitude', 
                    'profile_personal_image', 'profile_personal_image_is_public', 
                    'profile_personal_tag_line', 'profile_personal_tag_line_is_public', 
                    'profile_personal_short_bio', 'profile_personal_short_bio_is_public', 
                    'profile_personal_resume', 'profile_personal_resume_is_public', 
                    'profile_personal_notification_preference', 'profile_personal_location_preference', 'profile_personal_allow_banner_ads', 'profile_personal_banner_ads_bounty',
                    'profile_personal_experience_is_public', 
                    'profile_personal_education_is_public',
                    'profile_personal_expertise_is_public', 
                    'profile_personal_wishes_is_public',
                    'profile_personal_business_is_public'
                ]
                
                for field in personal_info_fields:
                    if field in payload:
                        personal_info[field] = payload.pop(field)
                
                print("Remaining payload fields: ", payload)
                
                # if 'profile_image' in request.files or 'delete_profile_image' in payload:
                #     payload_images = {}
                #     if 'profile_image' in request.files:
                #         payload_images['profile_image'] = request.files['profile_image']
                #     if 'delete_profile_image' in payload:
                #         payload_images['delete_profile_image'] = payload['delete_profile_image']
                #     # key = {'profile_personal_uid': profile_uid}
                #     personal_info['profile_personal_image'] = processImage(key, payload_images)

                if 'profile_image' in request.files or 'delete_profile_image' in payload:
                    print("In Profile Image")
                    payload_images = {}
                    if 'profile_image' in request.files:
                        payload_images['profile_image'] = request.files['profile_image']
                    if 'delete_profile_image' in payload:
                        payload_images['delete_profile_image'] = payload['delete_profile_image']
                    # key = {'profile_personal_uid': profile_uid}
                    personal_info['profile_personal_image'] = processImage(key, payload_images)



                if ('profile_resume_details' in payload and 'file_0' in request.files) or 'delete_documents' in payload:
                    print("In Profile Document")
                    # if new resume is added check if there is an existing resume.  Delete existing resume and add new resume
                    # --------------- PROCESS DOCUMENTS ------------------
        
                    processDocument(key, payload)
                    print("Payload after processDocument function: ", payload, type(payload))

                    # # Convert JSON string to a Python list
                    # resumes = json.loads(payload['profile_personal_resume'])

                    # # Access the first link
                    # first_link = resumes[0]['link']

                    # print(first_link)

                    # personal_info['profile_personal_resume'] = first_link

                    personal_info['profile_personal_resume'] = payload['profile_personal_resume']
                    # print(personal_info['profile_personal_resume'], type(personal_info['profile_personal_resume']))



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
                    print("Update complete ", updated_uids['profile_personal_uid'])
                
                # Update social media links if provided
                # First, get all social media platforms from the social_link table
                social_links_query = db.select('every_circle.social_link')
                social_links = {}
                
                if social_links_query['result']:
                    for link in social_links_query['result']:
                        social_links[link['social_link_name'].lower()] = link['social_link_uid']
                
                # Check for social media links in the payload
                if 'social_links' in payload:
                    print("In social_links")
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
                
                # Handle multiple education entries
                if 'education_info' in payload:
                    print("In educations")
                    try:
                        import json
                        educations_data = json.loads(payload.pop('education_info'))
                        education_uids = []
                        
                        # Process each education entry
                        for edu_data in educations_data:
                            print("edu_data", edu_data)
                            education_info = {}
                            
                            # Check if this is an existing education (has UID)
                            if 'profile_education_uid' in edu_data:
                                print("In existing education entry", edu_data['profile_education_uid'])
                                # Get the existing education UID
                                education_uid = edu_data.pop('profile_education_uid')
                                
                                # Check if education exists
                                education_exists_query = db.select('every_circle.profile_education', 
                                                                 where={'profile_education_uid': education_uid})
                                
                                if not education_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Education with UID {education_uid} not found")
                                    continue
                                
                                # Map fields from the education data
                                if 'school' in edu_data:
                                    education_info['profile_education_school_name'] = edu_data['school']
                                if 'degree' in edu_data:
                                    education_info['profile_education_degree'] = edu_data['degree']
                                if 'course' in edu_data:
                                    education_info['profile_education_course'] = edu_data['course']
                                if 'startDate' in edu_data:
                                    education_info['profile_education_start_date'] = edu_data['startDate']
                                if 'endDate' in edu_data:
                                    education_info['profile_education_end_date'] = edu_data['endDate']
                                if 'isPublic' in edu_data:
                                    education_info['profile_education_is_public'] = edu_data['isPublic']
                                
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
                                if 'school' in edu_data:
                                    education_info['profile_education_school_name'] = edu_data['school']
                                if 'degree' in edu_data:
                                    education_info['profile_education_degree'] = edu_data['degree']
                                if 'course' in edu_data:
                                    education_info['profile_education_course'] = edu_data['course']
                                if 'startDate' in edu_data:
                                    education_info['profile_education_start_date'] = edu_data['startDate']
                                if 'endDate' in edu_data:
                                    education_info['profile_education_end_date'] = edu_data['endDate']
                                if 'isPublic' in edu_data:
                                    education_info['profile_education_is_public'] = edu_data['isPublic']
                                
                                # Insert the education record
                                db.insert('every_circle.profile_education', education_info)
                                education_uids.append(new_education_uid)
                        
                        updated_uids['profile_education_uids'] = education_uids
                    except Exception as e:
                        print(f"Error processing educations JSON in PUT: {str(e)}")              
                
                # Handle multiple experiences
                if 'experience_info' in payload:
                    print("In experiences")
                    try:
                        import json
                        experiences_data = json.loads(payload.pop('experience_info'))
                        experience_uids = []
                        
                        # Process each experience entry
                        for exp_data in experiences_data:
                            print("exp_data", exp_data)
                            experience_info = {}
                            
                            # Check if this is an existing experience (has UID)
                            if 'profile_experience_uid' in exp_data:
                                print("In existing experience entry", exp_data['profile_experience_uid'])
                                # Get the existing experience UID
                                print("In existing experience entry")
                                experience_uid = exp_data.pop('profile_experience_uid')
                                
                                # Check if experience exists
                                experience_exists_query = db.select('every_circle.profile_experience', 
                                                                  where={'profile_experience_uid': experience_uid})
                                print("experience_exists_query", experience_exists_query)
                                
                                if not experience_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Experience with UID {experience_uid} not found")
                                    continue
                                
                                # Map fields from the experience data
                                if 'company' in exp_data:
                                    experience_info['profile_experience_company_name'] = exp_data['company']
                                if 'title' in exp_data:
                                    experience_info['profile_experience_position'] = exp_data['title']
                                if 'description' in exp_data:
                                    experience_info['profile_experience_description'] = exp_data['description']
                                if 'startDate' in exp_data:
                                    experience_info['profile_experience_start_date'] = exp_data['startDate']
                                if 'endDate' in exp_data:
                                    experience_info['profile_experience_end_date'] = exp_data['endDate']
                                if 'isPublic' in exp_data:
                                    experience_info['profile_experience_is_public'] = exp_data['isPublic']
                                
                                
                                # Update the existing experience
                                if experience_info:
                                    db.update('every_circle.profile_experience', 
                                             {'profile_experience_uid': experience_uid}, experience_info)
                                    
                                experience_uids.append(experience_uid)
                            else:
                                # This is a new experience entr
                                print("In new experience entry")
                                experience_stored_procedure_response = db.call(procedure='new_profile_experience_uid')
                                new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                                experience_info['profile_experience_uid'] = new_experience_uid
                                experience_info['profile_experience_profile_personal_id'] = profile_uid
                                
                                # Map fields from the experience data
                                if 'company' in exp_data:
                                    experience_info['profile_experience_company_name'] = exp_data['company']
                                if 'title' in exp_data:
                                    experience_info['profile_experience_position'] = exp_data['title']
                                if 'description' in exp_data:
                                    experience_info['profile_experience_description'] = exp_data['description']
                                if 'startDate' in exp_data:
                                    experience_info['profile_experience_start_date'] = exp_data['startDate']
                                if 'endDate' in exp_data:
                                    experience_info['profile_experience_end_date'] = exp_data['endDate']
                                if 'isPublic' in exp_data:
                                    experience_info['profile_experience_is_public'] = exp_data['isPublic']
                                
                                # Insert the experience record
                                print("Inserting experience record", experience_info)
                                db.insert('every_circle.profile_experience', experience_info)
                                experience_uids.append(new_experience_uid)
                        
                        updated_uids['profile_experience_uids'] = experience_uids
                    except Exception as e:
                        print(f"Error processing experiences JSON in PUT: {str(e)}")
                
                # Handle multiple expertise entries
                if 'expertise_info' in payload:
                    print("In expertises")
                    try:
                        import json
                        expertises_data = json.loads(payload.pop('expertise_info'))
                        expertise_uids = []
                        
                        # Process each expertise entry
                        for exp_data in expertises_data:
                            print("exp_data", exp_data)
                            expertise_info = {}
                            
                            # Check if this is an existing expertise (has UID)
                            if 'profile_expertise_uid' in exp_data:
                                print("In existing expertise entry", exp_data['profile_expertise_uid'])
                                # Get the existing expertise UID
                                expertise_uid = exp_data.pop('profile_expertise_uid')
                                
                                # Check if expertise exists
                                expertise_exists_query = db.select('every_circle.profile_expertise', 
                                                                 where={'profile_expertise_uid': expertise_uid})
                                
                                if not expertise_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Expertise with UID {expertise_uid} not found")
                                    continue
                                
                                # Map fields from the expertise data
                                if 'name' in exp_data:
                                    expertise_info['profile_expertise_title'] = exp_data['name']
                                if 'description' in exp_data:
                                    expertise_info['profile_expertise_description'] = exp_data['description']
                                if 'cost' in exp_data:
                                    expertise_info['profile_expertise_cost'] = exp_data['cost']
                                if 'bounty' in exp_data:
                                    expertise_info['profile_expertise_bounty'] = exp_data['bounty']
                                if 'isPublic' in exp_data:
                                    expertise_info['profile_expertise_is_public'] = exp_data['isPublic']
                                
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
                                if 'name' in exp_data:
                                    expertise_info['profile_expertise_title'] = exp_data['name']
                                if 'description' in exp_data:
                                    expertise_info['profile_expertise_description'] = exp_data['description']
                                if 'cost' in exp_data:
                                    expertise_info['profile_expertise_cost'] = exp_data['cost']
                                if 'bounty' in exp_data:
                                    expertise_info['profile_expertise_bounty'] = exp_data['bounty']
                                if 'isPublic' in exp_data:
                                    expertise_info['profile_expertise_is_public'] = exp_data['isPublic']
                                
                                # Insert the expertise record
                                db.insert('every_circle.profile_expertise', expertise_info)
                                expertise_uids.append(new_expertise_uid)
                        
                        updated_uids['profile_expertise_uids'] = expertise_uids
                    except Exception as e:
                        print(f"Error processing expertises JSON in PUT: {str(e)}")

                # Handle multiple wishes entries
                if 'wishes_info' in payload:
                    print("In wishes")
                    try:
                        import json
                        wishes_data = json.loads(payload.pop('wishes_info'))
                        wishes_uids = []
                        
                        # Process each wish entry
                        for wish_data in wishes_data:
                            print("wish_data", wish_data)
                            wish_info = {}
                            
                            # Check if this is an existing wish (has UID)
                            if 'profile_wish_uid' in wish_data:
                                print("In existing wish entry", wish_data['profile_wish_uid'])
                                # Get the existing wish UID
                                wish_uid = wish_data.pop('profile_wish_uid')
                                
                                # Check if wish exists
                                wish_exists_query = db.select('every_circle.profile_wish', 
                                                            where={'profile_wish_uid': wish_uid})
                                
                                if not wish_exists_query['result']:
                                    # Skip this one if it doesn't exist
                                    print(f"Warning: Wish with UID {wish_uid} not found")
                                    continue
                                
                                # Map fields from the wish data
                                if 'helpNeeds' in wish_data:
                                    wish_info['profile_wish_title'] = wish_data['helpNeeds']
                                if 'details' in wish_data:
                                    wish_info['profile_wish_description'] = wish_data['details']
                                if 'amount' in wish_data:
                                    wish_info['profile_wish_bounty'] = wish_data['amount']
                                if 'isPublic' in exp_data:
                                    wish_info['profile_wish_is_public'] = wish_data['isPublic']
                                
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
                                if 'helpNeeds' in wish_data:
                                    wish_info['profile_wish_title'] = wish_data['helpNeeds']
                                if 'details' in wish_data:
                                    wish_info['profile_wish_description'] = wish_data['details']
                                if 'amount' in wish_data:
                                    wish_info['profile_wish_bounty'] = wish_data['amount']
                                if 'isPublic' in exp_data:
                                    wish_info['profile_wish_is_public'] = wish_data['isPublic']
                                
                                # Insert the wish record
                                db.insert('every_circle.profile_wish', wish_info)
                                wishes_uids.append(new_wish_uid)
                        
                        updated_uids['profile_wish_uids'] = wishes_uids
                    except Exception as e:
                        print(f"Error processing wishes JSON in PUT: {str(e)}")
                
                # Handle multiple business entries
                if 'business_info' in payload:
                    print("In businesses")
                    try:
                        import json
                        businesses_data = json.loads(payload.pop('business_info'))
                        print(f"Parsed businesses_data: {businesses_data}")
                        businesses_uids = []
                        
                        # Get user_id from profile
                        profile_query = db.select('every_circle.profile_personal', 
                                                where={'profile_personal_uid': profile_uid})
                        user_id = profile_query['result'][0]['profile_personal_user_id'] if profile_query['result'] else None
                        print(f"user_id from profile: {user_id}")
                        
                        if not user_id:
                            print("Error: Could not find user_id for profile")
                            raise Exception("User ID not found for profile")
                        
                        # Process each business entry
                        for business_data in businesses_data:
                            print(f"=== Processing business_data: {business_data}")
                            
                            # The profile_business_uid in the payload is actually the business_uid (200-xxx)
                            # We need to look it up in business_user table
                            if 'profile_business_uid' in business_data and business_data['profile_business_uid']:
                                business_uid = business_data['profile_business_uid']  # This is actually business_uid
                                print(f"Found profile_business_uid (actually business_uid): {business_uid}")
                                
                                # Check if business_user entry exists for this user and business
                                print(f"Looking up in business_user: bu_user_id={user_id}, bu_business_id={business_uid}")
                                bu_check = db.select('every_circle.business_user',
                                                    where={'bu_user_id': user_id, 'bu_business_id': business_uid})
                                
                                print(f"bu_check result: {bu_check}")
                                
                                if not bu_check['result']:
                                    print(f"WARNING: No business_user entry found for user {user_id} and business {business_uid}")
                                    print("Skipping this business...")
                                    continue
                                
                                # Get the bu_uid
                                bu_uid = bu_check['result'][0]['bu_uid']
                                print(f"Found bu_uid: {bu_uid}")
                                
                                # Prepare update data for business_user table
                                business_user_info = {}
                                
                                if 'profile_business_role' in business_data:
                                    print(f"Found profile_business_role: {business_data['profile_business_role']}")
                                    business_user_info['bu_role'] = business_data['profile_business_role']
                                elif 'role' in business_data:
                                    print(f"Found role: {business_data['role']}")
                                    business_user_info['bu_role'] = business_data['role']
                                
                                if 'individualIsPublic' in business_data:
                                    print(f"Found individualIsPublic: {business_data['individualIsPublic']} (type: {type(business_data['individualIsPublic'])})")
                                    business_user_info['bu_individual_business_is_public'] = business_data['individualIsPublic']
                                else:
                                    print("WARNING: individualIsPublic NOT found in business_data")
                                
                                # Update the business_user entry
                                if business_user_info:
                                    print(f"Updating business_user {bu_uid} with data: {business_user_info}")
                                    update_result = db.update('every_circle.business_user',
                                                            {'bu_uid': bu_uid},
                                                            business_user_info)
                                    print(f"Update result: {update_result}")
                                else:
                                    print("WARNING: No data to update for business_user")
                                
                                businesses_uids.append(bu_uid)
                                print(f"Added bu_uid {bu_uid} to businesses_uids list")
                            else:
                                print("WARNING: profile_business_uid not found or empty in business_data")
                                # This is a new business entry - create business_user relationship
                                print("Attempting to create new business_user entry")
                                
                                if 'business_uid' not in business_data or not business_data['business_uid']:
                                    print("ERROR: No business_uid provided for new entry, skipping...")
                                    continue
                                
                                actual_business_uid = business_data['business_uid']
                                print(f"Creating new entry for business_uid: {actual_business_uid}")
                                
                                # Create business_user entry
                                new_bu_uid = db.call(procedure='new_business_user_uid')['result'][0]['new_id']
                                bu_info = {
                                    'bu_uid': new_bu_uid,
                                    'bu_user_id': user_id,
                                    'bu_business_id': actual_business_uid,
                                    'bu_individual_business_is_public': business_data.get('individualIsPublic', False)
                                }
                                
                                if 'profile_business_role' in business_data:
                                    bu_info['bu_role'] = business_data['profile_business_role']
                                elif 'role' in business_data:
                                    bu_info['bu_role'] = business_data['role']
                                
                                print(f"Inserting new business_user: {bu_info}")
                                insert_result = db.insert('every_circle.business_user', bu_info)
                                print(f"Insert result: {insert_result}")
                                businesses_uids.append(new_bu_uid)
                        
                        print(f"Final businesses_uids list: {businesses_uids}")
                        updated_uids['business_user_uids'] = businesses_uids
                    except Exception as e:
                        print(f"ERROR processing businesses JSON in PUT: {str(e)}")
                        import traceback
                        traceback.print_exc()

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
        