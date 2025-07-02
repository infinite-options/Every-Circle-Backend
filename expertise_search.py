from flask_restful import Resource
from flask import Flask, request, jsonify
import pandas as pd
from dotenv import load_dotenv
from network_connection import NetworkPath
import json
from data_ec import connect

load_dotenv()

class ExpertiseSearch(Resource):
    def get(self, uid, query):
        print("In Expertise Search")

        if not uid or not query:
            return {"message": "Missing input parameter"}, 400
        print('uid:', uid, 'query', query)

        try:
            with connect() as db:

                #1.Finding list of experts
                get_experts_query = f''' select  profile_expertise_profile_personal_id, profile_expertise_title from profile_expertise 
                                        where LOWER(profile_expertise_title) like LOWER('%{query}%');
                    '''
                
                response = db.execute(get_experts_query)
                print(response)

                if not response['result']:
                    response['message'] = f"'No experts found for {query}"
                    response['code'] = 404
                    return response, 404
                
                experts_info = response['result']
                print('experts_list info', experts_info)

                experts_list = [experts['profile_expertise_profile_personal_id'] for experts in experts_info]
                print('experts_ids', experts_list)

                experts_str = ",".join([ f"'{uids}'" for uids in experts_list])
                print('get_experts_str', experts_str)


                ##2.Get path for experts and user id
                # path_query = f''' select  profile_personal_uid, profile_personal_first_name,profile_personal_last_name, profile_personal_path from profile_personal where profile_personal_uid in ({experts_str}, '{uid}');
                #             '''

                path_query = f''' SELECT  
                                    profile_personal_uid, profile_personal_first_name,profile_personal_last_name, profile_personal_email_is_public,
                                    profile_personal_path, profile_personal_phone_number, profile_personal_phone_number_is_public,
                                    profile_personal_location_is_public, profile_personal_city, profile_personal_state, profile_personal_country,
                                    profile_personal_latitude, profile_personal_longitude, user_email_id 
                                    FROM profile_personal p 
                                    LEFT JOIN users u on p.profile_personal_uid = u.user_uid
                                    WHERE profile_personal_uid in ({experts_str}, '{uid}');
                             '''

                print('path_query', path_query)
                response = db.execute(path_query)
                print(response)

                if not response['result']:
                    response['message'] = f"'No path found for {query}"
                    response['code'] = 404
                    return response, 404
                        
                path_list = response['result']
                print('path_list info', path_list)

                # paths_dict = {row['profile_personal_uid']: row['profile_personal_path'] for row in path_list}
                # Update: Include name fields in the dict
                paths_dict = {
                        row['profile_personal_uid']: {
                            'path': row['profile_personal_path'],
                            'first_name': row['profile_personal_first_name'],
                            'last_name': row['profile_personal_last_name'],
                            'email_is_public': row['profile_personal_email_is_public'],
                            'phone': row['profile_personal_phone_number'],
                            'phone_is_public': row['profile_personal_phone_number_is_public'],
                            'location_is_public': row['profile_personal_location_is_public'],
                            'city': row['profile_personal_city'],
                            'state': row['profile_personal_state'],
                            'country': row['profile_personal_country'],
                            'latitude': row['profile_personal_latitude'],
                            'longitude': row['profile_personal_longitude'],
                            'email':row['user_email_id']
                        }
                        for row in path_list
                    }


                print('paths_dict', paths_dict)
                # user_path = paths_dict.get(uid)
                user_path = paths_dict.get(uid, {}).get('path')


                if not user_path:
                    print('User path not stored in table, finding the path till root node')
                    # return {"message": f"Path not found for user {uid}", "code": 404}, 404
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
                            WHERE user_id = '{uid}';
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

                    user_path = personal_path
                    print('user_path:', user_path)

                # Compute connection paths
                def combine_paths(user_path, expert_path, max_degree=3):
                    user_nodes = user_path.split(',')
                    expert_nodes = expert_path.split(',')

                    common_path = [u for u, e in zip(user_nodes, expert_nodes) if u == e]
                    if not common_path:
                        return None

                    lca = common_path[-1]
                    user_to_lca = len(user_nodes) - user_nodes.index(lca) - 1
                    expert_to_lca = len(expert_nodes) - expert_nodes.index(lca) - 1
                    total_degree = user_to_lca + expert_to_lca

                    if total_degree > max_degree:
                        return None

                    after_user = user_nodes[user_nodes.index(lca) + 1:][::-1]
                    after_expert = expert_nodes[expert_nodes.index(lca) + 1:]
                    combined = after_user + [lca] + after_expert

                    return {
                        "expert_uid": expert_uid,
                        "combined_path": combined,
                        "degree": total_degree
                    }

                # connections = []
                # for expert_uid in experts_list:
                #     expert_path = paths_dict.get(expert_uid)
                #     if expert_path:
                #         path_info = combine_paths(user_path, expert_path)
                #         if path_info:
                #             connections.append(path_info)


                connections = []
                for expert in experts_info:
                    print('steps expert', expert)
                    expert_uid = expert['profile_expertise_profile_personal_id']
                    expert_title = expert['profile_expertise_title']
                    expert_data = paths_dict.get(expert_uid, {})
                    expert_path = expert_data.get('path')

                    if expert_path:
                        path_info = combine_paths(user_path, expert_path)
                        print('steps path_info', path_info)

                        print('steps path_info combined_path', path_info.get('combined_path', []))

                        ## Need to loop over the combined path result to get the user contact details
                        
                        if path_info:
                            path_info['expert_uid'] = expert_uid
                            path_info['expertise_title'] = expert_title
                            path_info['first_name'] = expert_data.get('first_name')
                            path_info['last_name'] = expert_data.get('last_name')

                            if expert_data.get('email_is_public') == 1:
                                path_info['email'] = expert_data.get('email')

                            if expert_data.get('phone_is_public') == 1:
                                path_info['phone'] = expert_data.get('phone')

                            if expert_data.get('location_is_public') == 1:
                                path_info['city'] = expert_data.get('city')
                                path_info['state'] = expert_data.get('state')
                                path_info['country'] = expert_data.get('country')
                                path_info['latitude'] = expert_data.get('latitude')
                                path_info['longitude'] = expert_data.get('longitude')
                            connections.append(path_info)

                # for expert in experts_info:
                #     expert_uid = expert['profile_expertise_profile_personal_id']
                #     expert_title = expert['profile_expertise_title']
                #     expert_fname = expert['profile_personal_first_name']
                #     expert_lname = expert['profile_personal_last_name']
                #     expert_path = paths_dict.get(expert_uid)

                #     if expert_path:
                #         path_info = combine_paths(user_path, expert_path)
                #         if path_info:
                #             path_info['expert_uid'] = expert_uid
                #             path_info['expertise_title'] = expert_title
                #             path_info['expert_fname'] = expert_fname
                #             path_info['expert_lname'] = expert_lname
                #             connections.append(path_info)

                print('connections:', connections)

                connections.sort(key=lambda x: x['degree'])
                

                if not connections:
                    return {"message": f"No experts found in network for '{query}' within 3 degrees"}, 404

                #sort by degree
                # connections.sort(key=lambda x: x['degree'])
                # print('connections:', connections)
                return jsonify({"results": connections})
                # result = [
                #         {
                #             "expert_uid": conn["expert_uid"],
                #             "expertise_title": conn["expertise_title"],
                #             "degree": conn["degree"]
                #         }
                #         for conn in connections
                #     ]
                # return jsonify({"connections": result})

        except Exception as e:
            print("Error:", str(e))
            return {"message": f"Internal Server Error: {str(e)}"}, 500
        
        