from flask import request, abort , jsonify, Response
from flask_restful import Resource
from collections import defaultdict
from datetime import datetime
import json
from data_ec import connect

class NetworkPath(Resource):
    def get(self, target_uid, degree):
        print('target_uid', target_uid)
        print('degree', degree)

        seen = set([target_uid])
        down_nodes =[]
        up_nodes = []
        max_nodes  = 200
        #degree = 3
        store = {'descendants': defaultdict(list), 'ancestors': defaultdict(list), 'ancestors_down':defaultdict(list)}


        def fetch_descendants(uids):
            print('Inside fetch_descendants')

            if not uids:
                return []
            
            
            placeholders = ",".join(f"'{u}'" for u in uids)

            # down_query = f'''   SELECT profile_personal_uid
            #                     FROM profile_personal
            #                     WHERE profile_personal_referred_by in ({placeholders});
            #                 '''
            # down_query = f'''
            #                 SELECT profile_personal_uid, circles.*
            #                 FROM profile_personal
            #                 LEFT JOIN circles ON circle_related_person_id = profile_personal_uid
            #                 WHERE profile_personal_referred_by in ({placeholders});
            #                 '''
            down_query = f'''
                            SELECT 
                                pp.profile_personal_uid,
                                pp.profile_personal_referred_by,
                                pp.profile_personal_first_name,
                                pp.profile_personal_last_name,
                                CASE WHEN pp.profile_personal_tag_line_is_public = 1 THEN pp.profile_personal_tag_line ELSE NULL END as profile_personal_tag_line,
                                CASE WHEN pp.profile_personal_phone_number_is_public = 1 THEN pp.profile_personal_phone_number ELSE NULL END as profile_personal_phone_number,
                                CASE WHEN pp.profile_personal_image_is_public = 1 THEN pp.profile_personal_image ELSE NULL END as profile_personal_image,
                                pp.profile_personal_email_is_public,
                                pp.profile_personal_phone_number_is_public,
                                pp.profile_personal_tag_line_is_public,
                                pp.profile_personal_image_is_public,
                                c.*
                            FROM profile_personal AS pp
                            LEFT JOIN every_circle.circles AS c
                                ON c.circle_related_person_id = pp.profile_personal_uid
                                AND c.circle_profile_id = '{target_uid}'
                            WHERE pp.profile_personal_referred_by in ({placeholders});
                            '''
            # print('down_query', down_query)


            with connect() as db:
                response = db.execute(down_query)
            
            #print('down:', response)

            if not response or 'result' not in response or not response['result']:
                # response['message'] = 'No connection found'
                # response['code'] = 404
                # return response, 404
                return []


            down_query_details = response['result']
            # print('down_query_details: ', down_query_details)

            down_list = [{
                'uid': item['profile_personal_uid'],
                'profile_personal_referred_by': item.get('profile_personal_referred_by'),
                'profile_personal_first_name': item.get('profile_personal_first_name'),
                'profile_personal_last_name': item.get('profile_personal_last_name'),
                'profile_personal_tag_line': item.get('profile_personal_tag_line'),
                'profile_personal_phone_number': item.get('profile_personal_phone_number'),
                'profile_personal_image': item.get('profile_personal_image'),
                'profile_personal_email_is_public': item.get('profile_personal_email_is_public'),
                'profile_personal_phone_number_is_public': item.get('profile_personal_phone_number_is_public'),
                'profile_personal_tag_line_is_public': item.get('profile_personal_tag_line_is_public'),
                'profile_personal_image_is_public': item.get('profile_personal_image_is_public'),
                'circle_relationship': item.get('circle_relationship'),
                'circle_date': item.get('circle_date'),
                'circle_event': item.get('circle_event'),
                'circle_note': item.get('circle_note'),
                'circle_geotag': item.get('circle_geotag'),
                'circle_city': item.get('circle_city'),
                'circle_state': item.get('circle_state')
            } for item in down_query_details]
            
            # print('down_list with profile_personal_referred_by:', down_list)

            return down_list

            
        def fetch_ancestors(uids):
            #print('Inside fetch_ancestors')
            
            if not uids:
                return []
            
            placeholders = ",".join(f"'{u}'" for u in uids)
            # up_query = f'''
            #     SELECT profile_personal_referred_by
            #     FROM profile_personal
            #     WHERE profile_personal_uid in ({placeholders});
            # '''
            up_query = f'''
                            SELECT  
                                pp.profile_personal_uid,
                                pp.profile_personal_referred_by,
                                pp_parent.profile_personal_first_name,
                                pp_parent.profile_personal_last_name,
                                CASE WHEN pp_parent.profile_personal_tag_line_is_public = 1 THEN pp_parent.profile_personal_tag_line ELSE NULL END as profile_personal_tag_line,
                                CASE WHEN pp_parent.profile_personal_phone_number_is_public = 1 THEN pp_parent.profile_personal_phone_number ELSE NULL END as profile_personal_phone_number,
                                CASE WHEN pp_parent.profile_personal_image_is_public = 1 THEN pp_parent.profile_personal_image ELSE NULL END as profile_personal_image,
                                pp_parent.profile_personal_email_is_public,
                                pp_parent.profile_personal_phone_number_is_public,
                                pp_parent.profile_personal_tag_line_is_public,
                                pp_parent.profile_personal_image_is_public,
                                c.*
                            FROM profile_personal AS pp
                            LEFT JOIN profile_personal AS pp_parent
                                ON pp_parent.profile_personal_uid = pp.profile_personal_referred_by
                            LEFT JOIN every_circle.circles AS c
                                ON c.circle_related_person_id = pp_parent.profile_personal_uid
                                AND c.circle_profile_id = '{target_uid}'
                            WHERE pp.profile_personal_uid in ({placeholders});
                        '''
            # print('up_query', up_query)
        
            with connect() as db:
                response = db.execute(up_query)
                print(response)

            if not response or 'result' not in response or not response['result']:
                # response['message'] = 'No connection found'
                # response['code'] = 404
                return []

        
            up_query_details = response['result']
            print('up_query_details: ', up_query_details)

            up_list = [{
                'uid': item['profile_personal_referred_by'],  # The ancestor node
                'profile_personal_referred_by': item.get('profile_personal_uid'),  # The child this ancestor connects TO (for graph connection)
                'profile_personal_referred_by': item.get('profile_personal_uid'),
                'profile_personal_first_name': item.get('profile_personal_first_name'),
                'profile_personal_last_name': item.get('profile_personal_last_name'),
                'profile_personal_tag_line': item.get('profile_personal_tag_line'),
                'profile_personal_phone_number': item.get('profile_personal_phone_number'),
                'profile_personal_image': item.get('profile_personal_image'),
                'profile_personal_email_is_public': item.get('profile_personal_email_is_public'),
                'profile_personal_phone_number_is_public': item.get('profile_personal_phone_number_is_public'),
                'profile_personal_tag_line_is_public': item.get('profile_personal_tag_line_is_public'),
                'profile_personal_image_is_public': item.get('profile_personal_image_is_public'),
                'circle_relationship': item.get('circle_relationship'),
                'circle_date': item.get('circle_date'),
                'circle_event': item.get('circle_event'),
                'circle_note': item.get('circle_note'),
                'circle_geotag': item.get('circle_geotag'),
                'circle_city': item.get('circle_city'),
                'circle_state': item.get('circle_state')
            } for item in up_query_details]

            print('up_list with profile_personal_referred_by:', up_list)

            return up_list
        
        current_down = [target_uid]
        current_up = [target_uid]

        for deg in range(1, degree+1):

            if len(seen) >= max_nodes:
                break

            new_down = fetch_descendants(current_down)

            #print('new_down', new_down)
            new_down = [u for u in new_down if u['uid'] not in seen]
            store['descendants'][deg] = new_down
            down_nodes.extend(new_down)
            seen.update([u['uid'] for u in new_down])
            current_down = [u['uid'] for u in new_down]
            # degree = deg
            #print('down_nodes', down_nodes)
            

            if len(seen) >= max_nodes:
                break

            new_up = fetch_ancestors(current_up)

            #print('new_up', new_up)


            new_up = [u for u in new_up if u['uid'] not in seen]
            store['ancestors'][deg] = new_up
            up_nodes.extend(new_up)
            seen.update([u['uid'] for u in new_up])
            current_up = [u['uid'] for u in new_up]
            #print('up_nodes', up_nodes)

        # Phase 3a: for each ancestor group, fetch one level of descendants
        all_ancestors_down_nodes = []
        for id, val in store['ancestors'].items():

            if not val:
                continue

            if len(seen) >= max_nodes:
                break

            val_uids = [v['uid'] for v in val if isinstance(v, dict) and v.get('uid') is not None]
            if not val_uids:
                continue

            result = fetch_descendants(val_uids)
            result = [u for u in result if u['uid'] not in seen]
            store['ancestors_down'][id] = result
            seen.update([u['uid'] for u in result])
            all_ancestors_down_nodes.extend(result)

        # Phase 3b: continue BFS downward from ancestors_down nodes so that cousins,
        # second-cousins, etc. are found.  Each extra hop increments the level key by 1;
        # add_to_rows uses base_degree=1, so degree = level + 1 stays correct.
        max_anc_level = max(store['ancestors_down'].keys()) if store['ancestors_down'] else 0
        current_down_frontier = all_ancestors_down_nodes
        next_level = max_anc_level + 1

        while current_down_frontier and len(seen) < max_nodes and (next_level + 1) <= degree:
            frontier_uids = [u['uid'] for u in current_down_frontier if u.get('uid') is not None]
            if not frontier_uids:
                break
            result = fetch_descendants(frontier_uids)
            result = [u for u in result if u['uid'] not in seen]
            if not result:
                break
            store['ancestors_down'][next_level] = result
            seen.update([u['uid'] for u in result])
            current_down_frontier = result
            next_level += 1
                


        # result_down = fetch_descendants([target_uid])
        # print('result_down', result_down)


        # result_up = fetch_ancestors([target_uid])
        # print('result_up', result_up)
        # print('store', store)
        # print('store-ancestors', store['ancestors'])
        
        #Flatlining the datasets
        final_rows = []

        def add_to_rows(source_dict, base_degree=0):
            for level, items in source_dict.items(): #iterating through source_dict items(level, items)
                curr_degree = int(level) + base_degree #calculating current degree(level + base_degree)
                if curr_degree > degree:  #checking if current degree is greater than input degree
                    break #if true then breaking the loop
                else: #if false then iterating through items
                    for item in items: #iterating through items
                        # Handle both dict format and legacy string format(uid only), whichever format info is passed through item
                        if isinstance(item, dict):  #using isinstance to check if item is dict
                            final_rows.append({  #above if statement is true then appending all values to final_rows
                                "target_uid": target_uid, #using terget_uid instead of item['uid'] to ensure all connections point back to the original target_uid
                                "network_profile_personal_uid": item['uid'], #using item['uid'] to get the uid of the connection
                                "profile_personal_referred_by": item.get('profile_personal_referred_by'),
                                "profile_personal_first_name": item.get('profile_personal_first_name'),
                                "profile_personal_last_name": item.get('profile_personal_last_name'),
                                "profile_personal_tag_line": item.get('profile_personal_tag_line'),
                                "profile_personal_phone_number": item.get('profile_personal_phone_number'),
                                "profile_personal_image": item.get('profile_personal_image'),
                                "profile_personal_email_is_public": item.get('profile_personal_email_is_public'),
                                "profile_personal_phone_number_is_public": item.get('profile_personal_phone_number_is_public'),
                                "profile_personal_tag_line_is_public": item.get('profile_personal_tag_line_is_public'),
                                "profile_personal_image_is_public": item.get('profile_personal_image_is_public'),
                                "circle_relationship": item.get('circle_relationship'),
                                "circle_date": item.get('circle_date'),
                                "circle_event": item.get('circle_event'),
                                "circle_note": item.get('circle_note'),
                                "circle_geotag": item.get('circle_geotag'),
                                "circle_city": item.get('circle_city'),
                                "circle_state": item.get('circle_state'),
                                "degree": curr_degree
                            })
                        else:
                            #above if statement is false then appending only uid and everything else get the value None
                            final_rows.append({
                                "target_uid": target_uid,
                                "network_profile_personal_uid": item,
                                "profile_personal_referred_by": None,
                                "circle_relationship": None,
                                "circle_date": None,
                                "circle_event": None,
                                "circle_note": None,
                                "circle_geotag": None,
                                "degree": curr_degree
                            })
        add_to_rows(store['descendants'])            # Degree: as is
        add_to_rows(store['ancestors'])              # Degree: as is
        add_to_rows(store['ancestors_down'], 1)      # Degree: ancestor level + 1

        # sort by degree
        final_rows.sort(key=lambda x: x['degree'])

        # Debug: Log all relationships for graph construction
        print('\n=== GRAPH RELATIONSHIPS DEBUG ===')
        print(f'Total nodes: {len(final_rows)}')
        for row in final_rows:
            node_uid = row['network_profile_personal_uid']
            referred_by = row.get('profile_personal_referred_by')
            degree = row['degree']
            # print(f'  Node {node_uid} (degree {degree}):')
            # if referred_by:
            #     print(f'    -> profile_personal_referred_by (connects TO): {referred_by}')
        print('=== END GRAPH RELATIONSHIPS ===\n')

        # Return as JSON
        # return jsonify(final_rows)
        # Ensure consistent field order and correct JSON rendering
        json_output = json.dumps(final_rows, ensure_ascii=False, sort_keys=False)

        
        return Response(json_output, mimetype='application/json')

        # return store, 200

