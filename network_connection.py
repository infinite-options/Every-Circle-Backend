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
        max_nodes  = 30
        #degree = 3
        store = {'descendants': defaultdict(list), 'ancestors': defaultdict(list), 'ancestors_down':defaultdict(list)}


        def fetch_descendants(uids):
            print('Inside fetch_descendants')

            if not uids:
                return []
            
            
            placeholders = ",".join(f"'{u}'" for u in uids)

            down_query = f''' SELECT profile_personal_uid
                                FROM profile_personal
                                WHERE profile_personal_referred_by in ({placeholders});
                            '''
            print('down_query', down_query)


            with connect() as db:
                response = db.execute(down_query)
            
            print('down:', response)

            if not response or 'result' not in response or not response['result']:
                # response['message'] = 'No connection found'
                # response['code'] = 404
                # return response, 404
                return []


            down_query_details = response['result']
            print('down_query_details: ', down_query_details)

            down_list = [item['profile_personal_uid'] for item in down_query_details]
            
            print('down_list:', down_list)

            return down_list

            
        def fetch_ancestors(uids):
            print('Inside fetch_ancestors')
            
            if not uids:
                return []
            
            placeholders = ",".join(f"'{u}'" for u in uids)
            up_query = f'''
                SELECT profile_personal_referred_by
                FROM profile_personal
                WHERE profile_personal_uid in ({placeholders});
        '''
            print('up_query', up_query)
        
            with connect() as db:
                response = db.execute(up_query)
                print(response)

            if not response or 'result' not in response or not response['result']:
                # response['message'] = 'No connection found'
                # response['code'] = 404
                return []

        
            up_query_details = response['result']
            print('up_query_details: ', up_query_details)

            up_list = [item['profile_personal_referred_by'] for item in up_query_details]

            print('up_list:', up_list)

            return up_list
        
        current_down = [target_uid]
        current_up = [target_uid]

        for deg in range(1, degree+1):

            if len(seen) >= max_nodes:
                break

            new_down = fetch_descendants(current_down)

            print('new_down', new_down)
            new_down = [u for u in new_down if u not in seen]
            store['descendants'][deg] = new_down
            down_nodes.extend(new_down)
            seen.update(new_down)
            current_down = new_down
            # degree = deg
            print('down_nodes', down_nodes)
            

            if len(seen) >= max_nodes:
                break

            new_up = fetch_ancestors(current_up)

            print('new_up', new_up)


            new_up = [u for u in new_up if u not in seen]
            store['ancestors'][deg] = new_up
            up_nodes.extend(new_up)
            seen.update(new_up)
            current_up = new_up
            print('up_nodes', up_nodes)

        total_count = sum(len(v) for v in store['ancestors'].values()) + sum(len(v) for v in store['descendants'].values())
        print('total_count before the anscestors_down', total_count)


        for id, val in store['ancestors'].items():

            print('id, val inside the anscestors_down', id, val)
            
            total_count += sum(len(v) for v in store['ancestors_down'].values())

            print('total_count inside the anscestors_down', total_count)

            #if not val or '110-000001' in val:
            if not val:
                 continue

            #print('total_count', total_count)   

            if total_count >= max_nodes:
                break

            result = fetch_descendants(val)
            result = [u for u in result if u not in seen]
            store['ancestors_down'][id] = result
            seen.update(result)
                


        # result_down = fetch_descendants([target_uid])
        # print('result_down', result_down)


        # result_up = fetch_ancestors([target_uid])
        # print('result_up', result_up)
        print('store', store)
        print('store-ancestors', store['ancestors'])
        
        #Flatlining the datasets
        final_rows = []

        def add_to_rows(source_dict, base_degree=0):
            for level, uids in source_dict.items():
                curr_degree = int(level) + base_degree
                if curr_degree > degree:
                    break
                else:
                    for uid in uids:
                        final_rows.append({
                            "target_uid": target_uid,
                            "network_profile_personal_uid": uid,
                            "degree": curr_degree
                        })

        add_to_rows(store['descendants'])            # Degree: as is
        add_to_rows(store['ancestors'])              # Degree: as is
        add_to_rows(store['ancestors_down'], 1)      # Degree: ancestor level + 1

        # sort by degree
        final_rows.sort(key=lambda x: x['degree'])

        # Return as JSON
        # return jsonify(final_rows)
        # Ensure consistent field order and correct JSON rendering
        json_output = json.dumps(final_rows, ensure_ascii=False, sort_keys=False)

        
        return Response(json_output, mimetype='application/json')

        # return store, 200

