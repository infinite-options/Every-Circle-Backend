from flask import request, abort , jsonify
from flask_restful import Resource
from werkzeug.exceptions import BadRequest
from datetime import datetime

from data_ec import connect

class ConnectionsPath(Resource):
    def get(self, first_uid, second_uid):
        print("first_uid: ", first_uid)
        print("second_uid: ", second_uid)
        with connect() as db:

            first_connection_query = f''' WITH RECURSIVE ReferralPath AS (
                                    SELECT 
                                        profile_personal_uid AS user_id,
                                        profile_personal_referred_by,
                                        CAST(profile_personal_uid AS CHAR(255)) AS path
                                    FROM profile_personal
                                    WHERE profile_personal_uid = '110-000001'

                                    UNION ALL

                                    SELECT 
                                        p.profile_personal_uid,
                                        p.profile_personal_referred_by,
                                        CONCAT(r.path, '->', p.profile_personal_uid)
                                    FROM profile_personal p
                                    JOIN ReferralPath r ON p.profile_personal_referred_by = r.user_id
                                    WHERE LOCATE(p.profile_personal_uid, r.path) = 0 
                                )

                                SELECT *
                                FROM ReferralPath
                                WHERE user_id = '{first_uid}';
                        '''
            

            response = db.execute(first_connection_query)
            print(response)

            if not response['result']:
                response['message'] = 'No connection found'
                response['code'] = 404
                return response, 404

            first_user_details = response['result'][0]
            print('first_user_details: ', first_user_details)

            second_connection_query = f''' WITH RECURSIVE ReferralPath AS (
                                    SELECT 
                                        profile_personal_uid AS user_id,
                                        profile_personal_referred_by,
                                        CAST(profile_personal_uid AS CHAR(255)) AS path
                                    FROM profile_personal
                                    WHERE profile_personal_uid = '110-000001'

                                    UNION ALL

                                    SELECT 
                                        p.profile_personal_uid,
                                        p.profile_personal_referred_by,
                                        CONCAT(r.path, '->', p.profile_personal_uid)
                                    FROM profile_personal p
                                    JOIN ReferralPath r ON p.profile_personal_referred_by = r.user_id
                                    WHERE LOCATE(p.profile_personal_uid, r.path) = 0 
                                )

                                SELECT *
                                FROM ReferralPath
                                WHERE user_id = '{second_uid}';
                        '''


            response = db.execute(second_connection_query)
            print(response)

            if not response['result']:
                response['message'] = 'No connection found'
                response['code'] = 404
                return response, 404

            second_user_details = response['result'][0]
            print('second_user_details', second_user_details)

            
            #concatenation of the combined results
            def combine_paths(first_path, second_path):
                first_list = first_path.split('->')
                second_list = second_path.split('->')

                print('first_list:', first_list)
                print('second_list:', second_list)
                # Find the common ancestor path
                common_path = []
                for f, s in zip(first_list, second_list):
                    if f == s:
                        common_path.append(f)
                    else:
                        continue

                if not common_path:
                    return "No common ancestor found."

                print('common_path:', common_path)

                lca = common_path[-1]  # Last common node


                # Get paths after the common ancestor
                after_common_1 = first_list[first_list.index(lca) + 1:]
                after_common_2 = second_list[second_list.index(lca) + 1:]

                print('after_common_1:', after_common_1)
                print('after_common_2:', after_common_2)

                # if len(after_common_2) > len(after_common_1):

                # # Merge all: common -> after_common_1 -> after_common_2
                #     combined_path = after_common_1 +[lca]+ after_common_2[::-1]
                # else:

                if first_list[-1] != lca:  # first_list is the start
                    
                    reversed_suffix = after_common_1[::-1]
                    other_suffix = after_common_2
                else:  # second_list is the start
                    reversed_suffix = after_common_2[::-1] 
                    other_suffix = after_common_1
                    

                # combined_path = after_common_1 +[lca]+ after_common_2[::-1]
                print('reversed_suffix', reversed_suffix, 'lca', lca, 'other_suffix', other_suffix)
                combined_path = reversed_suffix + [lca] + other_suffix

                return ','.join(combined_path)

            
            combined = combine_paths(first_user_details['path'],
                                     second_user_details['path'])
            
            print("Combined path:", combined)

            return {'combined_path': combined}, 200

            # return jsonify({'combined':combined}), 200