from flask import request, abort , jsonify, Response
from flask_restful import Resource
from collections import defaultdict
from datetime import datetime
import json
from data_ec import connect

# EveryCircle ZeroNode — user UID 100-000001, profile UID 110-000001 in referral graph
EVERYCIRCLE_ZERO_NODE_UIDS = frozenset({'100-000001', '110-000001'})


def _is_zero_node(uid):
    return uid in EVERYCIRCLE_ZERO_NODE_UIDS


def _is_referred_by_zero_node(item):
    if not isinstance(item, dict):
        return False
    return item.get('profile_personal_referred_by') in EVERYCIRCLE_ZERO_NODE_UIDS


def _down_expandable_items(items):
    return [item for item in items if not _is_referred_by_zero_node(item)]


def _uids_after_zero_on_path(path_uids):
    for index, uid in enumerate(path_uids):
        if _is_zero_node(uid):
            return path_uids[index + 1:]
    return []


def _fetch_uids_above_zero_node(db):
    above = set()
    visited = set()
    frontier = list(EVERYCIRCLE_ZERO_NODE_UIDS)

    while frontier:
        placeholders = _uid_placeholders(frontier)
        query = f'''
            SELECT profile_personal_referred_by
            FROM profile_personal
            WHERE profile_personal_uid IN ({placeholders})
              AND profile_personal_referred_by IS NOT NULL;
        '''
        response = db.execute(query)
        next_frontier = []
        if response and response.get('result'):
            for row in response['result']:
                parent = row.get('profile_personal_referred_by')
                if parent and parent not in visited:
                    above.add(parent)
                    visited.add(parent)
                    next_frontier.append(parent)
        frontier = next_frontier

    return above


def _get_circle_paths_via_zero(target_uid, db):
    """Circle connections routed through ZeroNode on the shortest referral path."""
    circles_by_uid = {}
    connected_uids = []

    for circle in _fetch_circle_connections(target_uid):
        connected_uid = circle.get('circle_related_person_id') or circle.get('profile_personal_uid')
        if not connected_uid or connected_uid in circles_by_uid:
            continue
        circles_by_uid[connected_uid] = circle
        connected_uids.append(connected_uid)

    if not connected_uids:
        return {
            'circle_endpoints': {},
            'path_parents': {},
            'path_degrees': {},
            'exception_uids': set(),
            'path_uids': set(),
        }

    paths_by_uid = _bfs_shortest_paths(target_uid, connected_uids, db)
    circle_endpoints = {}
    path_parents = {}
    path_degrees = {}
    exception_uids = set()
    path_uids = set()

    for connected_uid, route in paths_by_uid.items():
        if not route or not any(_is_zero_node(uid) for uid in route):
            continue

        circle_endpoints[connected_uid] = circles_by_uid[connected_uid]
        path_uids.update(route)
        exception_uids.update(_uids_after_zero_on_path(route))

        for distance, uid in enumerate(route):
            path_degrees[uid] = distance
            if distance > 0:
                path_parents[uid] = route[distance - 1]

    return {
        'circle_endpoints': circle_endpoints,
        'path_parents': path_parents,
        'path_degrees': path_degrees,
        'exception_uids': exception_uids,
        'path_uids': path_uids,
    }


def _apply_circle_path_edges(target_uid, final_rows, circle_paths, db):
    circle_endpoints = circle_paths['circle_endpoints']
    path_parents = circle_paths['path_parents']
    path_degrees = circle_paths['path_degrees']
    valid_endpoints = set(circle_endpoints.keys())

    rows_by_uid = {row['network_profile_personal_uid']: row for row in final_rows}

    for row in final_rows:
        uid = row['network_profile_personal_uid']
        if uid in path_parents:
            row['profile_personal_referred_by'] = path_parents[uid]
        if uid in valid_endpoints:
            circle = circle_endpoints[uid]
            row['circle_relationship'] = circle.get('circle_relationship')
            row['circle_date'] = circle.get('circle_date')
            row['circle_event'] = circle.get('circle_event')
            row['circle_note'] = circle.get('circle_note')
            row['circle_geotag'] = circle.get('circle_geotag')
            row['circle_city'] = circle.get('circle_city')
            row['circle_state'] = circle.get('circle_state')
        elif uid not in valid_endpoints and uid != target_uid:
            row['circle_relationship'] = None
            row['circle_date'] = None
            row['circle_event'] = None
            row['circle_note'] = None
            row['circle_geotag'] = None
            row['circle_city'] = None
            row['circle_state'] = None

    missing_path_uids = circle_paths['path_uids'] - set(rows_by_uid.keys()) - {target_uid}
    if missing_path_uids:
        profiles_by_uid = _fetch_profiles_by_uids(list(missing_path_uids), target_uid, db=db)
        for uid in missing_path_uids:
            profile = profiles_by_uid.get(uid, {})
            row = {
                "target_uid": target_uid,
                "network_profile_personal_uid": uid,
                "profile_personal_referred_by": path_parents.get(uid),
                "profile_personal_first_name": profile.get('profile_personal_first_name'),
                "profile_personal_last_name": profile.get('profile_personal_last_name'),
                "profile_personal_tag_line": profile.get('profile_personal_tag_line'),
                "profile_personal_phone_number": profile.get('profile_personal_phone_number'),
                "profile_personal_image": profile.get('profile_personal_image'),
                "profile_personal_email_is_public": profile.get('profile_personal_email_is_public'),
                "profile_personal_phone_number_is_public": profile.get('profile_personal_phone_number_is_public'),
                "profile_personal_tag_line_is_public": profile.get('profile_personal_tag_line_is_public'),
                "profile_personal_image_is_public": profile.get('profile_personal_image_is_public'),
                "circle_relationship": None,
                "circle_date": None,
                "circle_event": None,
                "circle_note": None,
                "circle_geotag": None,
                "circle_city": None,
                "circle_state": None,
                "degree": path_degrees.get(uid, 0),
            }
            if uid in valid_endpoints:
                circle = circle_endpoints[uid]
                row['circle_relationship'] = circle.get('circle_relationship')
                row['circle_date'] = circle.get('circle_date')
                row['circle_event'] = circle.get('circle_event')
                row['circle_note'] = circle.get('circle_note')
                row['circle_geotag'] = circle.get('circle_geotag')
                row['circle_city'] = circle.get('circle_city')
                row['circle_state'] = circle.get('circle_state')
            final_rows.append(row)

    return final_rows


def _map_profile_row(item, uid):
    return {
        'uid': uid,
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
        'circle_state': item.get('circle_state'),
    }


def _fetch_profiles_by_uids(uids, viewer_uid, db=None):
    if not uids:
        return {}

    placeholders = ",".join(f"'{u}'" for u in uids)
    query = f'''
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
            AND c.circle_profile_id = '{viewer_uid}'
        WHERE pp.profile_personal_uid IN ({placeholders});
    '''

    if db is None:
        with connect() as conn:
            response = conn.execute(query)
    else:
        response = db.execute(query)

    if not response or 'result' not in response or not response['result']:
        return {}

    return {
        row['profile_personal_uid']: _map_profile_row(row, row['profile_personal_uid'])
        for row in response['result']
    }


def _uid_placeholders(uids):
    return ",".join(f"'{u}'" for u in uids)


def _fetch_neighbor_edges(frontier_uids, db):
    if not frontier_uids:
        return []

    placeholders = _uid_placeholders(frontier_uids)
    up_query = f'''
        SELECT profile_personal_uid, profile_personal_referred_by
        FROM profile_personal
        WHERE profile_personal_uid IN ({placeholders})
          AND profile_personal_referred_by IS NOT NULL;
    '''
    down_query = f'''
        SELECT profile_personal_referred_by, profile_personal_uid
        FROM profile_personal
        WHERE profile_personal_referred_by IN ({placeholders});
    '''

    edges = []
    up_response = db.execute(up_query)
    if up_response and up_response.get('result'):
        for row in up_response['result']:
            referrer = row.get('profile_personal_referred_by')
            uid = row.get('profile_personal_uid')
            if uid and referrer:
                edges.append((uid, referrer))

    down_response = db.execute(down_query)
    if down_response and down_response.get('result'):
        for row in down_response['result']:
            parent_uid = row.get('profile_personal_referred_by')
            child_uid = row.get('profile_personal_uid')
            if parent_uid and child_uid:
                edges.append((parent_uid, child_uid))

    return edges


def _reconstruct_path(parent, end_uid):
    path = []
    node = end_uid
    while node is not None:
        path.append(node)
        node = parent.get(node)
    path.reverse()
    return path


def _bfs_shortest_paths(start_uid, target_uids, db):
    targets = set(target_uids)
    found = {}

    if start_uid in targets:
        found[start_uid] = [start_uid]
        targets.discard(start_uid)

    if not targets:
        return found

    visited = {start_uid}
    parent = {start_uid: None}
    frontier = {start_uid}

    while frontier and targets:
        edges = _fetch_neighbor_edges(frontier, db)
        next_frontier = set()

        for from_uid, to_uid in edges:
            if from_uid not in frontier or to_uid in visited:
                continue
            visited.add(to_uid)
            parent[to_uid] = from_uid
            next_frontier.add(to_uid)
            if to_uid in targets:
                found[to_uid] = _reconstruct_path(parent, to_uid)
                targets.discard(to_uid)

        frontier = next_frontier

    return found


def _find_shortest_path_uids(first_uid, second_uid, db=None):
    if first_uid == second_uid:
        return [first_uid]

    if db is not None:
        return _bfs_shortest_paths(first_uid, [second_uid], db).get(second_uid)

    with connect() as conn:
        return _bfs_shortest_paths(first_uid, [second_uid], conn).get(second_uid)


def _build_path_nodes(path_uids, first_uid, second_uid, profiles_by_uid=None):
    if profiles_by_uid is None:
        profiles_by_uid = _fetch_profiles_by_uids(path_uids, first_uid)
    nodes = []

    for distance, uid in enumerate(path_uids):
        profile = profiles_by_uid.get(uid, {'uid': uid})
        nodes.append({
            'target_uid': first_uid,
            'end_uid': second_uid,
            'network_profile_personal_uid': uid,
            'profile_personal_referred_by': profile.get('profile_personal_referred_by'),
            'profile_personal_first_name': profile.get('profile_personal_first_name'),
            'profile_personal_last_name': profile.get('profile_personal_last_name'),
            'profile_personal_tag_line': profile.get('profile_personal_tag_line'),
            'profile_personal_phone_number': profile.get('profile_personal_phone_number'),
            'profile_personal_image': profile.get('profile_personal_image'),
            'profile_personal_email_is_public': profile.get('profile_personal_email_is_public'),
            'profile_personal_phone_number_is_public': profile.get('profile_personal_phone_number_is_public'),
            'profile_personal_tag_line_is_public': profile.get('profile_personal_tag_line_is_public'),
            'profile_personal_image_is_public': profile.get('profile_personal_image_is_public'),
            'circle_relationship': profile.get('circle_relationship'),
            'circle_date': profile.get('circle_date'),
            'circle_event': profile.get('circle_event'),
            'circle_note': profile.get('circle_note'),
            'circle_geotag': profile.get('circle_geotag'),
            'circle_city': profile.get('circle_city'),
            'circle_state': profile.get('circle_state'),
            'distance': distance,
        })

    return nodes


def _fetch_circle_connections(target_uid):
    circles_query = """
        SELECT circles.*
            , pp.profile_personal_uid, pp.profile_personal_user_id, pp.profile_personal_first_name, pp.profile_personal_last_name
            , u.user_email_id, pp.profile_personal_email_is_public, pp.profile_personal_phone_number, pp.profile_personal_phone_number_is_public
            , pp.profile_personal_image, pp.profile_personal_image_is_public
            , pp.profile_personal_city, pp.profile_personal_state, pp.profile_personal_country, pp.profile_personal_location_is_public, pp.profile_personal_latitude, pp.profile_personal_longitude
        FROM every_circle.circles
        LEFT JOIN profile_personal pp ON circle_related_person_id = profile_personal_uid
        LEFT JOIN users u ON pp.profile_personal_user_id = user_uid
        WHERE circle_profile_id = %s AND circle_relationship != ""
        ORDER BY circle_date DESC, circle_uid DESC
    """

    with connect() as db:
        response = db.execute(circles_query, (target_uid,))

    if not response or response.get('code') != 200:
        return []

    return response.get('result', [])


def get_circle_shortest_paths(target_uid):
    """Shortest referral path from target_uid to each profile in their circles."""
    circle_connections = _fetch_circle_connections(target_uid)
    circles_by_uid = {}
    connected_uids = []

    for circle in circle_connections:
        connected_uid = circle.get('circle_related_person_id') or circle.get('profile_personal_uid')
        if not connected_uid or connected_uid in circles_by_uid:
            continue
        circles_by_uid[connected_uid] = circle
        connected_uids.append(connected_uid)

    if not connected_uids:
        return []

    with connect() as db:
        paths_by_uid = _bfs_shortest_paths(target_uid, connected_uids, db)

        all_path_uids = set()
        for path_uids in paths_by_uid.values():
            all_path_uids.update(path_uids)

        profiles_by_uid = _fetch_profiles_by_uids(list(all_path_uids), target_uid, db=db)

    results = []
    for connected_uid in connected_uids:
        circle = circles_by_uid[connected_uid]
        path_uids = paths_by_uid.get(connected_uid)
        nodes = _build_path_nodes(
            path_uids, target_uid, connected_uid, profiles_by_uid=profiles_by_uid
        ) if path_uids else []

        results.append({
            'target_uid': target_uid,
            'connected_profile_uid': connected_uid,
            'circle_uid': circle.get('circle_uid'),
            'circle_relationship': circle.get('circle_relationship'),
            'circle_date': circle.get('circle_date'),
            'circle_event': circle.get('circle_event'),
            'circle_note': circle.get('circle_note'),
            'circle_geotag': circle.get('circle_geotag'),
            'circle_city': circle.get('circle_city'),
            'circle_state': circle.get('circle_state'),
            'circle_introduced_by': circle.get('circle_introduced_by'),
            'circle_num_nodes': circle.get('circle_num_nodes'),
            'profile_personal_first_name': circle.get('profile_personal_first_name'),
            'profile_personal_last_name': circle.get('profile_personal_last_name'),
            'profile_personal_image': circle.get('profile_personal_image'),
            'profile_personal_image_is_public': circle.get('profile_personal_image_is_public'),
            'path_length': len(nodes) - 1 if nodes else None,
            'path_found': path_uids is not None,
            'nodes': nodes,
        })

    return results


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

        def frontier_without_zero_nodes(uids):
            return [uid for uid in uids if not _is_zero_node(uid)]

        current_down = [target_uid]
        current_up = [] if _is_zero_node(target_uid) else [target_uid]

        for deg in range(1, degree+1):

            if len(seen) >= max_nodes:
                break

            new_down = fetch_descendants(current_down)

            #print('new_down', new_down)
            new_down = [u for u in new_down if u['uid'] not in seen]
            store['descendants'][deg] = new_down
            down_nodes.extend(new_down)
            seen.update([u['uid'] for u in new_down])
            current_down = [u['uid'] for u in _down_expandable_items(new_down)]
            # degree = deg
            #print('down_nodes', down_nodes)
            

            if len(seen) >= max_nodes:
                break

            if not current_up:
                continue

            new_up = fetch_ancestors(current_up)

            #print('new_up', new_up)


            new_up = [u for u in new_up if u['uid'] not in seen]
            store['ancestors'][deg] = new_up
            up_nodes.extend(new_up)
            seen.update([u['uid'] for u in new_up])
            current_up = frontier_without_zero_nodes([u['uid'] for u in new_up])
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
        # Nodes referred by ZeroNode are included once but never used as a down frontier.
        max_anc_level = max(store['ancestors_down'].keys()) if store['ancestors_down'] else 0
        current_down_frontier = _down_expandable_items(all_ancestors_down_nodes)
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
            current_down_frontier = _down_expandable_items(result)
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

        with connect() as db:
            above_zero = _fetch_uids_above_zero_node(db)
            circle_paths = _get_circle_paths_via_zero(target_uid, db)
            exception_uids = circle_paths['exception_uids']

            final_rows = [
                row for row in final_rows
                if row['network_profile_personal_uid'] not in above_zero
                or row['network_profile_personal_uid'] in exception_uids
            ]

            final_rows = _apply_circle_path_edges(target_uid, final_rows, circle_paths, db)

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


class NetworkShortestPath(Resource):
    def get(self, first_uid, second_uid):
        # print('first_uid', first_uid)
        # print('second_uid', second_uid)
        print('first_uid ', first_uid, 'second_uid ', second_uid)
        path_uids = _find_shortest_path_uids(first_uid, second_uid)
        if path_uids is None:
            return {'message': 'No connection found', 'code': 404}, 404

        nodes = _build_path_nodes(path_uids, first_uid, second_uid)
        return Response(
            json.dumps(nodes, ensure_ascii=False, sort_keys=False),
            mimetype='application/json',
        )


class NetworkCirclePaths(Resource):
    def get(self, target_uid):
        print('NetworkCirclePaths target_uid', target_uid)

        if not target_uid:
            return {'message': 'target_uid is required', 'code': 400}, 400
        print('target_uid', target_uid)
        paths = get_circle_shortest_paths(target_uid)
        return {
            'message': 'Circle shortest paths retrieved successfully',
            'code': 200,
            'target_uid': target_uid,
            'count': len(paths),
            'data': paths,
        }, 200


