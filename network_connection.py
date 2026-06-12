from flask import Response
from flask_restful import Resource
import json
from data_ec import connect


def _map_descendant_row(item):
    return {
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
        'circle_state': item.get('circle_state'),
    }


def _map_ancestor_row(item):
    return {
        'uid': item['profile_personal_referred_by'],
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
        'circle_state': item.get('circle_state'),
    }


def _fetch_neighbors(db, frontier_uids, target_uid):
    if not frontier_uids:
        return []

    placeholders = ",".join(f"'{u}'" for u in frontier_uids)

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
        WHERE pp.profile_personal_referred_by IN ({placeholders})
    '''

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
        WHERE pp.profile_personal_uid IN ({placeholders})
        AND pp.profile_personal_referred_by IS NOT NULL
    '''

    down_response = db.execute(down_query)
    up_response = db.execute(up_query)

    neighbors = []
    for item in (down_response or {}).get('result') or []:
        neighbors.append(_map_descendant_row(item))
    for item in (up_response or {}).get('result') or []:
        if item.get('profile_personal_referred_by'):
            neighbors.append(_map_ancestor_row(item))
    return neighbors


def _to_response_row(target_uid, item):
    return {
        "target_uid": target_uid,
        "network_profile_personal_uid": item['uid'],
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
        "degree": item['degree'],
    }


class NetworkPath(Resource):
    def get(self, target_uid, degree):
        print('target_uid', target_uid)
        print('degree', degree)

        max_nodes = 200
        seen = {target_uid}
        frontier = [target_uid]
        nodes_by_uid = {}

        with connect() as db:
            for current_degree in range(1, degree + 1):
                if not frontier or len(seen) >= max_nodes:
                    break

                neighbors = _fetch_neighbors(db, frontier, target_uid)
                next_frontier = []

                for item in neighbors:
                    uid = item.get('uid')
                    if not uid or uid in seen:
                        continue
                    seen.add(uid)
                    item['degree'] = current_degree
                    nodes_by_uid[uid] = item
                    next_frontier.append(uid)

                frontier = next_frontier

        final_rows = [
            _to_response_row(target_uid, item)
            for item in sorted(nodes_by_uid.values(), key=lambda x: x['degree'])
        ]

        print('\n=== GRAPH RELATIONSHIPS DEBUG ===')
        print(f'Total nodes: {len(final_rows)}')
        print('=== END GRAPH RELATIONSHIPS ===\n')

        json_output = json.dumps(final_rows, ensure_ascii=False, sort_keys=False)
        return Response(json_output, mimetype='application/json')
