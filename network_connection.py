from flask import Response
from flask_restful import Resource
import json
from data_ec import connect

ZERO_NODE = '110-000001'


def _get_circle_member_uids(db, target_uid):
    response = db.execute(
        f"""
        SELECT circle_related_person_id AS uid
        FROM every_circle.circles
        WHERE circle_profile_id = '{target_uid}'
          AND circle_related_person_id IS NOT NULL
          AND circle_relationship IS NOT NULL
          AND circle_relationship != ''
        """
    )
    return [
        row['uid']
        for row in (response or {}).get('result') or []
        if row.get('uid')
    ]


def _get_zn_branch_roots(db, circle_member_uids, zero_node=ZERO_NODE):
    """
    For each circle member, walk up the referral chain to the zero node.
    Return the direct children of the zero node on those paths (e.g. J and P).
    """
    if not circle_member_uids:
        return set()

    placeholders = ','.join(f"'{uid}'" for uid in circle_member_uids)
    response = db.execute(
        f"""
        WITH RECURSIVE up_chain AS (
            SELECT
                profile_personal_uid AS uid,
                profile_personal_referred_by AS parent
            FROM every_circle.profile_personal
            WHERE profile_personal_uid IN ({placeholders})

            UNION ALL

            SELECT
                pp.profile_personal_uid,
                pp.profile_personal_referred_by
            FROM every_circle.profile_personal pp
            INNER JOIN up_chain c ON pp.profile_personal_uid = c.parent
            WHERE c.parent IS NOT NULL
              AND c.parent != '{zero_node}'
              AND c.uid != '{zero_node}'
        )
        SELECT DISTINCT c.uid
        FROM up_chain c
        WHERE c.parent = '{zero_node}'
        """
    )
    return {
        row['uid']
        for row in (response or {}).get('result') or []
        if row.get('uid')
    }


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


def _fetch_descendants(db, referrer_uids, target_uid, uid_filter=None):
    if not referrer_uids:
        return []

    placeholders = ','.join(f"'{u}'" for u in referrer_uids)
    uid_clause = ''
    if uid_filter:
        uid_placeholders = ','.join(f"'{u}'" for u in uid_filter)
        uid_clause = f'AND pp.profile_personal_uid IN ({uid_placeholders})'

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
        {uid_clause}
    '''

    down_response = db.execute(down_query)
    return [
        _map_descendant_row(item)
        for item in (down_response or {}).get('result') or []
    ]


def _fetch_ancestors(db, frontier_uids, target_uid):
    if not frontier_uids:
        return []

    placeholders = ','.join(f"'{u}'" for u in frontier_uids)
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

    up_response = db.execute(up_query)
    neighbors = []
    for item in (up_response or {}).get('result') or []:
        if item.get('profile_personal_referred_by'):
            neighbors.append(_map_ancestor_row(item))
    return neighbors


def _fetch_neighbors(db, frontier_uids, target_uid, zn_branch_roots=None, zero_node=ZERO_NODE):
    if not frontier_uids:
        return []

    frontier_set = set(frontier_uids)
    neighbors = []

    other_referrers = [uid for uid in frontier_uids if uid != zero_node]
    if other_referrers:
        neighbors.extend(_fetch_descendants(db, other_referrers, target_uid))

    if zero_node in frontier_set and zn_branch_roots:
        neighbors.extend(
            _fetch_descendants(db, [zero_node], target_uid, uid_filter=zn_branch_roots)
        )

    neighbors.extend(_fetch_ancestors(db, frontier_uids, target_uid))
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
            circle_member_uids = _get_circle_member_uids(db, target_uid)
            zn_branch_roots = _get_zn_branch_roots(db, circle_member_uids)
            print('circle members:', circle_member_uids)
            print('zero-node branch roots:', sorted(zn_branch_roots))

            for current_degree in range(1, degree + 1):
                if not frontier or len(seen) >= max_nodes:
                    break

                neighbors = _fetch_neighbors(
                    db, frontier, target_uid, zn_branch_roots=zn_branch_roots
                )
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
