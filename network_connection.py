from flask import Response
from flask_restful import Resource
import json
from data_ec import connect
import time

ZERO_NODE = '110-000001'
# System profile linked above the zero node; not a real user (shows as Unknown / 000 in UI).
HIDDEN_NETWORK_UIDS = frozenset({'110-000000'})

_PROFILE_SELECT = '''
    pp.profile_personal_uid,
    pp.profile_personal_referred_by,
    pp.profile_personal_first_name,
    pp.profile_personal_last_name,
    CASE WHEN pp.profile_personal_tag_line_is_public = 1 THEN pp.profile_personal_tag_line ELSE NULL END AS profile_personal_tag_line,
    CASE WHEN pp.profile_personal_phone_number_is_public = 1 THEN pp.profile_personal_phone_number ELSE NULL END AS profile_personal_phone_number,
    CASE WHEN pp.profile_personal_image_is_public = 1 THEN pp.profile_personal_image ELSE NULL END AS profile_personal_image,
    pp.profile_personal_email_is_public,
    pp.profile_personal_phone_number_is_public,
    pp.profile_personal_tag_line_is_public,
    pp.profile_personal_image_is_public,
    c.circle_relationship,
    c.circle_date,
    c.circle_event,
    c.circle_note,
    c.circle_geotag,
    c.circle_city,
    c.circle_state
'''


class _CountingDB:
    def __init__(self, db):
        self._db = db
        self.call_count = 0

    def execute(self, *args, **kwargs):
        self.call_count += 1
        return self._db.execute(*args, **kwargs)


def _sql_in_list(uids):
    return ','.join(f"'{uid}'" for uid in uids)


def _hidden_uid_sql_clause(column):
    if not HIDDEN_NETWORK_UIDS:
        return ''
    placeholders = ','.join(f"'{uid}'" for uid in HIDDEN_NETWORK_UIDS)
    return f'AND {column} NOT IN ({placeholders})'


def _visible_referred_by(referred_by):
    if referred_by in HIDDEN_NETWORK_UIDS:
        return None
    return referred_by


def _get_circle_member_uids(db, target_uid):
    print('target_uid', target_uid)
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
    print('response', response)
    return [
        row['uid']
        for row in (response or {}).get('result') or []
        if row.get('uid')
    ]


def _walk_up_chains_batch(db, start_uids, limit=50):
    """Walk referral chains upward for many start UIDs in one recursive query."""
    unique_uids = list(dict.fromkeys(uid for uid in start_uids if uid))
    if not unique_uids:
        return {}

    placeholders = _sql_in_list(unique_uids)
    hidden_stop = ''
    if HIDDEN_NETWORK_UIDS:
        hidden_stop = f'AND c.parent NOT IN ({_sql_in_list(HIDDEN_NETWORK_UIDS)})'

    response = db.execute(
        f"""
        WITH RECURSIVE up_chain AS (
            SELECT
                pp.profile_personal_uid AS start_uid,
                pp.profile_personal_uid AS uid,
                pp.profile_personal_referred_by AS parent,
                0 AS depth
            FROM every_circle.profile_personal pp
            WHERE pp.profile_personal_uid IN ({placeholders})

            UNION ALL

            SELECT
                c.start_uid,
                pp.profile_personal_uid,
                pp.profile_personal_referred_by,
                c.depth + 1
            FROM up_chain c
            INNER JOIN every_circle.profile_personal pp
                ON pp.profile_personal_uid = c.parent
            WHERE c.parent IS NOT NULL
              AND c.parent != c.uid
              {hidden_stop}
              AND c.depth + 1 < {limit}
        )
        SELECT start_uid, uid, depth
        FROM up_chain
        ORDER BY start_uid, depth
        """
    )

    chains = {}
    for row in (response or {}).get('result') or []:
        start_uid = row['start_uid']
        chains.setdefault(start_uid, []).append(row['uid'])

    for uid in unique_uids:
        chains.setdefault(uid, [uid])
    return chains


def _get_essential_path_uids(db, target_uid, circle_member_uids):
    """
    Nodes on the referral-tree path from target_uid to each circle member.
    Used to keep direct circle paths when the 200-node ancillary cap is reached.
    """
    essential = {target_uid}
    if not circle_member_uids:
        return essential - HIDDEN_NETWORK_UIDS

    members_to_walk = [
        member
        for member in circle_member_uids
        if member not in HIDDEN_NETWORK_UIDS and member != target_uid
    ]
    chains = _walk_up_chains_batch(db, [target_uid] + members_to_walk)
    up_target = chains.get(target_uid, [target_uid])
    target_index = {uid: idx for idx, uid in enumerate(up_target)}

    for member in circle_member_uids:
        if member in HIDDEN_NETWORK_UIDS:
            continue
        if member == target_uid:
            essential.add(member)
            continue

        up_member = chains.get(member, [member])
        lca_idx_m = None
        lca_idx_t = None
        for idx, uid in enumerate(up_member):
            if uid in target_index:
                lca_idx_m = idx
                lca_idx_t = target_index[uid]
                break

        if lca_idx_m is None:
            continue

        essential.update(up_target[:lca_idx_t + 1])
        essential.update(up_member[:lca_idx_m + 1])

    return essential - HIDDEN_NETWORK_UIDS


def _get_zn_branch_roots(db, circle_member_uids, zero_node=ZERO_NODE):
    """
    For each circle member, walk up the referral chain to the zero node.
    Return the direct children of the zero node on those paths (e.g. J and P).
    """
    if not circle_member_uids:
        return set()

    placeholders = _sql_in_list(circle_member_uids)
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
        if row.get('uid') and row['uid'] not in HIDDEN_NETWORK_UIDS
    }


def _map_profile_row(item, referred_by_override=None):
    return {
        'uid': item['profile_personal_uid'],
        'profile_personal_referred_by': _visible_referred_by(
            referred_by_override if referred_by_override is not None else item.get('profile_personal_referred_by')
        ),
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


def _fetch_neighbor_uids(
    db,
    frontier_uids,
    zn_branch_roots=None,
    zero_node=ZERO_NODE,
    path_only_uids=None,
):
    if not frontier_uids:
        return []

    parts = []
    frontier_set = set(frontier_uids)
    other_referrers = [uid for uid in frontier_uids if uid != zero_node]

    if other_referrers:
        uid_clause = ''
        if path_only_uids is not None:
            uid_clause = f'AND pp.profile_personal_uid IN ({_sql_in_list(path_only_uids)})'
        parts.append(
            f"""
            SELECT
                pp.profile_personal_uid AS uid,
                pp.profile_personal_referred_by AS referred_by
            FROM every_circle.profile_personal AS pp
            WHERE pp.profile_personal_referred_by IN ({_sql_in_list(other_referrers)})
            {_hidden_uid_sql_clause('pp.profile_personal_uid')}
            {uid_clause}
            """
        )

    if zero_node in frontier_set and zn_branch_roots:
        zn_filter = zn_branch_roots
        if path_only_uids is not None:
            zn_filter = zn_branch_roots & path_only_uids
        if zn_filter:
            parts.append(
                f"""
                SELECT
                    pp.profile_personal_uid AS uid,
                    pp.profile_personal_referred_by AS referred_by
                FROM every_circle.profile_personal AS pp
                WHERE pp.profile_personal_referred_by = '{zero_node}'
                  AND pp.profile_personal_uid IN ({_sql_in_list(zn_filter)})
                {_hidden_uid_sql_clause('pp.profile_personal_uid')}
                """
            )

    parts.append(
        f"""
        SELECT
            pp.profile_personal_referred_by AS uid,
            pp.profile_personal_uid AS referred_by
        FROM every_circle.profile_personal AS pp
        WHERE pp.profile_personal_uid IN ({_sql_in_list(frontier_uids)})
          AND pp.profile_personal_referred_by IS NOT NULL
        {_hidden_uid_sql_clause('pp.profile_personal_referred_by')}
        """
    )

    response = db.execute(' UNION ALL '.join(parts))
    neighbors = []
    for row in (response or {}).get('result') or []:
        uid = row.get('uid')
        if not uid or uid in HIDDEN_NETWORK_UIDS:
            continue
        if path_only_uids is not None and uid not in path_only_uids:
            continue
        neighbors.append({
            'uid': uid,
            'referred_by': row.get('referred_by'),
        })
    return neighbors


def _fetch_profiles_batch(db, uids, target_uid):
    if not uids:
        return {}

    response = db.execute(
        f"""
        SELECT
            {_PROFILE_SELECT}
        FROM every_circle.profile_personal AS pp
        LEFT JOIN every_circle.circles AS c
            ON c.circle_related_person_id = pp.profile_personal_uid
            AND c.circle_profile_id = '{target_uid}'
        WHERE pp.profile_personal_uid IN ({_sql_in_list(uids)})
        {_hidden_uid_sql_clause('pp.profile_personal_uid')}
        """
    )

    profiles_by_uid = {}
    for row in (response or {}).get('result') or []:
        uid = row.get('profile_personal_uid')
        if uid and uid not in HIDDEN_NETWORK_UIDS:
            profiles_by_uid[uid] = row
    return profiles_by_uid


def _to_response_row(target_uid, item):
    return {
        "target_uid": target_uid,
        "network_profile_personal_uid": item['uid'],
        "profile_personal_referred_by": _visible_referred_by(item.get('profile_personal_referred_by')),
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
        start = time.perf_counter()
        print('target_uid', target_uid)
        print('degree', degree)

        max_nodes = 50
        seen = {target_uid}
        frontier = [target_uid]
        nodes_meta = {}
        non_essential_count = 0

        with connect() as db:
            db = _CountingDB(db)
            circle_member_uids = _get_circle_member_uids(db, target_uid)
            essential_uids = _get_essential_path_uids(db, target_uid, circle_member_uids)
            zn_branch_roots = _get_zn_branch_roots(db, circle_member_uids)
            print('circle members:', circle_member_uids)
            print('essential path nodes:', len(essential_uids))
            print('zero-node branch roots:', sorted(zn_branch_roots))

            for current_degree in range(1, degree + 1):
                if not frontier:
                    break

                path_only = essential_uids if non_essential_count >= max_nodes else None
                neighbors = _fetch_neighbor_uids(
                    db,
                    frontier,
                    zn_branch_roots=zn_branch_roots,
                    path_only_uids=path_only,
                )
                next_frontier = []

                for neighbor in neighbors:
                    uid = neighbor.get('uid')
                    if not uid or uid in seen or uid in HIDDEN_NETWORK_UIDS:
                        continue

                    on_essential_path = uid in essential_uids
                    if not on_essential_path and non_essential_count >= max_nodes:
                        continue

                    seen.add(uid)
                    if not on_essential_path:
                        non_essential_count += 1
                    nodes_meta[uid] = {
                        'degree': current_degree,
                        'referred_by': neighbor.get('referred_by'),
                    }
                    next_frontier.append(uid)

                frontier = next_frontier

            profiles_by_uid = _fetch_profiles_batch(db, list(nodes_meta.keys()), target_uid)
            nodes_by_uid = {}
            for uid, meta in nodes_meta.items():
                profile = profiles_by_uid.get(uid)
                if not profile:
                    continue
                item = _map_profile_row(profile, referred_by_override=meta['referred_by'])
                item['degree'] = meta['degree']
                nodes_by_uid[uid] = item

            print(f'Total DB calls: {db.call_count}')

        final_rows = [
            _to_response_row(target_uid, item)
            for item in sorted(nodes_by_uid.values(), key=lambda x: x['degree'])
        ]

        print('\n=== GRAPH RELATIONSHIPS DEBUG ===')
        print(f'Total nodes: {len(final_rows)}')
        print(f'Non-essential nodes: {non_essential_count}')
        print(f'Path-only mode active: {non_essential_count >= max_nodes}')
        print('=== END GRAPH RELATIONSHIPS ===\n')

        json_output = json.dumps(final_rows, ensure_ascii=False, sort_keys=False)
        end = time.perf_counter()
        print(f'Time taken: {end - start} seconds')
        return Response(json_output, mimetype='application/json')
