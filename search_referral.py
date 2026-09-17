from flask import request
from flask_restful import Resource
from data_ec import connect
from profile_status import deleted_profile_sql_clause
from profile_visibility import audience_visible_to_viewer

# Raw values + *_audience JSON needed to gate them per-viewer in Python
# (_apply_search_row_visibility below) - degree-aware gating can't be expressed
# as a plain SQL CASE WHEN since it depends on the searching viewer, not just
# the row. profile_personal_tag_line_is_public is still selected raw for the
# frontend field name it already reads; _apply_search_row_visibility keeps it
# in sync with the per-viewer decision.
PROFILE_SELECT = """
    pp.profile_personal_uid,
    pp.profile_personal_user_id,
    pp.profile_personal_first_name,
    pp.profile_personal_last_name,
    u.user_email_id as profile_email_id,
    pp.profile_personal_phone_number,
    pp.profile_personal_city,
    pp.profile_personal_state,
    pp.profile_personal_image,
    pp.profile_personal_tag_line,
    pp.profile_personal_tag_line_is_public,
    pp.profile_personal_short_bio,
    pp.profile_personal_email_audience,
    pp.profile_personal_phone_number_audience,
    pp.profile_personal_city_audience,
    pp.profile_personal_state_audience,
    pp.profile_personal_image_audience,
    pp.profile_personal_tag_line_audience,
    pp.profile_personal_short_bio_audience
"""

# Maps each gated search-result field to its *_audience column and the row
# key(s) to null out when hidden from a particular viewer.
_SEARCH_FIELD_GATES = {
    "email": {
        "audience_col": "profile_personal_email_audience",
        "value_keys": ["profile_email_id"],
    },
    "phone_number": {
        "audience_col": "profile_personal_phone_number_audience",
        "value_keys": ["profile_personal_phone_number"],
    },
    "city": {
        "audience_col": "profile_personal_city_audience",
        "value_keys": ["profile_personal_city"],
    },
    "state": {
        "audience_col": "profile_personal_state_audience",
        "value_keys": ["profile_personal_state"],
    },
    "image": {
        "audience_col": "profile_personal_image_audience",
        "value_keys": ["profile_personal_image"],
    },
    "tag_line": {
        "audience_col": "profile_personal_tag_line_audience",
        "value_keys": ["profile_personal_tag_line"],
        "is_public_key": "profile_personal_tag_line_is_public",
    },
    "short_bio": {
        "audience_col": "profile_personal_short_bio_audience",
        "value_keys": ["profile_personal_short_bio"],
    },
}


def _apply_search_row_visibility(rows, viewer_profile_uid, degree_map, relationships_map=None):
    """
    Null out any field on each search result row the searching viewer isn't
    allowed to see at their circle degree/relationship from that profile - the
    same audience rules set in Edit Profile, applied here the same way GET
    userprofileinfo applies them (profile_visibility.py), just against a batch
    of rows using precomputed viewer->target degree and relationship maps
    instead of one DB round trip per row.
    """
    relationships_map = relationships_map or {}
    for row in rows:
        row_uid = row.get("profile_personal_uid")
        is_owner_view = bool(viewer_profile_uid) and str(viewer_profile_uid) == str(row_uid)
        viewer_degree = degree_map.get(row_uid)
        viewer_relationships = relationships_map.get(row_uid)
        for cfg in _SEARCH_FIELD_GATES.values():
            if audience_visible_to_viewer(
                row.get(cfg["audience_col"]),
                viewer_degree,
                is_owner_view,
                False,
                viewer_relationships,
            ):
                continue
            for value_key in cfg["value_keys"]:
                if value_key in row:
                    row[value_key] = None
            if cfg.get("is_public_key") and cfg["is_public_key"] in row:
                row[cfg["is_public_key"]] = 0
    return rows


def _profile_search_clauses(parts, search_term):
    tagline_bio_clauses = [
        "(pp.profile_personal_tag_line_audience IS NOT NULL AND LOWER(COALESCE(pp.profile_personal_tag_line, '')) LIKE LOWER(%s))",
        "(pp.profile_personal_short_bio_audience IS NOT NULL AND LOWER(COALESCE(pp.profile_personal_short_bio, '')) LIKE LOWER(%s))",
    ]
    phone_clause = "(pp.profile_personal_phone_number_audience IS NOT NULL AND LOWER(COALESCE(pp.profile_personal_phone_number, '')) LIKE LOWER(%s))"

    if len(parts) >= 2:
        first_pattern = f"%{parts[0]}%"
        last_pattern = f"%{' '.join(parts[1:])}%"
        where_clauses = [
            """(
                LOWER(COALESCE(pp.profile_personal_first_name, '')) LIKE LOWER(%s)
                AND LOWER(COALESCE(pp.profile_personal_last_name, '')) LIKE LOWER(%s)
            )""",
            """LOWER(CONCAT(
                COALESCE(pp.profile_personal_first_name, ''),
                ' ',
                COALESCE(pp.profile_personal_last_name, '')
            )) LIKE LOWER(%s)""",
            "LOWER(COALESCE(pp.profile_personal_city, '')) LIKE LOWER(%s)",
            "LOWER(COALESCE(pp.profile_personal_state, '')) LIKE LOWER(%s)",
        ]
        params = [first_pattern, last_pattern, search_term, search_term, search_term]

        if len(parts) == 2:
            where_clauses.insert(
                1,
                """(
                    LOWER(COALESCE(pp.profile_personal_first_name, '')) LIKE LOWER(%s)
                    AND LOWER(COALESCE(pp.profile_personal_last_name, '')) LIKE LOWER(%s)
                )""",
            )
            params.insert(2, f"%{parts[1]}%")
            params.insert(3, f"%{parts[0]}%")
    else:
        where_clauses = [
            "LOWER(COALESCE(pp.profile_personal_first_name, '')) LIKE LOWER(%s)",
            "LOWER(COALESCE(pp.profile_personal_last_name, '')) LIKE LOWER(%s)",
            "LOWER(COALESCE(pp.profile_personal_city, '')) LIKE LOWER(%s)",
            "LOWER(COALESCE(pp.profile_personal_state, '')) LIKE LOWER(%s)",
        ]
        params = [search_term, search_term, search_term, search_term]

    where_clauses.append(phone_clause)
    params.append(search_term)
    where_clauses += tagline_bio_clauses
    params += [search_term, search_term]
    return where_clauses, params


def _circle_metadata_clauses(search_term):
    return [
        "LOWER(COALESCE(c.circle_note, '')) LIKE LOWER(%s)",
        "LOWER(COALESCE(c.circle_event, '')) LIKE LOWER(%s)",
        "LOWER(COALESCE(c.circle_introduced_by, '')) LIKE LOWER(%s)",
        "LOWER(COALESCE(c.circle_city, '')) LIKE LOWER(%s)",
        "LOWER(COALESCE(c.circle_state, '')) LIKE LOWER(%s)",
        "LOWER(COALESCE(c.circle_geotag, '')) LIKE LOWER(%s)",
        "LOWER(COALESCE(c.circle_relationship, '')) LIKE LOWER(%s)",
        "LOWER(COALESCE(CAST(c.circle_date AS CHAR), '')) LIKE LOWER(%s)",
    ], [search_term] * 8


def _merge_profile_rows(rows):
    merged = []
    seen = set()
    for row in rows or []:
        uid = row.get('profile_personal_uid')
        if not uid or uid in seen:
            continue
        seen.add(uid)
        merged.append(row)
    return merged


class SearchReferral(Resource):
    def get(self):
        """
        Search for users by profile fields and (optionally) connection metadata.
        Query params:
        - query: search term (single word searches name/city/state; multi-word searches first+last name)
        - profile_uid: when set, also search this user's circle notes, events, meeting location, etc.
        """
        try:
            from auth import get_current_profile_id, jwt_auth_required

            query = request.args.get('query', '').strip()
            profile_uid = request.args.get('profile_uid', '').strip()

            # Searching viewer, for degree-based field gating below: prefer the JWT identity;
            # fall back to the profile_uid query param (already sent by the app for circle-
            # metadata search) only when a JWT isn't required/present.
            viewer_profile_uid = get_current_profile_id() or (None if jwt_auth_required() else profile_uid or None)

            if not query or len(query) < 2:
                return {
                    'message': 'Search query must be at least 2 characters',
                    'code': 400,
                    'results': []
                }, 400

            parts = [part for part in query.split() if part]
            search_term = f"%{query}%"
            where_clauses, params = _profile_search_clauses(parts, search_term)

            search_query = f"""
                SELECT
                    {PROFILE_SELECT}
                FROM every_circle.profile_personal pp
                LEFT JOIN every_circle.users u ON pp.profile_personal_user_id = u.user_uid
                WHERE ({' OR '.join(where_clauses)})
                {deleted_profile_sql_clause('pp')}
                LIMIT 50
            """

            with connect() as db:
                response = db.execute(search_query, params)
                profile_rows = (response or {}).get('result') or []

                if profile_uid:
                    circle_clauses, circle_params = _circle_metadata_clauses(search_term)
                    profile_clauses, profile_params = _profile_search_clauses(parts, search_term)
                    combined_where = circle_clauses + profile_clauses
                    combined_params = circle_params + profile_params

                    circle_search_query = f"""
                        SELECT DISTINCT
                            {PROFILE_SELECT}
                        FROM every_circle.circles c
                        INNER JOIN every_circle.profile_personal pp
                            ON c.circle_related_person_id = pp.profile_personal_uid
                        LEFT JOIN every_circle.users u ON pp.profile_personal_user_id = u.user_uid
                        WHERE c.circle_profile_id = %s
                        AND ({' OR '.join(combined_where)})
                        LIMIT 50
                    """
                    circle_response = db.execute(
                        circle_search_query,
                        [profile_uid] + combined_params,
                    )
                    profile_rows = _merge_profile_rows(
                        profile_rows + ((circle_response or {}).get('result') or [])
                    )

                if profile_rows and viewer_profile_uid:
                    from business import compute_profile_degrees_from_viewer

                    target_uids = [r.get('profile_personal_uid') for r in profile_rows]
                    # Only degree1-3 affect gating, so cap the BFS at 3 hops - far cheaper
                    # than resolving each row's degree with its own ConnectionsPath lookup.
                    degree_map = compute_profile_degrees_from_viewer(db, viewer_profile_uid, target_uids, max_degree=3)

                    # One batch query for every result owner's circle_relationship with the
                    # viewer, instead of a resolve_viewer_relationships() round trip per row.
                    relationships_map = {}
                    placeholders = ','.join(['%s'] * len(target_uids))
                    rel_rows = db.execute(
                        f"""
                        SELECT circle_profile_id, circle_relationship
                        FROM every_circle.circles
                        WHERE circle_profile_id IN ({placeholders})
                          AND circle_related_person_id = %s
                          AND circle_relationship IS NOT NULL AND circle_relationship != ''
                        """,
                        target_uids + [viewer_profile_uid],
                    )
                    for r in (rel_rows or {}).get('result') or []:
                        relationships_map.setdefault(r['circle_profile_id'], set()).add(r['circle_relationship'])

                    _apply_search_row_visibility(profile_rows, viewer_profile_uid, degree_map, relationships_map)
                elif profile_rows:
                    # No known viewer (unauthenticated / no profile_uid sent): only Everyone-level
                    # fields are visible.
                    _apply_search_row_visibility(profile_rows, None, {})

            if profile_rows:
                print(f"Found {len(profile_rows)} results")
                return {
                    'message': 'Search results retrieved',
                    'code': 200,
                    'results': profile_rows,
                    'count': len(profile_rows)
                }, 200

            return {
                'message': 'No results found',
                'code': 200,
                'results': [],
                'count': 0
            }, 200

        except Exception as e:
            print(f"Error in SearchReferral: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'message': f'An error occurred: {str(e)}',
                'code': 500,
                'results': []
            }, 500
