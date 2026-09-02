from flask import request
from flask_restful import Resource
from data_ec import connect
from profile_status import deleted_profile_sql_clause

PROFILE_SELECT = """
    pp.profile_personal_uid,
    pp.profile_personal_user_id,
    pp.profile_personal_first_name,
    pp.profile_personal_last_name,
    CASE WHEN pp.profile_personal_email_is_public = 1
        THEN u.user_email_id
        ELSE NULL
    END as profile_email_id,
    CASE WHEN pp.profile_personal_phone_number_is_public = 1
        THEN pp.profile_personal_phone_number
        ELSE NULL
    END as profile_personal_phone_number,
    CASE WHEN pp.profile_personal_location_is_public = 1
        THEN pp.profile_personal_city
        ELSE NULL
    END as profile_personal_city,
    CASE WHEN pp.profile_personal_location_is_public = 1
        THEN pp.profile_personal_state
        ELSE NULL
    END as profile_personal_state,
    CASE WHEN pp.profile_personal_image_is_public = 1
        THEN pp.profile_personal_image
        ELSE NULL
    END as profile_personal_image,
    CASE WHEN pp.profile_personal_tag_line_is_public = 1
        THEN pp.profile_personal_tag_line
        ELSE NULL
    END as profile_personal_tag_line,
    pp.profile_personal_tag_line_is_public,
    CASE WHEN pp.profile_personal_short_bio_is_public = 1
        THEN pp.profile_personal_short_bio
        ELSE NULL
    END as profile_personal_short_bio
"""


def _profile_search_clauses(parts, search_term):
    tagline_bio_clauses = [
        "(pp.profile_personal_tag_line_is_public = 1 AND LOWER(COALESCE(pp.profile_personal_tag_line, '')) LIKE LOWER(%s))",
        "(pp.profile_personal_short_bio_is_public = 1 AND LOWER(COALESCE(pp.profile_personal_short_bio, '')) LIKE LOWER(%s))",
    ]
    phone_clause = "(pp.profile_personal_phone_number_is_public = 1 AND LOWER(COALESCE(pp.profile_personal_phone_number, '')) LIKE LOWER(%s))"

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
            query = request.args.get('query', '').strip()
            profile_uid = request.args.get('profile_uid', '').strip()

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
