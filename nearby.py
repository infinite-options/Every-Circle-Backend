from flask_restful import Resource
from flask import request
from data_ec import connect
import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# --- Configurable constants (mirror these in the frontend) ---
LOCATION_EXPIRY_HOURS = 1     # how long a stored location stays "fresh"
NEARBY_RADIUS_METERS  = 1609  # 1 mile

# Normalise frontend plural labels (friends / colleagues / family) to DB values
RELATIONSHIP_MAP = {
    'friends':    'friend',
    'colleagues': 'colleague',
    'family':     'family',
}


def _norm_types(types_list):
    """Map frontend plural labels to DB relationship values."""
    return [RELATIONSHIP_MAP.get(t, t) for t in (types_list or [])]


def _consent_clause():
    """
    SQL AND-clause that enforces pp's stored share_with preference.
    Must be appended to a query that already has pp aliased to profile_personal.
    Consumes 2 consecutive %s args = (viewer_uid, viewer_uid).
    """
    return """
        AND (
            COALESCE(pp.profile_personal_nearby_share_with, 'all_circles') = 'everyone'
            OR (
                COALESCE(pp.profile_personal_nearby_share_with, 'all_circles') = 'all_circles'
                AND EXISTS (
                    SELECT 1 FROM every_circle.circles c_consent
                    WHERE c_consent.circle_profile_id        = pp.profile_personal_uid
                      AND c_consent.circle_related_person_id = %s
                      AND c_consent.circle_relationship IS NOT NULL
                      AND c_consent.circle_relationship != ''
                )
            )
            OR (
                pp.profile_personal_nearby_share_with = 'specific'
                AND EXISTS (
                    SELECT 1 FROM every_circle.circles c_consent
                    WHERE c_consent.circle_profile_id        = pp.profile_personal_uid
                      AND c_consent.circle_related_person_id = %s
                      AND c_consent.circle_relationship IS NOT NULL
                      AND FIND_IN_SET(
                            c_consent.circle_relationship,
                            COALESCE(pp.profile_personal_nearby_share_types, '')
                          ) > 0
                )
            )
        )
    """


def _build_share_query(share_with, share_with_types, profile_uid, lat, lng):
    """
    Build a query that finds people to NOTIFY about the updater's location.
    Returns (sql_string, args_tuple).

    For each recipient the query also fetches:
      • recipient_relationship  — how THEY have labelled the sender in their circles
      • recipient_in_circles    — 1 if they have the sender in their circles, 0 otherwise
    These fields are forwarded in the Ably payload so each recipient's frontend can
    apply its own receiveFrom filter (Option A — frontend drops unwanted messages).

    TODO: For share_with='everyone' this scans all users with a fresh location, which
          may be expensive at scale.  Consider moving the fan-out to a dedicated
          notification service / pre-computed index.
    """
    db_types    = _norm_types(share_with_types) if share_with == 'specific' else []
    placeholders = ', '.join(['%s'] * len(db_types)) if db_types else ''

    dist_col = f"""ST_Distance_Sphere(
                    POINT(pp.profile_personal_nearby_lng, pp.profile_personal_nearby_lat),
                    POINT(%s, %s)
                ) AS distance_meters"""

    if share_with == 'everyone':
        # Notify ALL nearby users regardless of circle membership.
        # Left-join so we still know how each recipient has labelled the sender.
        query = f"""
            SELECT
                pp.profile_personal_uid           AS recipient_uid,
                pp.profile_personal_first_name,
                pp.profile_personal_last_name,
                pp.profile_personal_image,
                rc.circle_relationship            AS recipient_relationship,
                (rc.circle_profile_id IS NOT NULL
                 AND rc.circle_relationship IS NOT NULL
                 AND rc.circle_relationship != '') AS recipient_in_circles,
                {dist_col}
            FROM every_circle.profile_personal pp
            LEFT JOIN every_circle.circles rc
                ON  rc.circle_profile_id        = pp.profile_personal_uid
                AND rc.circle_related_person_id = %s
            WHERE pp.profile_personal_uid != %s
              AND pp.profile_personal_nearby_lat IS NOT NULL
              AND pp.profile_personal_nearby_updated_at > NOW() - INTERVAL {LOCATION_EXPIRY_HOURS} HOUR
            HAVING distance_meters < {NEARBY_RADIUS_METERS}
        """
        args = (lng, lat, profile_uid, profile_uid)
        return query, args

    # all_circles or specific: only people the sender has in their circles
    # For all_circles, exclude connections with no relationship assigned (NULL or empty string).
    rel_filter = f"AND c.circle_relationship IN ({placeholders})" if db_types else "AND c.circle_relationship IS NOT NULL AND c.circle_relationship != ''"
    query = f"""
        SELECT
            pp.profile_personal_uid           AS recipient_uid,
            pp.profile_personal_first_name,
            pp.profile_personal_last_name,
            pp.profile_personal_image,
            rc.circle_relationship            AS recipient_relationship,
            (rc.circle_profile_id IS NOT NULL
             AND rc.circle_relationship IS NOT NULL
             AND rc.circle_relationship != '') AS recipient_in_circles,
            {dist_col}
        FROM every_circle.circles c
        JOIN every_circle.profile_personal pp
            ON pp.profile_personal_uid = c.circle_related_person_id
        LEFT JOIN every_circle.circles rc
            ON  rc.circle_profile_id        = pp.profile_personal_uid
            AND rc.circle_related_person_id = %s
        WHERE c.circle_profile_id = %s
          AND pp.profile_personal_nearby_lat IS NOT NULL
          AND pp.profile_personal_nearby_updated_at > NOW() - INTERVAL {LOCATION_EXPIRY_HOURS} HOUR
          {rel_filter}
        HAVING distance_meters < {NEARBY_RADIUS_METERS}
    """
    args = (lng, lat, profile_uid, profile_uid) + tuple(db_types)
    return query, args


def _build_receive_query(receive_from, receive_from_types, profile_uid, lat, lng):
    """
    Build a query that finds nearby users to send AS notifications TO the updater.
    Returns (sql_string, args_tuple).
    """
    db_types     = _norm_types(receive_from_types) if receive_from == 'specific' else []
    placeholders = ', '.join(['%s'] * len(db_types)) if db_types else ''

    dist_col = f"""ST_Distance_Sphere(
                    POINT(pp.profile_personal_nearby_lng, pp.profile_personal_nearby_lat),
                    POINT(%s, %s)
                ) AS distance_meters"""

    consent = _consent_clause()

    if receive_from == 'everyone':
        query = f"""
            SELECT
                pp.profile_personal_uid       AS nearby_uid,
                pp.profile_personal_first_name,
                pp.profile_personal_last_name,
                pp.profile_personal_image,
                mc.circle_relationship        AS my_relationship,
                {dist_col}
            FROM every_circle.profile_personal pp
            LEFT JOIN every_circle.circles mc
                ON  mc.circle_profile_id        = %s
                AND mc.circle_related_person_id = pp.profile_personal_uid
            WHERE pp.profile_personal_uid != %s
              AND pp.profile_personal_nearby_lat IS NOT NULL
              AND pp.profile_personal_nearby_updated_at > NOW() - INTERVAL {LOCATION_EXPIRY_HOURS} HOUR
              {consent}
            HAVING distance_meters < {NEARBY_RADIUS_METERS}
        """
        # consent_clause needs profile_uid twice
        args = (lng, lat, profile_uid, profile_uid, profile_uid, profile_uid)
        return query, args

    # all_circles or specific
    # For all_circles, exclude connections with no relationship assigned (NULL or empty string).
    rel_filter = f"AND c.circle_relationship IN ({placeholders})" if db_types else "AND c.circle_relationship IS NOT NULL AND c.circle_relationship != ''"
    query = f"""
        SELECT
            pp.profile_personal_uid       AS nearby_uid,
            pp.profile_personal_first_name,
            pp.profile_personal_last_name,
            pp.profile_personal_image,
            c.circle_relationship         AS my_relationship,
            {dist_col}
        FROM every_circle.circles c
        JOIN every_circle.profile_personal pp
            ON pp.profile_personal_uid = c.circle_related_person_id
        WHERE c.circle_profile_id = %s
          AND pp.profile_personal_nearby_lat IS NOT NULL
          AND pp.profile_personal_nearby_updated_at > NOW() - INTERVAL {LOCATION_EXPIRY_HOURS} HOUR
          {rel_filter}
          {consent}
        HAVING distance_meters < {NEARBY_RADIUS_METERS}
    """
    # args: dist_col(lng,lat), viewer circle join, rel_filter types, consent uid×2
    args = (lng, lat, profile_uid) + tuple(db_types) + (profile_uid, profile_uid)
    return query, args


def _publish_nearby_alerts(
    profile_uid, lat, lng,
    share_with='all_circles', share_with_types=None,
    receive_from='all_circles', receive_from_types=None,
):
    """
    Publish nearby-alert Ably messages based on the user's sharing preferences.

    share_with / share_with_types  — who gets told the updater is nearby
    receive_from / receive_from_types — who the updater hears about

    Each payload includes `recipient_relationship` and `recipient_in_circles` so
    the recipient's frontend can apply its own receiveFrom filter (Option A).
    """
    try:
        import ably as ably_lib

        # 1. Get the updater's profile info
        with connect() as db:
            sender_resp = db.execute(
                """
                SELECT profile_personal_first_name,
                       profile_personal_last_name,
                       profile_personal_image
                FROM every_circle.profile_personal
                WHERE profile_personal_uid = %s
                """,
                args=(profile_uid,)
            )
        sender = (sender_resp.get('result') or [{}])[0]
        sender_name = (
            f"{sender.get('profile_personal_first_name') or ''} "
            f"{sender.get('profile_personal_last_name') or ''}"
        ).strip() or 'Someone'

        # 2. Build + run both queries
        share_query,   share_args   = _build_share_query(
            share_with,   share_with_types   or [], profile_uid, lat, lng)
        receive_query, receive_args = _build_receive_query(
            receive_from, receive_from_types or [], profile_uid, lat, lng)

        with connect() as db:
            share_resp   = db.execute(share_query,   args=share_args)
            receive_resp = db.execute(receive_query, args=receive_args)

        recipients    = share_resp.get('result')   or []
        nearby_for_me = receive_resp.get('result') or []

        if not recipients and not nearby_for_me:
            return

        api_key = os.getenv('ABLY_API_KEY', '')
        if not api_key:
            print('ABLY_API_KEY not set — skipping nearby alerts')
            return

        import asyncio

        async def _publish_all():
            async with ably_lib.AblyRest(api_key) as client:

                # 2a. Tell each recipient that the updater is nearby.
                #     source='share' means the sender explicitly chose to share with this
                #     person — the frontend should always show these, never filter them out.
                for r in recipients:
                    recipient_uid   = r.get('recipient_uid')
                    distance_meters = float(r.get('distance_meters') or 0)
                    distance_miles  = round(distance_meters / 1609.34, 2)
                    payload = {
                        'type':                  'nearby-alert',
                        'source':                'share',
                        'sender_uid':            profile_uid,
                        'sender_name':           sender_name,
                        'sender_image':          sender.get('profile_personal_image'),
                        'distance_meters':       round(distance_meters, 1),
                        'distance_miles':        distance_miles,
                        # Forwarded so the recipient's frontend can apply receiveFrom filter
                        # (only relevant when share_with='everyone' — see frontend handler)
                        'recipient_relationship': r.get('recipient_relationship'),
                        'recipient_in_circles':   bool(r.get('recipient_in_circles')),
                    }
                    channel = client.channels.get(f'/{recipient_uid}')
                    await channel.publish('nearby-alert', payload)
                    print(f'Notified {recipient_uid}: {sender_name} is {distance_miles} mi away')

                # 2b. Tell the updater about each nearby person they want to hear from.
                #     source='receive' — the updater's receiveFrom preference governs these.
                for p in nearby_for_me:
                    nearby_uid      = p.get('nearby_uid')
                    distance_meters = float(p.get('distance_meters') or 0)
                    distance_miles  = round(distance_meters / 1609.34, 2)
                    nearby_name     = (
                        f"{p.get('profile_personal_first_name') or ''} "
                        f"{p.get('profile_personal_last_name') or ''}"
                    ).strip() or 'Someone'
                    payload = {
                        'type':            'nearby-alert',
                        'source':          'receive',
                        'sender_uid':      nearby_uid,
                        'sender_name':     nearby_name,
                        'sender_image':    p.get('profile_personal_image'),
                        'distance_meters': round(distance_meters, 1),
                        'distance_miles':  distance_miles,
                        # How the updater has labelled this nearby person (their own circles)
                        'recipient_relationship': p.get('my_relationship'),
                        'recipient_in_circles':   True,
                    }
                    channel = client.channels.get(f'/{profile_uid}')
                    await channel.publish('nearby-alert', payload)
                    print(f'Notified updater {profile_uid}: {nearby_name} is {distance_miles} mi away')

                # [DISABLED] Old approach: queried users who had the updater in THEIR circles.
                # Now only people the updater has added (their own circle) are considered,
                # and the share_with / receive_from settings give fine-grained control.
                # notify_others_query = f"""
                #     SELECT
                #         c.circle_profile_id  AS recipient_uid,
                #         pp.profile_personal_first_name,
                #         pp.profile_personal_last_name,
                #         pp.profile_personal_image,
                #         ST_Distance_Sphere(
                #             POINT(pp.profile_personal_nearby_lng, pp.profile_personal_nearby_lat),
                #             POINT(%s, %s)
                #         ) AS distance_meters
                #     FROM every_circle.circles c
                #     JOIN every_circle.profile_personal pp
                #         ON pp.profile_personal_uid = c.circle_profile_id
                #     WHERE c.circle_related_person_id = %s
                #       AND pp.profile_personal_nearby_lat IS NOT NULL
                #       AND pp.profile_personal_nearby_updated_at > NOW() - INTERVAL {LOCATION_EXPIRY_HOURS} HOUR
                #     HAVING distance_meters < {NEARBY_RADIUS_METERS}
                # """

        asyncio.run(_publish_all())

    except Exception as e:
        print(f'Error in _publish_nearby_alerts: {e}')


class NearbyLocation(Resource):
    """PATCH /api/v1/nearby/location
    Updates the ephemeral nearby-location fields for a user.
    Body (JSON): {
        profile_uid, lat, lng,
        live_sharing?,       -- triggers Ably notifications when true
        share_with?,         -- 'everyone' | 'all_circles' | 'specific'
        share_with_types?,   -- list of types when share_with='specific'
        receive_from?,       -- 'everyone' | 'all_circles' | 'specific'
        receive_from_types?, -- list of types when receive_from='specific'
    }
    """

    def patch(self):
        data = request.get_json(silent=True) or {}
        profile_uid  = data.get('profile_uid')
        lat          = data.get('lat')
        lng          = data.get('lng')
        live_sharing = data.get('live_sharing', False)

        share_with         = data.get('share_with',         'all_circles')
        share_with_types   = data.get('share_with_types',   [])
        receive_from       = data.get('receive_from',       'all_circles')
        receive_from_types = data.get('receive_from_types', [])

        if not profile_uid:
            return {'message': 'profile_uid is required', 'code': 400}, 400

        prefs_only = lat is None and lng is None
        if not prefs_only and (lat is None or lng is None):
            return {'message': 'Both lat and lng are required when updating location', 'code': 400}, 400

        db_share_types = ','.join(_norm_types(share_with_types)) if share_with == 'specific' and share_with_types else None

        fields = {
            'profile_personal_nearby_share_with':  share_with,
            'profile_personal_nearby_share_types': db_share_types,
        }

        if not prefs_only:
            updated_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            fields.update({
                'profile_personal_nearby_lat':        lat,
                'profile_personal_nearby_lng':        lng,
                'profile_personal_nearby_updated_at': updated_at,
            })

        with connect() as db:
            result = db.update(
                'every_circle.profile_personal',
                {'profile_personal_uid': profile_uid},
                fields,
            )

        if result.get('code') not in (200, 201):
            return {'message': 'Failed to update', 'code': 500}, 500

        if not prefs_only and live_sharing:
            _publish_nearby_alerts(
                profile_uid, lat, lng,
                share_with=share_with,
                share_with_types=share_with_types,
                receive_from=receive_from,
                receive_from_types=receive_from_types,
            )

        return {
            'message':    'Preferences updated' if prefs_only else 'Location updated',
            'code':       200,
            'updated_at': None if prefs_only else updated_at,
        }, 200


class NearbyUsers(Resource):
    """GET /api/v1/nearby/<profile_uid>
    Returns nearby users based on the caller's visibility preferences.
    Query params:
        mode  — 'everyone' | 'all_circles' | 'specific'  (default: 'all_circles')
        types — comma-separated relationship types when mode='specific'
                e.g. types=friends,colleagues
    """

    def get(self, profile_uid):
        mode       = request.args.get('mode', 'all_circles')
        types_raw  = request.args.get('types', '')
        types_list = [t.strip() for t in types_raw.split(',') if t.strip()] if types_raw else []

        with connect() as db:
            user_resp = db.execute(
                """
                SELECT profile_personal_nearby_lat,
                       profile_personal_nearby_lng,
                       profile_personal_nearby_updated_at
                FROM every_circle.profile_personal
                WHERE profile_personal_uid = %s
                """,
                args=(profile_uid,)
            )

        if not user_resp.get('result'):
            return {'message': 'User not found', 'code': 404}, 404

        user = user_resp['result'][0]

        if user['profile_personal_nearby_lat'] is None:
            return {
                'message': 'No location set. Please update your location first.',
                'code': 400,
                'result': []
            }, 200

        # Freshness check
        updated_at = user['profile_personal_nearby_updated_at']
        if isinstance(updated_at, str):
            updated_at = datetime.datetime.strptime(updated_at, '%Y-%m-%d %H:%M:%S')

        expiry_threshold = datetime.datetime.utcnow() - datetime.timedelta(hours=LOCATION_EXPIRY_HOURS)
        if updated_at < expiry_threshold:
            return {
                'message': f'Your location has expired (older than {LOCATION_EXPIRY_HOURS}h). Please update it.',
                'code': 410,
                'result': []
            }, 200

        user_lat = float(user['profile_personal_nearby_lat'])
        user_lng = float(user['profile_personal_nearby_lng'])

        # Build query based on mode
        db_types     = _norm_types(types_list) if mode == 'specific' else []
        placeholders = ', '.join(['%s'] * len(db_types)) if db_types else ''
        # For all_circles, exclude connections with no relationship assigned (NULL or empty string).
        rel_filter   = f"AND c.circle_relationship IN ({placeholders})" if db_types else "AND c.circle_relationship IS NOT NULL AND c.circle_relationship != ''"

        dist_col = f"""ST_Distance_Sphere(
                    POINT(pp.profile_personal_nearby_lng, pp.profile_personal_nearby_lat),
                    POINT(%s, %s)
                ) AS distance_meters"""

        # Server-side consent: reads pp's stored share_with preference.
        # _consent_clause() needs viewer profile_uid passed twice as args.
        consent_clause = _consent_clause()

        if mode == 'everyone':
            # Viewer wants to see everyone, but we still respect each person's share_with.
            nearby_query = f"""
                SELECT
                    pp.profile_personal_uid,
                    pp.profile_personal_first_name,
                    pp.profile_personal_last_name,
                    pp.profile_personal_image,
                    pp.profile_personal_nearby_updated_at,
                    mc.circle_relationship,
                    {dist_col}
                FROM every_circle.profile_personal pp
                LEFT JOIN every_circle.circles mc
                    ON  mc.circle_profile_id        = %s
                    AND mc.circle_related_person_id = pp.profile_personal_uid
                WHERE pp.profile_personal_uid != %s
                  AND pp.profile_personal_nearby_lat IS NOT NULL
                  AND pp.profile_personal_nearby_updated_at > NOW() - INTERVAL {LOCATION_EXPIRY_HOURS} HOUR
                  {consent_clause}
                HAVING distance_meters < {NEARBY_RADIUS_METERS}
                ORDER BY distance_meters ASC
            """
            # consent_clause needs profile_uid twice (for each EXISTS)
            args = (user_lng, user_lat, profile_uid, profile_uid, profile_uid, profile_uid)
        else:
            # all_circles / specific: viewer's receiveFrom filter (rel_filter on viewer's circles)
            # combined with the nearby person's stored share_with consent check.
            nearby_query = f"""
                SELECT
                    pp.profile_personal_uid,
                    pp.profile_personal_first_name,
                    pp.profile_personal_last_name,
                    pp.profile_personal_image,
                    pp.profile_personal_nearby_updated_at,
                    c.circle_relationship,
                    {dist_col}
                FROM every_circle.profile_personal pp
                JOIN every_circle.circles c
                    ON  c.circle_related_person_id = pp.profile_personal_uid
                    AND c.circle_profile_id        = %s
                WHERE pp.profile_personal_nearby_lat IS NOT NULL
                  AND pp.profile_personal_nearby_updated_at > NOW() - INTERVAL {LOCATION_EXPIRY_HOURS} HOUR
                  {rel_filter}
                  {consent_clause}
                HAVING distance_meters < {NEARBY_RADIUS_METERS}
                ORDER BY distance_meters ASC
            """
            # args: dist_col(lng,lat), viewer JOIN, rel_filter types, consent_clause uid×2
            args = (user_lng, user_lat, profile_uid) + tuple(db_types) + (profile_uid, profile_uid)

        with connect() as db:
            nearby_resp = db.execute(nearby_query, args=args)

        nearby = nearby_resp.get('result', [])
        for row in nearby:
            if row.get('distance_meters') is not None:
                row['distance_meters'] = float(row['distance_meters'])

        return {
            'message':       'Success',
            'code':          200,
            'result':        nearby,
            'expiry_hours':  LOCATION_EXPIRY_HOURS,
            'radius_meters': NEARBY_RADIUS_METERS,
        }, 200
