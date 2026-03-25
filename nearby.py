from flask_restful import Resource
from flask import request
from data_ec import connect
import datetime

# --- Configurable constants (change these to test different behaviour) ---
LOCATION_EXPIRY_HOURS = 1     # how long a stored location stays "fresh"
NEARBY_RADIUS_METERS  = 1609  # 1 mile


class NearbyLocation(Resource):
    """PATCH /api/v1/nearby/location
    Updates the ephemeral nearby-location fields for a user.
    Body (JSON): { profile_uid, lat, lng }
    """

    def patch(self):
        data = request.get_json(silent=True) or {}
        profile_uid = data.get('profile_uid')
        lat         = data.get('lat')
        lng         = data.get('lng')

        if not profile_uid or lat is None or lng is None:
            return {'message': 'profile_uid, lat, and lng are required', 'code': 400}, 400

        updated_at = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        with connect() as db:
            result = db.update(
                'every_circle.profile_personal',
                {'profile_personal_uid': profile_uid},
                {
                    'profile_personal_nearby_lat':        lat,
                    'profile_personal_nearby_lng':        lng,
                    'profile_personal_nearby_updated_at': updated_at,
                }
            )

        if result.get('code') not in (200, 201):
            return {'message': 'Failed to update location', 'code': 500}, 500

        return {'message': 'Location updated', 'code': 200, 'updated_at': updated_at}, 200


class NearbyUsers(Resource):
    """GET /api/v1/nearby/<profile_uid>
    Returns circle members within NEARBY_RADIUS_METERS whose location is
    fresher than LOCATION_EXPIRY_HOURS.
    """

    def get(self, profile_uid):
        with connect() as db:
            # 1. Fetch the requesting user's current nearby location
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

        # 2. Check freshness of the user's own location
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

        # 3. Find circle members within radius with a fresh location
        # Constants are f-stringed (safe — they are hardcoded Python values, not user input).
        # User-supplied coordinates are parameterised via %s.
        nearby_query = f"""
            SELECT
                pp.profile_personal_uid,
                pp.profile_personal_first_name,
                pp.profile_personal_last_name,
                pp.profile_personal_image,
                pp.profile_personal_nearby_updated_at,
                ST_Distance_Sphere(
                    POINT(pp.profile_personal_nearby_lng, pp.profile_personal_nearby_lat),
                    POINT(%s, %s)
                ) AS distance_meters
            FROM every_circle.profile_personal pp
            JOIN every_circle.circles c
                ON c.circle_related_person_id = pp.profile_personal_uid
            WHERE c.circle_profile_id = %s
              AND pp.profile_personal_nearby_lat IS NOT NULL
              AND pp.profile_personal_nearby_updated_at > NOW() - INTERVAL {LOCATION_EXPIRY_HOURS} HOUR
            HAVING distance_meters < {NEARBY_RADIUS_METERS}
            ORDER BY distance_meters ASC
        """

        with connect() as db:
            nearby_resp = db.execute(nearby_query, args=(user_lng, user_lat, profile_uid))

        nearby = nearby_resp.get('result', [])

        # Convert distance to float for clean JSON serialisation
        for row in nearby:
            if row.get('distance_meters') is not None:
                row['distance_meters'] = float(row['distance_meters'])

        return {
            'message': 'Success',
            'code': 200,
            'result': nearby,
            'expiry_hours': LOCATION_EXPIRY_HOURS,
            'radius_meters': NEARBY_RADIUS_METERS,
        }, 200
