from flask_restful import Resource
from data_ec import connect


def _safe_float(value):
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalize_business_row(row):
    lat = _safe_float(row.get("business_latitude"))
    lng = _safe_float(row.get("business_longitude"))
    if lat is None or lng is None:
        return None
    if lat < -90 or lat > 90 or lng < -180 or lng > 180:
        return None

    google_id = (row.get("business_google_id") or "").strip()
    if not google_id:
        return None

    return {
        "business_uid": row.get("business_uid"),
        "business_name": row.get("business_name"),
        "business_google_id": google_id,
        "business_latitude": lat,
        "business_longitude": lng,
        "business_address_line_1": row.get("business_address_line_1"),
        "business_city": row.get("business_city"),
        "business_state": row.get("business_state"),
        "business_profile_img": row.get("business_profile_img"),
    }


class BusinessMap(Resource):
    """Businesses registered on Every Circle with a Google place id and coordinates."""

    def get(self):
        print("In BusinessMap GET")
        try:
            with connect() as db:
                query = """
                    SELECT
                        business_uid,
                        business_name,
                        business_google_id,
                        business_latitude,
                        business_longitude,
                        business_address_line_1,
                        business_city,
                        business_state,
                        business_profile_img
                    FROM every_circle.business
                    WHERE business_google_id IS NOT NULL
                      AND TRIM(business_google_id) != ''
                      AND business_latitude IS NOT NULL
                      AND business_longitude IS NOT NULL
                      AND business_is_active = 1
                    ORDER BY business_name
                """
                result = db.execute(query)
                rows = result.get("result") or []

                businesses = []
                for row in rows:
                    normalized = _normalize_business_row(row)
                    if normalized:
                        businesses.append(normalized)

                return {"result": businesses, "count": len(businesses), "code": 200}, 200
        except Exception as e:
            print(f"Error in BusinessMap GET: {e}")
            return {"message": "Internal Server Error", "code": 500}, 500
