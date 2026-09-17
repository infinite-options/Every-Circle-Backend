"""
Per-field connection-level privacy for profile_personal via *_audience JSON.

Each field has one nullable JSON column (e.g. profile_personal_email_audience):
    NULL  - private (only the owner / admins see the value)
    object - public to an audience:
        {"degree": "all" | 1 | 2 | 3, "circles": ["friend", "family", ...]}

degree:
    "all" - no hop-distance restriction
    N     - viewer must be at hop distance <= N (cumulative; 3 means 1st, 2nd, or 3rd)

circles:
    [] / omitted - no circle-relationship filter
    non-empty   - viewer must also be tagged by the owner as one of those types
                  (friend / colleague / family); ANDed with the degree gate

Legacy *_visibility / *_visibility_circles / *_visibility_degrees columns are
no longer written. On write we still derive companion *_is_public (and
location_is_public) so older SQL that gates on those flags keeps working
until those call sites are migrated.

Offering/wish *items* still use the older per-item visibility columns
(ITEM_VISIBILITY) until those tables get audience columns.
"""

from __future__ import annotations

import json

CIRCLE_TYPES = {"friend", "colleague", "family"}
VALID_DEGREES = {"all", 1, 2, 3}
# Legacy enum → cumulative max degree (or "all" / private)
_LEGACY_LEVEL_TO_DEGREE = {
    "everyone": "all",
    "specific": "all",
    "degree1": 1,
    "degree2": 2,
    "degree3": 3,
    "only_me": None,
}
DEGREE_LEVELS = {"degree1": 1, "degree2": 2, "degree3": 3}
VALID_LEVELS = {"everyone", "degree1", "degree2", "degree3", "specific", "only_me"}
DEGREE_NUMBERS = {"1", "2", "3"}

FIELD_VISIBILITY = {
    "phone_number": {
        "audience_col": "profile_personal_phone_number_audience",
        "is_public_col": "profile_personal_phone_number_is_public",
        "value_keys": ["profile_personal_phone_number"],
        # Legacy write-compat keys (converted into audience_col, then stripped)
        "visibility_col": "profile_personal_phone_number_visibility",
        "visibility_circles_col": "profile_personal_phone_number_visibility_circles",
        "visibility_degrees_col": "profile_personal_phone_number_visibility_degrees",
    },
    "email": {
        "audience_col": "profile_personal_email_audience",
        "is_public_col": "profile_personal_email_is_public",
        "value_keys": [],
        "visibility_col": "profile_personal_email_visibility",
        "visibility_circles_col": "profile_personal_email_visibility_circles",
        "visibility_degrees_col": "profile_personal_email_visibility_degrees",
    },
    "city": {
        "audience_col": "profile_personal_city_audience",
        "is_public_col": None,
        "value_keys": [
            "profile_personal_city",
            "profile_personal_country",
            "profile_personal_home_address",
            "profile_personal_latitude",
            "profile_personal_longitude",
        ],
        "visibility_col": "profile_personal_city_visibility",
        "visibility_circles_col": "profile_personal_city_visibility_circles",
        "visibility_degrees_col": "profile_personal_city_visibility_degrees",
    },
    "state": {
        "audience_col": "profile_personal_state_audience",
        "is_public_col": None,
        "value_keys": ["profile_personal_state"],
        "visibility_col": "profile_personal_state_visibility",
        "visibility_circles_col": "profile_personal_state_visibility_circles",
        "visibility_degrees_col": "profile_personal_state_visibility_degrees",
    },
    "tag_line": {
        "audience_col": "profile_personal_tag_line_audience",
        "is_public_col": "profile_personal_tag_line_is_public",
        "value_keys": ["profile_personal_tag_line"],
        "visibility_col": "profile_personal_tag_line_visibility",
        "visibility_circles_col": "profile_personal_tag_line_visibility_circles",
    },
    "short_bio": {
        "audience_col": "profile_personal_short_bio_audience",
        "is_public_col": "profile_personal_short_bio_is_public",
        "value_keys": ["profile_personal_short_bio"],
        "visibility_col": "profile_personal_short_bio_visibility",
        "visibility_circles_col": "profile_personal_short_bio_visibility_circles",
    },
    "image": {
        "audience_col": "profile_personal_image_audience",
        "is_public_col": "profile_personal_image_is_public",
        "value_keys": ["profile_personal_image"],
        "visibility_col": "profile_personal_image_visibility",
        "visibility_circles_col": "profile_personal_image_visibility_circles",
        "visibility_degrees_col": "profile_personal_image_visibility_degrees",
    },
    "resume": {
        "audience_col": "profile_personal_resume_audience",
        "is_public_col": "profile_personal_resume_is_public",
        "value_keys": ["profile_personal_resume"],
    },
    "experience": {
        "audience_col": "profile_personal_experience_audience",
        "is_public_col": "profile_personal_experience_is_public",
        "value_keys": [],
        "visibility_col": "profile_personal_experience_visibility",
    },
    "education": {
        "audience_col": "profile_personal_education_audience",
        "is_public_col": "profile_personal_education_is_public",
        "value_keys": [],
        "visibility_col": "profile_personal_education_visibility",
    },
    "expertise": {
        "audience_col": "profile_personal_expertise_audience",
        "is_public_col": "profile_personal_expertise_is_public",
        "value_keys": [],
        "visibility_col": "profile_personal_expertise_visibility",
        "visibility_circles_col": "profile_personal_expertise_visibility_circles",
    },
    "wishes": {
        "audience_col": "profile_personal_wishes_audience",
        "is_public_col": "profile_personal_wishes_is_public",
        "value_keys": [],
        "visibility_col": "profile_personal_wishes_visibility",
        "visibility_circles_col": "profile_personal_wishes_visibility_circles",
    },
    "business": {
        "audience_col": "profile_personal_business_audience",
        "is_public_col": "profile_personal_business_is_public",
        "value_keys": [],
        "visibility_col": "profile_personal_business_visibility",
    },
    "social": {
        "audience_col": "profile_personal_social_audience",
        "is_public_col": "profile_personal_social_is_public",
        "value_keys": [],
        "visibility_col": "profile_personal_social_visibility",
    },
}

AUDIENCE_COLUMNS = [cfg["audience_col"] for cfg in FIELD_VISIBILITY.values()]
_LEGACY_VISIBILITY_COLUMNS = (
    [cfg["visibility_col"] for cfg in FIELD_VISIBILITY.values() if cfg.get("visibility_col")]
    + [
        cfg["visibility_circles_col"]
        for cfg in FIELD_VISIBILITY.values()
        if cfg.get("visibility_circles_col")
    ]
    + [
        cfg["visibility_degrees_col"]
        for cfg in FIELD_VISIBILITY.values()
        if cfg.get("visibility_degrees_col")
    ]
)


def parse_audience(raw):
    """
    Normalize a DB/API audience value to a dict {"degree": ..., "circles": [...]}
    or None (private). Invalid shapes become None.
    """
    if raw is None or raw == "" or raw is False:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        raw = raw.strip()
        if not raw or raw.lower() in ("null", "none", "0", "false"):
            return None
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None
    if not isinstance(raw, dict):
        return None

    degree = raw.get("degree", "all")
    if degree == "all":
        pass
    else:
        try:
            degree = int(degree)
        except (TypeError, ValueError):
            return None
        if degree not in (1, 2, 3):
            return None

    circles_raw = raw.get("circles") or []
    if isinstance(circles_raw, str):
        circles_raw = [t.strip() for t in circles_raw.split(",") if t.strip()]
    if not isinstance(circles_raw, (list, tuple, set)):
        return None
    circles = sorted({str(t).strip() for t in circles_raw if str(t).strip() in CIRCLE_TYPES})
    return {"degree": degree, "circles": circles}


def audience_to_db(audience):
    """Serialize a parsed audience dict for MySQL JSON storage, or None."""
    parsed = parse_audience(audience)
    if parsed is None:
        return None
    return json.dumps(parsed, separators=(",", ":"))


def _normalize_circles_csv(raw_csv):
    types = {t.strip() for t in (raw_csv or "").split(",") if t.strip()}
    return ",".join(sorted(types & CIRCLE_TYPES))


def _normalize_degrees_csv(raw_csv):
    nums = {t.strip() for t in (raw_csv or "").split(",") if t.strip()}
    return ",".join(sorted(nums & DEGREE_NUMBERS))


def _audience_from_legacy(level, circles_csv=None, degrees_csv=None):
    """Convert old visibility + circles/degrees CSVs into audience dict or None."""
    if level not in VALID_LEVELS:
        return None
    if level == "only_me":
        return None

    circles = [t for t in _normalize_circles_csv(circles_csv).split(",") if t]
    if level == "specific" and not circles:
        return None

    degree = _LEGACY_LEVEL_TO_DEGREE.get(level, "all")
    if degree is None:
        return None

    # Old exact-set degrees CSV → cumulative max (degree N means <= N).
    if level != "everyone" and degrees_csv:
        nums = [int(n) for n in _normalize_degrees_csv(degrees_csv).split(",") if n]
        if not nums:
            # Degree checkboxes cleared without Everyone → private.
            if level in DEGREE_LEVELS:
                return None
        else:
            degree = max(nums)

    return parse_audience({"degree": degree, "circles": circles})


def _sync_location_legacy_flag(personal_info):
    """location_is_public = 1 when city or state audience is non-null."""
    city = parse_audience(personal_info.get("profile_personal_city_audience"))
    state = parse_audience(personal_info.get("profile_personal_state_audience"))
    if (
        "profile_personal_city_audience" not in personal_info
        and "profile_personal_state_audience" not in personal_info
    ):
        return
    personal_info["profile_personal_location_is_public"] = 1 if (city or state) else 0


def sync_audiences(personal_info):
    """
    Normalize *_audience on a PUT/POST personal_info dict, convert legacy
    visibility payloads when audience wasn't sent, strip legacy visibility
    columns so they are not written, and derive *_is_public / location_is_public.

    Mutates and returns personal_info.
    """
    if not personal_info:
        return personal_info

    for field_cfg in FIELD_VISIBILITY.values():
        audience_col = field_cfg["audience_col"]
        visibility_col = field_cfg.get("visibility_col")
        circles_col = field_cfg.get("visibility_circles_col")
        degrees_col = field_cfg.get("visibility_degrees_col")
        is_public_col = field_cfg.get("is_public_col")

        audience_submitted = audience_col in personal_info
        legacy_submitted = bool(visibility_col and visibility_col in personal_info)

        if audience_submitted:
            personal_info[audience_col] = audience_to_db(personal_info.get(audience_col))
        elif legacy_submitted:
            personal_info[audience_col] = audience_to_db(
                _audience_from_legacy(
                    personal_info.get(visibility_col),
                    personal_info.get(circles_col) if circles_col else None,
                    personal_info.get(degrees_col) if degrees_col else None,
                )
            )
        elif is_public_col and is_public_col in personal_info and audience_col not in personal_info:
            # Bare legacy boolean: public → everyone/no circles; private → NULL.
            try:
                is_pub = int(personal_info.get(is_public_col) or 0) == 1
            except (TypeError, ValueError):
                is_pub = False
            personal_info[audience_col] = (
                audience_to_db({"degree": "all", "circles": []}) if is_pub else None
            )

        # Drop legacy columns so db.insert/update never writes them.
        for legacy_col in (visibility_col, circles_col, degrees_col):
            if legacy_col:
                personal_info.pop(legacy_col, None)

        if audience_col in personal_info and is_public_col:
            personal_info[is_public_col] = (
                1 if parse_audience(personal_info.get(audience_col)) is not None else 0
            )

    _sync_location_legacy_flag(personal_info)
    return personal_info


# Back-compat alias for call sites still importing the old name.
sync_is_public_from_visibility = sync_audiences


def audience_visible_to_viewer(
    audience,
    viewer_degree,
    is_owner_view,
    viewer_is_admin,
    viewer_relationships=None,
):
    """True if this audience JSON permits the viewer."""
    if is_owner_view or viewer_is_admin:
        return True
    parsed = parse_audience(audience)
    if parsed is None:
        return False

    degree = parsed["degree"]
    if degree != "all":
        if viewer_degree is None or viewer_degree > int(degree):
            return False

    allowed = set(parsed.get("circles") or [])
    if not allowed:
        return True
    return bool(viewer_relationships) and not allowed.isdisjoint(viewer_relationships)


def field_visible_to_viewer(
    level,
    viewer_degree,
    is_owner_view,
    viewer_is_admin,
    viewer_relationships=None,
    allowed_types_csv=None,
    allowed_degrees_csv=None,
):
    """
    Legacy signature kept for offering/wish items and any remaining callers.
    Prefer audience_visible_to_viewer for profile_personal fields.
    """
    if is_owner_view or viewer_is_admin:
        return True
    if level not in VALID_LEVELS:
        level = "everyone"
    if level == "only_me":
        return False

    if level != "everyone":
        allowed_degrees = {d.strip() for d in (allowed_degrees_csv or "").split(",") if d.strip()}
        if allowed_degrees:
            # Historical exact-set checkboxes (items still use this).
            if viewer_degree is None or str(viewer_degree) not in allowed_degrees:
                return False
        elif level in DEGREE_LEVELS:
            max_degree = DEGREE_LEVELS[level]
            if viewer_degree is None or viewer_degree > max_degree:
                return False

    allowed = {t.strip() for t in (allowed_types_csv or "").split(",") if t.strip()}
    if not allowed:
        return level != "specific"
    return bool(viewer_relationships) and not allowed.isdisjoint(viewer_relationships)


def normalize_audiences_for_response(personal_info):
    """Ensure *_audience values on a response dict are parsed objects or null."""
    if not personal_info:
        return personal_info
    for col in AUDIENCE_COLUMNS:
        if col in personal_info:
            personal_info[col] = parse_audience(personal_info.get(col))
    return personal_info


def apply_profile_field_visibility(
    db, personal_info, profile_id, viewer_profile_uid, is_owner_view, viewer_is_admin
):
    """
    Null out values / flip *_is_public to 0 for fields the viewer isn't
    allowed to see. No-op for the owner or an admin.
    """
    if not personal_info or is_owner_view or viewer_is_admin:
        return personal_info

    viewer_degree = resolve_viewer_degree(db, profile_id, viewer_profile_uid)
    viewer_relationships = resolve_viewer_relationships(db, profile_id, viewer_profile_uid)

    for field_cfg in FIELD_VISIBILITY.values():
        if audience_visible_to_viewer(
            personal_info.get(field_cfg["audience_col"]),
            viewer_degree,
            is_owner_view,
            viewer_is_admin,
            viewer_relationships,
        ):
            continue
        if field_cfg["is_public_col"]:
            personal_info[field_cfg["is_public_col"]] = 0
        for value_key in field_cfg["value_keys"]:
            if value_key in personal_info:
                personal_info[value_key] = None

    city_ok = audience_visible_to_viewer(
        personal_info.get("profile_personal_city_audience"),
        viewer_degree,
        is_owner_view,
        viewer_is_admin,
        viewer_relationships,
    )
    state_ok = audience_visible_to_viewer(
        personal_info.get("profile_personal_state_audience"),
        viewer_degree,
        is_owner_view,
        viewer_is_admin,
        viewer_relationships,
    )
    personal_info["profile_personal_location_is_public"] = 1 if (city_ok or state_ok) else 0

    return personal_info


# Per-item (not per-profile-field) config for individual Offering / Seeking rows.
ITEM_VISIBILITY = {
    "expertise": {
        "visibility_col": "profile_expertise_visibility",
        "is_public_col": "profile_expertise_is_public",
        "visibility_circles_col": "profile_expertise_visibility_circles",
        "visibility_degrees_col": "profile_expertise_visibility_degrees",
    },
    "wish": {
        "visibility_col": "profile_wish_visibility",
        "is_public_col": "profile_wish_is_public",
        "visibility_circles_col": "profile_wish_visibility_circles",
        "visibility_degrees_col": "profile_wish_visibility_degrees",
    },
}


def sync_item_is_public_from_visibility(item, item_type):
    """Same legacy sync for a single Offering/Seeking item dict."""
    cfg = ITEM_VISIBILITY[item_type]
    visibility_col = cfg["visibility_col"]
    circles_col = cfg["visibility_circles_col"]
    degrees_col = cfg["visibility_degrees_col"]
    if visibility_col not in item:
        return item

    level = item[visibility_col]
    if level not in VALID_LEVELS:
        item.pop(visibility_col, None)
        return item

    normalized_circles = ""
    if circles_col in item:
        normalized_circles = _normalize_circles_csv(item.get(circles_col))
        item[circles_col] = normalized_circles

    normalized_degrees = ""
    if degrees_col in item:
        normalized_degrees = "" if level == "everyone" else _normalize_degrees_csv(item.get(degrees_col))
        item[degrees_col] = normalized_degrees

    if level == "only_me":
        is_visible = False
    elif level == "specific":
        is_visible = bool(normalized_circles)
    elif degrees_col in item and level != "everyone" and not normalized_degrees:
        is_visible = False
    else:
        is_visible = True

    item[cfg["is_public_col"]] = 1 if is_visible else 0
    return item


def item_visible_to_viewer(
    item, item_type, viewer_degree, is_owner_view, viewer_is_admin, viewer_relationships=None
):
    cfg = ITEM_VISIBILITY[item_type]
    level = item.get(cfg["visibility_col"]) or "everyone"
    allowed_csv = item.get(cfg["visibility_circles_col"])
    allowed_degrees_csv = item.get(cfg["visibility_degrees_col"])
    return field_visible_to_viewer(
        level,
        viewer_degree,
        is_owner_view,
        viewer_is_admin,
        viewer_relationships,
        allowed_csv,
        allowed_degrees_csv,
    )


def resolve_viewer_relationships(db, profile_id, viewer_profile_uid):
    if not profile_id or not viewer_profile_uid:
        return set()
    try:
        rows = db.execute(
            """
            SELECT circle_relationship FROM every_circle.circles
            WHERE circle_profile_id = %s AND circle_related_person_id = %s
              AND circle_relationship IS NOT NULL AND circle_relationship != ''
            """,
            (profile_id, viewer_profile_uid),
        )
        return {r["circle_relationship"] for r in (rows.get("result") or [])}
    except Exception as e:
        print(f"resolve_viewer_relationships failed for {profile_id} -> {viewer_profile_uid}: {e}")
        return set()


def resolve_viewer_degree(db, profile_id, viewer_profile_uid):
    if not viewer_profile_uid or not profile_id or viewer_profile_uid == profile_id:
        return 0 if viewer_profile_uid == profile_id else None

    try:
        from user_path_connection import ConnectionsPath

        path_response, path_status = ConnectionsPath().get(viewer_profile_uid, profile_id)
        if path_status == 200 and path_response.get("combined_path"):
            nodes = path_response["combined_path"].split(",")
            return int(len(nodes) - 1)
    except Exception as e:
        print(f"resolve_viewer_degree failed for {viewer_profile_uid} -> {profile_id}: {e}")

    return None
