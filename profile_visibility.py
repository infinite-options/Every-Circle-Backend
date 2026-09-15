"""
Per-field connection-level privacy for profile_personal.

Each of the fields below can be set by the owner to one of:
    'everyone'  - visible to any viewer
    'degree1'   - visible to viewers at circle degree <= 1 (direct connections)
    'degree2'   - visible to viewers at circle degree <= 2
    'degree3'   - visible to viewers at circle degree <= 3
    'specific'  - visible to viewers the owner has tagged with one of the
                  circle_relationship types listed in the field's companion
                  *_visibility_circles CSV column ('friend'/'colleague'/
                  'family' - or a mix). Personal-info fields only (see
                  FIELD_VISIBILITY's visibility_circles_col below); mirrors
                  Messages Privacy / Nearby Location Privacy's "Specific
                  Circles" option (chat.py's _audience_allows,
                  nearby.py's _consent_clause).
    'only_me'   - visible only to the owner (and admins)

Degree is the referral-tree hop distance already used elsewhere in the app as
"Level N Connection" (see ratings.py's circle_num_nodes and
user_path_connection.ConnectionsPath).

FIELD_VISIBILITY maps each logical field to:
    - the *_visibility enum column that is the source of truth
    - the *_is_public boolean column kept in sync for legacy readers, or None
      when no 1:1 legacy column exists for this field (city/state - see
      _sync_location_legacy_flag below)
    - the *_visibility_circles CSV column (personal-info fields only; None
      for section-level fields, which don't offer the 'specific' level)
    - the personal_info value key(s) to null out when the field is hidden
      (section-level fields have no value keys - only their _is_public flag
      is toggled, since the frontend gates the whole section on that flag)

City and state are independent fields (their own picker each), but legacy
code only ever had a single combined profile_personal_location_is_public
flag. That flag is kept in sync as "at least one of city/state is visible"
(_sync_location_legacy_flag) so legacy readers keep seeing a location line
rather than losing it outright; the precise per-field gating below is what
actually governs GET userprofileinfo.
"""

DEGREE_LEVELS = {"degree1": 1, "degree2": 2, "degree3": 3}
VALID_LEVELS = {"everyone", "degree1", "degree2", "degree3", "specific", "only_me"}
CIRCLE_TYPES = {"friend", "colleague", "family"}

FIELD_VISIBILITY = {
    "phone_number": {
        "visibility_col": "profile_personal_phone_number_visibility",
        "is_public_col": "profile_personal_phone_number_is_public",
        "visibility_circles_col": "profile_personal_phone_number_visibility_circles",
        "value_keys": ["profile_personal_phone_number"],
    },
    "email": {
        "visibility_col": "profile_personal_email_visibility",
        "is_public_col": "profile_personal_email_is_public",
        "visibility_circles_col": "profile_personal_email_visibility_circles",
        # Email itself lives on every_circle.users, not personal_info; only the flag applies here.
        "value_keys": [],
    },
    "city": {
        "visibility_col": "profile_personal_city_visibility",
        "is_public_col": None,  # see _sync_location_legacy_flag
        "visibility_circles_col": "profile_personal_city_visibility_circles",
        "value_keys": [
            "profile_personal_city",
            "profile_personal_country",
            "profile_personal_home_address",
            "profile_personal_latitude",
            "profile_personal_longitude",
        ],
    },
    "state": {
        "visibility_col": "profile_personal_state_visibility",
        "is_public_col": None,  # see _sync_location_legacy_flag
        "visibility_circles_col": "profile_personal_state_visibility_circles",
        "value_keys": ["profile_personal_state"],
    },
    "tag_line": {
        "visibility_col": "profile_personal_tag_line_visibility",
        "is_public_col": "profile_personal_tag_line_is_public",
        "visibility_circles_col": "profile_personal_tag_line_visibility_circles",
        "value_keys": ["profile_personal_tag_line"],
    },
    "short_bio": {
        "visibility_col": "profile_personal_short_bio_visibility",
        "is_public_col": "profile_personal_short_bio_is_public",
        "visibility_circles_col": "profile_personal_short_bio_visibility_circles",
        "value_keys": ["profile_personal_short_bio"],
    },
    "image": {
        "visibility_col": "profile_personal_image_visibility",
        "is_public_col": "profile_personal_image_is_public",
        "visibility_circles_col": "profile_personal_image_visibility_circles",
        "value_keys": ["profile_personal_image"],
    },
    "experience": {
        "visibility_col": "profile_personal_experience_visibility",
        "is_public_col": "profile_personal_experience_is_public",
        "value_keys": [],
    },
    "education": {
        "visibility_col": "profile_personal_education_visibility",
        "is_public_col": "profile_personal_education_is_public",
        "value_keys": [],
    },
    "expertise": {
        "visibility_col": "profile_personal_expertise_visibility",
        "is_public_col": "profile_personal_expertise_is_public",
        "visibility_circles_col": "profile_personal_expertise_visibility_circles",
        "value_keys": [],
    },
    "wishes": {
        "visibility_col": "profile_personal_wishes_visibility",
        "is_public_col": "profile_personal_wishes_is_public",
        "visibility_circles_col": "profile_personal_wishes_visibility_circles",
        "value_keys": [],
    },
    "business": {
        "visibility_col": "profile_personal_business_visibility",
        "is_public_col": "profile_personal_business_is_public",
        "value_keys": [],
    },
    "social": {
        "visibility_col": "profile_personal_social_visibility",
        "is_public_col": "profile_personal_social_is_public",
        "value_keys": [],
    },
}


def _sync_location_legacy_flag(personal_info):
    """
    City and state each get their own visibility level but share one legacy
    profile_personal_location_is_public column - keep it truthy whenever
    either one is visible to at least Everyone/some degree, so old code that
    still reads the single flag keeps showing a location line.
    """
    city_level = personal_info.get("profile_personal_city_visibility")
    state_level = personal_info.get("profile_personal_state_visibility")
    if city_level is None and state_level is None:
        return
    visible = (city_level not in (None, "only_me")) or (state_level not in (None, "only_me"))
    personal_info["profile_personal_location_is_public"] = 1 if visible else 0


def _normalize_circles_csv(raw_csv):
    """Validate/dedupe a submitted circles CSV down to known circle_relationship values."""
    types = {t.strip() for t in (raw_csv or "").split(",") if t.strip()}
    return ",".join(sorted(types & CIRCLE_TYPES))


def sync_is_public_from_visibility(personal_info):
    """
    For any *_visibility field present in a PUT/POST payload dict, validate it
    and set the companion legacy *_is_public boolean from it, so the two never
    drift apart. Mutates and returns personal_info. Invalid levels are dropped
    (left unset) rather than trusted.
    """
    for field_cfg in FIELD_VISIBILITY.values():
        visibility_col = field_cfg["visibility_col"]
        circles_col = field_cfg.get("visibility_circles_col")
        if visibility_col not in personal_info:
            continue
        level = personal_info[visibility_col]
        if level not in VALID_LEVELS:
            personal_info.pop(visibility_col, None)
            continue

        if level == "specific" and circles_col:
            normalized = _normalize_circles_csv(personal_info.get(circles_col))
            personal_info[circles_col] = normalized
            is_visible = bool(normalized)
        else:
            if circles_col and circles_col in personal_info:
                personal_info.pop(circles_col, None)
            is_visible = level != "only_me"

        if field_cfg["is_public_col"]:
            personal_info[field_cfg["is_public_col"]] = 1 if is_visible else 0
    _sync_location_legacy_flag(personal_info)
    return personal_info


# Per-item (not per-profile-field) config for individual Offering (profile_expertise) and
# Seeking (profile_wish) entries - same everyone/degree1-3/specific/only_me levels as
# personal-info fields, via the generic sync_item_is_public_from_visibility /
# item_visible_to_viewer helpers below instead of a FIELD_VISIBILITY-style value_keys list
# (callers null the whole item out of the response themselves when it isn't visible).
ITEM_VISIBILITY = {
    "expertise": {
        "visibility_col": "profile_expertise_visibility",
        "is_public_col": "profile_expertise_is_public",
        "visibility_circles_col": "profile_expertise_visibility_circles",
    },
    "wish": {
        "visibility_col": "profile_wish_visibility",
        "is_public_col": "profile_wish_is_public",
        "visibility_circles_col": "profile_wish_visibility_circles",
    },
}


def sync_item_is_public_from_visibility(item, item_type):
    """
    Same as sync_is_public_from_visibility but for a single Offering/Seeking
    item dict (item_type is 'expertise' or 'wish'). No-op if the item's
    visibility column wasn't submitted. Mutates and returns item.
    """
    cfg = ITEM_VISIBILITY[item_type]
    visibility_col = cfg["visibility_col"]
    circles_col = cfg["visibility_circles_col"]
    if visibility_col not in item:
        return item

    level = item[visibility_col]
    if level not in VALID_LEVELS:
        item.pop(visibility_col, None)
        return item

    if level == "specific":
        normalized = _normalize_circles_csv(item.get(circles_col))
        item[circles_col] = normalized
        is_visible = bool(normalized)
    else:
        if circles_col in item:
            item.pop(circles_col, None)
        is_visible = level != "only_me"

    item[cfg["is_public_col"]] = 1 if is_visible else 0
    return item


def item_visible_to_viewer(item, item_type, viewer_degree, is_owner_view, viewer_is_admin, viewer_relationships=None):
    """
    True if an Offering/Seeking item's visibility level permits this viewer.
    Independent of moderation - callers should AND this with their own
    moderation check (is_offering_publicly_visible / is_wish_publicly_visible
    already run first in user_profile_info.py's _filter_and_enrich_* helpers).
    """
    cfg = ITEM_VISIBILITY[item_type]
    level = item.get(cfg["visibility_col"]) or "everyone"
    allowed_csv = item.get(cfg["visibility_circles_col"])
    return field_visible_to_viewer(level, viewer_degree, is_owner_view, viewer_is_admin, viewer_relationships, allowed_csv)


def resolve_viewer_relationships(db, profile_id, viewer_profile_uid):
    """
    circle_relationship values profile_id's owner has recorded for
    viewer_profile_uid (empty set if none/unknown). Same direction as
    chat.py's _circle_relationship_types / nearby.py's _consent_clause: only
    the profile owner's own circle tagging of the viewer counts.
    """
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
    """
    Circle-degree distance between viewer_profile_uid and profile_id, or None
    if it could not be determined. Always computed live via ConnectionsPath's
    referral-tree path (fast in practice: it prefers each side's precomputed
    profile_personal_path column over the recursive-CTE fallback).

    Deliberately does NOT use every_circle.circles.circle_num_nodes as a
    shortcut (unlike ratings.py's display-only sort key, which can tolerate
    a stale value): that column is set directly from whatever circle_num_nodes
    the client sends on circle creation (circles.py POST), not computed
    server-side, and was found stale/wrong for most existing rows - trusting
    it here let a real 2nd-degree viewer read fields gated to 1st-degree-only
    whenever a stale circles row understated their actual degree.
    """
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


def field_visible_to_viewer(
    level, viewer_degree, is_owner_view, viewer_is_admin, viewer_relationships=None, allowed_types_csv=None
):
    """True if a field set to `level` should be shown to this viewer."""
    if is_owner_view or viewer_is_admin:
        return True
    if level not in VALID_LEVELS:
        return level == "everyone"
    if level == "only_me":
        return False
    if level == "everyone":
        return True
    if level == "specific":
        allowed = {t.strip() for t in (allowed_types_csv or "").split(",") if t.strip()}
        return bool(allowed) and bool(viewer_relationships) and not allowed.isdisjoint(viewer_relationships)
    max_degree = DEGREE_LEVELS[level]
    return viewer_degree is not None and viewer_degree <= max_degree


def apply_profile_field_visibility(
    db, personal_info, profile_id, viewer_profile_uid, is_owner_view, viewer_is_admin
):
    """
    Null out values / flip *_is_public to 0 for fields the viewer isn't
    allowed to see at their circle degree (or circle relationship, for
    'specific'-level personal-info fields). No-op for the owner or an admin.
    Mutates and returns personal_info.
    """
    if not personal_info or is_owner_view or viewer_is_admin:
        return personal_info

    viewer_degree = resolve_viewer_degree(db, profile_id, viewer_profile_uid)
    viewer_relationships = resolve_viewer_relationships(db, profile_id, viewer_profile_uid)

    def _visible(level, circles_col):
        allowed_csv = personal_info.get(circles_col) if circles_col else None
        return field_visible_to_viewer(
            level, viewer_degree, is_owner_view, viewer_is_admin, viewer_relationships, allowed_csv
        )

    for field_cfg in FIELD_VISIBILITY.values():
        level = personal_info.get(field_cfg["visibility_col"]) or "everyone"
        if _visible(level, field_cfg.get("visibility_circles_col")):
            continue
        if field_cfg["is_public_col"]:
            personal_info[field_cfg["is_public_col"]] = 0
        for value_key in field_cfg["value_keys"]:
            if value_key in personal_info:
                personal_info[value_key] = None

    # Recompute the legacy combined flag from THIS viewer's actual city/state
    # visibility (degree/circle-aware, unlike the raw level check
    # _sync_location_legacy_flag uses for save-time syncing), so old code
    # reading the single flag doesn't think a gated location is visible to a
    # viewer who doesn't qualify.
    city_level = personal_info.get("profile_personal_city_visibility") or "everyone"
    state_level = personal_info.get("profile_personal_state_visibility") or "everyone"
    location_visible = _visible(city_level, "profile_personal_city_visibility_circles") or _visible(
        state_level, "profile_personal_state_visibility_circles"
    )
    personal_info["profile_personal_location_is_public"] = 1 if location_visible else 0

    return personal_info
