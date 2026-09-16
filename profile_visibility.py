"""
Per-field connection-level privacy for profile_personal.

Each field's *_visibility enum sets a base state:
    'everyone'  - no degree restriction (mutually exclusive with any degree
                  selection in the UI - picking Everyone clears them)
    'degree1'/'degree2'/'degree3' - legacy cumulative cap ("this degree or
                  closer"), read only when the field has no
                  visibility_degrees_col value saved (see below)
    'only_me'   - visible only to the owner (and admins) - always wins,
                  ignores any degree/circle filter below
    'specific'  - legacy-only value from before degree+circle could combine;
                  read as 'everyone' with the circle filter applied

For fields with a visibility_degrees_col (the ones whose picker offers
degree checkboxes: phone, email, city, state, image, and individual
offering/wish items), a non-empty *_visibility_degrees CSV of exact degree
numbers ('1'/'2'/'3' - any combination, e.g. '1,3' skipping 2nd degree) is
the real degree filter, checked by exact membership rather than a cumulative
cap - this lets an owner pick 1st AND 3rd degree while excluding 2nd, which
a single "up to degree N" cap can't express. An empty/absent CSV falls back
to the legacy enum's cumulative degree1/2/3 reading, so rows saved before
this existed keep working unchanged.

Independently (fields with a visibility_circles_col - personal-info fields
only), a non-empty *_visibility_circles CSV ('friend'/'colleague'/'family' -
or a mix) ANDs an additional circle_relationship filter on top of the degree
gate: the viewer must satisfy the degree filter AND be tagged by the owner as
one of the listed circle types. An empty/absent CSV means no circle filter -
degree alone decides. So degrees=[2] + circles=['family'] means "2nd degree
connections who are also tagged Family"; 'everyone' + circles=['family']
means "any Family member regardless of degree" (what 'specific' used to mean
on its own); degrees=[2] with no circles means plain "2nd degree, any
relation". Mirrors Messages Privacy / Nearby Location Privacy's "Specific
Circles" option (chat.py's _audience_allows, nearby.py's _consent_clause),
generalized to compose with degree instead of being a separate exclusive
choice.

Degree is the referral-tree hop distance already used elsewhere in the app as
"Level N Connection" (see ratings.py's circle_num_nodes and
user_path_connection.ConnectionsPath).

FIELD_VISIBILITY maps each logical field to:
    - the *_visibility enum column that is the source of truth for the
      everyone/only_me/legacy-cumulative-degree state
    - the *_is_public boolean column kept in sync for legacy readers, or None
      when no 1:1 legacy column exists for this field (city/state - see
      _sync_location_legacy_flag below)
    - the *_visibility_circles CSV column (fields with the full picker only)
    - the *_visibility_degrees CSV column (same fields; None where absent)
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
DEGREE_NUMBERS = {"1", "2", "3"}

FIELD_VISIBILITY = {
    "phone_number": {
        "visibility_col": "profile_personal_phone_number_visibility",
        "is_public_col": "profile_personal_phone_number_is_public",
        "visibility_circles_col": "profile_personal_phone_number_visibility_circles",
        "visibility_degrees_col": "profile_personal_phone_number_visibility_degrees",
        "value_keys": ["profile_personal_phone_number"],
    },
    "email": {
        "visibility_col": "profile_personal_email_visibility",
        "is_public_col": "profile_personal_email_is_public",
        "visibility_circles_col": "profile_personal_email_visibility_circles",
        "visibility_degrees_col": "profile_personal_email_visibility_degrees",
        # Email itself lives on every_circle.users, not personal_info; only the flag applies here.
        "value_keys": [],
    },
    "city": {
        "visibility_col": "profile_personal_city_visibility",
        "is_public_col": None,  # see _sync_location_legacy_flag
        "visibility_circles_col": "profile_personal_city_visibility_circles",
        "visibility_degrees_col": "profile_personal_city_visibility_degrees",
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
        "visibility_degrees_col": "profile_personal_state_visibility_degrees",
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
        "visibility_degrees_col": "profile_personal_image_visibility_degrees",
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


def _normalize_degrees_csv(raw_csv):
    """Validate/dedupe a submitted degrees CSV down to '1'/'2'/'3', sorted."""
    nums = {t.strip() for t in (raw_csv or "").split(",") if t.strip()}
    return ",".join(sorted(nums & DEGREE_NUMBERS))


def sync_is_public_from_visibility(personal_info):
    """
    For any *_visibility field present in a PUT/POST payload dict, validate it
    and set the companion legacy *_is_public boolean from it, so the two never
    drift apart. Mutates and returns personal_info. Invalid levels are dropped
    (left unset) rather than trusted.

    The circles/degrees CSVs (when the field submits them) are independent
    filters that AND with whatever base level was chosen, rather than being
    tied to a single exclusive value - so they're normalized/stored whenever
    present, regardless of level.
    """
    for field_cfg in FIELD_VISIBILITY.values():
        visibility_col = field_cfg["visibility_col"]
        circles_col = field_cfg.get("visibility_circles_col")
        degrees_col = field_cfg.get("visibility_degrees_col")
        if visibility_col not in personal_info:
            continue
        level = personal_info[visibility_col]
        if level not in VALID_LEVELS:
            personal_info.pop(visibility_col, None)
            continue

        normalized_circles = ""
        if circles_col and circles_col in personal_info:
            normalized_circles = _normalize_circles_csv(personal_info.get(circles_col))
            personal_info[circles_col] = normalized_circles

        normalized_degrees = ""
        if degrees_col and degrees_col in personal_info:
            # Everyone is mutually exclusive with a degree selection - clear it here too,
            # even if the client sent one, so the two columns never disagree.
            normalized_degrees = "" if level == "everyone" else _normalize_degrees_csv(personal_info.get(degrees_col))
            personal_info[degrees_col] = normalized_degrees

        if level == "only_me":
            is_visible = False
        elif level == "specific":
            # Legacy exclusive value: visible only if it actually has circles to match.
            is_visible = bool(normalized_circles)
        elif degrees_col and degrees_col in personal_info and level != "everyone" and not normalized_degrees:
            # Degree checkboxes were all cleared without picking Everyone - no valid audience.
            is_visible = False
        else:
            is_visible = True

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
    """
    Same as sync_is_public_from_visibility but for a single Offering/Seeking
    item dict (item_type is 'expertise' or 'wish'). No-op if the item's
    visibility column wasn't submitted. Mutates and returns item.
    """
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
    allowed_degrees_csv = item.get(cfg["visibility_degrees_col"])
    return field_visible_to_viewer(
        level, viewer_degree, is_owner_view, viewer_is_admin, viewer_relationships, allowed_csv, allowed_degrees_csv
    )


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
    level,
    viewer_degree,
    is_owner_view,
    viewer_is_admin,
    viewer_relationships=None,
    allowed_types_csv=None,
    allowed_degrees_csv=None,
):
    """
    True if a field set to `level` (+ optional allowed_types_csv circle
    filter and allowed_degrees_csv exact-degree filter) should be shown to
    this viewer. The degree gate and circle filter are independent and ANDed
    together: a viewer must satisfy both when both are set. 'specific' is the
    legacy exclusive value - equivalent to 'everyone' with the circle filter
    applied. 'everyone' always ignores allowed_degrees_csv (mutually
    exclusive with a degree selection in the UI).
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
            # New exact-set checkboxes take over entirely when present.
            if viewer_degree is None or str(viewer_degree) not in allowed_degrees:
                return False
        elif level in DEGREE_LEVELS:
            # No degrees CSV saved for this field - fall back to the legacy cumulative cap.
            max_degree = DEGREE_LEVELS[level]
            if viewer_degree is None or viewer_degree > max_degree:
                return False

    allowed = {t.strip() for t in (allowed_types_csv or "").split(",") if t.strip()}
    if not allowed:
        # 'specific' with no circles left to match has no possible audience.
        return level != "specific"
    return bool(viewer_relationships) and not allowed.isdisjoint(viewer_relationships)


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

    def _visible(level, circles_col, degrees_col=None):
        allowed_csv = personal_info.get(circles_col) if circles_col else None
        allowed_degrees_csv = personal_info.get(degrees_col) if degrees_col else None
        return field_visible_to_viewer(
            level, viewer_degree, is_owner_view, viewer_is_admin, viewer_relationships, allowed_csv, allowed_degrees_csv
        )

    for field_cfg in FIELD_VISIBILITY.values():
        level = personal_info.get(field_cfg["visibility_col"]) or "everyone"
        if _visible(level, field_cfg.get("visibility_circles_col"), field_cfg.get("visibility_degrees_col")):
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
    location_visible = _visible(
        city_level, "profile_personal_city_visibility_circles", "profile_personal_city_visibility_degrees"
    ) or _visible(state_level, "profile_personal_state_visibility_circles", "profile_personal_state_visibility_degrees")
    personal_info["profile_personal_location_is_public"] = 1 if location_visible else 0

    return personal_info
