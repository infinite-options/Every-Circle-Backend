import os

from flask import request
from flask_restful import Resource
from datetime import datetime

from data_ec import connect, deleteFolder, processImage, processDocument, processSingleImageUpload
from moderation import (
    MODERATED_ACKNOWLEDGED,
    MODERATED_PENDING_REVIEW,
    MODERATED_TAKEN_DOWN,
    build_business_moderation_metadata,
    build_offering_moderation_metadata,
    build_user_moderation_metadata,
    build_wish_moderation_metadata,
    can_offering_be_edited,
    can_user_profile_be_edited,
    can_wish_be_edited,
    get_user_moderated_value,
    is_offering_publicly_visible,
    is_wish_publicly_visible,
)


_EXPERTISE_PREFIX = "profile_expertise_"
_WISH_PREFIX = "profile_wish_"
# S3 key prefixes used by processSingleImageUpload for offering / seeking images.
_EXPERTISE_S3_PREFIX = "profile_expertise"
_WISH_S3_PREFIX = "profile_wish"
_EXPERIENCE_S3_PREFIX = "profile_experience"
_EDUCATION_S3_PREFIX = "profile_education"

_OFFERING_RETURNABLE_COLUMNS_READY = False


def _ensure_offering_returnable_columns(db):
    """Add is_returnable on expertise/wish offerings when missing."""
    global _OFFERING_RETURNABLE_COLUMNS_READY
    if _OFFERING_RETURNABLE_COLUMNS_READY:
        return
    db.execute(
        "ALTER TABLE every_circle.profile_expertise "
        "ADD COLUMN profile_expertise_is_returnable TINYINT(1) NULL DEFAULT 1",
        cmd="post",
    )
    db.execute(
        "ALTER TABLE every_circle.profile_wish "
        "ADD COLUMN profile_wish_is_returnable TINYINT(1) NULL DEFAULT 1",
        cmd="post",
    )
    _OFFERING_RETURNABLE_COLUMNS_READY = True


def _delete_expertise_s3_assets(expertise_uid):
    try:
        deleteFolder(_EXPERTISE_S3_PREFIX, expertise_uid)
    except Exception as e:
        print(f"[EXPERTISE S3] Error deleting assets for {expertise_uid}: {e}")


def _delete_wish_s3_assets(wish_uid):
    try:
        deleteFolder(_WISH_S3_PREFIX, wish_uid)
    except Exception as e:
        print(f"[WISH S3] Error deleting assets for {wish_uid}: {e}")


def _delete_experience_s3_assets(experience_uid):
    try:
        deleteFolder(_EXPERIENCE_S3_PREFIX, experience_uid)
    except Exception as e:
        print(f"[EXPERIENCE S3] Error deleting assets for {experience_uid}: {e}")


def _delete_education_s3_assets(education_uid):
    try:
        deleteFolder(_EDUCATION_S3_PREFIX, education_uid)
    except Exception as e:
        print(f"[EDUCATION S3] Error deleting assets for {education_uid}: {e}")


def _expertise_dict_from_payload(exp_data):
    """Map offering (expertise) JSON keys to profile_expertise columns."""
    m = {}
    if "title" in exp_data:
        m["profile_expertise_title"] = exp_data["title"]
    elif "name" in exp_data:
        m["profile_expertise_title"] = exp_data["name"]
    if "description" in exp_data:
        m["profile_expertise_description"] = exp_data["description"]
    if "cost" in exp_data:
        m["profile_expertise_cost"] = exp_data["cost"]
    if "bounty" in exp_data:
        m["profile_expertise_bounty"] = exp_data["bounty"]
    if "quantity" in exp_data:
        m["profile_expertise_quantity"] = exp_data["quantity"]
    if "isPublic" in exp_data:
        m["profile_expertise_is_public"] = exp_data["isPublic"]
    _set_if_present(m, exp_data, "profile_expertise_details", "details")
    _set_if_present(m, exp_data, "profile_expertise_cost_currency", "costCurrency")
    _set_if_present(m, exp_data, "profile_expertise_is_taxable", "isTaxable")
    _set_if_present(m, exp_data, "profile_expertise_tax_rate", "taxRate")
    _set_if_present(m, exp_data, "profile_expertise_refund_policy", "refundPolicy")
    _set_if_present(m, exp_data, "profile_expertise_return_window_days", "returnWindowDays")
    _set_if_present(m, exp_data, "profile_expertise_is_returnable", "isReturnable")
    if "startDateTime" in exp_data:
        m["profile_expertise_start"] = exp_data["startDateTime"]
    elif "start" in exp_data:
        m["profile_expertise_start"] = exp_data["start"]
    if "endDateTime" in exp_data:
        m["profile_expertise_end"] = exp_data["endDateTime"]
    elif "end" in exp_data:
        m["profile_expertise_end"] = exp_data["end"]
    if "location" in exp_data:
        m["profile_expertise_location"] = exp_data["location"]
    if "latitude" in exp_data:
        m["profile_expertise_latitude"] = exp_data["latitude"]
    if "longitude" in exp_data:
        m["profile_expertise_longitude"] = exp_data["longitude"]
    if "city" in exp_data:
        m["profile_expertise_city"] = exp_data["city"]
    if "state" in exp_data:
        m["profile_expertise_state"] = exp_data["state"]
    if "mode" in exp_data:
        m["profile_expertise_mode"] = exp_data["mode"]
    for k, v in exp_data.items():
        if k.startswith(_EXPERTISE_PREFIX) and k not in (
            "profile_expertise_uid",
            "profile_expertise_profile_personal_id",
            "profile_expertise_moderated",
        ):
            m[k] = v
    return m


def _wish_dict_from_payload(wish_data):
    """Map seeking (wish) JSON keys to profile_wish columns."""
    m = {}
    if "title" in wish_data:
        m["profile_wish_title"] = wish_data["title"]
    elif "helpNeeds" in wish_data:
        m["profile_wish_title"] = wish_data["helpNeeds"]
    if "description" in wish_data:
        m["profile_wish_description"] = wish_data["description"]
    elif "details" in wish_data:
        m["profile_wish_description"] = wish_data["details"]
    if "bounty" in wish_data:
        m["profile_wish_bounty"] = wish_data["bounty"]
    elif "amount" in wish_data:
        m["profile_wish_bounty"] = wish_data["amount"]
    if "cost" in wish_data:
        m["profile_wish_cost"] = wish_data["cost"]
    if "quantity" in wish_data:
        m["profile_wish_quantity"] = wish_data["quantity"]
    if "isPublic" in wish_data:
        m["profile_wish_is_public"] = wish_data["isPublic"]
    _set_if_present(m, wish_data, "profile_wish_cost_currency", "costCurrency")
    _set_if_present(m, wish_data, "profile_wish_is_taxable", "isTaxable")
    _set_if_present(m, wish_data, "profile_wish_tax_rate", "taxRate")
    _set_if_present(m, wish_data, "profile_wish_refund_policy", "refundPolicy")
    _set_if_present(m, wish_data, "profile_wish_return_window_days", "returnWindowDays")
    _set_if_present(m, wish_data, "profile_wish_is_returnable", "isReturnable")
    if "startDateTime" in wish_data:
        m["profile_wish_start"] = wish_data["startDateTime"]
    elif "start" in wish_data:
        m["profile_wish_start"] = wish_data["start"]
    if "endDateTime" in wish_data:
        m["profile_wish_end"] = wish_data["endDateTime"]
    elif "end" in wish_data:
        m["profile_wish_end"] = wish_data["end"]
    if "location" in wish_data:
        m["profile_wish_location"] = wish_data["location"]
    if "profile_wish_latitude" in wish_data:
        m["profile_wish_latitude"] = wish_data["profile_wish_latitude"]
    if "profile_wish_longitude" in wish_data:
        m["profile_wish_longitude"] = wish_data["profile_wish_longitude"]
    if "profile_wish_city" in wish_data:
        m["profile_wish_city"] = wish_data["profile_wish_city"]
    if "profile_wish_state" in wish_data:
        m["profile_wish_state"] = wish_data["profile_wish_state"]
    if "mode" in wish_data:
        m["profile_wish_mode"] = wish_data["mode"]
    for k, v in wish_data.items():
        if k.startswith(_WISH_PREFIX) and k not in (
            "profile_wish_uid",
            "profile_wish_profile_personal_id",
            "profile_wish_moderated",
        ):
            m[k] = v
    return m


def _set_if_present(target, src, db_key, client_key):
    if client_key in src:
        target[db_key] = src[client_key]


def _normalize_coordinate_fields(personal_info):
    """Blank latitude/longitude strings must store as NULL, not "" (which parses to NaN client-side)."""
    for coord_field in ("profile_personal_latitude", "profile_personal_longitude"):
        if personal_info.get(coord_field) == "":
            personal_info[coord_field] = None


def _stamp_messages_off_timestamp(personal_info):
    """
    When the global "turn off messages" flag is being switched on, record the server time it
    happened. Chat message history filtering uses this cutoff so only messages sent AFTER
    muting are hidden from the recipient — earlier messages remain visible on both sides.
    """
    if str(personal_info.get('profile_personal_messages_off')) in ('1', 'True', 'true'):
        personal_info['profile_personal_messages_off_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _normalize_record_uid(value):
    """Treat missing/blank UID as absent so PUT creates rows instead of update-by-empty-id."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.strip() or None
    return str(value).strip() or None


def _db_write_succeeded(res):
    return bool(res) and res.get("code") == 200


def _viewer_context_from_request(profile_id):
    """Resolve optional viewer identity for expertise moderation visibility."""
    viewer_profile_uid = (request.args.get("viewer_profile_uid") or "").strip() or None
    viewer_is_admin = _form_truthy_public(request.args.get("viewer_is_admin"))
    is_owner_view = bool(
        viewer_profile_uid and str(viewer_profile_uid) == str(profile_id)
    )
    return viewer_profile_uid, viewer_is_admin, is_owner_view


def _filter_and_enrich_expertise_info(
    db,
    expertise_rows,
    profile_id,
    viewer_profile_uid=None,
    viewer_is_admin=False,
    owner_moderated=None,
):
    """Hide moderated offerings from public viewers; attach moderation metadata for owners."""
    if not expertise_rows:
        return []

    if owner_moderated is None:
        owner_moderated = get_user_moderated_value(db, profile_id)

    visible = []
    is_owner_view = bool(
        viewer_profile_uid and str(viewer_profile_uid) == str(profile_id)
    )
    for row in expertise_rows:
        if not is_offering_publicly_visible(
            row,
            viewer_profile_uid=viewer_profile_uid,
            viewer_is_admin=viewer_is_admin,
            owner_moderated=owner_moderated,
        ):
            continue
        item = dict(row)
        if is_owner_view:
            item["moderation"] = build_offering_moderation_metadata(
                db, item.get("profile_expertise_uid")
            )
        visible.append(item)
    return visible


def _filter_and_enrich_wish_info(
    db,
    wish_rows,
    profile_id,
    viewer_profile_uid=None,
    viewer_is_admin=False,
    owner_moderated=None,
):
    """Hide moderated seeking posts from public viewers; attach moderation metadata for owners."""
    if not wish_rows:
        return []

    if owner_moderated is None:
        owner_moderated = get_user_moderated_value(db, profile_id)

    visible = []
    is_owner_view = bool(
        viewer_profile_uid and str(viewer_profile_uid) == str(profile_id)
    )
    for row in wish_rows:
        if not is_wish_publicly_visible(
            row,
            viewer_profile_uid=viewer_profile_uid,
            viewer_is_admin=viewer_is_admin,
            owner_moderated=owner_moderated,
        ):
            continue
        item = dict(row)
        if is_owner_view:
            item["moderation"] = build_wish_moderation_metadata(
                db, item.get("profile_wish_uid")
            )
        visible.append(item)
    return visible


def _filter_and_enrich_business_info(db, business_rows, is_owner_view, viewer_is_admin=False):
    """
    Hide moderated businesses from non-owner viewers; attach moderation metadata
    (and let the owner still see them) so the owner's own profile can show the same
    pending_review / taken_down status banner used for offerings and seeking posts.

    - acknowledged (3): hidden from everyone, including the owner
    - pending_review (2) / taken_down (1): hidden from other viewers; owner still sees
      the row with a "moderation" object attached
    """
    if not business_rows:
        return []

    visible = []
    for row in business_rows:
        moderated = int(row.get("business_moderated") or 0)
        if moderated == MODERATED_ACKNOWLEDGED and not viewer_is_admin:
            continue
        if (
            moderated in (MODERATED_TAKEN_DOWN, MODERATED_PENDING_REVIEW)
            and not is_owner_view
            and not viewer_is_admin
        ):
            continue
        item = dict(row)
        if is_owner_view or viewer_is_admin:
            item["moderation"] = build_business_moderation_metadata(
                db, item.get("business_uid")
            )
        visible.append(item)
    return visible


def _enrich_personal_info_for_owner(db, personal_info, profile_id, is_owner_view):
    """Attach user moderation metadata on login / owner profile views."""
    if not personal_info or not is_owner_view:
        return personal_info

    enriched = dict(personal_info)
    enriched.pop("profile_personal_moderated", None)
    enriched["moderation"] = build_user_moderation_metadata(db, profile_id)
    return enriched


def _strip_personal_moderated_fields(personal_info):
    personal_info.pop("profile_personal_moderated", None)


def _strip_expertise_moderated_fields(expertise_info):
    expertise_info.pop("profile_expertise_moderated", None)


def _enforce_moderated_is_public(existing_row, expertise_info):
    """Prevent owners from making a moderated offering public via PUT."""
    moderated = int(existing_row.get("profile_expertise_moderated") or 0)
    if moderated in (
        MODERATED_TAKEN_DOWN,
        MODERATED_PENDING_REVIEW,
        MODERATED_ACKNOWLEDGED,
    ):
        expertise_info["profile_expertise_is_public"] = 0


def _strip_wish_moderated_fields(wish_info):
    wish_info.pop("profile_wish_moderated", None)


def _enforce_wish_moderated_is_public(existing_row, wish_info):
    """Prevent owners from making a moderated seeking post public via PUT."""
    moderated = int(existing_row.get("profile_wish_moderated") or 0)
    if moderated in (
        MODERATED_TAKEN_DOWN,
        MODERATED_PENDING_REVIEW,
        MODERATED_ACKNOWLEDGED,
    ):
        wish_info["profile_wish_is_public"] = 0


def _form_truthy_public(value):
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    return s in ("1", "true", "yes", "on")


def _apply_profile_expertise_multipart_image(
    db, payload, profile_expertise_uid, idx, expertise_info, is_create
):
    """
    Upload profile_expertise_image_N from multipart form to S3; merge URL into expertise_info.
    Form keys: profile_expertise_image_<idx>, delete_profile_expertise_image_<idx>,
    profile_expertise_image_<idx>_is_public
    """
    file_key = f"profile_expertise_image_{idx}"
    delete_key = f"delete_profile_expertise_image_{idx}"
    pub_key = f"profile_expertise_image_{idx}_is_public"

    fobj = request.files.get(file_key) if request.files else None
    has_file = bool(
        fobj and getattr(fobj, "filename", None)
    )
    del_val = payload.get(delete_key)
    has_del = delete_key in payload and del_val not in (None, "", "null", "false", False)

    print(
        "[PROFILE EXPERTISE IMAGE] idx=%s uid=%s keys in request.files=%s file_key=%s has_file=%s has_del=%s is_create=%s"
        % (
            idx,
            profile_expertise_uid,
            list(request.files.keys()) if request.files else [],
            file_key,
            has_file,
            has_del,
            is_create,
        )
    )

    if has_file or has_del:
        new_url = processSingleImageUpload(
            db,
            profile_expertise_uid,
            "every_circle.profile_expertise",
            "profile_expertise_uid",
            "profile_expertise_image",
            file_key,
            delete_key,
            "profile_expertise",
            payload,
            is_create=is_create,
        )
        print(
            "[PROFILE EXPERTISE IMAGE] idx=%s uid=%s S3/profile_expertise_image URL=%r (has_del=%s has_file=%s)"
            % (idx, profile_expertise_uid, new_url, has_del, has_file)
        )
        if new_url is not None:
            expertise_info["profile_expertise_image"] = new_url
        elif has_del and not has_file:
            expertise_info["profile_expertise_image"] = None

    if pub_key in payload:
        expertise_info["profile_expertise_image_is_public"] = _form_truthy_public(
            payload.pop(pub_key)
        )


def _apply_profile_wish_multipart_image(
    db, payload, profile_wish_uid, idx, wish_info, is_create
):
    """
    Upload profile_wish_image_N from multipart form to S3; merge URL into wish_info.
    Mirrors offering/expertise: profile_wish_image_<idx>,
    delete_profile_wish_image_<idx>, profile_wish_image_<idx>_is_public
    """
    file_key = f"profile_wish_image_{idx}"
    delete_key = f"delete_profile_wish_image_{idx}"
    pub_key = f"profile_wish_image_{idx}_is_public"

    fobj = request.files.get(file_key) if request.files else None
    has_file = bool(fobj and getattr(fobj, "filename", None))
    del_val = payload.get(delete_key)
    has_del = delete_key in payload and del_val not in (None, "", "null", "false", False)

    print(
        "[PROFILE WISH IMAGE] idx=%s uid=%s keys in request.files=%s file_key=%s has_file=%s has_del=%s is_create=%s"
        % (
            idx,
            profile_wish_uid,
            list(request.files.keys()) if request.files else [],
            file_key,
            has_file,
            has_del,
            is_create,
        )
    )

    if has_file or has_del:
        new_url = processSingleImageUpload(
            db,
            profile_wish_uid,
            "every_circle.profile_wish",
            "profile_wish_uid",
            "profile_wish_image",
            file_key,
            delete_key,
            "profile_wish",
            payload,
            is_create=is_create,
        )
        print(
            "[PROFILE WISH IMAGE] idx=%s uid=%s S3/profile_wish_image URL=%r (has_del=%s has_file=%s)"
            % (idx, profile_wish_uid, new_url, has_del, has_file)
        )
        if new_url is not None:
            wish_info["profile_wish_image"] = new_url
        elif has_del and not has_file:
            wish_info["profile_wish_image"] = None

    if pub_key in payload:
        wish_info["profile_wish_image_is_public"] = _form_truthy_public(
            payload.pop(pub_key)
        )


def _multipart_file_present(field_name):
    fobj = request.files.get(field_name) if request.files else None
    return bool(fobj and getattr(fobj, "filename", None))


def _pick_profile_experience_image_file_key(idx):
    indexed = f"profile_experience_image_{idx}"
    if _multipart_file_present(indexed):
        return indexed
    if idx == 0 and _multipart_file_present("profile_experience_image"):
        return "profile_experience_image"
    return indexed


def _pick_profile_experience_image_delete_key(idx, payload):
    indexed = f"delete_profile_experience_image_{idx}"
    plain = "delete_profile_experience_image"
    if indexed in payload:
        return indexed
    if idx == 0 and plain in payload:
        return plain
    return indexed


def _apply_profile_experience_multipart_image(
    db, payload, profile_experience_uid, idx, experience_info, is_create
):
    """
    Multipart: profile_experience_image_<idx> or (idx 0) profile_experience_image;
    delete: delete_profile_experience_image_<idx> or delete_profile_experience_image;
    is_public: profile_experience_image_<idx>_is_public or profile_experience_image_is_public.
    DB: profile_experience_image, profile_experience_image_is_public
    """
    file_key = _pick_profile_experience_image_file_key(idx)
    delete_key = _pick_profile_experience_image_delete_key(idx, payload)
    pub_idx = f"profile_experience_image_{idx}_is_public"
    pub_plain = "profile_experience_image_is_public"

    fobj = request.files.get(file_key) if request.files else None
    has_file = bool(fobj and getattr(fobj, "filename", None))
    del_val = payload.get(delete_key)
    has_del = delete_key in payload and del_val not in (
        None,
        "",
        "null",
        "false",
        False,
    )

    print(
        "[PROFILE EXPERIENCE IMAGE] idx=%s uid=%s request.files=%s file_key=%s has_file=%s has_del=%s is_create=%s"
        % (
            idx,
            profile_experience_uid,
            list(request.files.keys()) if request.files else [],
            file_key,
            has_file,
            has_del,
            is_create,
        )
    )

    if has_file or has_del:
        new_url = processSingleImageUpload(
            db,
            profile_experience_uid,
            "every_circle.profile_experience",
            "profile_experience_uid",
            "profile_experience_image",
            file_key,
            delete_key,
            _EXPERIENCE_S3_PREFIX,
            payload,
            is_create=is_create,
        )
        print(
            "[PROFILE EXPERIENCE IMAGE] idx=%s uid=%s profile_experience_image URL=%r (has_del=%s has_file=%s)"
            % (idx, profile_experience_uid, new_url, has_del, has_file)
        )
        if new_url is not None:
            experience_info["profile_experience_image"] = new_url
        elif has_del and not has_file:
            experience_info["profile_experience_image"] = None

    if pub_idx in payload:
        experience_info["profile_experience_image_is_public"] = _form_truthy_public(
            payload.pop(pub_idx)
        )
    elif idx == 0 and pub_plain in payload:
        experience_info["profile_experience_image_is_public"] = _form_truthy_public(
            payload.pop(pub_plain)
        )


def _pick_profile_education_image_file_key(idx):
    indexed = f"profile_education_image_{idx}"
    if _multipart_file_present(indexed):
        return indexed
    if idx == 0 and _multipart_file_present("profile_education_image"):
        return "profile_education_image"
    return indexed


def _pick_profile_education_image_delete_key(idx, payload):
    indexed = f"delete_profile_education_image_{idx}"
    plain = "delete_profile_education_image"
    if indexed in payload:
        return indexed
    if idx == 0 and plain in payload:
        return plain
    return indexed


def _apply_profile_education_multipart_image(
    db, payload, profile_education_uid, idx, education_info, is_create
):
    """
    Multipart: profile_education_image_<idx> or (idx 0) profile_education_image;
    delete: delete_profile_education_image_<idx> or delete_profile_education_image;
    is_public: profile_education_image_<idx>_is_public or profile_education_image_is_public.
    DB: profile_education_image, profile_education_image_is_public
    """
    file_key = _pick_profile_education_image_file_key(idx)
    delete_key = _pick_profile_education_image_delete_key(idx, payload)
    pub_idx = f"profile_education_image_{idx}_is_public"
    pub_plain = "profile_education_image_is_public"

    fobj = request.files.get(file_key) if request.files else None
    has_file = bool(fobj and getattr(fobj, "filename", None))
    del_val = payload.get(delete_key)
    has_del = delete_key in payload and del_val not in (
        None,
        "",
        "null",
        "false",
        False,
    )

    print(
        "[PROFILE EDUCATION IMAGE] idx=%s uid=%s request.files=%s file_key=%s has_file=%s has_del=%s is_create=%s"
        % (
            idx,
            profile_education_uid,
            list(request.files.keys()) if request.files else [],
            file_key,
            has_file,
            has_del,
            is_create,
        )
    )

    if has_file or has_del:
        new_url = processSingleImageUpload(
            db,
            profile_education_uid,
            "every_circle.profile_education",
            "profile_education_uid",
            "profile_education_image",
            file_key,
            delete_key,
            _EDUCATION_S3_PREFIX,
            payload,
            is_create=is_create,
        )
        print(
            "[PROFILE EDUCATION IMAGE] idx=%s uid=%s profile_education_image URL=%r (has_del=%s has_file=%s)"
            % (idx, profile_education_uid, new_url, has_del, has_file)
        )
        if new_url is not None:
            education_info["profile_education_image"] = new_url
        elif has_del and not has_file:
            education_info["profile_education_image"] = None

    if pub_idx in payload:
        education_info["profile_education_image_is_public"] = _form_truthy_public(
            payload.pop(pub_idx)
        )
    elif idx == 0 and pub_plain in payload:
        education_info["profile_education_image_is_public"] = _form_truthy_public(
            payload.pop(pub_plain)
        )


class UserProfileInfo(Resource):
    def get(self, uid):
        print("In UserProfileInfo GET", uid, type(uid))
        response = {}
        with connect() as db:
            try:

                if uid[:3] == "110":
                    print("Profile UID Passed")
                    is_self_lookup = False
                    # Check if the profile exists
                    profile_response = db.select('every_circle.profile_personal', where={'profile_personal_uid': uid})
                    if not profile_response['result']:
                        response['message'] = f'No profile found for {uid}'
                        response['code'] = 404
                        return response, 404

                    # print("profile_response: ", profile_response)
                    profile_id = uid
                    user_uid = profile_response['result'][0]['profile_personal_user_id']
                    print("User UID: ", user_uid)

                    # Get Email ID
                    user_response = db.select('every_circle.users', where={'user_uid': user_uid})
                    if not user_response['result']:
                        response['message'] = f'No user found for {user_uid}'
                        response['code'] = 404
                        return response, 404

                    # print("user_response: ", user_response)
                    email_id = user_response['result'][0]['user_email_id']
                    print("Email ID: ", email_id)
                    

                else:
                    is_self_lookup = True
            
                    if uid[:3] == "100":
                        print("User UID Passed")

                        # Check if the user exists
                        user_response = db.select('every_circle.users', where={'user_uid': uid})
                        if not user_response['result']:
                            response['message'] = f'No user found for {uid}'
                            response['code'] = 404
                            return response, 404

                        # print("user_response: ", user_response['result'][0])
                        email_id = user_response['result'][0]['user_email_id']
                        user_uid = uid
                        # print("User UID Passed", email_id, user_uid)
                            

                    elif "@" in uid:
                        print("Email UID Passed")

                        # Check if the user exists
                        user_response = db.select('every_circle.users', where={'user_email_id': uid})
                        # print("user_response: ", user_response)
                        if not user_response['result']:
                            response['message'] = f'No user found for {uid}'
                            response['code'] = 404
                            return response, 404

                        # print("user_response: ", user_response['result'][0])
                        email_id = uid
                        user_uid = user_response['result'][0]['user_uid']
                        # print("User UID: ", email_id, user_uid)

                    # Get profile info
                    profile_response = db.select('every_circle.profile_personal', where={'profile_personal_user_id': user_uid})
                    if not profile_response['result']:
                        response['message'] = 'Profile not found for this user'
                        response['code'] = 404
                        return response, 404

                    # print("profile_response: ", profile_response)
                    # print("profile_response: ", profile_response['result'][0])
                    profile_id = profile_response['result'][0]['profile_personal_uid']
                    print("Profile UID: ", profile_id)

                viewer_profile_uid, viewer_is_admin, _ = _viewer_context_from_request(
                    profile_id
                )
                is_owner_view = is_self_lookup or bool(
                    viewer_profile_uid and str(viewer_profile_uid) == str(profile_id)
                )
                owner_moderated = get_user_moderated_value(db, profile_id)

                # Taken down (1), pending review (2), and acknowledged (3): do not return
                # the profile to other users. Acknowledged (3) also hides the profile from
                # the owner (same as dismissed content) — only moderation status is returned.
                if not viewer_is_admin:
                    if (
                        not is_owner_view
                        and owner_moderated
                        in (
                            MODERATED_TAKEN_DOWN,
                            MODERATED_PENDING_REVIEW,
                            MODERATED_ACKNOWLEDGED,
                        )
                    ):
                        response["message"] = "Profile not available"
                        response["code"] = 404
                        return response, 404

                    if is_owner_view and owner_moderated == MODERATED_ACKNOWLEDGED:
                        response["message"] = "Profile acknowledged"
                        response["code"] = 200
                        response["personal_info"] = {
                            "profile_personal_uid": profile_id,
                            "moderation": build_user_moderation_metadata(db, profile_id),
                        }
                        response["user_email"] = email_id
                        response["links_info"] = []
                        response["experience_info"] = []
                        response["expertise_info"] = []
                        response["education_info"] = []
                        response["wishes_info"] = []
                        response["ratings_info"] = []
                        response["business_info"] = []
                        return response, 200

                response['personal_info'] = _enrich_personal_info_for_owner(
                    db,
                    profile_response['result'][0],
                    profile_id,
                    is_owner_view,
                )
                response['user_email'] = email_id
                # print("Get 1")
                # return profile_response['result'][0], 200

                try:
                    social_links_query = f"""
                        SELECT social_link_uid, social_link_name, social_link_url, social_link_is_public
                        FROM every_circle.social_link
                        WHERE social_link_personal_profile_id = '{profile_id}'
                    """
                    social_links_response = db.execute(social_links_query)
                    response['links_info'] = social_links_response.get('result') or []
                except Exception as sl_err:
                    print(f"social_link query failed: {sl_err}")
                    response['links_info'] = []
                # print("Get 2")
                    # Get experience info - returning all experiences for this profile
                experience_info = db.select('every_circle.profile_experience', 
                                            where={'profile_experience_profile_personal_id': profile_id})
                response['experience_info'] = experience_info['result'] if experience_info['result'] else []
                
                # Get expertise info - returning all expertise entries for this profile with message response counts
                # When the profile owner is pending_review / taken_down, hide offerings from
                # other users. When acknowledged (3), hide from everyone including the owner.
                # Offering/seeking moderation rows are never changed — filter-only.
                owner_content_hidden = False
                if not viewer_is_admin:
                    if owner_moderated == MODERATED_ACKNOWLEDGED:
                        owner_content_hidden = True
                    elif (
                        not is_owner_view
                        and owner_moderated
                        in (MODERATED_TAKEN_DOWN, MODERATED_PENDING_REVIEW)
                    ):
                        owner_content_hidden = True
                if owner_content_hidden:
                    response['expertise_info'] = []
                else:
                    moderated_filter = ""
                    if not is_owner_view and not viewer_is_admin:
                        moderated_filter = " AND COALESCE(profile_expertise_moderated, 0) = 0"
                    expertise_query = f"""
                        SELECT profile_expertise.*, COUNT(er_profile_expertise_id) AS expertise_responses, COUNT(ti_bs_qty) AS expertise_sales
                        FROM every_circle.profile_expertise
                        LEFT JOIN every_circle.expertise_response ON er_profile_expertise_id = profile_expertise_uid
                        LEFT JOIN every_circle.transactions_items ON ti_bs_id = profile_expertise_uid
                        -- WHERE profile_expertise_profile_personal_id ='110-000015'
                        WHERE profile_expertise_profile_personal_id = %s
                          AND (profile_expertise_is_deleted IS NULL OR profile_expertise_is_deleted = 0)
                          {moderated_filter}
                        GROUP BY profile_expertise_uid
                    """
                    expertise_info = db.execute(expertise_query, (profile_id,))
                    expertise_rows = (
                        expertise_info['result'] if expertise_info.get('result') else []
                    )
                    # Self-lookup via user_uid/email has no viewer_profile_uid query param;
                    # treat the profile owner as the viewer so pending_review/taken_down
                    # content remains visible to them (acknowledged is still hidden above).
                    effective_viewer_uid = (
                        profile_id if is_owner_view else viewer_profile_uid
                    )
                    response['expertise_info'] = _filter_and_enrich_expertise_info(
                        db,
                        expertise_rows,
                        profile_id,
                        viewer_profile_uid=effective_viewer_uid,
                        viewer_is_admin=viewer_is_admin,
                        owner_moderated=owner_moderated,
                    )

                # Get education info - returning all education entries for this profile
                education_info = db.select('every_circle.profile_education', 
                                        where={'profile_education_profile_personal_id': profile_id})
                response['education_info'] = education_info['result'] if education_info['result'] else []

                # Get wishes info - returning all wishes entries for this profile with response counts
                if owner_content_hidden:
                    response['wishes_info'] = []
                else:
                    wish_moderated_filter = ""
                    if not is_owner_view and not viewer_is_admin:
                        wish_moderated_filter = " AND COALESCE(profile_wish_moderated, 0) = 0"
                    wishes_query = f"""
                        SELECT profile_wish.*, COUNT(wr_profile_wish_id) AS wish_responses
                        FROM every_circle.profile_wish
                        LEFT JOIN every_circle.wish_response ON wr_profile_wish_id = profile_wish_uid
                        WHERE profile_wish_profile_personal_id = %s
                          {wish_moderated_filter}
                        GROUP BY profile_wish_uid
                    """
                    wishes_info = db.execute(wishes_query, (profile_id,))
                    wish_rows = wishes_info['result'] if wishes_info.get('result') else []
                    effective_viewer_uid = (
                        profile_id if is_owner_view else viewer_profile_uid
                    )
                    response['wishes_info'] = _filter_and_enrich_wish_info(
                        db,
                        wish_rows,
                        profile_id,
                        viewer_profile_uid=effective_viewer_uid,
                        viewer_is_admin=viewer_is_admin,
                        owner_moderated=owner_moderated,
                    )

                # print("Get 3")

                # Get ratings info - returning all ratings entries for this profile
                # Get ratings info with business name
                ratings_query = f"""
                    SELECT r.*, b.business_name, b.business_phone_number, b.business_city, b.business_state
                    FROM every_circle.ratings r
                    LEFT JOIN every_circle.business b ON r.rating_business_id = b.business_uid
                    WHERE r.rating_profile_id = '{profile_id}'
                """
                ratings_result = db.execute(ratings_query)
                response['ratings_info'] = ratings_result['result'] if ratings_result['result'] else []

                # Get business info - returning all business entries for this profile
                # business_info = db.select('every_circle.profile_has_business',
                #                          where={'profile_business_profile_personal_id': profile_id})
                business_info = f"""
                        SELECT -- * 
                            b.business_uid,
                            b.business_name,
                            b.business_phone_number,
                            b.business_phone_number_is_public,
                            b.business_email_id,
                            b.business_email_id_is_public,
                            b.business_location,
                            b.business_address_line_1,
                            b.business_address_line_2,
                            b.business_city,
                            b.business_state,
                            b.business_country,
                            b.business_zip_code,
                            b.business_tag_line,
                            b.business_tag_line_is_public,
                            b.business_profile_img,
                            b.business_profile_img_is_public,
                            b.business_cc_fee_payer,
                            b.business_moderated,
                            bu.bu_uid,
                            bu.bu_role,
                            bu.bu_individual_business_is_public
                        FROM every_circle.business b
                        LEFT JOIN every_circle.business_user bu ON b.business_uid = bu.bu_business_id
                        LEFT JOIN every_circle.profile_personal p ON p.profile_personal_user_id = bu.bu_user_id
                        -- WHERE p.profile_personal_uid = '110-000015'
                        WHERE p.profile_personal_uid = '{profile_id}';
                    """
                # print("business_info query:", business_info)
                business_result = db.execute(business_info)
                # print("business_result:", business_result)
                business_rows = business_result['result'] if business_result['result'] else []
                response['business_info'] = _filter_and_enrich_business_info(
                    db, business_rows, is_owner_view, viewer_is_admin
                )

                # print("Get 4")
                
                return response, 200


            except Exception as e:
                print(f"Error in UserProfileInfo GET: {str(e)}")
                response['message'] = 'Improper Credentials'
                response['code'] = 500
                return response, 500

                


    def post(self):
        print("In UserProfileInfo POST")
        response = {}

        try:
            payload = request.form.to_dict() 
            print("payload", payload)

            if 'user_uid' not in payload:
                response['message'] = 'user_uid is required'
                response['code'] = 400
                return response, 400

            user_uid = payload.pop('user_uid')

            with connect() as db:
                # Check if the user exists
                user_exists_query = db.select('every_circle.users', where={'user_uid': user_uid})
                if not user_exists_query['result']:
                    response['message'] = 'User does not exist'
                    response['code'] = 404
                    return response, 404
                
                # Check if the user already has a profile
                profile_exists_query = db.select('every_circle.profile_personal', where={'profile_personal_user_id': user_uid})
                if profile_exists_query['result']:
                    response['message'] = 'Profile already exists for this user'
                    response['code'] = 400
                    return response, 400

                # Generate new profile UID
                profile_stored_procedure_response = db.call(procedure='new_profile_personal_uid')
                new_profile_uid = profile_stored_procedure_response['result'][0]['new_id']
                
                # Create personal info record
                personal_info = {}
                personal_info['profile_personal_uid'] = new_profile_uid
                personal_info['profile_personal_user_id'] = user_uid
                
                # Set default referred by if not provided
                print("processing referred by",new_profile_uid, user_uid)
                if 'profile_personal_referred_by' not in payload:
                    personal_info['profile_personal_referred_by'] = "110-000001"
                elif payload.get('profile_personal_referred_by', '').strip() in ['', 'null']:
                    personal_info['profile_personal_referred_by'] = "110-000001"
                else:
                    referred_by_value = payload.pop('profile_personal_referred_by')
                    
                    if referred_by_value [0:3] == "110":
                        print("referred by is a profile uid")
                        personal_info['profile_personal_referred_by'] = referred_by_value
                    elif referred_by_value [0:3] == "100":
                        print("referred by is a user uid", 'referred_by_value:',referred_by_value)
                        uid_query = f"""
                            SELECT profile_personal_uid
                            FROM every_circle.profile_personal
                            WHERE profile_personal_user_id = "{referred_by_value}"
                        """
                        uid_result = db.execute(uid_query)
                        print("uid_result", uid_result)
                        personal_info['profile_personal_referred_by'] = uid_result['result'][0]['profile_personal_uid']

                    # Check if the value is an email address (contains @ symbol)
                    elif '@' in referred_by_value:
                        print("processing referred by email")
                        # Query to find profile_personal_uid by email
                        email_query = f"""
                            SELECT profile_personal_uid
                            FROM every_circle.users
                            LEFT JOIN every_circle.profile_personal ON user_uid = profile_personal_user_id
                            WHERE user_email_id = "{referred_by_value}"
                        """
                        email_result = db.execute(email_query)
                        print("email_result", email_result)
                        
                        if email_result['result'] and len(email_result['result']) > 0:
                            personal_info['profile_personal_referred_by'] = email_result['result'][0]['profile_personal_uid']
                        else:
                            personal_info['profile_personal_referred_by'] = "110-000001"
                    else:
                        # Use the original value if it's not an email
                        personal_info['profile_personal_referred_by'] = '110-000001'
                        

                # Extract personal info fields from payload
                personal_info_fields = [
                    'profile_personal_first_name', 'profile_personal_last_name', 'profile_personal_email_is_public', 
                    'profile_personal_phone_number', 'profile_personal_phone_number_is_public', 
                    'profile_personal_city', 'profile_personal_state', 'profile_personal_country',
                    'profile_personal_location_is_public', 'profile_personal_latitude', 'profile_personal_longitude',
                    'profile_personal_home_address',
                    'profile_personal_image', 'profile_personal_image_is_public', 'profile_personal_tag_line',
                    'profile_personal_tag_line_is_public', 'profile_personal_short_bio',
                    'profile_personal_short_bio_is_public', 'profile_personal_resume',
                    'profile_personal_resume_is_public', 'profile_personal_notification_preference',
                    'profile_personal_location_preference', 'profile_personal_allow_banner_ads', 'profile_personal_banner_ads_bounty',
                    'profile_personal_messages_off',
                    'profile_personal_messages_receive_from', 'profile_personal_messages_receive_types',
                    'profile_personal_experience_is_public', 'profile_personal_education_is_public',
                    'profile_personal_expertise_is_public', 'profile_personal_wishes_is_public', 'profile_personal_business_is_public',
                    'profile_personal_social_is_public'
                ]

                for field in personal_info_fields:
                    if field in payload:
                        personal_info[field] = payload.pop(field)
                _normalize_coordinate_fields(personal_info)
                _stamp_messages_off_timestamp(personal_info)

                # Process profile image if provided
                if 'profile_image' in request.files:
                    payload_images = {}
                    payload_images['profile_image'] = request.files['profile_image']
                    if 'delete_profile_image' in request.files:
                        payload_images['delete_profile_image'] = request.files['delete_profile_image']
                    print("[PERSONAL PROFILE IMAGE] POST - request.files keys=%s, sending to processImage" % (list(request.files.keys()) if request.files else []))
                    key = {'profile_personal_uid': new_profile_uid}
                    personal_info['profile_personal_image'] = processImage(key, payload_images)
                    print("[PERSONAL PROFILE IMAGE] POST - result (profile_personal_image)=%s" % (personal_info.get('profile_personal_image')))
                
                # Set last updated timestamp
                personal_info['profile_personal_last_updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                # Insert personal info
                db.insert('every_circle.profile_personal', personal_info)


                # Determine Path to Main Node
                personal_path_query = f'''
                                            WITH RECURSIVE ReferralPath AS (
                                SELECT 
                                    profile_personal_uid AS user_id,
                                    profile_personal_referred_by,
                                    CAST(CONCAT("'", profile_personal_uid, "'") AS CHAR(255)) AS path
                                FROM profile_personal
                                WHERE profile_personal_uid = '110-000001'

                                UNION ALL

                                SELECT 
                                    p.profile_personal_uid,
                                    p.profile_personal_referred_by,
                                    CONCAT(r.path, ',', "'", p.profile_personal_uid, "'")
                                FROM profile_personal p
                                JOIN ReferralPath r ON p.profile_personal_referred_by = r.user_id
                                WHERE LOCATE(p.profile_personal_uid, r.path) = 0 
                            )

                            SELECT path
                            FROM ReferralPath
                            WHERE user_id = '{new_profile_uid}';
                        '''
                print(personal_path_query)
                response = db.execute(personal_path_query)
                print(response)

                if not response['result']:
                    response['message'] = 'No connection found'
                    response['code'] = 404
                    return response, 404

                personal_path = response['result'][0]['path']
                print('personal_path_query: ', personal_path)

                # print('personal_path: ', personal_path['path'])

                db.update('every_circle.profile_personal', {'profile_personal_uid': new_profile_uid}, {'profile_personal_path': personal_path})
                
                # Create social media links if provided
                # First, get all social media platforms from the social_link table
                social_links_query = db.select('every_circle.social_link')
                social_links = {}
                
                if social_links_query['result']:
                    for link in social_links_query['result']:
                        social_links[link['social_link_name'].lower()] = link['social_link_uid']
                
                # Check for social media links in the payload
                social_media_links = {}
                if 'social_links' in payload:
                    try:
                        import json
                        social_media_links = json.loads(payload.pop('social_links'))
                    except Exception as e:
                        print(f"Error parsing social_links JSON: {str(e)}")
                
                # Process each social media link
                link_uids = []
                for platform, url in social_media_links.items():
                    platform_lower = platform.lower()
                    if platform_lower in social_links and url:
                        # Generate new profile link UID
                        link_stored_procedure_response = db.call(procedure='new_profile_link_uid')
                        new_link_uid = link_stored_procedure_response['result'][0]['new_id']
                        
                        link_info = {
                            'profile_link_uid': new_link_uid,
                            'profile_link_profile_personal_id': new_profile_uid,
                            'profile_link_social_link_id': social_links[platform_lower],
                            'profile_link_url': url
                        }
                        
                        db.insert('every_circle.profile_link', link_info)
                        link_uids.append(new_link_uid)
                
                # For expertise (handling multiple entries)
                expertise_entries = []
                expertise_uids = []
                
                # Check if expertise data is provided in JSON array format
                if 'expertises' in payload:
                    try:
                        import json
                        expertises_data = json.loads(payload.pop('expertises'))
                        print("expertise data: ", expertises_data)
                        _ensure_offering_returnable_columns(db)
                        
                        # Process each expertise entry (multipart: profile_expertise_image_0, ...)
                        for expertise_idx, exp_data in enumerate(expertises_data):
                            expertise_info = {}
                            expertise_stored_procedure_response = db.call(procedure='new_profile_expertise_uid')
                            new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                            expertise_info['profile_expertise_uid'] = new_expertise_uid
                            expertise_info['profile_expertise_profile_personal_id'] = new_profile_uid
                            expertise_info.update(_expertise_dict_from_payload(exp_data))
                            _apply_profile_expertise_multipart_image(
                                db,
                                payload,
                                new_expertise_uid,
                                expertise_idx,
                                expertise_info,
                                is_create=True,
                            )

                            # Insert the expertise record
                            db.insert('every_circle.profile_expertise', expertise_info)
                            expertise_entries.append(expertise_info)
                            expertise_uids.append(new_expertise_uid)
                    except Exception as e:
                        print(f"Error processing expertises JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('profile_expertise_') for key in payload):
                    expertise_info = {k: v for k, v in payload.items() if k.startswith('profile_expertise_')}
                    if expertise_info:
                        expertise_stored_procedure_response = db.call(procedure='new_profile_expertise_uid')
                        new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                        expertise_info['profile_expertise_uid'] = new_expertise_uid
                        expertise_info['profile_expertise_profile_personal_id'] = new_profile_uid
                        db.insert('every_circle.profile_expertise', expertise_info)
                        expertise_entries.append(expertise_info)
                        expertise_uids.append(new_expertise_uid)
                        # Remove used items
                        for k in list(expertise_info.keys()):
                            if k in payload:
                                payload.pop(k)
                
                # For wishes (handling multiple entries)
                wishes_entries = []
                wishes_uids = []
                
                # Check if wishes data is provided in JSON array format
                if 'wishes' in payload:
                    try:
                        import json
                        wishes_data = json.loads(payload.pop('wishes'))
                        _ensure_offering_returnable_columns(db)
                        print("wishes data: ", wishes_data)
                        
                        # Process each wish entry (multipart: profile_wish_image_0, ...)
                        for wish_idx, wish_data in enumerate(wishes_data):
                            wish_info = {}
                            wishes_stored_procedure_response = db.call(procedure='new_profile_wish_uid')
                            new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                            wish_info['profile_wish_uid'] = new_wish_uid
                            wish_info['profile_wish_profile_personal_id'] = new_profile_uid
                            wish_info.update(_wish_dict_from_payload(wish_data))
                            _apply_profile_wish_multipart_image(
                                db,
                                payload,
                                new_wish_uid,
                                wish_idx,
                                wish_info,
                                is_create=True,
                            )

                            # Insert the wish record
                            db.insert('every_circle.profile_wish', wish_info)
                            wishes_entries.append(wish_info)
                            wishes_uids.append(new_wish_uid)
                    except Exception as e:
                        print(f"Error processing wishes JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('profile_wish_') for key in payload):
                    wish_info = {k: v for k, v in payload.items() if k.startswith('profile_wish_')}
                    if wish_info:
                        wishes_stored_procedure_response = db.call(procedure='new_profile_wish_uid')
                        new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                        wish_info['profile_wish_uid'] = new_wish_uid
                        wish_info['profile_wish_profile_personal_id'] = new_profile_uid
                        db.insert('every_circle.profile_wish', wish_info)
                        wishes_entries.append(wish_info)
                        wishes_uids.append(new_wish_uid)
                        # Remove used items
                        for k in list(wish_info.keys()):
                            if k in payload:
                                payload.pop(k)
                
                # For experience (handling multiple experiences)
                experience_entries = []
                experience_uids = []
                
                # Check if experience data is provided in JSON array format
                if 'experiences' in payload:
                    try:
                        import json
                        experiences_data = json.loads(payload.pop('experiences'))
                        print("experience data: ", experiences_data)
                        
                        # Process each experience entry (multipart: profile_experience_image_0 / profile_experience_image)
                        for exp_idx, exp_data in enumerate(experiences_data):
                            experience_info = {}
                            experience_stored_procedure_response = db.call(procedure='new_profile_experience_uid')
                            new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                            experience_info['profile_experience_uid'] = new_experience_uid
                            experience_info['profile_experience_profile_personal_id'] = new_profile_uid
                            
                            # Map fields from the experience data
                            if 'company' in exp_data:
                                experience_info['profile_experience_company_name'] = exp_data['company']
                            if 'title' in exp_data:
                                experience_info['profile_experience_position'] = exp_data['title']
                            if 'description' in exp_data:
                                experience_info['profile_experience_description'] = exp_data['description']
                            if 'startDate' in exp_data:
                                experience_info['profile_experience_start_date'] = exp_data['startDate']
                            if 'endDate' in exp_data:
                                experience_info['profile_experience_end_date'] = exp_data['endDate']
                            
                            _apply_profile_experience_multipart_image(
                                db,
                                payload,
                                new_experience_uid,
                                exp_idx,
                                experience_info,
                                is_create=True,
                            )

                            # Insert the experience record
                            db.insert('every_circle.profile_experience', experience_info)
                            experience_entries.append(experience_info)
                            experience_uids.append(new_experience_uid)
                    except Exception as e:
                        print(f"Error processing experiences JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('profile_experience_') for key in payload):
                    experience_info = {k: v for k, v in payload.items() if k.startswith('profile_experience_')}
                    if experience_info:
                        experience_stored_procedure_response = db.call(procedure='new_profile_experience_uid')
                        new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                        experience_info['profile_experience_uid'] = new_experience_uid
                        experience_info['profile_experience_profile_personal_id'] = new_profile_uid
                        db.insert('every_circle.profile_experience', experience_info)
                        experience_entries.append(experience_info)
                        experience_uids.append(new_experience_uid)
                        # Remove used items
                        for k in list(experience_info.keys()):
                            if k in payload:
                                payload.pop(k)
                
                # For education (handling multiple education entries)
                education_entries = []
                education_uids = []
                
                # Check if education data is provided in JSON array format
                if 'educations' in payload:
                    try:
                        import json
                        educations_data = json.loads(payload.pop('educations'))
                        print("education data: ", educations_data)
                        
                        # Process each education entry (multipart: profile_education_image_0 / profile_education_image)
                        for edu_idx, edu_data in enumerate(educations_data):
                            education_info = {}
                            education_stored_procedure_response = db.call(procedure='new_profile_education_uid')
                            new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                            education_info['profile_education_uid'] = new_education_uid
                            education_info['profile_education_profile_personal_id'] = new_profile_uid
                            
                            # Map fields from the education data
                            if 'school_name' in edu_data:
                                education_info['profile_education_school_name'] = edu_data['school_name']
                            if 'degree' in edu_data:
                                education_info['profile_education_degree'] = edu_data['degree']
                            if 'course' in edu_data:
                                education_info['profile_education_course'] = edu_data['course']
                            if 'startDate' in edu_data:
                                education_info['profile_education_start_date'] = edu_data['startDate']
                            if 'endDate' in edu_data:
                                education_info['profile_education_end_date'] = edu_data['endDate']
                            
                            _apply_profile_education_multipart_image(
                                db,
                                payload,
                                new_education_uid,
                                edu_idx,
                                education_info,
                                is_create=True,
                            )

                            # Insert the education record
                            db.insert('every_circle.profile_education', education_info)
                            education_entries.append(education_info)
                            education_uids.append(new_education_uid)
                    except Exception as e:
                        print(f"Error processing educations JSON: {str(e)}")
                
                # Also handle legacy format (for backward compatibility)
                elif any(key.startswith('profile_education_') for key in payload):
                    education_info = {k: v for k, v in payload.items() if k.startswith('profile_education_')}
                    if education_info:
                        education_stored_procedure_response = db.call(procedure='new_profile_education_uid')
                        new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                        education_info['profile_education_uid'] = new_education_uid
                        education_info['profile_education_profile_personal_id'] = new_profile_uid
                        db.insert('every_circle.profile_education', education_info)
                        education_entries.append(education_info)
                        education_uids.append(new_education_uid)
                        # Remove used items
                        for k in list(education_info.keys()):
                            if k in payload:
                                payload.pop(k)
            
            # Include all created UIDs in the response
            response['uids'] = {
                'profile_personal_uid': new_profile_uid,  # Main profile UID
            }
            
            # Add social media link UIDs
            if link_uids:
                response['uids']['profile_link_uids'] = link_uids
            
            # Add expertise UIDs if created
            if expertise_uids:
                response['uids']['profile_expertise_uids'] = expertise_uids
                
            # Add wishes UIDs if created
            if wishes_uids:
                response['uids']['profile_wish_uids'] = wishes_uids
            
            # Add experience UIDs if created
            if experience_uids:
                response['uids']['profile_experience_uids'] = experience_uids
            
            # Add education UIDs if created
            if education_uids:
                response['uids']['profile_education_uids'] = education_uids
            
            response['message'] = 'Profile created successfully'
            return response, 200
        
        except Exception as e:
            print(f"Error in UserProfileInfo POST: {str(e)}")
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
        
    def put(self):
        print("In UserProfileInfo PUT")
        response = {}
        import json

        try:
            payload = request.form.to_dict()
            print("PUT Payload: ", payload)

            # profile_uid often sent as query param (e.g. API Gateway); form-only clients still work
            if 'profile_uid' not in payload:
                qa = request.args.get('profile_uid')
                if qa:
                    payload['profile_uid'] = qa.strip()

            if 'profile_uid' not in payload:
                response['message'] = 'profile_uid is required'
                response['code'] = 400
                return response, 400

            profile_uid = payload.pop('profile_uid')
            key = {'profile_personal_uid': profile_uid}
            print("UPDATED Payload: ", payload)
            updated_uids = {}
            deleted_uids = {}
            expertise_payload_refresh = False
            wishes_payload_refresh = False

            with connect() as db:
                # Check if the profile exists
                profile_exists_query = db.select('every_circle.profile_personal', where={'profile_personal_uid': profile_uid})
                print("Current Profile: ", profile_exists_query)
                if not profile_exists_query['result']:
                    response['message'] = 'Profile does not exist'
                    response['code'] = 404
                    return response, 404
                
                # Handle deletion requests first
                # Format: delete_experiences=["120-000001", "120-000002"]
                
                # Delete experiences if requested
                if 'delete_experiences' in payload:
                    try:
                        import json
                        experience_uids_to_delete = json.loads(payload.pop('delete_experiences'))
                        deleted_experience_uids = []
                        
                        for exp_uid in experience_uids_to_delete:
                            # Verify the experience exists and belongs to this profile
                            exp_exists_query = db.select('every_circle.profile_experience', 
                                                      where={'profile_experience_uid': exp_uid, 
                                                             'profile_experience_profile_personal_id': profile_uid})
                            
                            if exp_exists_query['result']:
                                _delete_experience_s3_assets(exp_uid)
                                delete_query = f"DELETE FROM every_circle.profile_experience WHERE profile_experience_uid = '{exp_uid}'"
                                delete_result = db.delete(delete_query)
                                deleted_experience_uids.append(exp_uid)
                        
                        if deleted_experience_uids:
                            deleted_uids['experiences'] = deleted_experience_uids
                    except Exception as e:
                        print(f"Error deleting experiences: {str(e)}")
                
                # Delete educations if requested
                if 'delete_educations' in payload:
                    try:
                        import json
                        education_uids_to_delete = json.loads(payload.pop('delete_educations'))
                        deleted_education_uids = []
                        
                        for edu_uid in education_uids_to_delete:
                            # Verify the education exists and belongs to this profile
                            edu_exists_query = db.select('every_circle.profile_education', 
                                                      where={'profile_education_uid': edu_uid, 
                                                             'profile_education_profile_personal_id': profile_uid})
                            
                            if edu_exists_query['result']:
                                _delete_education_s3_assets(edu_uid)
                                delete_query = f"DELETE FROM every_circle.profile_education WHERE profile_education_uid = '{edu_uid}'"
                                delete_result = db.delete(delete_query)
                                deleted_education_uids.append(edu_uid)
                        
                        if deleted_education_uids:
                            deleted_uids['educations'] = deleted_education_uids
                    except Exception as e:
                        print(f"Error deleting educations: {str(e)}")
                
                # Delete expertises if requested
                if 'delete_expertises' in payload:
                    try:
                        import json
                        expertise_uids_to_delete = json.loads(payload.pop('delete_expertises'))
                        deleted_expertise_uids = []
                        
                        for exp_uid in expertise_uids_to_delete:
                            # Verify the expertise exists and belongs to this profile
                            exp_exists_query = db.select('every_circle.profile_expertise',
                                                      where={'profile_expertise_uid': exp_uid,
                                                             'profile_expertise_profile_personal_id': profile_uid})

                            if exp_exists_query['result']:
                                # Check if this expertise has been purchased — if so, soft-delete only
                                sold_check = db.execute(
                                    "SELECT 1 FROM every_circle.transactions_items WHERE ti_bs_id = %s LIMIT 1",
                                    (exp_uid,)
                                )
                                if sold_check.get('result'):
                                    # Soft delete: flag the row, keep it for transaction history
                                    db.execute(
                                        "UPDATE every_circle.profile_expertise SET profile_expertise_is_deleted = 1, profile_expertise_is_public = 0 WHERE profile_expertise_uid = %s",
                                        (exp_uid,),
                                        cmd="post",
                                    )
                                    print(f"Soft-deleted expertise {exp_uid} (has transactions)")
                                else:
                                    # No transactions — hard delete and clean up S3
                                    _delete_expertise_s3_assets(exp_uid)
                                    delete_query = f"DELETE FROM every_circle.profile_expertise WHERE profile_expertise_uid = '{exp_uid}'"
                                    db.delete(delete_query)
                                    print(f"Hard-deleted expertise {exp_uid}")
                                deleted_expertise_uids.append(exp_uid)
                        
                        if deleted_expertise_uids:
                            deleted_uids['expertises'] = deleted_expertise_uids
                    except Exception as e:
                        print(f"Error deleting expertises: {str(e)}")
                
                # Delete wishes if requested
                if 'delete_wishes' in payload:
                    try:
                        import json
                        wish_uids_to_delete = json.loads(payload.pop('delete_wishes'))
                        deleted_wish_uids = []
                        
                        for wish_uid in wish_uids_to_delete:
                            # Verify the wish exists and belongs to this profile
                            wish_exists_query = db.select('every_circle.profile_wish', 
                                                       where={'profile_wish_uid': wish_uid, 
                                                              'profile_wish_profile_personal_id': profile_uid})
                            
                            if wish_exists_query['result']:
                                _delete_wish_s3_assets(wish_uid)
                                delete_query = f"DELETE FROM every_circle.profile_wish WHERE profile_wish_uid = '{wish_uid}'"
                                delete_result = db.delete(delete_query)
                                deleted_wish_uids.append(wish_uid)
                        
                        if deleted_wish_uids:
                            deleted_uids['wishes'] = deleted_wish_uids
                    except Exception as e:
                        print(f"Error deleting wishes: {str(e)}")
                
                # Delete social links if requested
                if 'delete_social_links' in payload:
                    try:
                        import json
                        social_link_uids_to_delete = json.loads(payload.pop('delete_social_links'))
                        deleted_social_link_uids = []
                        
                        for link_uid in social_link_uids_to_delete:
                            # Verify the link exists and belongs to this profile
                            link_exists_query = db.select('every_circle.profile_link', 
                                                       where={'profile_link_uid': link_uid, 
                                                              'profile_link_profile_personal_id': profile_uid})
                            
                            if link_exists_query['result']:
                                delete_query = f"DELETE FROM every_circle.profile_link WHERE profile_link_uid = '{link_uid}'"
                                delete_result = db.delete(delete_query)
                                deleted_social_link_uids.append(link_uid)
                        
                        if deleted_social_link_uids:
                            deleted_uids['social_links'] = deleted_social_link_uids
                    except Exception as e:
                        print(f"Error deleting social links: {str(e)}")
                
                # Now proceed with the regular update logic
                
                # Update personal info fields
                personal_info = {}
                personal_info_fields = [
                    'profile_personal_first_name', 'profile_personal_last_name', 'profile_personal_email_is_public', 
                    'profile_personal_phone_number', 'profile_personal_phone_number_is_public', 
                    'profile_personal_city', 'profile_personal_state', 'profile_personal_country','profile_personal_location_is_public',
                    'profile_personal_latitude', 'profile_personal_longitude',
                    'profile_personal_home_address',
                    'profile_personal_image', 'profile_personal_image_is_public',
                    'profile_personal_tag_line', 'profile_personal_tag_line_is_public', 
                    'profile_personal_short_bio', 'profile_personal_short_bio_is_public', 
                    'profile_personal_resume', 'profile_personal_resume_is_public', 
                    'profile_personal_notification_preference', 'profile_personal_location_preference', 'profile_personal_allow_banner_ads', 'profile_personal_banner_ads_bounty',
                    'profile_personal_messages_off',
                    'profile_personal_messages_receive_from', 'profile_personal_messages_receive_types',
                    'profile_personal_experience_is_public',
                    'profile_personal_education_is_public',
                    'profile_personal_expertise_is_public',
                    'profile_personal_wishes_is_public',
                    'profile_personal_business_is_public',
                    'profile_personal_social_is_public'
                ]
                
                for field in personal_info_fields:
                    if field in payload:
                        personal_info[field] = payload.pop(field)
                _normalize_coordinate_fields(personal_info)
                _stamp_messages_off_timestamp(personal_info)

                print("Remaining payload fields: ", payload)
                
                # if 'profile_image' in request.files or 'delete_profile_image' in payload:
                #     payload_images = {}
                #     if 'profile_image' in request.files:
                #         payload_images['profile_image'] = request.files['profile_image']
                #     if 'delete_profile_image' in payload:
                #         payload_images['delete_profile_image'] = payload['delete_profile_image']
                #     # key = {'profile_personal_uid': profile_uid}
                #     personal_info['profile_personal_image'] = processImage(key, payload_images)

                if 'profile_image' in request.files or 'delete_profile_image' in payload:
                    print("In Profile Image")
                    payload_images = {}
                    if 'profile_image' in request.files:
                        payload_images['profile_image'] = request.files['profile_image']
                    if 'delete_profile_image' in payload:
                        payload_images['delete_profile_image'] = payload['delete_profile_image']
                    print("[PERSONAL PROFILE IMAGE] PUT - sending to processImage: payload_images keys=%s" % (list(payload_images.keys())))
                    # key = {'profile_personal_uid': profile_uid}
                    personal_info['profile_personal_image'] = processImage(key, payload_images)
                    print("[PERSONAL PROFILE IMAGE] PUT - result from processImage (stored in profile_personal_image)=%s" % (personal_info.get('profile_personal_image')))



                if ('profile_resume_details' in payload and 'file_0' in request.files) or 'delete_documents' in payload:
                    print("In Profile Document")
                    # if new resume is added check if there is an existing resume.  Delete existing resume and add new resume
                    # --------------- PROCESS DOCUMENTS ------------------
        
                    processDocument(key, payload)
                    print("Payload after processDocument function: ", payload, type(payload))

                    # # Convert JSON string to a Python list
                    # resumes = json.loads(payload['profile_personal_resume'])

                    # # Access the first link
                    # first_link = resumes[0]['link']

                    # print(first_link)

                    # personal_info['profile_personal_resume'] = first_link

                    personal_info['profile_personal_resume'] = payload['profile_personal_resume']
                    # print(personal_info['profile_personal_resume'], type(personal_info['profile_personal_resume']))



                    print("Data to update: ", personal_info)

                    
                    # --------------- PROCESS DOCUMENTS ------------------


                    # if just deleting, then delete resume


                    # if just adding, then add resume


                    # payload_images = {}
                    # if 'profile_resume' in request.files:
                    #     payload_images['profile_resume'] = request.files['profile_resume']
                    # if 'delete_profile__resume' in payload:
                    #     payload_images['delete_profile_resume'] = payload['delete_profile_resume']
                    # key = {'profile_personal_uid': profile_uid}
                    # personal_info['profile_personal_resume'] = processDocument(key, payload)
                    # print(personal_info['profile_personal_resume'], type(personal_info['profile_personal_resume']))

                if personal_info:
                    # Process profile image if provided
                    existing_profile = profile_exists_query['result'][0]
                    if not can_user_profile_be_edited(db, profile_uid, existing_profile):
                        raise RuntimeError(
                            "This profile cannot be edited while it is under moderation"
                        )
                    _strip_personal_moderated_fields(personal_info)
                    # if 'profile_image' in personal_info:
                    
                    
                    # Set last updated timestamp
                    personal_info['profile_personal_last_updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Update personal info
                    db.update('every_circle.profile_personal', {'profile_personal_uid': profile_uid}, personal_info)

                    updated_uids['profile_personal_uid'] = profile_uid
                    print("Update complete ", updated_uids['profile_personal_uid'])
                
                # Update social media links if provided
                # Handle social media links — store directly in social_link table
                if 'social_links' in payload:
                    print("In social_links")
                    try:
                        import json, uuid
                        social_media_links = json.loads(payload.pop('social_links'))
                        social_media_public = {}
                        if 'social_links_public' in payload:
                            try:
                                social_media_public = json.loads(payload.pop('social_links_public'))
                            except:
                                pass

                        # Load existing rows for this profile
                        existing_resp = db.execute(
                            "SELECT social_link_uid, social_link_name FROM every_circle.social_link WHERE social_link_personal_profile_id = %s",
                            (profile_uid,)
                        )
                        existing_map = {}
                        if existing_resp.get('result'):
                            for row in existing_resp['result']:
                                existing_map[row['social_link_name'].lower()] = row['social_link_uid']

                        for platform, url in social_media_links.items():
                            platform_name = platform.strip()
                            platform_lower = platform_name.lower()
                            if not platform_lower:
                                continue
                            url = (url or '').strip()
                            is_public = 1 if social_media_public.get(platform_lower, True) else 0

                            if platform_lower in existing_map:
                                link_uid = existing_map[platform_lower]
                                if url:
                                    db.execute(
                                        "UPDATE every_circle.social_link SET social_link_url = %s, social_link_is_public = %s WHERE social_link_uid = %s",
                                        (url, is_public, link_uid),
                                        cmd='post'
                                    )
                                else:
                                    db.execute(
                                        "DELETE FROM every_circle.social_link WHERE social_link_uid = %s",
                                        (link_uid,),
                                        cmd='post'
                                    )
                            elif url:
                                max_resp = db.execute(
                                    "SELECT MAX(CAST(SUBSTRING(social_link_uid, 5) AS UNSIGNED)) AS max_num FROM every_circle.social_link WHERE social_link_uid LIKE '700-%' AND social_link_personal_profile_id IS NOT NULL"
                                )
                                max_num = 1000
                                if max_resp.get('result') and max_resp['result'][0]['max_num']:
                                    max_num = max(1000, int(max_resp['result'][0]['max_num']) + 1)
                                new_uid = f"700-{str(max_num).zfill(6)}"
                                db.execute(
                                    "INSERT INTO every_circle.social_link (social_link_uid, social_link_name, social_link_personal_profile_id, social_link_url, social_link_is_public) VALUES (%s, %s, %s, %s, %s)",
                                    (new_uid, platform_name, profile_uid, url, is_public),
                                    cmd='post'
                                )
                    except Exception as e:
                        print(f"Error processing social_links: {str(e)}")
                
                # Handle multiple education entries
                if 'education_info' in payload:
                    print("In educations")
                    try:
                        import json
                        educations_data = json.loads(payload.pop('education_info'))
                        education_uids = []

                        # Process each education entry (multipart: profile_education_image_0 / profile_education_image)
                        for edu_idx, edu_data in enumerate(educations_data):
                            print("edu_data", edu_data)
                            education_info = {}

                            education_uid = _normalize_record_uid(
                                edu_data.pop('profile_education_uid', None)
                            )
                            if education_uid:
                                print("In existing education entry", education_uid)
                                education_exists_query = db.select(
                                    'every_circle.profile_education',
                                    where={
                                        'profile_education_uid': education_uid,
                                        'profile_education_profile_personal_id': profile_uid,
                                    },
                                )

                                if not education_exists_query['result']:
                                    print(
                                        f"Warning: Education {education_uid} not found for profile {profile_uid}"
                                    )
                                    continue

                                if 'school' in edu_data:
                                    education_info['profile_education_school_name'] = edu_data['school']
                                if 'degree' in edu_data:
                                    education_info['profile_education_degree'] = edu_data['degree']
                                if 'course' in edu_data:
                                    education_info['profile_education_course'] = edu_data['course']
                                if 'startDate' in edu_data:
                                    education_info['profile_education_start_date'] = edu_data['startDate']
                                if 'endDate' in edu_data:
                                    education_info['profile_education_end_date'] = edu_data['endDate']
                                if 'isPublic' in edu_data:
                                    education_info['profile_education_is_public'] = edu_data['isPublic']

                                _apply_profile_education_multipart_image(
                                    db,
                                    payload,
                                    education_uid,
                                    edu_idx,
                                    education_info,
                                    is_create=False,
                                )

                                if education_info:
                                    upd_res = db.update(
                                        'every_circle.profile_education',
                                        {'profile_education_uid': education_uid},
                                        education_info,
                                    )
                                    if not _db_write_succeeded(upd_res):
                                        raise RuntimeError(
                                            upd_res.get("message", "Education update failed")
                                        )

                                education_uids.append(education_uid)
                            else:
                                education_stored_procedure_response = db.call(
                                    procedure='new_profile_education_uid'
                                )
                                new_education_uid = education_stored_procedure_response['result'][0]['new_id']
                                education_info['profile_education_uid'] = new_education_uid
                                education_info['profile_education_profile_personal_id'] = profile_uid

                                if 'school' in edu_data:
                                    education_info['profile_education_school_name'] = edu_data['school']
                                if 'degree' in edu_data:
                                    education_info['profile_education_degree'] = edu_data['degree']
                                if 'course' in edu_data:
                                    education_info['profile_education_course'] = edu_data['course']
                                if 'startDate' in edu_data:
                                    education_info['profile_education_start_date'] = edu_data['startDate']
                                if 'endDate' in edu_data:
                                    education_info['profile_education_end_date'] = edu_data['endDate']
                                if 'isPublic' in edu_data:
                                    education_info['profile_education_is_public'] = edu_data['isPublic']

                                _apply_profile_education_multipart_image(
                                    db,
                                    payload,
                                    new_education_uid,
                                    edu_idx,
                                    education_info,
                                    is_create=True,
                                )

                                ins_res = db.insert(
                                    'every_circle.profile_education', education_info
                                )
                                if not _db_write_succeeded(ins_res):
                                    raise RuntimeError(
                                        ins_res.get("message", "Education insert failed")
                                    )
                                education_uids.append(new_education_uid)

                        updated_uids['profile_education_uids'] = education_uids
                    except RuntimeError:
                        raise
                    except Exception as e:
                        print(f"Error processing educations JSON in PUT: {str(e)}")
                
                # Handle multiple experiences
                if 'experience_info' in payload:
                    print("In experiences")
                    try:
                        import json
                        experiences_data = json.loads(payload.pop('experience_info'))
                        experience_uids = []

                        # Process each experience entry (multipart: profile_experience_image_0 / profile_experience_image)
                        for exp_idx, exp_data in enumerate(experiences_data):
                            print("exp_data", exp_data)
                            experience_info = {}

                            experience_uid = _normalize_record_uid(
                                exp_data.pop('profile_experience_uid', None)
                            )
                            if experience_uid:
                                print("In existing experience entry", experience_uid)
                                experience_exists_query = db.select(
                                    'every_circle.profile_experience',
                                    where={
                                        'profile_experience_uid': experience_uid,
                                        'profile_experience_profile_personal_id': profile_uid,
                                    },
                                )
                                print("experience_exists_query", experience_exists_query)

                                if not experience_exists_query['result']:
                                    print(
                                        f"Warning: Experience {experience_uid} not found for profile {profile_uid}"
                                    )
                                    continue

                                if 'company' in exp_data:
                                    experience_info['profile_experience_company_name'] = exp_data['company']
                                if 'title' in exp_data:
                                    experience_info['profile_experience_position'] = exp_data['title']
                                if 'description' in exp_data:
                                    experience_info['profile_experience_description'] = exp_data['description']
                                if 'startDate' in exp_data:
                                    experience_info['profile_experience_start_date'] = exp_data['startDate']
                                if 'endDate' in exp_data:
                                    experience_info['profile_experience_end_date'] = exp_data['endDate']
                                if 'isPublic' in exp_data:
                                    experience_info['profile_experience_is_public'] = exp_data['isPublic']

                                _apply_profile_experience_multipart_image(
                                    db,
                                    payload,
                                    experience_uid,
                                    exp_idx,
                                    experience_info,
                                    is_create=False,
                                )

                                if experience_info:
                                    upd_res = db.update(
                                        'every_circle.profile_experience',
                                        {'profile_experience_uid': experience_uid},
                                        experience_info,
                                    )
                                    if not _db_write_succeeded(upd_res):
                                        raise RuntimeError(
                                            upd_res.get("message", "Experience update failed")
                                        )

                                experience_uids.append(experience_uid)
                            else:
                                print("In new experience entry")
                                experience_stored_procedure_response = db.call(
                                    procedure='new_profile_experience_uid'
                                )
                                new_experience_uid = experience_stored_procedure_response['result'][0]['new_id']
                                experience_info['profile_experience_uid'] = new_experience_uid
                                experience_info['profile_experience_profile_personal_id'] = profile_uid

                                if 'company' in exp_data:
                                    experience_info['profile_experience_company_name'] = exp_data['company']
                                if 'title' in exp_data:
                                    experience_info['profile_experience_position'] = exp_data['title']
                                if 'description' in exp_data:
                                    experience_info['profile_experience_description'] = exp_data['description']
                                if 'startDate' in exp_data:
                                    experience_info['profile_experience_start_date'] = exp_data['startDate']
                                if 'endDate' in exp_data:
                                    experience_info['profile_experience_end_date'] = exp_data['endDate']
                                if 'isPublic' in exp_data:
                                    experience_info['profile_experience_is_public'] = exp_data['isPublic']

                                _apply_profile_experience_multipart_image(
                                    db,
                                    payload,
                                    new_experience_uid,
                                    exp_idx,
                                    experience_info,
                                    is_create=True,
                                )

                                print("Inserting experience record", experience_info)
                                ins_res = db.insert(
                                    'every_circle.profile_experience', experience_info
                                )
                                if not _db_write_succeeded(ins_res):
                                    raise RuntimeError(
                                        ins_res.get("message", "Experience insert failed")
                                    )
                                experience_uids.append(new_experience_uid)

                        updated_uids['profile_experience_uids'] = experience_uids
                    except RuntimeError:
                        raise
                    except Exception as e:
                        print(f"Error processing experiences JSON in PUT: {str(e)}")
                
                # Handle multiple expertise entries
                if 'expertise_info' in payload:
                    print("In expertises")
                    try:
                        import json
                        expertises_data = json.loads(payload.pop('expertise_info'))
                        expertise_payload_refresh = True
                        expertise_uids = []
                        _ensure_offering_returnable_columns(db)
                        
                        # Process each expertise entry (multipart files: profile_expertise_image_0, _1, ...)
                        for expertise_idx, exp_data in enumerate(expertises_data):
                            print("exp_data", exp_data)
                            expertise_info = {}
                            
                            expertise_uid = _normalize_record_uid(exp_data.pop('profile_expertise_uid', None))
                            if expertise_uid:
                                print("In existing expertise entry", expertise_uid)
                                expertise_exists_query = db.select(
                                    'every_circle.profile_expertise',
                                    where={
                                        'profile_expertise_uid': expertise_uid,
                                        'profile_expertise_profile_personal_id': profile_uid,
                                    },
                                )

                                if not expertise_exists_query['result']:
                                    print(
                                        f"Warning: Expertise {expertise_uid} not found for profile {profile_uid}"
                                    )
                                    continue

                                existing_expertise = expertise_exists_query['result'][0]
                                if not can_offering_be_edited(
                                    db, expertise_uid, existing_expertise
                                ):
                                    raise RuntimeError(
                                        "This offering cannot be edited while it is taken down"
                                    )

                                expertise_info.update(_expertise_dict_from_payload(exp_data))
                                _strip_expertise_moderated_fields(expertise_info)
                                _enforce_moderated_is_public(existing_expertise, expertise_info)
                                _apply_profile_expertise_multipart_image(
                                    db,
                                    payload,
                                    expertise_uid,
                                    expertise_idx,
                                    expertise_info,
                                    is_create=False,
                                )

                                if expertise_info:
                                    upd_res = db.update(
                                        'every_circle.profile_expertise',
                                        {'profile_expertise_uid': expertise_uid},
                                        expertise_info,
                                    )
                                    if not _db_write_succeeded(upd_res):
                                        raise RuntimeError(
                                            upd_res.get("message", "Expertise update failed")
                                        )

                                expertise_uids.append(expertise_uid)
                            else:
                                # This is a new expertise entry
                                expertise_stored_procedure_response = db.call(procedure='new_profile_expertise_uid')
                                new_expertise_uid = expertise_stored_procedure_response['result'][0]['new_id']
                                expertise_info['profile_expertise_uid'] = new_expertise_uid
                                expertise_info['profile_expertise_profile_personal_id'] = profile_uid
                                expertise_info.update(_expertise_dict_from_payload(exp_data))
                                _apply_profile_expertise_multipart_image(
                                    db,
                                    payload,
                                    new_expertise_uid,
                                    expertise_idx,
                                    expertise_info,
                                    is_create=True,
                                )

                                # Insert the expertise record
                                ins_res = db.insert(
                                    'every_circle.profile_expertise', expertise_info
                                )
                                if not _db_write_succeeded(ins_res):
                                    raise RuntimeError(
                                        ins_res.get("message", "Expertise insert failed")
                                    )
                                expertise_uids.append(new_expertise_uid)
                        
                        updated_uids['profile_expertise_uids'] = expertise_uids
                    except RuntimeError:
                        raise
                    except Exception as e:
                        print(f"Error processing expertises JSON in PUT: {str(e)}")

                # Handle multiple wishes entries
                if 'wishes_info' in payload:
                    print("In wishes")
                    try:
                        import json
                        wishes_data = json.loads(payload.pop('wishes_info'))
                        wishes_payload_refresh = True
                        wishes_uids = []
                        _ensure_offering_returnable_columns(db)
                        
                        # Process each wish entry (multipart: profile_wish_image_0, ...)
                        for wish_idx, wish_data in enumerate(wishes_data):
                            print("wish_data", wish_data)
                            wish_info = {}
                            
                            wish_uid = _normalize_record_uid(wish_data.pop('profile_wish_uid', None))
                            if wish_uid:
                                print("In existing wish entry", wish_uid)
                                wish_exists_query = db.select(
                                    'every_circle.profile_wish',
                                    where={
                                        'profile_wish_uid': wish_uid,
                                        'profile_wish_profile_personal_id': profile_uid,
                                    },
                                )

                                if not wish_exists_query['result']:
                                    print(f"Warning: Wish {wish_uid} not found for profile {profile_uid}")
                                    continue

                                existing_wish = wish_exists_query['result'][0]
                                if not can_wish_be_edited(db, wish_uid, existing_wish):
                                    raise RuntimeError(
                                        "This seeking post cannot be edited while it is taken down"
                                    )

                                wish_info.update(_wish_dict_from_payload(wish_data))
                                _strip_wish_moderated_fields(wish_info)
                                _enforce_wish_moderated_is_public(existing_wish, wish_info)
                                _apply_profile_wish_multipart_image(
                                    db,
                                    payload,
                                    wish_uid,
                                    wish_idx,
                                    wish_info,
                                    is_create=False,
                                )

                                if wish_info:
                                    upd_res = db.update(
                                        'every_circle.profile_wish',
                                        {'profile_wish_uid': wish_uid},
                                        wish_info,
                                    )
                                    if not _db_write_succeeded(upd_res):
                                        raise RuntimeError(
                                            upd_res.get("message", "Wish update failed")
                                        )

                                wishes_uids.append(wish_uid)
                            else:
                                # This is a new wish entry
                                wishes_stored_procedure_response = db.call(procedure='new_profile_wish_uid')
                                new_wish_uid = wishes_stored_procedure_response['result'][0]['new_id']
                                wish_info['profile_wish_uid'] = new_wish_uid
                                wish_info['profile_wish_profile_personal_id'] = profile_uid
                                wish_info.update(_wish_dict_from_payload(wish_data))
                                _apply_profile_wish_multipart_image(
                                    db,
                                    payload,
                                    new_wish_uid,
                                    wish_idx,
                                    wish_info,
                                    is_create=True,
                                )

                                # Insert the wish record
                                ins_res = db.insert('every_circle.profile_wish', wish_info)
                                if not _db_write_succeeded(ins_res):
                                    raise RuntimeError(
                                        ins_res.get("message", "Wish insert failed")
                                    )
                                wishes_uids.append(new_wish_uid)
                        
                        updated_uids['profile_wish_uids'] = wishes_uids
                    except RuntimeError:
                        raise
                    except Exception as e:
                        print(f"Error processing wishes JSON in PUT: {str(e)}")
                
                # Handle multiple business entries
                if 'business_info' in payload:
                    print("In businesses")
                    try:
                        import json
                        businesses_data = json.loads(payload.pop('business_info'))
                        print(f"Parsed businesses_data: {businesses_data}")
                        businesses_uids = []
                        
                        # Get user_id from profile
                        profile_query = db.select('every_circle.profile_personal', 
                                                where={'profile_personal_uid': profile_uid})
                        user_id = profile_query['result'][0]['profile_personal_user_id'] if profile_query['result'] else None
                        print(f"user_id from profile: {user_id}")
                        
                        if not user_id:
                            print("Error: Could not find user_id for profile")
                            raise Exception("User ID not found for profile")
                        
                        # Process each business entry
                        for business_data in businesses_data:
                            print(f"=== Processing business_data: {business_data}")
                            
                            # The profile_business_uid in the payload is actually the business_uid (200-xxx)
                            # We need to look it up in business_user table
                            if 'profile_business_uid' in business_data and business_data['profile_business_uid']:
                                business_uid = business_data['profile_business_uid']  # This is actually business_uid
                                print(f"Found profile_business_uid (actually business_uid): {business_uid}")
                                
                                # Check if business_user entry exists for this user and business
                                print(f"Looking up in business_user: bu_user_id={user_id}, bu_business_id={business_uid}")
                                bu_check = db.select('every_circle.business_user',
                                                    where={'bu_user_id': user_id, 'bu_business_id': business_uid})
                                
                                print(f"bu_check result: {bu_check}")
                                
                                if not bu_check['result']:
                                    print(f"WARNING: No business_user entry found for user {user_id} and business {business_uid}")
                                    print("Skipping this business...")
                                    continue
                                
                                # Get the bu_uid
                                bu_uid = bu_check['result'][0]['bu_uid']
                                print(f"Found bu_uid: {bu_uid}")
                                
                                # Prepare update data for business_user table
                                business_user_info = {}
                                
                                if 'profile_business_role' in business_data:
                                    print(f"Found profile_business_role: {business_data['profile_business_role']}")
                                    business_user_info['bu_role'] = business_data['profile_business_role']
                                elif 'role' in business_data:
                                    print(f"Found role: {business_data['role']}")
                                    business_user_info['bu_role'] = business_data['role']
                                
                                if 'individualIsPublic' in business_data:
                                    print(f"Found individualIsPublic: {business_data['individualIsPublic']} (type: {type(business_data['individualIsPublic'])})")
                                    business_user_info['bu_individual_business_is_public'] = business_data['individualIsPublic']
                                else:
                                    print("WARNING: individualIsPublic NOT found in business_data")
                                
                                # Update the business_user entry
                                if business_user_info:
                                    print(f"Updating business_user {bu_uid} with data: {business_user_info}")
                                    update_result = db.update('every_circle.business_user',
                                                            {'bu_uid': bu_uid},
                                                            business_user_info)
                                    print(f"Update result: {update_result}")
                                else:
                                    print("WARNING: No data to update for business_user")
                                
                                businesses_uids.append(bu_uid)
                                print(f"Added bu_uid {bu_uid} to businesses_uids list")
                            else:
                                print("WARNING: profile_business_uid not found or empty in business_data")
                                # This is a new business entry - create business_user relationship
                                print("Attempting to create new business_user entry")
                                
                                if 'business_uid' not in business_data or not business_data['business_uid']:
                                    print("ERROR: No business_uid provided for new entry, skipping...")
                                    continue
                                
                                actual_business_uid = business_data['business_uid']
                                print(f"Creating new entry for business_uid: {actual_business_uid}")
                                
                                # Create business_user entry
                                new_bu_uid = db.call(procedure='new_business_user_uid')['result'][0]['new_id']
                                bu_info = {
                                    'bu_uid': new_bu_uid,
                                    'bu_user_id': user_id,
                                    'bu_business_id': actual_business_uid,
                                    'bu_individual_business_is_public': business_data.get('individualIsPublic', False)
                                }
                                
                                if 'profile_business_role' in business_data:
                                    bu_info['bu_role'] = business_data['profile_business_role']
                                elif 'role' in business_data:
                                    bu_info['bu_role'] = business_data['role']
                                
                                print(f"Inserting new business_user: {bu_info}")
                                insert_result = db.insert('every_circle.business_user', bu_info)
                                print(f"Insert result: {insert_result}")
                                businesses_uids.append(new_bu_uid)
                        
                        print(f"Final businesses_uids list: {businesses_uids}")
                        updated_uids['business_user_uids'] = businesses_uids
                    except Exception as e:
                        print(f"ERROR processing businesses JSON in PUT: {str(e)}")
                        import traceback
                        traceback.print_exc()

                if expertise_payload_refresh:
                    _ex_snap = db.select(
                        'every_circle.profile_expertise',
                        where={'profile_expertise_profile_personal_id': profile_uid},
                    )
                    expertise_rows = (
                        _ex_snap['result'] if _ex_snap.get('result') else []
                    )
                    response['expertise_info'] = _filter_and_enrich_expertise_info(
                        db,
                        expertise_rows,
                        profile_uid,
                        viewer_profile_uid=profile_uid,
                        viewer_is_admin=False,
                    )

                if wishes_payload_refresh:
                    _wish_snap = db.execute(
                        """
                        SELECT profile_wish.*, COUNT(wr_profile_wish_id) AS wish_responses
                        FROM every_circle.profile_wish
                        LEFT JOIN every_circle.wish_response
                               ON wr_profile_wish_id = profile_wish_uid
                        WHERE profile_wish_profile_personal_id = %s
                        GROUP BY profile_wish_uid
                        """,
                        (profile_uid,),
                    )
                    wish_rows = (
                        _wish_snap['result'] if _wish_snap.get('result') else []
                    )
                    response['wishes_info'] = _filter_and_enrich_wish_info(
                        db,
                        wish_rows,
                        profile_uid,
                        viewer_profile_uid=profile_uid,
                        viewer_is_admin=False,
                    )

                # Prepare the response with both updated and deleted UIDs
            response['updated_uids'] = updated_uids
            if deleted_uids:
                response['deleted_uids'] = deleted_uids
            response['message'] = 'Profile updated successfully'
            return response, 200
        
        except RuntimeError as e:
            print(f"Error in UserProfileInfo PUT: {str(e)}")
            response["message"] = str(e)
            response["code"] = 400
            return response, 400
        except Exception as e:
            print(f"Error in UserProfileInfo PUT: {str(e)}")
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
                                          
    def delete(self, uid):
        print("In UserProfileInfo DELETE")
        response = {}
        
        try:
            with connect() as db:
                # Handle different types of UIDs based on prefix
                prefix = uid[:3]
                
                # Case 1: User UID (100) or Profile UID (110) - Delete all profile data
                if prefix in ["100", "110"]:
                    if prefix == "100":
                        # This is a user UID, need to find their profile first
                        profile_exists_query = db.select('every_circle.profile_personal', where={'profile_personal_user_id': uid})
                        
                        if not profile_exists_query['result']:
                            response['message'] = 'Profile not found for this user'
                            response['code'] = 404
                            return response, 404
                        
                        profile_uid = profile_exists_query['result'][0]['profile_personal_uid']
                    else:
                        # This is already a profile UID
                        profile_exists_query = db.select('every_circle.profile_personal', where={'profile_personal_uid': uid})
                        
                        if not profile_exists_query['result']:
                            response['message'] = f'No profile found for {uid}'
                            response['code'] = 404
                            return response, 404
                        
                        profile_uid = uid
                    
                    # Delete all profile-related records
                    delete_results = {}
                    
                    # Delete social media links
                    links_query = f"DELETE FROM every_circle.profile_link WHERE profile_link_profile_personal_id = '{profile_uid}'"
                    delete_results['links'] = db.delete(links_query)
                    
                    expertise_rows = db.select(
                        'every_circle.profile_expertise',
                        where={
                            'profile_expertise_profile_personal_id': profile_uid},
                    )
                    for row in expertise_rows.get('result') or []:
                        eid = row.get('profile_expertise_uid')
                        if eid:
                            _delete_expertise_s3_assets(eid)

                    wishes_rows = db.select(
                        'every_circle.profile_wish',
                        where={
                            'profile_wish_profile_personal_id': profile_uid},
                    )
                    for row in wishes_rows.get('result') or []:
                        wid = row.get('profile_wish_uid')
                        if wid:
                            _delete_wish_s3_assets(wid)

                    exp_rows_bulk = db.select(
                        'every_circle.profile_experience',
                        where={'profile_experience_profile_personal_id': profile_uid},
                    )
                    for row in exp_rows_bulk.get('result') or []:
                        xid = row.get('profile_experience_uid')
                        if xid:
                            _delete_experience_s3_assets(xid)

                    edu_rows_bulk = db.select(
                        'every_circle.profile_education',
                        where={'profile_education_profile_personal_id': profile_uid},
                    )
                    for row in edu_rows_bulk.get('result') or []:
                        did = row.get('profile_education_uid')
                        if did:
                            _delete_education_s3_assets(did)

                    # Delete expertise
                    expertise_query = f"DELETE FROM every_circle.profile_expertise WHERE profile_expertise_profile_personal_id = '{profile_uid}'"
                    delete_results['expertise'] = db.delete(expertise_query)
                    
                    # Delete wishes
                    wishes_query = f"DELETE FROM every_circle.profile_wish WHERE profile_wish_profile_personal_id = '{profile_uid}'"
                    delete_results['wishes'] = db.delete(wishes_query)
                    
                    # Delete experiences
                    experiences_query = f"DELETE FROM every_circle.profile_experience WHERE profile_experience_profile_personal_id = '{profile_uid}'"
                    delete_results['experiences'] = db.delete(experiences_query)
                    
                    # Delete education
                    education_query = f"DELETE FROM every_circle.profile_education WHERE profile_education_profile_personal_id = '{profile_uid}'"
                    delete_results['education'] = db.delete(education_query)
                    
                    # Finally delete the personal info (main profile)
                    personal_info_query = f"DELETE FROM every_circle.profile_personal WHERE profile_personal_uid = '{profile_uid}'"
                    delete_results['personal_info'] = db.delete(personal_info_query)
                    
                    response['results'] = delete_results
                    response['message'] = 'Profile information deleted successfully'
                
                # Case 2: Experience UID (120) - Delete a specific experience entry
                elif prefix == "120":
                    # First verify the experience exists
                    experience_exists_query = db.select('every_circle.profile_experience', where={'profile_experience_uid': uid})
                    
                    if not experience_exists_query['result']:
                        response['message'] = f'No experience found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    _delete_experience_s3_assets(uid)

                    # Delete the specific experience
                    experience_query = f"DELETE FROM every_circle.profile_experience WHERE profile_experience_uid = '{uid}'"
                    delete_result = db.delete(experience_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Experience with UID {uid} deleted successfully'
                
                # Case 3: Education UID (130) - Delete a specific education entry
                elif prefix == "130":
                    # First verify the education exists
                    education_exists_query = db.select('every_circle.profile_education', where={'profile_education_uid': uid})
                    
                    if not education_exists_query['result']:
                        response['message'] = f'No education found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    _delete_education_s3_assets(uid)

                    # Delete the specific education
                    education_query = f"DELETE FROM every_circle.profile_education WHERE profile_education_uid = '{uid}'"
                    delete_result = db.delete(education_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Education with UID {uid} deleted successfully'
                
                # Case 4: Links UID (140) - Delete a specific links entry
                elif prefix == "140":
                    # First verify the link exists
                    link_exists_query = db.select('every_circle.profile_link', where={'profile_link_uid': uid})
                    
                    if not link_exists_query['result']:
                        response['message'] = f'No link found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    # Delete the specific link
                    link_query = f"DELETE FROM every_circle.profile_link WHERE profile_link_uid = '{uid}'"
                    delete_result = db.delete(link_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Link with UID {uid} deleted successfully'
                
                # Case 5: Expertise UID (150) - Delete a specific expertise entry
                elif prefix == "150":
                    # First verify the expertise exists
                    expertise_exists_query = db.select('every_circle.profile_expertise', where={'profile_expertise_uid': uid})
                    
                    if not expertise_exists_query['result']:
                        response['message'] = f'No expertise found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    _delete_expertise_s3_assets(uid)

                    # Delete the specific expertise
                    expertise_query = f"DELETE FROM every_circle.profile_expertise WHERE profile_expertise_uid = '{uid}'"
                    delete_result = db.delete(expertise_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Expertise with UID {uid} deleted successfully'
                
                # Case 6: Wishes UID (160) - Delete a specific wishes entry
                elif prefix == "160":
                    # First verify the wish exists
                    wish_exists_query = db.select('every_circle.profile_wish', where={'profile_wish_uid': uid})
                    
                    if not wish_exists_query['result']:
                        response['message'] = f'No wish found with UID {uid}'
                        response['code'] = 404
                        return response, 404
                    
                    _delete_wish_s3_assets(uid)

                    # Delete the specific wish
                    wish_query = f"DELETE FROM every_circle.profile_wish WHERE profile_wish_uid = '{uid}'"
                    delete_result = db.delete(wish_query)
                    
                    response['result'] = delete_result
                    response['message'] = f'Wish with UID {uid} deleted successfully'
                
                else:
                    response['message'] = 'Invalid UID prefix'
                    response['code'] = 400
                    return response, 400
                
                return response, 200
        
        except Exception as e:
            print(f"Error in UserProfileInfo DELETE: {str(e)}")
            response['message'] = 'Internal Server Error'
            response['code'] = 500
            return response, 500
        