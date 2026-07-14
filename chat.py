from flask_restful import Resource
from flask import request
from data_ec import connect
import datetime
import uuid
import os
from dotenv import load_dotenv
import ably
import asyncio

load_dotenv()


# --------------- helpers ---------------


def _generate_uid_from_db(db_function_name, fallback_prefix):
    """
    Try DB UID generator function first, fallback to local UUID.
    Example db_function_name: "new_message_uid"
    """
    try:
        with connect() as db:
            uid_result = db.execute(f"CALL every_circle.{db_function_name}()")
        rows = (uid_result or {}).get("result") or []
        if rows and rows[0].get("new_id"):
            return rows[0]["new_id"]
    except Exception as e:
        print(f"UID generation via {db_function_name} failed, using fallback: {e}")
    return f"{fallback_prefix}-{uuid.uuid4().hex[:12]}"

def _get_participant_info(uid):
    """
    Return { first_name, last_name, image } for any UID type:
      110-...  →  every_circle.profile_personal
      200-...  →  every_circle.business  (business_name used as first_name)
    """
    if not uid:
        return {}
    try:
        with connect() as db:
            if uid[:3] == "200":
                rows = db.execute(
                    "SELECT business_name, business_profile_img FROM every_circle.business WHERE business_uid = %s",
                    args=(uid,),
                )
                row = (rows.get("result") or [{}])[0]
                return {
                    "first_name": row.get("business_name") or "Business",
                    "last_name":  "",
                    "image":      row.get("business_profile_img"),
                }
            else:
                rows = db.execute(
                    """SELECT profile_personal_first_name, profile_personal_last_name, profile_personal_image
                       FROM every_circle.profile_personal WHERE profile_personal_uid = %s""",
                    args=(uid,),
                )
                row = (rows.get("result") or [{}])[0]
                return {
                    "first_name": row.get("profile_personal_first_name") or "",
                    "last_name":  row.get("profile_personal_last_name") or "",
                    "image":      row.get("profile_personal_image"),
                }
    except Exception as e:
        print(f"_get_participant_info error for {uid}: {e}")
        return {}


def _optional_message_context(data):
    """
    Map optional POST body context fields to messages table columns.
    Client omits empty values; only non-empty strings are persisted.
    """
    fields = {}

    context_type = (data.get("message_context_type") or "").strip()
    if context_type in ("offering", "seeking"):
        fields["message_context_type"] = context_type

    for col in ("message_context_uid", "message_context_response_uid"):
        val = (data.get(col) or "").strip()
        if val:
            fields[col] = val

    return fields


def _get_or_create_conversation(uid_a, uid_b):
    """Return (conversation_uid, created) for the pair. Order is normalised."""
    p1, p2 = sorted([uid_a, uid_b])
    with connect() as db:
        rows = db.execute(
            """
            SELECT conversation_uid
            FROM every_circle.conversations
            WHERE (participant_a_uid = %s AND participant_b_uid = %s)
               OR (participant_a_uid = %s AND participant_b_uid = %s)
            LIMIT 1
            """,
            args=(p1, p2, p2, p1),
        )
    result = rows.get("result") or []
    if result:
        return result[0]["conversation_uid"], False

    conv_uid = _generate_uid_from_db("new_conversation_uid", "conv")
    now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with connect() as db:
        db.execute(
            """
            INSERT INTO every_circle.conversations
                (conversation_uid, participant_a_uid, participant_b_uid, created_at, last_message_at)
            VALUES (%s, %s, %s, %s, %s)
            """,
            args=(conv_uid, p1, p2, now, now),
            cmd="post",
        )
    return conv_uid, True


def _get_recipient_uid(conversation_uid, sender_uid):
    """Return the participant in conversation_uid who is NOT sender_uid."""
    try:
        with connect() as db:
            conv_resp = db.execute(
                """
                SELECT participant_a_uid, participant_b_uid
                FROM every_circle.conversations
                WHERE conversation_uid = %s
                """,
                args=(conversation_uid,),
            )
        conv = (conv_resp.get("result") or [{}])[0]
        if conv.get("participant_a_uid") == sender_uid:
            return conv.get("participant_b_uid")
        return conv.get("participant_a_uid")
    except Exception as e:
        print(f"Could not find recipient for {conversation_uid}: {e}")
        return None


def _recipient_has_messages_disabled(sender_uid, recipient_uid):
    """
    True if recipient_uid has blocked sender_uid, or has globally turned off messages.
    Only enforced for personal (110-) recipients — business recipients are out of scope.
    """
    if not recipient_uid or recipient_uid[:3] != "110":
        return False
    try:
        with connect() as db:
            blocked = db.execute(
                "SELECT 1 FROM every_circle.blocked_users WHERE blocker_uid = %s AND blocked_uid = %s LIMIT 1",
                args=(recipient_uid, sender_uid),
            )
            if blocked.get("result"):
                return True
            muted = db.execute(
                "SELECT profile_personal_messages_off FROM every_circle.profile_personal WHERE profile_personal_uid = %s",
                args=(recipient_uid,),
            )
            row = (muted.get("result") or [{}])[0]
            return bool(row.get("profile_personal_messages_off"))
    except Exception as e:
        print(f"_recipient_has_messages_disabled error: {e}")
        return False


def _hidden_after_cutoff(sender_uid, recipient_uid):
    """
    Earliest timestamp (string, comparable to message_sent_at) after which sender_uid's messages
    should be hidden from recipient_uid — because recipient_uid blocked sender_uid, and/or muted
    all messages globally, at that point in time. Returns None if nothing should be hidden.
    Messages sent BEFORE this cutoff remain visible to both sides.
    """
    if not recipient_uid or recipient_uid[:3] != "110":
        return None
    try:
        with connect() as db:
            blocked = db.execute(
                "SELECT created_at FROM every_circle.blocked_users WHERE blocker_uid = %s AND blocked_uid = %s LIMIT 1",
                args=(recipient_uid, sender_uid),
            )
            blocked_row = (blocked.get("result") or [None])[0]
            block_cutoff = str(blocked_row["created_at"]) if blocked_row and blocked_row.get("created_at") else None

            muted = db.execute(
                "SELECT profile_personal_messages_off, profile_personal_messages_off_at FROM every_circle.profile_personal WHERE profile_personal_uid = %s",
                args=(recipient_uid,),
            )
            row = (muted.get("result") or [{}])[0]
            mute_cutoff = (
                str(row["profile_personal_messages_off_at"])
                if row.get("profile_personal_messages_off") and row.get("profile_personal_messages_off_at")
                else None
            )

            cutoffs = [c for c in (block_cutoff, mute_cutoff) if c]
            return min(cutoffs) if cutoffs else None
    except Exception as e:
        print(f"_hidden_after_cutoff error: {e}")
        return None


def _latest_visible_message(conversation_uid, viewer_uid, cutoff):
    """Most recent message in conversation_uid that viewer_uid is still allowed to see as a preview."""
    try:
        with connect() as db:
            rows = db.execute(
                """
                SELECT message_body, message_sent_at, message_sender_uid
                FROM every_circle.messages
                WHERE message_conversation_id = %s
                  AND (message_sender_uid = %s OR message_sent_at < %s)
                ORDER BY message_sent_at DESC
                LIMIT 1
                """,
                args=(conversation_uid, viewer_uid, cutoff),
            )
        result = rows.get("result") or []
        return result[0] if result else None
    except Exception as e:
        print(f"_latest_visible_message error: {e}")
        return None


def _publish_message(conversation_uid, message_uid, sender_uid, sender_name, sender_image, body, sent_at, recipient_uid):
    """
    Publish a new-message event to:
      1. chat::<conversation_uid>  — for the ChatScreen real-time feed
      2. /<recipient_uid>          — for the unread-dot / notification banner
    """
    try:
        api_key = os.getenv("ABLY_API_KEY", "")
        if not api_key:
            print("ABLY_API_KEY not set — skipping chat publish")
            return

        async def _pub():
            async with ably.AblyRest(api_key) as client:
                # 1. Conversation channel — ChatScreen listens here
                conv_channel = client.channels.get(f"chat::{conversation_uid}")
                await conv_channel.publish(
                    "new-message",
                    {
                        "message_uid":      message_uid,
                        "conversation_uid": conversation_uid,
                        "sender_uid":       sender_uid,
                        "body":             body,
                        "sent_at":          sent_at,
                    },
                )

                # 2. Recipient's personal channel — UnreadContext listens here
                if recipient_uid:
                    personal_channel = client.channels.get(f"/{recipient_uid}")
                    await personal_channel.publish(
                        "new-message",
                        {
                            "message_uid":      message_uid,
                            "conversation_uid": conversation_uid,
                            "sender_uid":       sender_uid,
                            "sender_name":      sender_name,
                            "sender_image":     sender_image,
                            "body":             body[:120],  # preview only
                            "sent_at":          sent_at,
                        },
                    )

        asyncio.run(_pub())
    except Exception as e:
        print(f"Error publishing chat message: {e}")


# --------------- resources ---------------

class Conversations(Resource):
    """
    POST /api/v1/chat/conversations
        Body: { uid_a, uid_b }
        Returns or creates a conversation between two users.

    GET  /api/v1/chat/conversations/<profile_uid>
        Returns all conversations for a user with the last message preview.
    """

    def post(self):
        data = request.get_json(silent=True) or {}
        uid_a = data.get("uid_a")
        uid_b = data.get("uid_b")

        if not uid_a or not uid_b:
            return {"message": "uid_a and uid_b are required", "code": 400}, 400
        if uid_a == uid_b:
            return {"message": "Cannot create a conversation with yourself", "code": 400}, 400

        conv_uid, created = _get_or_create_conversation(uid_a, uid_b)
        return {
            "message": "Created" if created else "OK",
            "code": 200,
            "conversation_uid": conv_uid,
            "created": created,
        }, 200

    # def get(self, profile_uid):
    #     # Fetch all conversations with last message joined in
    #     print("Conversations chat get profile_uid: ", profile_uid)

    #     query = """
    #             SELECT
    #                 c.conversation_uid,
    #                 c.last_message_at,
    #                 CASE
    #                     WHEN c.participant_a_uid = %s THEN c.participant_b_uid
    #                     ELSE c.participant_a_uid
    #                 END AS other_uid,
    #                 m.body       AS last_message,
    #                 m.sender_uid AS last_sender_uid,
    #                 m.sent_at    AS last_sent_at
    #             FROM every_circle.conversations c
    #             LEFT JOIN every_circle.messages m
    #                 ON m.message_uid = (
    #                     SELECT message_uid
    #                     FROM every_circle.messages
    #                     WHERE conversation_uid = c.conversation_uid
    #                     ORDER BY sent_at DESC
    #                     LIMIT 1
    #                 )
    #             WHERE c.participant_a_uid = %s
    #                OR c.participant_b_uid = %s
    #             ORDER BY c.last_message_at DESC
    #         """

    #     args=(profile_uid, profile_uid, profile_uid)
    #     with connect() as db:
    #         print("chat query: ", query)
    #         print("chat args: ", args)
    #         rows = db.execute(
    #             query,
    #             args,
    #         )
    #     conversations = rows.get("result") or []

    #     # Enrich each conversation with the other participant's profile
    #     result = []
    #     for conv in conversations:
    #         other_uid = conv.get("other_uid")
    #         info = _get_participant_info(other_uid) if other_uid else {}
    #         other_info = {
    #             "first_name": info.get("first_name"),
    #             "last_name":  info.get("last_name"),
    #             "image":      info.get("image"),
    #         }

    #         result.append(
    #             {
    #                 "conversation_uid": conv.get("conversation_uid"),
    #                 "last_message_at": str(conv.get("last_message_at") or ""),
    #                 "last_message": conv.get("last_message"),
    #                 "last_sent_at": str(conv.get("last_sent_at") or ""),
    #                 "other_uid": other_uid,
    #                 "my_uid": profile_uid,  # lets the client know which side it is
    #                 **other_info,
    #             }
    #         )

    #     return {"message": "Success", "code": 200, "result": result}, 200

    def get(self, profile_uid):
        # Fetch all conversations with last message joined in
        print("Conversations chat get profile_uid: ", profile_uid)

        query = """
                SELECT
                    c.*,
                    pa.profile_personal_first_name AS partipant_a_first_name,
                    pa.profile_personal_last_name AS partipant_a_last_name,
                    pa.profile_personal_image AS partipant_a_image,
                    pa.profile_personal_image_is_public AS partipant_a_image_is_public,
                    pb.profile_personal_first_name AS partipant_b_first_name,
                    pb.profile_personal_last_name AS partipant_b_last_name,
                    pb.profile_personal_image AS partipant_b_image,
                    pb.profile_personal_image_is_public AS partipant_b_image_is_public,

                    ba.business_name,
                    bb.business_name,

                    bua.bu_business_id AS bua_business_id,
                    bua.bu_user_id AS bua_user_id,
                    bua.bu_role AS bua_role,
                    bua.bu_individual_business_is_public AS bua_public,

                    bub.bu_business_id AS bub_business_id,
                    bub.bu_user_id AS bub_user_id,
                    bub.bu_role AS bub_role,
                    bub.bu_individual_business_is_public AS bub_public,

                    bpa.profile_personal_uid AS bpa_profile_uid,
                    bpa.profile_personal_first_name AS bpa_first_name,
                    bpa.profile_personal_last_name AS bpa_last_name,
                    bpa.profile_personal_image AS bpa_partipant_a_image,
                    bpa.profile_personal_image_is_public AS bpa_partipant_a_image_is_public,
                    bpb.profile_personal_uid AS bpb_profile_uid,
                    bpb.profile_personal_first_name AS bpb_first_name,
                    bpb.profile_personal_last_name AS bpb_last_name,
                    bpb.profile_personal_image AS bpb_partipant_a_image,
                    bpb.profile_personal_image_is_public AS bpb_partipant_a_image_is_public,
                    
                    m.*

                FROM every_circle.conversations c
                LEFT JOIN every_circle.profile_personal pa ON c.participant_a_uid = pa.profile_personal_uid
                LEFT JOIN every_circle.profile_personal pb ON c.participant_b_uid = pb.profile_personal_uid

                LEFT JOIN every_circle.business ba ON c.participant_a_uid = ba.business_uid
                LEFT JOIN every_circle.business bb ON c.participant_b_uid = bb.business_uid
                LEFT JOIN every_circle.business_user bua ON c.participant_a_uid = bua.bu_business_id
                LEFT JOIN every_circle.business_user bub ON c.participant_b_uid = bub.bu_business_id
                LEFT JOIN every_circle.profile_personal bpa ON bua.bu_user_id = bpa.profile_personal_user_id
                LEFT JOIN every_circle.profile_personal bpb ON bub.bu_user_id = bpb.profile_personal_user_id
                LEFT JOIN every_circle.messages m ON c.conversation_uid = m.message_conversation_id
                # WHERE  (c.participant_a_uid = '110-000018'
                #     OR c.participant_b_uid = '110-000018'
                #     OR bpa.profile_personal_user_id = '110-000018'
                #     OR bpb.profile_personal_user_id = '110-000018')
                #     AND last_message_at = message_sent_at
                WHERE  (c.participant_a_uid = %s
                    OR c.participant_b_uid = %s
                    OR bpa.profile_personal_user_id = %s
                    OR bpb.profile_personal_user_id = %s)
                    AND last_message_at = message_sent_at
                ORDER BY c.created_at DESC;
            """

        args=(profile_uid, profile_uid, profile_uid, profile_uid)
        with connect() as db:
            # print("chat query: ", query)
            # print("chat args: ", args)
            rows = db.execute(
                query,
                args,
            )
        conversations = rows.get("result") or []

        # If the shown last-message preview came from someone the viewer has since blocked/muted,
        # replace it with the latest message the viewer is still allowed to see (or blank it out).
        for conv in conversations:
            a = conv.get("participant_a_uid")
            b = conv.get("participant_b_uid")
            other_uid = b if a == profile_uid else a
            last_sender = conv.get("message_sender_uid")
            last_sent_at = conv.get("message_sent_at")
            if not other_uid or last_sender != other_uid or not last_sent_at:
                continue
            cutoff = _hidden_after_cutoff(other_uid, profile_uid)
            if cutoff and str(last_sent_at) >= cutoff:
                visible = _latest_visible_message(conv.get("conversation_uid"), profile_uid, cutoff)
                conv["message_body"] = visible.get("message_body") if visible else None
                conv["message_sent_at"] = visible.get("message_sent_at") if visible else None
                conv["message_sender_uid"] = visible.get("message_sender_uid") if visible else None

        return {"message": "Success", "code": 200, "result": conversations}, 200


class Messages(Resource):
    """
    GET  /api/v1/chat/messages/<conversation_uid>
        Query params: limit (default 50), before (ISO datetime cursor)

    POST /api/v1/chat/messages
        Body: { conversation_uid, sender_uid, body, message_context_type?,
                message_context_uid?, message_context_response_uid? }
        Persists: message_conversation_id, message_sender_uid, message_body, message_sent_at,
                  plus any optional context columns when present

    PUT  /api/v1/chat/messages  or  /api/v1/chat/messages/<conversation_uid>
        Body: { message_uid, message_read_at? } — sets message_read_at (defaults to now UTC).
        If conversation_uid is in the path, the row must belong to that conversation.
    """

    def get(self, conversation_uid):
        limit = int(request.args.get("limit", 50))
        before = request.args.get("before")
        viewer_uid = request.args.get("viewer_uid")

        query = """
            SELECT *
            FROM every_circle.messages
            -- WHERE message_conversation_id = '800-000002'
            WHERE message_conversation_id = %s
            ORDER BY message_sent_at DESC
            LIMIT %s
        """
        args = [conversation_uid, limit]
        # if before:
        #     query += " AND message_sent_at < %s"
        #     args.append(before)
        # query += " ORDER BY message_sent_at DESC LIMIT %s"
        # args.append(limit)

        with connect() as db:
            # print("chat query: ", query)
            # print("chat args: ", args)
            rows = db.execute(query, args=tuple(args))
            messages = rows.get("result") or []

        recipient_messages_disabled = False
        if viewer_uid:
            other_uid = _get_recipient_uid(conversation_uid, viewer_uid)
            # Does the OTHER participant block/mute ME — drives the "Messages turned off" banner on my own sent messages.
            recipient_messages_disabled = _recipient_has_messages_disabled(viewer_uid, other_uid)
            # Do I block/mute the OTHER participant — if so, hide only their messages sent AFTER that
            # point; anything from before the block/mute stays visible on both sides.
            hidden_cutoff = _hidden_after_cutoff(other_uid, viewer_uid)
            if hidden_cutoff:
                messages = [
                    m for m in messages
                    if m.get("message_sender_uid") == viewer_uid or str(m.get("message_sent_at") or "") < hidden_cutoff
                ]

        return {
            "message": "Success",
            "code": 200,
            "result": messages,
            "recipient_messages_disabled": recipient_messages_disabled,
        }, 200

    def post(self):
        data = request.get_json(silent=True) or {}
        conv_uid = data.get("conversation_uid")
        sender_uid = data.get("sender_uid")
        body = (data.get("body") or "").strip()

        if not conv_uid or not sender_uid or not body:
            return {
                "message": "conversation_uid, sender_uid, and body are required",
                "code": 400,
            }, 400

        msg_uid = _generate_uid_from_db("new_message_uid", "msg")
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        context_fields = _optional_message_context(data)

        base_cols = [
            "message_uid",
            "message_conversation_id",
            "message_sender_uid",
            "message_body",
            "message_sent_at",
        ]
        base_vals = [msg_uid, conv_uid, sender_uid, body, now]
        insert_cols = base_cols + list(context_fields.keys())
        insert_vals = base_vals + list(context_fields.values())
        placeholders = ", ".join(["%s"] * len(insert_cols))
        col_list = ", ".join(insert_cols)

        with connect() as db:
            insert_res = db.execute(
                f"""
                INSERT INTO every_circle.messages
                    ({col_list})
                VALUES ({placeholders})
                """,
                args=tuple(insert_vals),
                cmd="post",
            )
            if isinstance(insert_res, dict) and insert_res.get("code") not in (None, 200):
                # Don't advance the conversation timestamp if we failed to persist the message.
                return {
                    "message": insert_res.get("message", "Failed to insert message"),
                    "code": insert_res.get("code", 500),
                    "error": insert_res.get("error"),
                }, insert_res.get("code", 500)
            db.execute(
                """
                UPDATE every_circle.conversations
                SET last_message_at = %s
                WHERE conversation_uid = %s
                """,
                args=(now, conv_uid),
                cmd="post",
            )

        recipient_uid = _get_recipient_uid(conv_uid, sender_uid)
        recipient_disabled = _recipient_has_messages_disabled(sender_uid, recipient_uid)

        if not recipient_disabled:
            # Look up sender name + image for the notification preview (handles 110- and 200- UIDs)
            sender_name  = "Someone"
            sender_image = None
            try:
                info = _get_participant_info(sender_uid)
                sender_name  = (f"{info.get('first_name') or ''} {info.get('last_name') or ''}").strip() or "Someone"
                sender_image = info.get("image")
            except Exception:
                pass

            _publish_message(conv_uid, msg_uid, sender_uid, sender_name, sender_image, body, now, recipient_uid)

        return {
            "message": "Message sent",
            "code": 200,
            "message_uid": msg_uid,
            "sent_at": now,
            "message_sent_at": now,
            "recipient_messages_disabled": recipient_disabled,
        }, 200

    def put(self, conversation_uid=None):
        data = request.get_json(silent=True) or {}
        message_uid = data.get("message_uid")
        if not message_uid:
            return {"message": "message_uid is required", "code": 400}, 400

        read_at = data.get("message_read_at")
        if read_at:
            read_at = str(read_at).strip()
        if not read_at:
            read_at = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        if conversation_uid:
            sql = """
                UPDATE every_circle.messages
                SET message_read_at = %s
                WHERE message_uid = %s AND message_conversation_id = %s
            """
            args = (read_at, message_uid, conversation_uid)
        else:
            sql = """
                UPDATE every_circle.messages
                SET message_read_at = %s
                WHERE message_uid = %s
            """
            args = (read_at, message_uid)

        with connect() as db:
            res = db.execute(sql, args=args, cmd="post")
            if res.get("code") != 200:
                return {
                    "message": res.get("message", "Update failed"),
                    "code": res.get("code", 500),
                }, res.get("code", 500)
            affected = str(res.get("change", ""))
            if affected.startswith("0 rows"):
                return {
                    "message": "No message updated (check message_uid or conversation)",
                    "code": 404,
                }, 404

        return {
            "message": "Message updated",
            "code": 200,
            "message_uid": message_uid,
            "message_read_at": read_at,
        }, 200
