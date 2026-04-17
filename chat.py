from flask_restful import Resource
from flask import request
from data_ec import connect
import datetime
import uuid
import os
from dotenv import load_dotenv

load_dotenv()


# --------------- helpers ---------------

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

    conv_uid = f"conv-{uuid.uuid4().hex[:12]}"
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


def _publish_message(conversation_uid, message_uid, sender_uid, sender_name, sender_image, body, sent_at):
    """
    Publish a new-message event to:
      1. chat::<conversation_uid>  — for the ChatScreen real-time feed
      2. /<recipient_uid>          — for the unread-dot / notification banner
    """
    try:
        import ably
        import asyncio

        api_key = os.getenv("ABLY_API_KEY", "")
        if not api_key:
            print("ABLY_API_KEY not set — skipping chat publish")
            return

        # Find the recipient (the participant who is NOT the sender)
        recipient_uid = None
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
                recipient_uid = conv.get("participant_b_uid")
            else:
                recipient_uid = conv.get("participant_a_uid")
        except Exception as e:
            print(f"Could not find recipient for notification: {e}")

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

    def get(self, profile_uid):
        # Fetch all conversations with last message joined in
        with connect() as db:
            rows = db.execute(
                """
                SELECT
                    c.conversation_uid,
                    c.last_message_at,
                    CASE
                        WHEN c.participant_a_uid = %s THEN c.participant_b_uid
                        ELSE c.participant_a_uid
                    END AS other_uid,
                    m.body       AS last_message,
                    m.sender_uid AS last_sender_uid,
                    m.sent_at    AS last_sent_at
                FROM every_circle.conversations c
                LEFT JOIN every_circle.messages m
                    ON m.message_uid = (
                        SELECT message_uid
                        FROM every_circle.messages
                        WHERE conversation_uid = c.conversation_uid
                        ORDER BY sent_at DESC
                        LIMIT 1
                    )
                WHERE c.participant_a_uid = %s
                   OR c.participant_b_uid = %s
                ORDER BY c.last_message_at DESC
                """,
                args=(profile_uid, profile_uid, profile_uid),
            )
        conversations = rows.get("result") or []

        # Enrich each conversation with the other participant's profile
        result = []
        for conv in conversations:
            other_uid = conv.get("other_uid")
            info = _get_participant_info(other_uid) if other_uid else {}
            other_info = {
                "first_name": info.get("first_name"),
                "last_name":  info.get("last_name"),
                "image":      info.get("image"),
            }

            result.append(
                {
                    "conversation_uid": conv.get("conversation_uid"),
                    "last_message_at": str(conv.get("last_message_at") or ""),
                    "last_message": conv.get("last_message"),
                    "last_sent_at": str(conv.get("last_sent_at") or ""),
                    "other_uid": other_uid,
                    "my_uid": profile_uid,  # lets the client know which side it is
                    **other_info,
                }
            )

        return {"message": "Success", "code": 200, "result": result}, 200


class Messages(Resource):
    """
    GET  /api/v1/chat/messages/<conversation_uid>
        Query params: limit (default 50), before (ISO datetime cursor)

    POST /api/v1/chat/messages
        Body: { conversation_uid, sender_uid, body }
    """

    def get(self, conversation_uid):
        limit = int(request.args.get("limit", 50))
        before = request.args.get("before")

        query = """
            SELECT message_uid, sender_uid, body, sent_at
            FROM every_circle.messages
            WHERE conversation_uid = %s
        """
        args = [conversation_uid]
        if before:
            query += " AND sent_at < %s"
            args.append(before)
        query += " ORDER BY sent_at DESC LIMIT %s"
        args.append(limit)

        with connect() as db:
            rows = db.execute(query, args=tuple(args))

        # Return oldest-first so the frontend can render top-to-bottom
        messages = list(reversed(rows.get("result") or []))
        for m in messages:
            m["sent_at"] = str(m.get("sent_at") or "")

        return {"message": "Success", "code": 200, "result": messages}, 200

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

        msg_uid = f"msg-{uuid.uuid4().hex[:12]}"
        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

        with connect() as db:
            db.execute(
                """
                INSERT INTO every_circle.messages
                    (message_uid, conversation_uid, sender_uid, body, sent_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                args=(msg_uid, conv_uid, sender_uid, body, now),
                cmd="post",
            )
            db.execute(
                """
                UPDATE every_circle.conversations
                SET last_message_at = %s
                WHERE conversation_uid = %s
                """,
                args=(now, conv_uid),
                cmd="post",
            )

        # Look up sender name + image for the notification preview (handles 110- and 200- UIDs)
        sender_name  = "Someone"
        sender_image = None
        try:
            info = _get_participant_info(sender_uid)
            sender_name  = (f"{info.get('first_name') or ''} {info.get('last_name') or ''}").strip() or "Someone"
            sender_image = info.get("image")
        except Exception:
            pass

        _publish_message(conv_uid, msg_uid, sender_uid, sender_name, sender_image, body, now)

        return {
            "message": "Message sent",
            "code": 200,
            "message_uid": msg_uid,
            "sent_at": now,
        }, 200
