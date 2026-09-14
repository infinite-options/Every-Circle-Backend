"""
SMS fallback notifications, sent via Twilio.

Real-time events in this app (new chat message, etc.) are delivered over Ably
(see ably_auth.py / chat.py) — but Ably only reaches a user while their app is
open and connected. Nothing reaches them once they close or background it.

This module adds an SMS fallback for that case: before texting someone, it
checks Ably Presence on that user's personal channel (`/<uid>`) — the same
channel chat.py already publishes "new-message" events to — and only sends
if they're not currently present there (i.e. their app isn't in the
foreground right now). If they *are* present, Ably already delivered the
event live, so no SMS is sent.

Frontend counterpart: contexts/UnreadContext.js enters/leaves presence on
that channel as the app foregrounds/backgrounds (see AppState handling
there). If that ever gets removed, `is_uid_present` degrades to "always
absent" and every notification falls back to SMS, which is safe, just noisier.

Usage:
    from notifications_service import notify_uid_if_away
    notify_uid_if_away(recipient_uid, "Jane sent you a message: Hey, is this still available?")
"""
import asyncio
import os

import ably
from dotenv import load_dotenv
from twilio.rest import Client

from data_ec import connect

load_dotenv()

#twilio credentials
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
# Same sending number already used for referral SMS in ec_api.py.
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "+19254815757")
ABLY_API_KEY = os.getenv("ABLY_API_KEY", "")


#checks Ably presence for a given uid
def is_uid_present(uid):
    if not uid or not ABLY_API_KEY:
        return False

    async def _check():
        async with ably.AblyRest(ABLY_API_KEY) as client:
            channel = client.channels.get(f"/{uid}")
            page = await channel.presence.get()
            return len(page.items) > 0

    try:
        return asyncio.run(_check())
    except Exception as e:
        print(f"is_uid_present error for {uid}: {e}")
        return False

# Look up phone number for a given uid 
def get_phone_number_for_uid(uid):
    if not uid:
        return None
    try:
        with connect() as db:
            if uid[:3] == "200":
                rows = db.execute(
                    "SELECT business_phone_number FROM every_circle.business WHERE business_uid = %s",
                    args=(uid,),
                )
                row = (rows.get("result") or [{}])[0]
                return row.get("business_phone_number")
            else:
                rows = db.execute(
                    """SELECT profile_personal_phone_number
                       FROM every_circle.profile_personal WHERE profile_personal_uid = %s""",
                    args=(uid,),
                )
                row = (rows.get("result") or [{}])[0]
                return row.get("profile_personal_phone_number")
    except Exception as e:
        print(f"get_phone_number_for_uid error for {uid}: {e}")
        return None

# Convert a raw phone number to twilio-compatible 
def _to_e164(raw):
    if not raw:
        return None
    digits = "".join(ch for ch in str(raw) if ch.isdigit())
    if len(digits) == 10:
        return "+1" + digits
    if len(digits) == 11 and digits.startswith("1"):
        return "+" + digits
    return None

# Send an SMS via Twilio
def send_sms(phone_number, message):
    to_number = _to_e164(phone_number)
    if not to_number:
        print(f"send_sms: no usable phone number ({phone_number!r}) — skipping")
        return False
    if not TWILIO_ACCOUNT_SID or not TWILIO_AUTH_TOKEN:
        print("send_sms: Twilio credentials not configured — skipping")
        return False
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        client.messages.create(body=message, from_=TWILIO_FROM_NUMBER, to=to_number)
        return True
    except Exception as e:
        print(f"send_sms error sending to {to_number}: {e}")
        return False

# Default link appended to a notification SMS when the caller doesn't have anything
# more specific to point at. Once everycircle.com hosts the verification files and a
# rebuild goes out (see app.config.js associatedDomains/intentFilters), these are real
# Universal/App Links — this exact path ("/") and "/chat" (see build_chat_link below)
# are the only two paths currently allow-listed on both platforms.
EVERYCIRCLE_URL = "https://everycircle.com"


def build_chat_link(conversation_uid):
    """Deep link straight to a conversation — matches the Chat route's `path: "chat"`
    + `parse: { conversation_uid }` in App.js's linking config."""
    if not conversation_uid:
        return EVERYCIRCLE_URL
    return f"{EVERYCIRCLE_URL}/chat?conversation_uid={conversation_uid}"


# Notify a user via SMS if they're not currently active in the app. `link`
# overrides the default EVERYCIRCLE_URL when the caller has somewhere more
# specific to send them (e.g. build_chat_link) — must be one of the paths
# allow-listed in app.config.js / the hosted apple-app-site-association.
def notify_uid_if_away(uid, message, link=None):
    try:
        if is_uid_present(uid):
            return False  # app is open right now — Ably already delivered this live
        phone_number = get_phone_number_for_uid(uid)
        if not phone_number:
            return False
        return send_sms(phone_number, f"{message} {link or EVERYCIRCLE_URL}")
    except Exception as e:
        print(f"notify_uid_if_away error for {uid}: {e}")
        return False
