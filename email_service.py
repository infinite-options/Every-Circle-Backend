"""
Send email via Microsoft Graph (app-only / client credentials).

Usage:
    from email_service import sendEmail
    sendEmail("user@example.com", "Subject", "Body text")
    sendEmail(["a@example.com", "b@example.com"], "Subject", "Body text")
"""
import os

import msal
import requests
from dotenv import load_dotenv

load_dotenv()

TENANT_ID = os.getenv("MS_TENANT_ID")
CLIENT_ID = os.getenv("MS_CLIENT_ID")
CLIENT_SECRET = os.getenv("MS_CLIENT_SECRET")
SENDER_EMAIL = os.getenv("MS_SENDER_EMAIL")

_msal_app = msal.ConfidentialClientApplication(
    CLIENT_ID,
    authority=f"https://login.microsoftonline.com/{TENANT_ID}",
    client_credential=CLIENT_SECRET,
)


def get_msal_token():
    result = _msal_app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"],
    )
    if "access_token" in result:
        return result["access_token"]
    raise Exception(result.get("error_description"))


def sendEmail(recipient, subject, body):
    if isinstance(recipient, str):
        recipient = [recipient]

    payload = {
        "message": {
            "subject": subject,
            "body": {"ContentType": "Text", "Content": body},
            "toRecipients": [
                {"emailAddress": {"address": r}} for r in recipient
            ],
        },
        "saveToSentItems": True,
    }

    resp = requests.post(
        f"https://graph.microsoft.com/v1.0/users/{SENDER_EMAIL}/sendMail",
        headers={"Authorization": f"Bearer {get_msal_token()}"},
        json=payload,
        timeout=30,
    )
    if resp.status_code != 202:
        raise Exception(f"Failed to send email: {resp.status_code} {resp.text}")
