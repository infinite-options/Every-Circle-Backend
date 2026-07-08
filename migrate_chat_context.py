"""
One-time migration: add optional offering/seeking context columns to messages.
Run from the backend directory: python migrate_chat_context.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_ec import connect

COLUMNS = [
    ("message_context_type",
     "VARCHAR(20) DEFAULT NULL COMMENT 'offering | seeking'"),
    ("message_context_uid",
     "VARCHAR(64) DEFAULT NULL COMMENT 'profile_expertise_uid or profile_wish_uid'"),
    ("message_context_response_uid",
     "VARCHAR(64) DEFAULT NULL COMMENT 'expertise_response_uid or wish_response_uid'"),
]

with connect() as db:
    for col_name, col_def in COLUMNS:
        ddl = f"ALTER TABLE every_circle.messages ADD COLUMN {col_name} {col_def}"
        result = db.execute(ddl, cmd='post')
        msg = result.get("message", "") if isinstance(result, dict) else str(result)
        if "Duplicate column" in msg:
            print(f"  {col_name}: already exists, skipping.")
        elif result.get("code", 200) != 200 if isinstance(result, dict) else False:
            print(f"  {col_name}: ERROR - {msg}")
        else:
            print(f"  {col_name}: added successfully.")

print("Migration complete.")
