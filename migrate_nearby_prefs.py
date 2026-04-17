"""
One-time migration: add share_with preference columns to profile_personal.
Run from the backend directory: python migrate_nearby_prefs.py
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from data_ec import connect

COLUMNS = [
    ("profile_personal_nearby_share_with",
     "VARCHAR(20) DEFAULT 'all_circles' COMMENT 'everyone | all_circles | specific'"),
    ("profile_personal_nearby_share_types",
     "TEXT DEFAULT NULL COMMENT 'comma-separated DB types e.g. friend,colleague'"),
]

with connect() as db:
    for col_name, col_def in COLUMNS:
        ddl = f"ALTER TABLE every_circle.profile_personal ADD COLUMN {col_name} {col_def}"
        result = db.execute(ddl, cmd='post')
        msg = result.get("message", "") if isinstance(result, dict) else str(result)
        if "Duplicate column" in msg:
            print(f"  {col_name}: already exists, skipping.")
        elif result.get("code", 200) != 200 if isinstance(result, dict) else False:
            print(f"  {col_name}: ERROR - {msg}")
        else:
            print(f"  {col_name}: added successfully.")

print("Migration complete.")
