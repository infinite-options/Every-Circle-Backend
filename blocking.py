from flask_restful import Resource
from flask import request
from data_ec import connect
import datetime
from chat import _get_participant_info


class BlockedUsers(Resource):
    """
    GET /api/v1/blocked-users/<blocker_uid>
        Returns the list of people <blocker_uid> has blocked, enriched with name/image.

    POST /api/v1/blocked-users
        Body: { blocker_uid, blocked_uid }
        Idempotent — INSERT IGNORE.

    DELETE /api/v1/blocked-users
        Body: { blocker_uid, blocked_uid }  (unblock)
    """

    def get(self, blocker_uid):
        with connect() as db:
            rows = db.execute(
                "SELECT blocked_uid, created_at FROM every_circle.blocked_users WHERE blocker_uid = %s ORDER BY created_at DESC",
                args=(blocker_uid,),
            )
        blocked = rows.get("result") or []
        result = []
        for row in blocked:
            info = _get_participant_info(row["blocked_uid"])
            result.append({
                "blocked_uid": row["blocked_uid"],
                "created_at": str(row.get("created_at") or ""),
                "first_name": info.get("first_name"),
                "last_name": info.get("last_name"),
                "image": info.get("image"),
            })
        return {"message": "Success", "code": 200, "result": result}, 200

    def post(self):
        data = request.get_json(silent=True) or {}
        blocker_uid = data.get("blocker_uid")
        blocked_uid = data.get("blocked_uid")
        if not blocker_uid or not blocked_uid:
            return {"message": "blocker_uid and blocked_uid are required", "code": 400}, 400
        if blocker_uid == blocked_uid:
            return {"message": "Cannot block yourself", "code": 400}, 400

        now = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        with connect() as db:
            db.execute(
                "INSERT IGNORE INTO every_circle.blocked_users (blocker_uid, blocked_uid, created_at) VALUES (%s, %s, %s)",
                args=(blocker_uid, blocked_uid, now),
                cmd="post",
            )
        return {"message": "Blocked", "code": 200}, 200

    def delete(self):
        data = request.get_json(silent=True) or {}
        blocker_uid = data.get("blocker_uid")
        blocked_uid = data.get("blocked_uid")
        if not blocker_uid or not blocked_uid:
            return {"message": "blocker_uid and blocked_uid are required", "code": 400}, 400

        with connect() as db:
            db.execute(
                "DELETE FROM every_circle.blocked_users WHERE blocker_uid = %s AND blocked_uid = %s",
                args=(blocker_uid, blocked_uid),
                cmd="post",
            )
        return {"message": "Unblocked", "code": 200}, 200
