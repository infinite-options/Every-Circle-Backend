import json
import threading
import time
import traceback
from datetime import datetime

from flask import g, request
from flask_jwt_extended import get_jwt_identity, verify_jwt_in_request

from data_ec import connect


def _get_profile_id():
    try:
        verify_jwt_in_request(optional=True)
        identity = get_jwt_identity()
        if identity:
            return str(identity)[:10]
    except Exception:
        pass

    for key in ("profile_id", "user_uid", "user_id"):
        value = request.args.get(key)
        if value:
            return str(value)[:10]

    try:
        body = request.get_json(silent=True)
        if isinstance(body, dict):
            for key in ("profile_id", "user_uid", "user_id"):
                if body.get(key):
                    return str(body[key])[:10]
    except Exception:
        pass

    return None


def _get_client_ip():
    forwarded = request.headers.get("X-Forwarded-For", "")
    if forwarded:
        return forwarded.split(",")[0].strip()[:45]
    return (request.remote_addr or "")[:45]


def _format_runtime(duration_sec):
    ms = duration_sec * 1000
    if ms < 1000:
        return f"{ms:.2f}ms"[:16]
    return f"{duration_sec:.3f}s"[:16]


def _get_query_parameters():
    if not request.args:
        return ""
    serialized = json.dumps(request.args.to_dict(flat=False), default=str)
    return serialized[:1028]


def _get_request_body():
    try:
        if request.method in ("GET", "HEAD", "OPTIONS"):
            return None

        body = request.get_json(silent=True)
        print("BODY:", body)
        if body is not None:
            return json.dumps(body, default=str)

        if request.form:
            return json.dumps(dict(request.form), default=str)
    except Exception:
        pass

    return None


def _get_error_message(response, status_code):
    if status_code < 400:
        return None

    try:
        data = response.get_json(silent=True)
        if isinstance(data, dict):
            for key in ("message", "error", "msg"):
                if data.get(key):
                    return str(data[key])[:100]
    except Exception:
        pass

    return (response.status or "")[:100] or None


def _persist_log(log_data):
    try:
        with connect() as db:
            uid_result = db.execute("CALL every_circle.new_logs_uid()")
            if not uid_result or "result" not in uid_result or not uid_result["result"]:
                print("Request logging: failed to generate logs_uid")
                return

            logs_uid = uid_result["result"][0]["new_id"]

            insert_query = """
                INSERT INTO every_circle.logs (
                    logs_uid,
                    logs_profile_id,
                    logs_endpoint,
                    logs_parameters,
                    logs_runtime,
                    logs_http_method,
                    logs_status_code,
                    logs_client_ip,
                    logs_user_agent,
                    logs_request_body,
                    logs_error_message,
                    logs_created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            insert_params = [
                logs_uid,
                log_data["profile_id"],
                log_data["endpoint"],
                log_data["parameters"],
                log_data["runtime"],
                log_data["http_method"],
                log_data["status_code"],
                log_data["client_ip"],
                log_data["user_agent"],
                log_data["request_body"],
                log_data["error_message"],
                log_data["created_at"],
            ]

            db.execute(insert_query, insert_params, "post")
    except Exception as e:
        print(f"Request logging error: {e}")
        traceback.print_exc()


def _schedule_log(log_data):
    thread = threading.Thread(target=_persist_log, args=(log_data,), daemon=True)
    thread.start()


def register_request_logging(app):
    """
    Request flow:
        Request comes in -> start timer -> endpoint runs ->
        capture status code + duration -> async insert into logs table
    """

    @app.before_request
    def _start_request_timer():
        if request.method == "OPTIONS":
            return
        g._log_request_start = time.perf_counter()

    @app.after_request
    def _log_request(response):
        if request.method == "OPTIONS":
            return response

        start = getattr(g, "_log_request_start", None)
        if start is None:
            return response

        duration_sec = time.perf_counter() - start
        status_code = response.status_code

        log_data = {
            "profile_id": _get_profile_id(),
            "endpoint": request.path[:256],
            "parameters": _get_query_parameters(),
            "runtime": _format_runtime(duration_sec),
            "http_method": request.method[:10],
            "status_code": status_code,
            "client_ip": _get_client_ip(),
            "user_agent": (request.headers.get("User-Agent") or "")[:100],
            "request_body": _get_request_body(),
            "error_message": _get_error_message(response, status_code),
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }

        _schedule_log(log_data)
        return response
