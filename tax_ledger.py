"""
Tax ledger: per-transaction sales-tax liability history for a seller profile.

GET /api/v1/tax_ledger/<profile_id>
"""

import traceback

from flask import request
from flask_restful import Resource

from data_ec import connect
from datetime_utils import enrich_datetime_fields, parse_stored_datetime
from tax_owed_schema import ensure_tax_owed_tables
from tax_owed_service import (
    TT_TYPE_TAX_COLLECTED,
    TT_TYPE_TAX_REVERSED,
    TT_TYPE_TAX_REMITTED,
    REMIT_SENTINEL,
    build_tax_owed_summary,
    remit_tax_owed,
)
from wallet_service import _round_money, _to_float


def _request_timezone():
    return request.args.get("timezone") or request.args.get("tz")


def _parse_pagination():
    try:
        limit = int(request.args.get("limit", 100))
    except (TypeError, ValueError):
        limit = 100
    try:
        offset = int(request.args.get("offset", 0))
    except (TypeError, ValueError):
        offset = 0
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    return limit, offset


def _entry_type_label(entry_type):
    labels = {
        TT_TYPE_TAX_COLLECTED: "Tax collected",
        TT_TYPE_TAX_REVERSED: "Tax reversed",
        TT_TYPE_TAX_REMITTED: "Tax remitted",
    }
    if not entry_type:
        return "Tax"
    return labels.get(entry_type, str(entry_type).replace("_", " ").title())


def _buyer_display_name(row):
    name = (row.get("buyer_name") or "").strip()
    if name:
        return name
    return row.get("tt_buyer_id") or "Unknown"


def _format_date_label(entry_datetime, tz_name=None):
    dt = parse_stored_datetime(entry_datetime)
    if dt is None:
        return None
    if tz_name:
        try:
            from zoneinfo import ZoneInfo

            dt = dt.astimezone(ZoneInfo(tz_name))
        except Exception:
            pass
    try:
        return dt.strftime("%b ") + str(dt.day)
    except Exception:
        return None


def _amount_label(amount):
    value = _round_money(amount)
    if value > 0:
        return f"+${value:,.2f}"
    if value < 0:
        return f"-${abs(value):,.2f}"
    return "$0.00"


def _apply_tax_entry_display(entry, tz_name=None):
    out = dict(entry)
    date_label = _format_date_label(out.get("entry_datetime"), tz_name)
    amount_label = _amount_label(out.get("amount"))
    out["display"] = {
        "date_label": date_label or "—",
        "amount_label": amount_label,
        "type_label": out.get("entry_type_label")
        or _entry_type_label(out.get("entry_type")),
    }
    return out


def get_tax_ledger(db, profile_id, *, limit=100, offset=0):
    """
    Build tax ledger entries for profile_id (personal or business wallet id).

    Newest-first with running balance_after from current tax_owed_balance.
    """
    ensure_tax_owed_tables(db)
    tax_owed = build_tax_owed_summary(db, profile_id)

    count_q = db.execute(
        """
        SELECT COUNT(*) AS cnt
        FROM every_circle.tax_transactions
        WHERE tt_profile_id = %s
        """,
        (profile_id,),
    )
    count_rows = count_q.get("result") or []
    total_entries = int((count_rows[0] or {}).get("cnt") or 0) if count_rows else 0

    rows_q = db.execute(
        """
        SELECT
            tt.tt_uid,
            tt.tt_profile_id,
            tt.tt_buyer_id,
            tt.tt_seller_id,
            tt.tt_transaction_id,
            tt.tt_ti_id,
            tt.tt_type,
            tt.tt_status,
            tt.tt_qty,
            tt.tt_amount,
            tt.tt_currency,
            tt.tt_tax_rate,
            tt.tt_note,
            tt.tt_created_at,
            TRIM(
                CONCAT(
                    COALESCE(pp.profile_personal_first_name, ''),
                    ' ',
                    COALESCE(pp.profile_personal_last_name, '')
                )
            ) AS buyer_name
        FROM every_circle.tax_transactions tt
        LEFT JOIN every_circle.profile_personal pp
            ON pp.profile_personal_uid = tt.tt_buyer_id
        WHERE tt.tt_profile_id = %s
        ORDER BY tt.tt_created_at DESC, tt.tt_uid DESC
        LIMIT %s OFFSET %s
        """,
        (profile_id, limit, offset),
    )
    rows = rows_q.get("result") or []

    # Running balance for the page: start from current balance, then add back
    # amounts of newer rows skipped by offset so page[0].balance_after is correct.
    running = _round_money(tax_owed.get("tax_owed_balance"))
    if offset > 0:
        newer_q = db.execute(
            """
            SELECT tt_amount
            FROM every_circle.tax_transactions
            WHERE tt_profile_id = %s
            ORDER BY tt_created_at DESC, tt_uid DESC
            LIMIT %s
            """,
            (profile_id, offset),
        )
        for newer in newer_q.get("result") or []:
            running = _round_money(running - _to_float(newer.get("tt_amount")))

    data = []
    for row in rows:
        amount = _round_money(row.get("tt_amount"))
        balance_after = running
        running = _round_money(running - amount)

        buyer = _buyer_display_name(row)
        entry_type = row.get("tt_type")
        if entry_type == TT_TYPE_TAX_REVERSED:
            description = f"Sales tax reversed — {buyer}"
        elif entry_type == TT_TYPE_TAX_REMITTED:
            description = "Sales tax remitted"
            if row.get("tt_note"):
                note = str(row.get("tt_note")).strip()
                if note and note.lower() != "sales tax remitted by seller":
                    description = f"Sales tax remitted — {note}"
        else:
            description = f"Sales tax collected — {buyer}"

        txn_id = row.get("tt_transaction_id")
        if txn_id == REMIT_SENTINEL:
            txn_id = None
        ti_id = row.get("tt_ti_id")
        if ti_id == REMIT_SENTINEL:
            ti_id = None

        data.append(
            {
                "ledger_entry_uid": row.get("tt_uid"),
                "entry_id": row.get("tt_uid"),
                "entry_type": entry_type,
                "entry_type_label": _entry_type_label(entry_type),
                "amount": amount,
                "qty": int(row.get("tt_qty") or 0),
                "tax_rate": (
                    _to_float(row.get("tt_tax_rate"))
                    if row.get("tt_tax_rate") is not None
                    else None
                ),
                "currency": (row.get("tt_currency") or "USD"),
                "order_uid": txn_id,
                "transaction_uid": txn_id,
                "ti_uid": ti_id,
                "entry_datetime": row.get("tt_created_at"),
                "description": description,
                "counterparty_name": (
                    None if entry_type == TT_TYPE_TAX_REMITTED else buyer
                ),
                "purchaser_name": (
                    None if entry_type == TT_TYPE_TAX_REMITTED else buyer
                ),
                "balance_after": balance_after,
                "note": row.get("tt_note"),
            }
        )

    return {
        "code": 200,
        "message": "Tax ledger retrieved successfully",
        "profile_id": profile_id,
        "tax_owed": tax_owed,
        "total_entries": total_entries,
        "limit": limit,
        "offset": offset,
        "data": data,
    }


class TaxLedger(Resource):
    """
    GET /api/v1/tax_ledger/<profile_id>

    Sales-tax liability ledger for a personal or business seller profile.
    Query params: limit (default 100, max 500), offset, timezone/tz
    """

    def get(self, profile_id):
        if not profile_id:
            return {"code": 400, "message": "profile_id is required"}, 400

        from auth import require_actor_or_admin

        _, error = require_actor_or_admin(profile_id, allow_business=True)
        if error:
            return error, error["code"]

        limit, offset = _parse_pagination()
        tz_name = _request_timezone()

        try:
            with connect() as db:
                result = get_tax_ledger(
                    db, profile_id, limit=limit, offset=offset
                )

            enriched = []
            for row in result.get("data") or []:
                if isinstance(row, dict):
                    row = enrich_datetime_fields(
                        dict(row), "entry_datetime", tz_name
                    )
                    enriched.append(_apply_tax_entry_display(row, tz_name))
                else:
                    enriched.append(row)
            result["data"] = enriched
            if tz_name:
                result["timezone"] = tz_name
            result["datetime_storage"] = "UTC"
            return result, 200
        except Exception as e:
            print(f"Error in TaxLedger GET: {e}")
            print(traceback.format_exc())
            return {"code": 500, "message": f"An error occurred: {e}"}, 500


class TaxLedgerRemit(Resource):
    """
    POST /api/v1/tax_ledger/<profile_id>/remit

    Record that the seller remitted sales tax (outside the app).
    Body: { amount, note?, idempotency_key? }
    """

    def post(self, profile_id):
        if not profile_id:
            return {"code": 400, "message": "profile_id is required"}, 400

        from auth import require_actor_or_admin

        _, error = require_actor_or_admin(profile_id, allow_business=True)
        if error:
            return error, error["code"]

        payload = request.get_json(silent=True) or {}
        amount = payload.get("amount")
        note = payload.get("note")
        idempotency_key = payload.get("idempotency_key")

        try:
            with connect() as db:
                result = remit_tax_owed(
                    db,
                    profile_id,
                    amount,
                    note=note,
                    idempotency_key=idempotency_key,
                )
            code = result.get("code", 500)
            if code != 200:
                return result, code
            return result, 200
        except Exception as e:
            print(f"Error in TaxLedgerRemit POST: {e}")
            print(traceback.format_exc())
            return {"code": 500, "message": f"An error occurred: {e}"}, 500
