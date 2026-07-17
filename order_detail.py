"""Combined order view: original sale plus linked return transactions."""

import traceback

from flask import request
from flask_restful import Resource
from flask_jwt_extended import get_jwt_identity, verify_jwt_in_request

from data_ec import connect
from datetime_utils import enrich_datetime_fields
from transaction_receipt import _parse_selected_options_field
from transaction_shipping import (
    load_shipping_for_transaction,
    shipping_payload_from_row,
    fulfillment_fields_from_row,
)
from transactions import (
    _load_return_request,
    _load_open_return_requests,
    _pair_for_sale,
    _status_payload,
)


def _request_timezone():
    return request.args.get("timezone") or request.args.get("tz")


def _to_float(value):
    if value is None:
        return 0.0
    if isinstance(value, str):
        value = value.replace("$", "").replace(",", "").strip()
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _enrich_order_datetimes(payload, tz_name):
    if not isinstance(payload, dict):
        return payload

    for key in ("sale",):
        section = payload.get(key)
        if isinstance(section, dict):
            enrich_datetime_fields(section, "transaction_datetime", tz_name)

    for section in payload.get("returns") or []:
        if isinstance(section, dict):
            enrich_datetime_fields(section, "transaction_datetime", tz_name)

    return payload


def resolve_order_uid(db, transaction_uid):
    """Map a sale or return transaction_uid to the root order (sale) uid."""
    row_q = db.execute(
        """
        SELECT transaction_uid,
               transaction_original_uid,
               COALESCE(transaction_type, 'sale') AS transaction_type
        FROM every_circle.transactions
        WHERE transaction_uid = %s
        """,
        (transaction_uid,),
    )
    rows = row_q.get("result") or []
    if not rows:
        return None, None

    row = rows[0]
    tx_type = row.get("transaction_type") or "sale"
    if tx_type == "return" and row.get("transaction_original_uid"):
        return row["transaction_original_uid"], row.get("transaction_uid")

    return row.get("transaction_uid"), None


def _viewer_profile_id():
    profile_id = request.args.get("profile_id")
    if profile_id:
        return str(profile_id)

    try:
        verify_jwt_in_request(optional=True)
        identity = get_jwt_identity()
        if identity:
            return str(identity)
    except Exception:
        pass

    return None


def _viewer_business_uid():
    return request.args.get("business_uid") or request.args.get("seller_id")


def _can_view_order(sale_row, profile_id, business_uid):
    if not sale_row:
        return False
    if profile_id and str(sale_row.get("transaction_profile_id")) == str(profile_id):
        return True
    if business_uid and str(sale_row.get("transaction_business_id")) == str(
        business_uid
    ):
        return True
    return False


def _load_sale_header(db, order_uid):
    sale_q = db.execute(
        """
        SELECT
            transaction_uid,
            transaction_datetime,
            transaction_profile_id,
            transaction_business_id,
            transaction_stripe_pi,
            transaction_total,
            transaction_amount,
            transaction_taxes,
            transaction_fees,
            transaction_in_escrow,
            transaction_return_requested,
            transaction_return_note,
            transaction_return_status,
            transaction_return_seller_note,
            COALESCE(transaction_type, 'sale') AS transaction_type
        FROM every_circle.transactions
        WHERE transaction_uid = %s
        """,
        (order_uid,),
    )
    rows = sale_q.get("result") or []
    if not rows:
        return None

    sale = rows[0]
    if (sale.get("transaction_type") or "sale") != "sale":
        return None
    return sale


def _line_name_case_sql():
    return """
        CASE
            WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_service_name
            WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_title
            WHEN ti.ti_bs_id LIKE '165-%%' THEN pw.profile_wish_title
            ELSE 'Unknown'
        END
    """


def _load_sale_lines(db, order_uid):
    name_case = _line_name_case_sql()
    lines_q = db.execute(
        f"""
        SELECT
            ti.ti_uid,
            ti.ti_bs_id,
            ti.ti_bs_qty,
            COALESCE(ti.ti_received_qty, 0) AS ti_received_qty,
            ti.ti_bs_cost,
            ti.ti_choices_extra_cost,
            ti.ti_special_instructions,
            ti.ti_selected_options,
            COALESCE(ti.ti_fulfillment_status, 'not_required') AS ti_fulfillment_status,
            COALESCE(ti.ti_shipped_qty, 0) AS ti_shipped_qty,
            ti.ti_shipped_at,
            ti.ti_tracking_carrier,
            ti.ti_tracking_number,
            ti.ti_fulfillment_note,
            {name_case} AS item_name,
            COALESCE((
                SELECT SUM(ABS(rti.ti_bs_qty))
                FROM every_circle.transactions_items rti
                INNER JOIN every_circle.transactions rt
                    ON rti.ti_transaction_id = rt.transaction_uid
                WHERE rt.transaction_original_uid = %s
                  AND COALESCE(rt.transaction_type, 'return') = 'return'
                  AND (
                      rti.ti_original_ti_uid = ti.ti_uid
                      OR (
                          rti.ti_original_ti_uid IS NULL
                          AND rti.ti_bs_id = ti.ti_bs_id
                      )
                  )
            ), 0) AS returned_qty
        FROM every_circle.transactions_items ti
        LEFT JOIN every_circle.business_services bs ON ti.ti_bs_id = bs.bs_uid
        LEFT JOIN every_circle.profile_expertise pe ON ti.ti_bs_id = pe.profile_expertise_uid
        LEFT JOIN every_circle.profile_wish pw ON ti.ti_bs_id = pw.profile_wish_uid
        WHERE ti.ti_transaction_id = %s
        ORDER BY ti.ti_uid ASC
        """,
        (order_uid, order_uid),
    )
    lines = []
    for row in lines_q.get("result") or []:
        order_qty = int(row.get("ti_bs_qty") or 0)
        returned_qty = int(row.get("returned_qty") or 0)
        line = {
            "ti_uid": row.get("ti_uid"),
            "ti_bs_id": row.get("ti_bs_id"),
            "ti_bs_qty": order_qty,
            "ti_received_qty": int(row.get("ti_received_qty") or 0),
            "returned_qty": returned_qty,
            "remaining_qty": max(order_qty - returned_qty, 0),
            "ti_bs_cost": row.get("ti_bs_cost"),
            "ti_choices_extra_cost": row.get("ti_choices_extra_cost"),
            "ti_special_instructions": row.get("ti_special_instructions"),
            "item_name": row.get("item_name"),
            "selected_options": _parse_selected_options_field(
                row.get("ti_selected_options")
            ),
            **fulfillment_fields_from_row(row),
        }
        lines.append(line)
    return lines


def _load_return_transactions(db, order_uid):
    name_case = _line_name_case_sql()
    returns_q = db.execute(
        """
        SELECT
            transaction_uid,
            transaction_datetime,
            transaction_total,
            transaction_amount,
            transaction_taxes,
            transaction_fees,
            transaction_return_note
        FROM every_circle.transactions
        WHERE transaction_original_uid = %s
          AND COALESCE(transaction_type, 'return') = 'return'
        ORDER BY transaction_datetime ASC, transaction_uid ASC
        """,
        (order_uid,),
    )
    returns = []
    for header in returns_q.get("result") or []:
        return_uid = header.get("transaction_uid")
        lines_q = db.execute(
            f"""
            SELECT
                ti.ti_uid,
                ti.ti_original_ti_uid,
                ti.ti_bs_id,
                ti.ti_bs_qty,
                ti.ti_bs_cost,
                ti.ti_choices_extra_cost,
                ti.ti_special_instructions,
                ti.ti_selected_options,
                {name_case} AS item_name
            FROM every_circle.transactions_items ti
            LEFT JOIN every_circle.business_services bs ON ti.ti_bs_id = bs.bs_uid
            LEFT JOIN every_circle.profile_expertise pe ON ti.ti_bs_id = pe.profile_expertise_uid
            LEFT JOIN every_circle.profile_wish pw ON ti.ti_bs_id = pw.profile_wish_uid
            WHERE ti.ti_transaction_id = %s
            ORDER BY ti.ti_uid ASC
            """,
            (return_uid,),
        )
        return_lines = []
        for row in lines_q.get("result") or []:
            qty = int(row.get("ti_bs_qty") or 0)
            return_lines.append(
                {
                    "ti_uid": row.get("ti_uid"),
                    "ti_original_ti_uid": row.get("ti_original_ti_uid"),
                    "ti_bs_id": row.get("ti_bs_id"),
                    "ti_bs_qty": qty,
                    "return_quantity": abs(qty),
                    "ti_bs_cost": row.get("ti_bs_cost"),
                    "ti_choices_extra_cost": row.get("ti_choices_extra_cost"),
                    "ti_special_instructions": row.get("ti_special_instructions"),
                    "item_name": row.get("item_name"),
                    "selected_options": _parse_selected_options_field(
                        row.get("ti_selected_options")
                    ),
                }
            )

        returns.append(
            {
                "transaction_uid": return_uid,
                "transaction_datetime": header.get("transaction_datetime"),
                "transaction_total": header.get("transaction_total"),
                "transaction_amount": header.get("transaction_amount"),
                "transaction_taxes": header.get("transaction_taxes"),
                "transaction_fees": header.get("transaction_fees"),
                "transaction_return_note": header.get("transaction_return_note"),
                "lines": return_lines,
            }
        )
    return returns


def _build_summary(sale, returns):
    gross_total = _to_float(sale.get("transaction_total"))
    returned_total = sum(abs(_to_float(r.get("transaction_total"))) for r in returns)
    net_total = round(gross_total - returned_total, 4)
    return {
        "gross_total": round(gross_total, 4),
        "returned_total": round(returned_total, 4),
        "net_total": net_total,
        "return_count": len(returns),
    }


class OrderDetail(Resource):
    """
    GET /api/v1/orders/<transaction_uid>

    transaction_uid may be the sale uid (order_uid) or any linked return uid.
    Caller must be the buyer (profile_id query param or JWT) or seller
    (business_uid / seller_id query param).
    """

    def get(self, transaction_uid):
        response = {}
        try:
            profile_id = _viewer_profile_id()
            business_uid = _viewer_business_uid()

            if not profile_id and not business_uid:
                response["message"] = (
                    "profile_id or business_uid is required to view this order"
                )
                response["code"] = 403
                return response, 403

            with connect() as db:
                order_uid, resolved_from_return_uid = resolve_order_uid(
                    db, transaction_uid
                )
                if not order_uid:
                    response["message"] = "Order not found"
                    response["code"] = 404
                    return response, 404

                sale = _load_sale_header(db, order_uid)
                if not sale:
                    response["message"] = "Order not found"
                    response["code"] = 404
                    return response, 404

                if not _can_view_order(sale, profile_id, business_uid):
                    response["message"] = "Not authorized to view this order"
                    response["code"] = 403
                    return response, 403

                sale_lines = _load_sale_lines(db, order_uid)
                returns = _load_return_transactions(db, order_uid)
                shipping = shipping_payload_from_row(
                    load_shipping_for_transaction(db, order_uid)
                )
                open_returns = _load_open_return_requests(db, order_uid)
                pending_return = open_returns[0] if open_returns else _load_return_request(
                    db, order_uid
                )
                return_status, refund_status = _pair_for_sale(sale, pending_return)
                status_fields = _status_payload(return_status, refund_status)

                def _pending_payload(req):
                    if not req:
                        return None
                    rs, fs = _pair_for_sale(sale, req)
                    return {
                        "trr_uid": req.get("trr_uid"),
                        "note": req.get("trr_note"),
                        "estimated_total": req.get("trr_estimated_total"),
                        "items": req.get("items") or [],
                        "return_transaction_uid": req.get("trr_return_transaction_uid"),
                        "stripe_refund_id": req.get("trr_stripe_refund_id"),
                        "created_at": req.get("trr_created_at"),
                        "updated_at": req.get("trr_updated_at"),
                        **_status_payload(rs, fs),
                    }

                pending_returns_payload = [
                    _pending_payload(req) for req in open_returns
                ]
                pending_return_payload = (
                    pending_returns_payload[0] if pending_returns_payload else None
                )

                sale_payload = dict(sale)
                sale_payload["lines"] = sale_lines
                sale_payload["pending_return"] = pending_return_payload
                sale_payload["pending_returns"] = pending_returns_payload
                sale_payload.update(status_fields)
                sale_payload.update(shipping)

                payload = {
                    "order_uid": order_uid,
                    "requested_transaction_uid": transaction_uid,
                    "resolved_from_return_uid": resolved_from_return_uid,
                    "sale": sale_payload,
                    "returns": returns,
                    "pending_return": pending_return_payload,
                    "pending_returns": pending_returns_payload,
                    "summary": _build_summary(sale, returns),
                    **status_fields,
                    **shipping,
                }

                tz_name = _request_timezone()
                payload = _enrich_order_datetimes(payload, tz_name)

                response["message"] = "Order retrieved successfully"
                response["code"] = 200
                response.update(payload)
                if tz_name:
                    response["timezone"] = tz_name
                return response, 200

        except Exception as e:
            print(f"Error in OrderDetail GET: {e}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {e}"
            response["code"] = 500
            return response, 500
