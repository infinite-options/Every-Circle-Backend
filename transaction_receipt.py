from flask_restful import Resource
from flask import request
import json
import re

from data_ec import connect
from transaction_shipping import (
    load_shipping_for_transaction,
    shipping_payload_from_row,
    fulfillment_fields_from_row,
)
from units_ledger import (
    sale_units_ledger,
    attach_line_units_ledgers,
    sale_display,
    fulfillment_method,
)


def _parse_selected_options_field(raw):
    if raw is None:
        return []
    if isinstance(raw, list):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, list) else []
        except (TypeError, ValueError, json.JSONDecodeError):
            return []
    return []


def _format_offering_rate_display(cost, currency):
    """Format expertise rate like '$25/each' from snapshotted checkout fields."""
    from line_commerce_fields import format_offering_rate_display

    if cost is None or str(cost).strip() == "":
        return None
    rate = format_offering_rate_display(cost)
    if rate:
        return rate
    display = str(cost).strip()
    currency = (currency or "").strip()
    if currency and currency not in display:
        display = f"{display}{currency}"
    if not display.startswith("$") and re.match(r"^\d", display):
        display = f"${display}"
    if "/each" not in display:
        display = f"{display}/each"
    return display


def _append_seller_filter(query, params, seller_id):
    """Match product, expertise, or wish lines for common seller_id shapes."""
    if not seller_id:
        return query, params
    if seller_id.startswith("150-"):
        query += " AND ti.ti_bs_id = %s"
        params.append(seller_id)
    elif seller_id.startswith("165-"):
        query += " AND ti.ti_bs_id = %s"
        params.append(seller_id)
    else:
        query += """
            AND (
                bs.bs_business_id = %s
                OR pe.profile_expertise_profile_personal_id = %s
                OR t.transaction_business_id = %s
            )
        """
        params.extend([seller_id, seller_id, seller_id])
    return query, params


def _enrich_receipt_line(row):
    """Expose persisted checkout choices and aliases expected by Account receipt UI."""
    if not isinstance(row, dict):
        return row

    ti_bs_id = row.get("ti_bs_id") or ""
    if str(ti_bs_id).startswith("250-"):
        row["bs_uid"] = ti_bs_id

    choices_extra = row.get("ti_choices_extra_cost")
    if choices_extra is not None:
        row["choices_extra_cost"] = choices_extra

    special = row.get("ti_special_instructions")
    if special:
        row["special_instructions"] = special

    unit_price = row.get("ti_bs_cost")
    if unit_price is not None:
        row["unit_price"] = unit_price

    selected_options = row.get("selected_options") or []
    selected_choice_items = []
    selected_choices = {}
    selected_choice_labels = {}
    for opt in selected_options:
        if not isinstance(opt, dict):
            continue
        group = (opt.get("group_title") or opt.get("groupTitle") or "").strip()
        label = (opt.get("label") or "").strip()
        bso_uid = (opt.get("bso_uid") or opt.get("id") or "").strip()
        extra_cost = opt.get("extra_cost")
        if extra_cost is None:
            extra_cost = 0
        selected_choice_items.append(
            {
                "groupTitle": group,
                "label": label,
                "extra_cost": extra_cost,
                "bso_uid": bso_uid,
            }
        )
        if group:
            if bso_uid:
                selected_choices[group] = bso_uid
            if label:
                selected_choice_labels[group] = label

    if selected_choice_items:
        row["selected_choice_items"] = selected_choice_items
        row["selected_choices"] = selected_choices
        row["selected_choice_labels"] = selected_choice_labels
    else:
        row.setdefault("selected_options", [])

    if str(ti_bs_id).startswith("150-"):
        rate = _format_offering_rate_display(
            row.get("profile_expertise_cost") or row.get("ti_bs_cost"),
            row.get("profile_expertise_cost_currency"),
        )
        if rate:
            row["offering_rate_display"] = rate
        row["purchase_type"] = "expertise"
    elif str(ti_bs_id).startswith("250-"):
        rate = _format_offering_rate_display(row.get("ti_bs_cost"))
        if rate:
            row["offering_rate_display"] = rate
        row["purchase_type"] = "service"

    from line_commerce_fields import (
        line_merchandise_total_from_row,
        line_snapshot_api_fields,
        order_money_from_line_snapshots,
    )

    money = order_money_from_line_snapshots(row)
    if money.get("known"):
        row["money"] = money
        row.update(line_snapshot_api_fields(row))
        merch_total = line_merchandise_total_from_row(row)
        if merch_total is not None:
            row["line_merchandise_total"] = merch_total

    return row


_RECEIPT_LINE_SELECT = """
    SELECT
        t.transaction_uid,
        t.transaction_profile_id,
        t.transaction_datetime,
        t.transaction_total,
        t.transaction_amount,
        t.transaction_taxes,
        t.transaction_fees,
        t.transaction_shipping,
        t.transaction_in_escrow,
        ti.ti_uid,
        ti.ti_bs_id,
        ti.ti_bs_qty,
        COALESCE(ti.ti_received_qty, 0) AS ti_received_qty,
        ti.ti_bs_cost,
        ti.ti_choices_extra_cost,
        ti.ti_shipping_amount,
        ti.ti_shipping_refundable,
        ti.ti_special_instructions,
        ti.ti_selected_options,
        COALESCE(ti.ti_fulfillment_status, 'not_required') AS ti_fulfillment_status,
        COALESCE(ti.ti_shipped_qty, 0) AS ti_shipped_qty,
        ti.ti_shipped_at,
        ti.ti_tracking_carrier,
        ti.ti_tracking_number,
        ti.ti_fulfillment_note,
        ti.ti_fulfillment_method,
        ti.ti_shipping_not_required,
        ti.ti_line_shipping_amount,
        ti.ti_listing_shipping,
        CASE
            WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_service_name
            WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_title
            WHEN ti.ti_bs_id LIKE '165-%%' THEN pw.profile_wish_title
            ELSE 'Unknown'
        END AS bs_service_name,
        CASE
            WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_business_id
            WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_uid
            ELSE NULL
        END AS seller_ref_id,
        pe.profile_expertise_cost,
        pe.profile_expertise_cost_currency,
        bs.bs_bounty,
        bs.bs_bounty_type,
        pe.profile_expertise_bounty,
        pe.profile_expertise_bounty_type,
        COALESCE(
            (
                SELECT JSON_ARRAYAGG(
                    JSON_OBJECT(
                        'group_title', bso.bso_group_title,
                        'option_label', bso.bso_option_label,
                        'extra_cost',   bso.bso_extra_cost
                    )
                )
                FROM every_circle.business_services_options bso
                WHERE bso.bso_business_service_id = ti.ti_bs_id
                AND bso.bso_is_active = 1
                AND bso.bso_extra_cost > 0
            ),
            JSON_ARRAY()
        ) AS available_options
    FROM every_circle.transactions t
    INNER JOIN every_circle.transactions_items ti
        ON ti.ti_transaction_id = t.transaction_uid
    LEFT JOIN every_circle.business_services bs
        ON ti.ti_bs_id = bs.bs_uid
    LEFT JOIN every_circle.profile_expertise pe
        ON ti.ti_bs_id = pe.profile_expertise_uid
    LEFT JOIN every_circle.profile_wish pw
        ON ti.ti_bs_id = pw.profile_wish_uid
    WHERE t.transaction_profile_id = %s
        AND t.transaction_uid = %s
"""


def _load_receipt_lines(db, profile_id, transaction_uid, seller_id=None):
    query = _RECEIPT_LINE_SELECT
    params = [profile_id, transaction_uid]
    query, params = _append_seller_filter(query, list(params), seller_id)
    result = db.execute(query, tuple(params))
    if result.get("code") != 200:
        return None, result
    return result.get("result") or [], None


def _load_expertise_receipt_lines(db, profile_id, transaction_uid, seller_id=None):
    """Offering lines from transactions_items when the main query returns nothing."""
    params = [profile_id, transaction_uid]
    seller_clause = ""
    if seller_id:
        if str(seller_id).startswith("150-"):
            seller_clause = " AND ti.ti_bs_id = %s"
            params.append(seller_id)
        else:
            seller_clause = """
                AND (
                    pe.profile_expertise_profile_personal_id = %s
                    OR t.transaction_business_id = %s
                )
            """
            params.extend([seller_id, seller_id])

    q = db.execute(
        f"""
        {_RECEIPT_LINE_SELECT}
          AND ti.ti_bs_id LIKE '150-%%'
          {seller_clause}
        ORDER BY ti.ti_uid ASC
        """,
        tuple(params),
    )
    return q.get("result") or []


def _enrich_receipt_fulfillment_line(db, row):
    """Line-level cancel/ship fields aligned with order detail sale.lines."""
    from transactions import _confirmed_return_split, _remaining_to_ship_qty

    order_uid = row.get("transaction_uid")
    ti_uid = row.get("ti_uid")
    if not order_uid or not ti_uid:
        return row

    order_qty = int(row.get("ti_bs_qty") or 0)
    shipped_qty = int(row.get("ti_shipped_qty") or 0)
    returned_qty, cancelled_qty = _confirmed_return_split(db, order_uid, ti_uid)
    remaining_to_ship = _remaining_to_ship_qty(
        db,
        order_uid,
        ti_uid,
        order_qty,
        shipped_qty,
        ti_row=row,
    )
    active_units = max(order_qty - cancelled_qty - returned_qty, 0)
    row["cancelled_qty"] = cancelled_qty
    row["cancel_unshipped_qty"] = cancelled_qty
    row["returned_qty"] = returned_qty
    row["returned_qty_total"] = cancelled_qty + returned_qty
    row["remaining_qty"] = active_units
    row["remaining_to_ship"] = remaining_to_ship
    row.update(fulfillment_fields_from_row(row))
    return row


def _process_receipt_rows(db, rows):
    enriched_rows = []
    for row in rows:
        if not row.get("ti_uid"):
            continue
        raw = row.get("available_options")
        if isinstance(raw, str):
            try:
                row["available_options"] = json.loads(raw)
            except Exception:
                row["available_options"] = []
        elif raw is None:
            row["available_options"] = []

        row["selected_options"] = _parse_selected_options_field(
            row.pop("ti_selected_options", None)
        )
        enriched_rows.append(
            _enrich_receipt_line(_enrich_receipt_fulfillment_line(db, row))
        )
    return enriched_rows


def _attach_receipt_bounty_fields(db, lines, profile_id):
    """Per-line seller bounty pool + optional buyer referrer share (receipt Bounty column)."""
    from line_commerce_fields import attach_sale_lines_commerce, round_money
    from transactions import _seller_bounty_pool_for_line_row
    from account_screen_v3_contract import normalize_tb_percentage_display

    if not lines:
        return lines

    attach_sale_lines_commerce(db, lines, buyer_profile_id=profile_id)

    for line in lines:
        if not isinstance(line, dict):
            continue
        pool = round_money(line.get("line_bounty_paid"))
        if pool <= 0:
            pool = round_money(_seller_bounty_pool_for_line_row(line))
            if pool > 0:
                line["line_bounty_paid"] = pool

        if pool <= 0:
            for key in (
                "line_bounty_paid",
                "ti_bs_bounty",
                "ti_bs_bounty_type",
                "bs_bounty",
                "bs_bounty_type",
                "bounty_amount",
                "item_bounty",
                "bounty_earned",
                "tb_amount",
                "tb_percentage",
            ):
                line.pop(key, None)
            continue

        pct = line.get("tb_percentage")
        if pct is not None:
            normalized = normalize_tb_percentage_display(pct)
            if normalized is not None:
                line["tb_percentage"] = normalized

    return lines


def _attach_receipt_bounty_totals(rows, payload):
    """Transaction-level bounty footer fields from line pools."""
    from line_commerce_fields import round_money

    if not rows:
        return payload

    total_pool = round_money(
        sum(round_money(row.get("line_bounty_paid")) for row in rows if isinstance(row, dict))
    )
    if total_pool <= 0:
        return payload

    payload["bounty_paid"] = total_pool
    payload["total_bounty_paid"] = total_pool
    payload["transaction_bounty"] = total_pool

    header = rows[0] if isinstance(rows[0], dict) else {}
    amount = round_money(header.get("transaction_amount"))
    taxes = round_money(header.get("transaction_taxes"))
    shipping = round_money(header.get("transaction_shipping"))
    fees = round_money(header.get("transaction_fees"))
    total_paid = round_money(amount + taxes + shipping + fees)
    if total_paid > 0:
        payload["total_amount_paid"] = total_paid

    return payload


def _build_receipt_v2(db, order_uid, enriched_rows, shipping):
    """v2 envelope: order-level + line-level units matching account-screen."""
    if not enriched_rows:
        return {}

    header = dict(enriched_rows[0])
    header.update(shipping or {})
    if not header.get("fulfillment_method"):
        header["fulfillment_method"] = fulfillment_method(header)

    units = sale_units_ledger(db, order_uid)
    lines = attach_line_units_ledgers(db, order_uid, enriched_rows)

    v2 = {
        "schema_version": 3,
        "order_uid": order_uid,
        "transaction_uid": order_uid,
        "units": units,
        "display": sale_display(header, units, include_qty=False),
        "lines": lines,
    }
    return v2


class TransactionReceipt(Resource):
    def get(self, profile_id, transaction_uid):
        print(f"In TransactionReceipt GET for profile_id: {profile_id}, transaction_uid: {transaction_uid}")
        response = {}
        seller_id = request.args.get('seller_id')

        try:
            with connect() as db:
                rows, err = _load_receipt_lines(
                    db, profile_id, transaction_uid, seller_id
                )
                if err:
                    response["message"] = err.get(
                        "message", "Error retrieving transaction receipt"
                    )
                    response["code"] = err.get("code", 500)
                    return response, response["code"]

                enriched_rows = _process_receipt_rows(db, rows)

                if not enriched_rows:
                    expertise_rows = _load_expertise_receipt_lines(
                        db, profile_id, transaction_uid, seller_id
                    )
                    enriched_rows = _process_receipt_rows(db, expertise_rows)

                if not enriched_rows and not seller_id:
                    expertise_rows = _load_expertise_receipt_lines(
                        db, profile_id, transaction_uid, None
                    )
                    enriched_rows = _process_receipt_rows(db, expertise_rows)

                _attach_receipt_bounty_fields(db, enriched_rows, profile_id)

                shipping = shipping_payload_from_row(
                    load_shipping_for_transaction(db, transaction_uid)
                )

                v2 = _build_receipt_v2(db, transaction_uid, enriched_rows, shipping)

                response["message"] = "Transaction receipt retrieved successfully"
                response["code"] = 200
                response["data"] = enriched_rows
                response.update(shipping)
                if v2:
                    response.update(v2)
                _attach_receipt_bounty_totals(enriched_rows, response)
                if v2:
                    _attach_receipt_bounty_totals(enriched_rows, v2)
                return response, 200

        except Exception as e:
            print(f"Error in TransactionReceipt GET: {str(e)}")
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500
