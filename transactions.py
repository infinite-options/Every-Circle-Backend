from aiohttp import payload
from flask_restful import Resource
from datetime import datetime
import traceback
from flask import request, jsonify
import json

from flask_jwt_extended import get_jwt_identity, verify_jwt_in_request

from data_ec import connect, processImage
from moderation import MODERATED_ACTIVE, is_owner_available_for_public_interaction
from user_path_connection import ConnectionsPath
from escrow_release import release_escrow_for_transaction, summarize_escrow_result
from wallet_ids import EC_WALLET_ID
from wallet_service import credit_bounty_to_wallet, debit_bounty_from_wallet
from datetime_utils import utc_now_str, enrich_datetime_fields


def _request_timezone():
    return request.args.get("timezone") or request.args.get("tz")


def _enrich_transaction_rows(rows):
    tz_name = _request_timezone()
    enriched = []
    for row in rows or []:
        if isinstance(row, dict):
            enriched.append(
                enrich_datetime_fields(dict(row), "transaction_datetime", tz_name)
            )
        else:
            enriched.append(row)
    return enriched


def _strip_currency(value):
    """Remove $ and commas from currency values before storing."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.replace("$", "").replace(",", "").strip()
    return value


def _to_float(value):
    if value is None:
        return 0.0
    try:
        return float(_strip_currency(value))
    except (TypeError, ValueError):
        return 0.0


def _build_selected_options(item):
    """Normalize selected service options from checkout payload."""
    selected = item.get("selected_choices") or {}
    labels = item.get("selected_choice_labels") or {}
    choice_items = item.get("selected_choice_items") or []

    options = []
    if choice_items:
        for opt in choice_items:
            group = (opt.get("groupTitle") or opt.get("group_title") or "").strip()
            options.append(
                {
                    "group_title": group,
                    "bso_uid": selected.get(group),
                    "label": opt.get("label") or labels.get(group),
                    "extra_cost": _to_float(opt.get("extra_cost")),
                }
            )
    elif selected:
        for group, bso_uid in selected.items():
            options.append(
                {
                    "group_title": group,
                    "bso_uid": bso_uid,
                    "label": labels.get(group),
                    "extra_cost": 0.0,
                }
            )
    return options or None


def _selected_options_json(item):
    options = _build_selected_options(item)
    return json.dumps(options) if options else None


def _apply_item_options_to_tx_item(tx_item, item, ti_bs_id):
    """Persist selected options, special instructions, and line price from checkout."""
    selected_json = _selected_options_json(item)
    if selected_json:
        tx_item["ti_selected_options"] = selected_json

    special = (item.get("special_instructions") or "").strip()
    if special:
        tx_item["ti_special_instructions"] = special

    if item.get("choices_extra_cost") is not None:
        tx_item["ti_choices_extra_cost"] = _to_float(item.get("choices_extra_cost"))

    unit_price = item.get("unit_price")
    if unit_price is not None and ti_bs_id and str(ti_bs_id).startswith("250"):
        tx_item["ti_bs_cost"] = _strip_currency(unit_price)


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


def _bounty_scale_for_line(return_qty, original_qty):
    """Scale bounty reversal for a partial line return (return_qty / original_qty)."""
    if original_qty <= 0:
        return None
    rq = int(return_qty)
    oq = int(original_qty)
    if rq <= 0 or rq > oq:
        return None
    return rq / float(oq)


def _tax_amount_for_line(line_subtotal, ti_bs_is_taxable, ti_bs_tax_rate):
    if not ti_bs_is_taxable:
        return 0.0
    rate = _to_float(ti_bs_tax_rate)
    if rate <= 0:
        return 0.0
    # Rates may be stored as whole percent (e.g. 8.25) or fraction (0.0825).
    if rate > 1:
        rate = rate / 100.0
    return round(line_subtotal * rate, 4)


CHARITY_PROFILE_ID = "charity"
_BOUNTY_NETWORK_POOL = 0.40
_BOUNTY_NETWORK_MAX_PERSON = 0.20


def _fetch_connection_path(path_from, path_to):
    """Return combined_path string or None."""
    if not path_from or not path_to:
        return None
    try:
        connections_path = ConnectionsPath()
        network_response, network_status = connections_path.get(path_from, path_to)
        if network_status != 200 or not network_response.get("combined_path"):
            print(
                f"Warning: Could not find connection path "
                f"{path_from} -> {path_to}. Status: {network_status}, "
                f"Response: {network_response}"
            )
            return None
        combined_path = network_response["combined_path"]
        print("network combined_path: ", combined_path)
        return combined_path
    except Exception as e:
        print(f"Error getting connection path: {str(e)}")
        return None


def _middle_path_nodes(combined_path, seen):
    """Nodes strictly between path endpoints, excluding known bounty recipients."""
    if not combined_path:
        return []
    try:
        uids = combined_path.split(",")
        middle = uids[1:-1] if len(uids) > 2 else []
        return [uid for uid in middle if uid and uid not in seen]
    except Exception as e:
        print(f"Error processing network path: {str(e)}")
        return []


def _bounty_pct_amount(effective_bounty, percentage):
    return {
        "tb_percentage": str(percentage),
        "tb_amount": round(percentage * effective_bounty, 4),
    }


def _charity_share_is_payable(charity_amount, charity_pct):
    """Skip charity bounty rows when there is nothing meaningful to pay."""
    if not charity_amount or charity_amount <= 0:
        return False
    try:
        return float(charity_pct) > 0
    except (TypeError, ValueError):
        return charity_amount > 0


def _without_zero_charity(participants):
    return [
        p
        for p in participants
        if p.get("tb_profile_id") != CHARITY_PROFILE_ID
        or _charity_share_is_payable(p.get("tb_amount"), p.get("tb_percentage"))
    ]


def _network_participants_capped(middle_uids, effective_bounty):
    """
    Split the 40% network pool across intermediaries; cap each at 20%.
    Any undistributed share goes to charity.
    """
    pool = round(_BOUNTY_NETWORK_POOL * effective_bounty, 4)
    max_per = round(_BOUNTY_NETWORK_MAX_PERSON * effective_bounty, 4)
    if not middle_uids:
        if pool <= 0:
            return []
        return [
            {
                "tb_profile_id": CHARITY_PROFILE_ID,
                **_bounty_pct_amount(effective_bounty, _BOUNTY_NETWORK_POOL),
            }
        ]

    per_person = min(round(pool / len(middle_uids), 4), max_per)
    total_paid = round(per_person * len(middle_uids), 4)
    charity_amount = round(pool - total_paid, 4)
    person_pct = round(per_person / effective_bounty, 4) if effective_bounty else 0

    participants = [
        {
            "tb_profile_id": uid,
            "tb_percentage": str(person_pct),
            "tb_amount": per_person,
        }
        for uid in middle_uids
    ]
    if charity_amount > 0:
        charity_pct = (
            round(charity_amount / effective_bounty, 4) if effective_bounty else 0
        )
        if _charity_share_is_payable(charity_amount, charity_pct):
            participants.append(
                {
                    "tb_profile_id": CHARITY_PROFILE_ID,
                    "tb_percentage": str(charity_pct),
                    "tb_amount": charity_amount,
                }
            )
    return participants


def _network_participants_business(middle_uids, effective_bounty, seen):
    """Legacy business network split: equal shares of 40%, pad with charity if < 2."""
    network_result = list(middle_uids)
    if len(network_result) < 2 and CHARITY_PROFILE_ID not in seen:
        network_result.append(CHARITY_PROFILE_ID)
    if not network_result:
        return []
    network_percentage = _BOUNTY_NETWORK_POOL / len(network_result)
    return _without_zero_charity(
        [
            {
                "tb_profile_id": uid,
                **_bounty_pct_amount(effective_bounty, network_percentage),
            }
            for uid in network_result
        ]
    )


def _get_authenticated_profile_id():
    try:
        verify_jwt_in_request(optional=True)
        identity = get_jwt_identity()
        if identity:
            return str(identity)
    except Exception:
        pass

    body = request.get_json(silent=True) or {}
    profile_id = body.get("profile_id")
    return str(profile_id) if profile_id else None


def _resolve_transaction_item(db, transaction_uid, transaction_item_uid):
    """Resolve a line item by ti_uid first, then ti_bs_id (same pattern as returns)."""
    ti_q = db.execute(
        """
        SELECT ti_uid, ti_bs_id, ti_bs_qty, COALESCE(ti_received_qty, 0) AS ti_received_qty
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s AND ti_uid = %s
        """,
        (transaction_uid, transaction_item_uid),
    )
    ti_rows = ti_q.get("result") or []
    if ti_rows:
        return ti_rows[0]

    ti_q = db.execute(
        """
        SELECT ti_uid, ti_bs_id, ti_bs_qty, COALESCE(ti_received_qty, 0) AS ti_received_qty
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s AND ti_bs_id = %s
        """,
        (transaction_uid, transaction_item_uid),
    )
    ti_rows = ti_q.get("result") or []
    return ti_rows[0] if ti_rows else None


def _all_lines_fully_received(db, transaction_uid):
    incomplete_q = db.execute(
        """
        SELECT COUNT(*) AS incomplete_count
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s
          AND ti_bs_qty > 0
          AND COALESCE(ti_received_qty, 0) < ti_bs_qty
        """,
        (transaction_uid,),
    )
    rows = incomplete_q.get("result") or []
    return int(rows[0].get("incomplete_count") or 0) == 0 if rows else False


class ReturnTransaction(Resource):
    """
    POST: record a return as a negative sale (new transaction + negative line items + bounty reversals).

    Required from Front End:
      - profile_id: buyer profile (must match original transaction_profile_id).
      - transaction_uid: original sale to return against.
      - transaction_return_items: [{ "transaction_item_uid": "<ti_uid>", "return_quantity": <int> }, ...]

    Optional from Front End:
      - transaction_return_note
      - transaction_return_requested / transaction_return_status (updates original tx metadata after success)

    Loaded from DB (not required from FE):
      - Original transaction row (seller, buyer, stripe PI, historical taxes/fees).
      - Each transactions_items row (ti_bs_cost, qty, tax flags, ti_bs_id).
      - transactions_bounty rows per original ti_uid for reversal amounts.

    Still missing / follow-ups:
      - Cumulative returned qty per line (needs column or ledger query) to block over-returning.
      - Stripe Refund API call + storing refund id (money movement).
      - Restocking via BusinessServicePurchase inverse when applicable.
    """

    def post(self):
        print("In ReturnTransaction POST")
        response = {}

        try:
            payload = request.get_json()
            if not payload:
                response["message"] = "Request body is required"
                response["code"] = 400
                return response, 400

            profile_id = payload.get("profile_id")
            original_tx_uid = payload.get("transaction_uid")
            items_payload = payload.get("transaction_return_items") or []

            if not profile_id:
                response["message"] = "profile_id is required"
                response["code"] = 400
                return response, 400
            if not original_tx_uid:
                response["message"] = "transaction_uid is required"
                response["code"] = 400
                return response, 400
            if not isinstance(items_payload, list) or len(items_payload) == 0:
                response["message"] = (
                    "transaction_return_items must be a non-empty list"
                )
                response["code"] = 400
                return response, 400

            return_note = payload.get("transaction_return_note")

            with connect() as db:
                tx_row_q = db.execute(
                    """
                    SELECT transaction_uid, transaction_profile_id, transaction_business_id,
                           transaction_stripe_pi, transaction_total, transaction_amount,
                           transaction_taxes, transaction_fees
                    FROM every_circle.transactions
                    WHERE transaction_uid = %s
                    """,
                    (original_tx_uid,),
                )
                tx_rows = tx_row_q.get("result") or []
                if not tx_rows:
                    response["message"] = "Original transaction not found"
                    response["code"] = 404
                    return response, 404

                orig_tx = tx_rows[0]
                if orig_tx.get("transaction_profile_id") != profile_id:
                    response["message"] = (
                        "profile_id does not match the buyer on this transaction"
                    )
                    response["code"] = 403
                    return response, 403

                subtotal_q = db.execute(
                    """
                    SELECT COALESCE(SUM(CAST(ti_bs_cost AS DECIMAL(18,6)) * ti_bs_qty), 0) AS order_subtotal
                    FROM every_circle.transactions_items
                    WHERE ti_transaction_id = %s
                    """,
                    (original_tx_uid,),
                )
                order_subtotal_rows = subtotal_q.get("result") or []
                order_subtotal = _to_float(
                    order_subtotal_rows[0].get("order_subtotal")
                    if order_subtotal_rows
                    else 0
                )

                refund_subtotal = 0.0
                refund_tax = 0.0
                lines_processed = []

                seen_ti = set()
                for entry in items_payload:
                    ti_uid = entry.get("transaction_item_uid")
                    try:
                        rq = int(entry.get("return_quantity"))
                    except (TypeError, ValueError):
                        rq = -1

                    if not ti_uid:
                        response["message"] = (
                            "Each entry requires transaction_item_uid"
                        )
                        response["code"] = 400
                        return response, 400
                    if rq < 1:
                        response["message"] = (
                            f"Invalid return_quantity for item {ti_uid}"
                        )
                        response["code"] = 400
                        return response, 400
                    if ti_uid in seen_ti:
                        response["message"] = (
                            f"Duplicate transaction_item_uid: {ti_uid}"
                        )
                        response["code"] = 400
                        return response, 400
                    seen_ti.add(ti_uid)

                    ti_q = db.execute(
                        """
                        SELECT ti_uid, ti_transaction_id, ti_bs_id, ti_bs_qty, ti_bs_cost,
                               ti_bs_cost_currency, ti_bs_sku, ti_bs_is_taxable, ti_bs_tax_rate,
                               ti_bs_refund_policy, ti_bs_return_window_days,
                               ti_selected_options, ti_special_instructions, ti_choices_extra_cost
                        FROM every_circle.transactions_items
                        WHERE ti_uid = %s AND ti_transaction_id = %s
                        """,
                        (ti_uid, original_tx_uid),
                    )
                    ti_rows = ti_q.get("result") or []
                    if not ti_rows:
                        response["message"] = (
                            f"Transaction item not found on this sale: {ti_uid}"
                        )
                        response["code"] = 404
                        return response, 404

                    ti_row = ti_rows[0]
                    original_qty = int(ti_row.get("ti_bs_qty") or 0)
                    unit_cost = _to_float(ti_row.get("ti_bs_cost"))
                    scale = _bounty_scale_for_line(rq, original_qty)
                    if scale is None:
                        response["message"] = (
                            f"return_quantity must be between 1 and {original_qty} for {ti_uid}"
                        )
                        response["code"] = 400
                        return response, 400

                    line_subtotal = round(unit_cost * rq, 4)
                    line_tax = _tax_amount_for_line(
                        line_subtotal,
                        ti_row.get("ti_bs_is_taxable"),
                        ti_row.get("ti_bs_tax_rate"),
                    )
                    refund_subtotal += line_subtotal
                    refund_tax += line_tax

                    lines_processed.append(
                        {
                            "original_ti_uid": ti_uid,
                            "ti_bs_id": ti_row.get("ti_bs_id"),
                            "return_quantity": rq,
                            "original_quantity": original_qty,
                            "unit_cost": unit_cost,
                            "line_subtotal": line_subtotal,
                            "line_tax": line_tax,
                            "snapshot": ti_row,
                        }
                    )

                orig_fees = abs(_to_float(orig_tx.get("transaction_fees")))
                fee_ratio = (
                    refund_subtotal / order_subtotal
                    if order_subtotal > 0
                    else 0.0
                )
                refund_fees = round(orig_fees * fee_ratio, 4)

                refund_grand = round(refund_subtotal + refund_tax + refund_fees, 4)

                new_uid_resp = db.call(procedure="new_transaction_uid")
                if (
                    not new_uid_resp.get("result")
                    or len(new_uid_resp["result"]) == 0
                ):
                    response["message"] = "Failed to generate return transaction UID"
                    response["code"] = 500
                    return response, 500

                new_transaction_uid = new_uid_resp["result"][0]["new_id"]
                transactions_datetime = utc_now_str()

                new_transaction = {
                    "transaction_uid": new_transaction_uid,
                    "transaction_datetime": transactions_datetime,
                    "transaction_profile_id": orig_tx.get("transaction_profile_id"),
                    "transaction_business_id": orig_tx.get("transaction_business_id"),
                    "transaction_stripe_pi": orig_tx.get("transaction_stripe_pi"),
                    "transaction_total": f"{-refund_grand:.4f}",
                    "transaction_amount": f"{-refund_subtotal:.4f}",
                    "transaction_taxes": f"{-refund_tax:.4f}",
                    "transaction_fees": f"{-refund_fees:.4f}",
                    "transaction_in_escrow": 0,
                    "transaction_return_note": return_note,
                }

                tx_insert = db.insert("every_circle.transactions", new_transaction)
                if tx_insert.get("code") != 200:
                    response["message"] = tx_insert.get(
                        "message", "Failed to insert return transaction"
                    )
                    response["code"] = tx_insert.get("code", 500)
                    return response, response["code"]

                bounty_insert_count = 0
                item_insert_count = 0
                response_lines = []

                for line in lines_processed:
                    ti_row = line["snapshot"]
                    rq = line["return_quantity"]
                    original_qty = line["original_quantity"]
                    ti_bs_id = ti_row.get("ti_bs_id")

                    ti_uid_resp = db.call(procedure="new_transaction_item_uid")
                    if (
                        not ti_uid_resp.get("result")
                        or len(ti_uid_resp["result"]) == 0
                    ):
                        response["message"] = "Failed to generate return line item UID"
                        response["code"] = 500
                        return response, 500

                    new_ti_uid = ti_uid_resp["result"][0]["new_id"]
                    neg_qty = -int(rq)

                    tx_item = {
                        "ti_uid": new_ti_uid,
                        "ti_transaction_id": new_transaction_uid,
                        "ti_bs_id": ti_bs_id,
                        "ti_bs_qty": neg_qty,
                        "ti_bs_cost": ti_row.get("ti_bs_cost"),
                        "ti_bs_cost_currency": ti_row.get("ti_bs_cost_currency"),
                        "ti_bs_sku": ti_row.get("ti_bs_sku"),
                        "ti_bs_is_taxable": ti_row.get("ti_bs_is_taxable"),
                        "ti_bs_tax_rate": ti_row.get("ti_bs_tax_rate"),
                        "ti_bs_refund_policy": ti_row.get("ti_bs_refund_policy"),
                        "ti_bs_return_window_days": ti_row.get(
                            "ti_bs_return_window_days"
                        ),
                    }
                    if ti_row.get("ti_selected_options") is not None:
                        tx_item["ti_selected_options"] = ti_row.get("ti_selected_options")
                    if ti_row.get("ti_special_instructions"):
                        tx_item["ti_special_instructions"] = ti_row.get(
                            "ti_special_instructions"
                        )
                    if ti_row.get("ti_choices_extra_cost") is not None:
                        tx_item["ti_choices_extra_cost"] = ti_row.get(
                            "ti_choices_extra_cost"
                        )

                    ti_insert = db.insert(
                        "every_circle.transactions_items", tx_item
                    )
                    if ti_insert.get("code") != 200:
                        response["message"] = ti_insert.get(
                            "message", "Failed to insert return transaction item"
                        )
                        response["code"] = ti_insert.get("code", 500)
                        return response, response["code"]
                    item_insert_count += 1

                    scale = _bounty_scale_for_line(rq, original_qty)
                    if scale is None:
                        scale = 0.0

                    bounty_q = db.execute(
                        """
                        SELECT tb_uid, tb_profile_id, tb_percentage, tb_amount
                        FROM every_circle.transactions_bounty
                        WHERE tb_ti_id = %s
                        """,
                        (line["original_ti_uid"],),
                    )
                    bounty_rows = bounty_q.get("result") or []

                    for br in bounty_rows:
                        raw_amt = _to_float(br.get("tb_amount"))
                        reversal = round(-scale * raw_amt, 4)
                        if reversal == 0:
                            continue

                        bounty_uid_resp = db.call(procedure="new_transaction_bounty_uid")
                        if (
                            not bounty_uid_resp.get("result")
                            or len(bounty_uid_resp["result"]) == 0
                        ):
                            print(
                                "Warning: Failed to generate bounty UID for reversal"
                            )
                            continue

                        new_tb_uid = bounty_uid_resp["result"][0]["new_id"]
                        tx_bounty = {
                            "tb_uid": new_tb_uid,
                            "tb_ti_id": new_ti_uid,
                            "tb_profile_id": br.get("tb_profile_id"),
                            "tb_percentage": br.get("tb_percentage"),
                            "tb_amount": reversal,
                        }
                        bins = db.insert(
                            "every_circle.transactions_bounty", tx_bounty
                        )
                        if bins.get("code") == 200:
                            bounty_insert_count += 1
                            reversal_abs = abs(reversal)
                            if reversal_abs > 0:
                                wallet_result = debit_bounty_from_wallet(
                                    db,
                                    br.get("tb_profile_id"),
                                    reversal_abs,
                                )
                                if wallet_result.get("code") != 200:
                                    print(
                                        "Warning: Failed to debit wallet on return for "
                                        f"{br.get('tb_profile_id')}: {wallet_result}"
                                    )

                    response_lines.append(
                        {
                            "original_transaction_item_uid": line[
                                "original_ti_uid"
                            ],
                            "new_transaction_item_uid": new_ti_uid,
                            "return_quantity": rq,
                            "line_subtotal": line["line_subtotal"],
                            "line_tax": line["line_tax"],
                        }
                    )

                update_original = {}
                if "transaction_return_requested" in payload:
                    update_original["transaction_return_requested"] = (
                        1 if payload.get("transaction_return_requested") else 0
                    )
                if "transaction_return_status" in payload:
                    update_original["transaction_return_status"] = payload.get(
                        "transaction_return_status"
                    )
                if return_note is not None:
                    update_original["transaction_return_note"] = return_note

                if update_original:
                    db.update(
                        "every_circle.transactions",
                        {"transaction_uid": original_tx_uid},
                        update_original,
                    )

                response["message"] = "Return transaction recorded successfully"
                response["code"] = 200
                response["return_transaction_uid"] = new_transaction_uid
                response["original_transaction_uid"] = original_tx_uid
                response["refund_breakdown"] = {
                    "subtotal": round(refund_subtotal, 4),
                    "taxes": round(refund_tax, 4),
                    "fees_allocated": round(refund_fees, 4),
                    "total_customer_credit": round(refund_grand, 4),
                    "fee_allocation_ratio": round(fee_ratio, 6),
                    "original_order_subtotal": round(order_subtotal, 4),
                }
                response["ledger_amounts_negative"] = {
                    "transaction_total": new_transaction["transaction_total"],
                    "transaction_amount": new_transaction["transaction_amount"],
                    "transaction_taxes": new_transaction["transaction_taxes"],
                    "transaction_fees": new_transaction["transaction_fees"],
                }
                response["transaction_items_created"] = item_insert_count
                response["bounty_reversal_rows_created"] = bounty_insert_count
                response["lines"] = response_lines

                return response, 200

        except Exception as e:
            print(f"Error in ReturnTransaction POST: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500


class Transactions(Resource):

    def get(self, profile_id=None):
        print(f"In Transactions GET with profile_id: {profile_id}")
        response = {}

        try:
            if not profile_id:
                response["message"] = "profile_id is required"
                response["code"] = 400
                return response, 400

            with connect() as db:
                # Execute query with parameterized profile_id for security
                query = """
                    SELECT
                    t.transaction_uid,
                    t.transaction_datetime,
                    t.transaction_total,
                    t.transaction_taxes,
                    t.transaction_fees,
                    t.transaction_profile_id,
                    t.transaction_in_escrow,
                    t.transaction_return_requested,
                    t.transaction_return_note,
                    t.transaction_return_status,
                    t.transaction_business_id AS seller_id,
                    -- ti.*,
                    CASE
                        WHEN ti.ti_bs_id LIKE '250-%%' THEN biz.business_name
                        WHEN ti.ti_bs_id LIKE '150-%%' THEN
                            CONCAT(expertise_pp.profile_personal_first_name, ' ', expertise_pp.profile_personal_last_name)
                        WHEN ti.ti_bs_id LIKE '165-%%' THEN
                            CONCAT(wish_pp.profile_personal_first_name, ' ', wish_pp.profile_personal_last_name)
                        ELSE NULL
                    END AS business_name,
                    CASE
                        WHEN ti.ti_bs_id LIKE '250-%%' THEN 'Business'
                        WHEN ti.ti_bs_id LIKE '150-%%' THEN 'Offering'
                        WHEN ti.ti_bs_id LIKE '165-%%' THEN 'Seeking'
                        ELSE 'Unknown'
                    END AS purchase_type,
                    GROUP_CONCAT(
                        CASE
                            WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_service_name
                            WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_title
                            WHEN ti.ti_bs_id LIKE '165-%%' THEN pw.profile_wish_title
                            ELSE 'See Receipt'
                        END
                        ORDER BY ti.ti_uid
                        SEPARATOR ', '
                    ) AS purchased_item,
                    SUM(ti.ti_bs_qty) AS ti_bs_qty,
                    MIN(ti.ti_uid) AS ti_uid
                    FROM every_circle.transactions t
                    LEFT JOIN every_circle.transactions_items ti
                    ON t.transaction_uid = ti.ti_transaction_id
                    LEFT JOIN every_circle.business_services bs
                    ON ti.ti_bs_id = bs.bs_uid
                    LEFT JOIN every_circle.business biz
                    ON bs.bs_business_id = biz.business_uid
                    
                    LEFT JOIN every_circle.profile_personal seller_pp
                    ON t.transaction_business_id = seller_pp.profile_personal_user_id
                    LEFT JOIN every_circle.profile_expertise pe
                    ON ti.ti_bs_id = pe.profile_expertise_uid
                    LEFT JOIN every_circle.profile_personal expertise_pp
                    ON pe.profile_expertise_profile_personal_id = expertise_pp.profile_personal_uid
                    LEFT JOIN every_circle.wish_response wr
                    ON ti.ti_bs_id = wr.wish_response_uid
                    LEFT JOIN every_circle.profile_wish pw
                    ON wr.wr_profile_wish_id = pw.profile_wish_uid
                    LEFT JOIN every_circle.profile_personal wish_pp
                    ON pw.profile_wish_profile_personal_id = wish_pp.profile_personal_uid
                    WHERE t.transaction_profile_id = %s
                    -- WHERE t.transaction_profile_id = '110-000014'
                    GROUP BY
                    t.transaction_uid,
                    t.transaction_datetime,
                    t.transaction_total,
                    t.transaction_profile_id,
                    seller_id,
                    business_name,
                    purchase_type
                    ORDER BY t.transaction_datetime DESC, ti_uid ASC
               """

                print(f"Executing query for profile_id: {profile_id}")
                result = db.execute(query, (profile_id,))
                # print(f"Query result: {result}")

                if result.get("code") == 200:
                    rows = _enrich_transaction_rows(result.get("result", []))
                    response["message"] = "Transactions retrieved successfully"
                    response["code"] = 200
                    response["data"] = rows
                    response["count"] = len(rows)
                    if _request_timezone():
                        response["timezone"] = _request_timezone()
                else:
                    response["message"] = "Query execution failed"
                    response["code"] = result.get("code", 500)
                    response["error"] = result.get("error", "Unknown error")
                    return response, response["code"]

                return response, 200

        except Exception as e:
            print(f"Error in Transactions GET: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500

    def post(self):
        print("In Transactions POST New")
        response = {}

        try:
            # Get JSON payload from request
            payload = request.get_json()
            print(payload)

            # Enter Data in Transactions Table
            # Validate required fields
            required_fields = [
                "profile_id",
                "stripe_payment_intent",
                "total_amount_paid",
                "total_costs",
                "items",
            ]
            missing_fields = [
                field for field in required_fields if not payload.get(field)
            ]

            if missing_fields:
                response["message"] = (
                    f"Missing required fields: {', '.join(missing_fields)}"
                )
                response["code"] = 400
                return response, 400
            print("No Missing Fields")

            # Extract required fields from payload
            transaction = {
                "transaction_profile_id": payload.get("profile_id"),
                "transaction_business_id": payload.get("business_id"),
                "transaction_stripe_pi": payload.get("stripe_payment_intent"),
                "transaction_total": payload.get("total_amount_paid"),
                "transaction_amount": payload.get("total_costs"),
                "transaction_taxes": payload.get("total_taxes"),
                "transaction_fees": payload.get("total_fees"),
                "transaction_in_escrow": (
                    1 if payload.get("transaction_in_escrow") else 0
                ),
            }

            with connect() as db:
                # Generate new transaction UID
                transaction_stored_procedure_response = db.call(
                    procedure="new_transaction_uid"
                )
                if (
                    not transaction_stored_procedure_response.get("result")
                    or len(transaction_stored_procedure_response["result"]) == 0
                ):
                    response["message"] = "Failed to generate transaction UID"
                    response["code"] = 500
                    return response, 500

                new_transaction_uid = transaction_stored_procedure_response["result"][0]["new_id"]
                transaction["transaction_uid"] = new_transaction_uid
                transactions_datetime = utc_now_str()
                transaction["transaction_datetime"] = transactions_datetime

                # Insert transaction
                transaction_response = db.insert(
                    "every_circle.transactions", transaction
                )
                print("transaction post response: ", transaction_response)

                if transaction_response.get("code") != 200:
                    response["message"] = transaction_response.get(
                        "message", "Failed to insert transaction"
                    )
                    response["code"] = transaction_response.get("code", 500)
                    return response, response["code"]

                response["transaction"] = transaction_response

                # Enter Data in Transactions_ItemsTable
                print("items: ", payload.get("items"))
                items_count = 0
                bounty_count = 0

                for item in payload.get("items", []):
                    print(item)
                    # {'bs_uid': '250-000021', 'quantity': 9, 'recommender_profile_id': '110-000231'}

                    # Validate required item fields
                    if (
                        not item.get("bs_uid")
                        and not item.get("expertise_uid")
                        and not item.get("wish_response_uid")
                    ):
                        print(
                            f"Warning: Skipping item missing bs_uid or expertise_uid or wish_response_uid: {item}"
                        )
                        continue

                    # Generate new transaction item UID
                    transaction_item_stored_procedure_response = db.call(
                        procedure="new_transaction_item_uid"
                    )
                    if (
                        not transaction_item_stored_procedure_response.get("result")
                        or len(transaction_item_stored_procedure_response["result"])
                        == 0
                    ):
                        print(
                            f"Warning: Failed to generate transaction item UID for item: {item}"
                        )
                        continue

                    new_transaction_item_uid = (
                        transaction_item_stored_procedure_response["result"][0]["new_id"]
                    )
                    print(
                        "new_transaction_item_uid: ",
                        new_transaction_item_uid,
                        type(new_transaction_item_uid),
                    )

                    # Load transaction item data from payload
                    tx_item = {
                        "ti_uid": new_transaction_item_uid,
                        "ti_transaction_id": new_transaction_uid,
                        "ti_bs_id": item.get("bs_uid")
                        or item.get("expertise_uid")
                        or item.get("wish_response_uid"),
                        "ti_bs_qty": item.get("quantity"),
                    }
                    print("tx_item: ", tx_item)
                    ti_bs_id = tx_item.get("ti_bs_id")
                    # item_bounty_type = "per_item"
                    item_bounty_type = item.get("bounty_type", "per_item")
                    is_wish_item = False

                    if ti_bs_id and str(ti_bs_id).startswith("250"):
                        print("ti_bs_id is a business service")
                        bs_query = """
                           SELECT *
                           FROM every_circle.business_services
                           WHERE bs_uid = %s
                       """
                        bs_response = db.execute(bs_query, ti_bs_id)
                        print("bs_response: ", bs_response)

                        if (
                            not bs_response.get("result")
                            or len(bs_response["result"]) == 0
                        ):
                            response["message"] = (
                                f"Business service not found: {item.get('bs_uid')}"
                            )
                            response["code"] = 404
                            return response, 404

                        bs_data = bs_response["result"][0]
                        tx_item["ti_bs_cost"] = _strip_currency(bs_data.get("bs_cost"))
                        tx_item["ti_bs_cost_currency"] = bs_data.get("bs_cost_currency")
                        tx_item["ti_bs_sku"] = bs_data.get("bs_sku")
                        tx_item["ti_bs_is_taxable"] = bs_data.get("bs_is_taxable")
                        tx_item["ti_bs_tax_rate"] = bs_data.get("bs_tax_rate")
                        tx_item["ti_bs_refund_policy"] = bs_data.get("bs_refund_policy")
                        tx_item["ti_bs_return_window_days"] = bs_data.get(
                            "bs_return_window_days"
                        )
                        item_bounty_type = (
                            bs_data.get("bs_bounty_type", "per_item") or "per_item"
                        )
                        print("tx_item: ", tx_item)

                    elif ti_bs_id and str(ti_bs_id).startswith("150"):
                        print("ti_bs_id is an expertise")
                        # Get other item details from expertise table using parameterized query
                        expertise_query = """
                           SELECT *
                           FROM every_circle.profile_expertise
                           WHERE profile_expertise_uid = %s
                       """
                        bs_response = db.execute(expertise_query, ti_bs_id)
                        print("expertise_response: ", bs_response)
                        # Check if expertise exists
                        if (
                            not bs_response.get("result")
                            or len(bs_response["result"]) == 0
                        ):
                            response["message"] = (
                                f"Expertise not found: {item.get('profile_expertise_uid')}"
                            )
                            response["code"] = 404
                            return response, 404

                        bs_data = bs_response["result"][0]
                        if (
                            int(bs_data.get("profile_expertise_moderated") or 0)
                            != MODERATED_ACTIVE
                        ):
                            response["message"] = "Offering is not available"
                            response["code"] = 403
                            return response, 403

                        owner_uid = bs_data.get("profile_expertise_profile_personal_id")
                        if not is_owner_available_for_public_interaction(db, owner_uid):
                            response["message"] = "Offering is not available"
                            response["code"] = 403
                            return response, 403

                        tx_item["ti_bs_cost"] = _strip_currency(
                            bs_data.get("profile_expertise_cost")
                        )
                        tx_item["ti_bs_cost_currency"] = bs_data.get(
                            "profile_expertise_cost_currency"
                        )
                        tx_item["ti_bs_sku"] = bs_data.get(
                            "profile_expertise_sku"
                        )  # Doesn't exist
                        tx_item["ti_bs_is_taxable"] = bs_data.get(
                            "profile_expertise_is_taxable"
                        )
                        tx_item["ti_bs_tax_rate"] = bs_data.get(
                            "profile_expertise_tax_rate"
                        )
                        tx_item["ti_bs_refund_policy"] = bs_data.get(
                            "profile_expertise_refund_policy"
                        )
                        tx_item["ti_bs_return_window_days"] = bs_data.get(
                            "profile_expertise_return_window_days"
                        )
                        item_bounty_type = (
                            bs_data.get("profile_expertise_bounty_type", "per_item") or "per_item"
                        )
                        print("tx_item: ", tx_item)

                    elif ti_bs_id and str(ti_bs_id).startswith("165"):
                        print("ti_bs_id is a wish")
                        is_wish_item = True
                        # Get other item details from wish table using parameterized query
                        wish_query = """
                           SELECT wish_response.wish_response_uid, profile_wish.*
                           FROM every_circle.profile_wish
                           LEFT JOIN every_circle.wish_response ON wr_profile_wish_id = profile_wish_uid
                           WHERE wish_response_uid = %s
                       """
                        bs_response = db.execute(
                            wish_query, (item.get("wish_response_uid"),)
                        )
                        print("wish_response: ", bs_response)
                        # Check if wish exists
                        if (
                            not bs_response.get("result")
                            or len(bs_response["result"]) == 0
                        ):
                            response["message"] = (
                                f"Wish not found: {item.get('wish_response_uid')}"
                            )
                            response["code"] = 404
                            return response, 404

                        bs_data = bs_response["result"][0]
                        if (
                            int(bs_data.get("profile_wish_moderated") or 0)
                            != MODERATED_ACTIVE
                        ):
                            response["message"] = "Seeking post is not available"
                            response["code"] = 403
                            return response, 403

                        owner_uid = bs_data.get("profile_wish_profile_personal_id")
                        if not is_owner_available_for_public_interaction(db, owner_uid):
                            response["message"] = "Seeking post is not available"
                            response["code"] = 403
                            return response, 403

                        tx_item["ti_bs_cost"] = _strip_currency(
                            bs_data.get("profile_wish_cost")
                        )
                        tx_item["ti_bs_cost_currency"] = bs_data.get(
                            "profile_wish_cost_currency"
                        )
                        tx_item["ti_bs_sku"] = bs_data.get(
                            "profile_wish_sku"
                        )  # Doesn't exist
                        tx_item["ti_bs_is_taxable"] = bs_data.get(
                            "profile_wish_is_taxable"
                        )
                        tx_item["ti_bs_tax_rate"] = bs_data.get("profile_wish_tax_rate")
                        tx_item["ti_bs_refund_policy"] = bs_data.get(
                            "profile_wish_refund_policy"
                        )
                        tx_item["ti_bs_return_window_days"] = bs_data.get(
                            "profile_wish_return_window_days"
                        )
                        item_bounty_type = (
                            bs_data.get("profile_wish_bounty_type", "per_item") or "per_item"
                        )
                        print("tx_item: ", tx_item)

                    else:
                        print("ti_bs_id is not a valid ID")
                        continue

                    _apply_item_options_to_tx_item(tx_item, item, ti_bs_id)

                    # # Get other item details from business services table using parameterized query
                    # bs_query = """
                    #     SELECT *
                    #     FROM every_circle.business_services
                    #     WHERE bs_uid = %s
                    # """
                    # bs_response = db.execute(bs_query, (item.get('bs_uid'),))
                    # print("bs_response: ", bs_response)

                    # # Check if business service exists
                    # if not bs_response.get('result') or len(bs_response['result']) == 0:
                    #     response['message'] = f"Business service not found: {item.get('bs_uid')}"
                    #     response['code'] = 404
                    #     return response, 404

                    # bs_data = bs_response['result'][0]
                    # tx_item['ti_bs_cost'] = bs_data.get('bs_cost')
                    # tx_item['ti_bs_cost_currency'] = bs_data.get('bs_cost_currency')
                    # tx_item['ti_bs_sku'] = bs_data.get('bs_sku')
                    # tx_item['ti_bs_is_taxable'] = bs_data.get('bs_is_taxable')
                    # tx_item['ti_bs_tax_rate'] = bs_data.get('bs_tax_rate')
                    # tx_item['ti_bs_refund_policy'] = bs_data.get('bs_refund_policy')
                    # tx_item['ti_bs_return_window_days'] = bs_data.get('bs_return_window_days')
                    # print("tx_item: ", tx_item)

                    # Insert transaction item
                    transaction_item_response = db.insert(
                        "every_circle.transactions_items", tx_item
                    )
                    print("transaction_item post response: ", transaction_item_response)

                    if transaction_item_response.get("code") == 200:
                        items_count += 1
                    else:
                        print(
                            f"Warning: Failed to insert transaction item: {transaction_item_response}"
                        )
                        continue

                    # Decrement expertise quantity in DB when an offering with limited stock is sold
                    if ti_bs_id and str(ti_bs_id).startswith("150"):
                        purchased_qty = int(item.get("quantity") or 1)
                        db.execute(
                            """
                            UPDATE every_circle.profile_expertise
                            SET profile_expertise_quantity = GREATEST(0, profile_expertise_quantity - %s)
                            WHERE profile_expertise_uid = %s
                              AND profile_expertise_quantity IS NOT NULL
                              AND profile_expertise_quantity > 0
                            """,
                            (purchased_qty, ti_bs_id),
                            cmd="post",
                        )
                        print(f"Decremented expertise quantity for {ti_bs_id} by {purchased_qty}")

                    # Process bounty if applicable
                    bounty_amount = item.get("bounty", 0)
                    # item_bounty_type = item.get("bounty_type", "per_item")
                    if bounty_amount and float(bounty_amount) > 0:
                        quantity = item.get("quantity", 1) or 1
                        # Determine effective bounty based on type:
                        # 'total'    -> fixed bounty for the whole order (ignore quantity)
                        # 'per_item' -> bounty per unit, multiply by quantity
                        if item_bounty_type == "total":
                            effective_bounty = float(bounty_amount)
                            print(
                                f"Bounty type: total (fixed), bounty_amount: {bounty_amount}, effective_bounty: {effective_bounty}"
                            )
                        else:
                            effective_bounty = float(bounty_amount) * int(quantity)
                            print(
                                f"Bounty type: per_item, bounty_amount: {bounty_amount}, quantity: {quantity}, effective_bounty: {effective_bounty}"
                            )
                        print("Processing bounty: ", effective_bounty)

                        recommender_profile_id = item.get("recommender_profile_id")
                        if not recommender_profile_id:
                            print("Warning: No recommender_profile_id provided")
                            recommender_profile_id = payload.get("profile_id")

                        profile_id = payload.get("profile_id")
                        buyer_is_recommender = (
                            profile_id
                            and recommender_profile_id
                            and profile_id == recommender_profile_id
                        )
                        is_expertise_item = (
                            ti_bs_id and str(ti_bs_id).startswith("150")
                        )

                        if is_expertise_item:
                            seller_profile_id = (
                                bs_data.get("profile_expertise_profile_personal_id")
                                or payload.get("business_id")
                            )
                            path_from, path_to = seller_profile_id, profile_id
                        elif is_wish_item:
                            path_from, path_to = profile_id, recommender_profile_id
                        else:
                            path_from, path_to = profile_id, recommender_profile_id

                        combined_path = _fetch_connection_path(path_from, path_to)

                        known_participants = []
                        if is_expertise_item:
                            if profile_id:
                                known_participants.append(
                                    {
                                        "tb_profile_id": profile_id,
                                        **_bounty_pct_amount(effective_bounty, 0.40),
                                    }
                                )
                        elif is_wish_item:
                            if buyer_is_recommender:
                                if profile_id:
                                    known_participants.append(
                                        {
                                            "tb_profile_id": profile_id,
                                            **_bounty_pct_amount(effective_bounty, 0.40),
                                        }
                                    )
                            else:
                                if profile_id:
                                    known_participants.append(
                                        {
                                            "tb_profile_id": profile_id,
                                            **_bounty_pct_amount(effective_bounty, 0.20),
                                        }
                                    )
                                if recommender_profile_id:
                                    known_participants.append(
                                        {
                                            "tb_profile_id": recommender_profile_id,
                                            **_bounty_pct_amount(effective_bounty, 0.20),
                                        }
                                    )
                        else:
                            if buyer_is_recommender:
                                known_participants.append(
                                    {
                                        "tb_profile_id": profile_id,
                                        **_bounty_pct_amount(effective_bounty, 0.40),
                                    }
                                )
                            else:
                                if profile_id:
                                    known_participants.append(
                                        {
                                            "tb_profile_id": profile_id,
                                            **_bounty_pct_amount(effective_bounty, 0.20),
                                        }
                                    )
                                if recommender_profile_id:
                                    known_participants.append(
                                        {
                                            "tb_profile_id": recommender_profile_id,
                                            **_bounty_pct_amount(effective_bounty, 0.20),
                                        }
                                    )
                        known_participants.append(
                            {
                                "tb_profile_id": EC_WALLET_ID,
                                **_bounty_pct_amount(effective_bounty, 0.20),
                            }
                        )
                        seen = {
                            p["tb_profile_id"]
                            for p in known_participants
                            if p["tb_profile_id"]
                        }

                        middle_nodes = _middle_path_nodes(combined_path, seen)
                        if is_expertise_item or is_wish_item:
                            network_participants = _network_participants_capped(
                                middle_nodes, effective_bounty
                            )
                        else:
                            network_participants = _network_participants_business(
                                middle_nodes, effective_bounty, seen
                            )
                        print("network_participants: ", network_participants)

                        in_escrow = bool(transaction.get("transaction_in_escrow"))

                        # Process known participants (buyer, recommender, ec-wallet)
                        for participant in known_participants:
                            participant_id = participant.get("tb_profile_id")
                            if not participant_id:
                                continue

                            print(f"Processing known participant: {participant_id}")

                            try:
                                transaction_bounty_stored_procedure_response = db.call(
                                    procedure="new_transaction_bounty_uid"
                                )
                                if (
                                    not transaction_bounty_stored_procedure_response.get(
                                        "result"
                                    )
                                    or len(
                                        transaction_bounty_stored_procedure_response[
                                            "result"
                                        ]
                                    )
                                    == 0
                                ):
                                    print(
                                        f"Warning: Failed to generate bounty UID for participant: {participant_id}"
                                    )
                                    continue

                                new_transaction_bounty_uid = (
                                    transaction_bounty_stored_procedure_response[
                                        "result"
                                    ][0]["new_id"]
                                )
                                print(
                                    "new_transaction_bounty_uid: ",
                                    new_transaction_bounty_uid,
                                    type(new_transaction_bounty_uid),
                                )

                                # Create new dictionary for each bounty to avoid data leakage
                                tx_bounty = {
                                    "tb_uid": new_transaction_bounty_uid,
                                    "tb_ti_id": new_transaction_item_uid,
                                    "tb_profile_id": participant_id,
                                    "tb_percentage": participant["tb_percentage"],
                                    "tb_amount": participant["tb_amount"],
                                }
                                print("tx_bounty: ", tx_bounty)

                                bounty_response = db.insert(
                                    "every_circle.transactions_bounty", tx_bounty
                                )
                                print(
                                    "transaction_bounty post response: ",
                                    bounty_response,
                                )

                                if bounty_response.get("code") == 200:
                                    bounty_count += 1

                                    print("bounty_count: ", bounty_count)

                                    bounty_amount = tx_bounty["tb_amount"]
                                    wallet_result = credit_bounty_to_wallet(
                                        db,
                                        participant_id,
                                        bounty_amount,
                                        in_escrow=in_escrow,
                                    )
                                    print("wallet_result: ", wallet_result)
                                    if wallet_result.get("code") != 200:
                                        print(
                                            f"Warning: Failed to update wallet for "
                                            f"participant {participant_id}: {wallet_result}"
                                        )
                                    
                                else:
                                    print(
                                        f"Warning: Failed to insert bounty for participant {participant_id}: {bounty_response}"
                                    )
                            except Exception as e:
                                print(
                                    f"Error processing bounty for participant {participant_id}: {str(e)}"
                                )
                                continue

                        # Process network participants
                        for participant in network_participants:
                            participant_id = participant.get("tb_profile_id")
                            if not participant_id:
                                continue

                            print(f"Processing network participant: {participant_id}")

                            try:
                                transaction_bounty_stored_procedure_response = db.call(
                                    procedure="new_transaction_bounty_uid"
                                )
                                if (
                                    not transaction_bounty_stored_procedure_response.get(
                                        "result"
                                    )
                                    or len(
                                        transaction_bounty_stored_procedure_response[
                                            "result"
                                        ]
                                    )
                                    == 0
                                ):
                                    print(
                                        f"Warning: Failed to generate bounty UID for network participant: {participant_id}"
                                    )
                                    continue

                                new_transaction_bounty_uid = (
                                    transaction_bounty_stored_procedure_response[
                                        "result"
                                    ][0]["new_id"]
                                )
                                print(
                                    "new_transaction_bounty_uid: ",
                                    new_transaction_bounty_uid,
                                    type(new_transaction_bounty_uid),
                                )

                                tx_bounty = {
                                    "tb_uid": new_transaction_bounty_uid,
                                    "tb_ti_id": new_transaction_item_uid,
                                    "tb_profile_id": participant_id,
                                    "tb_percentage": participant["tb_percentage"],
                                    "tb_amount": participant["tb_amount"],
                                }
                                print("tx_bounty: ", tx_bounty)

                                bounty_response = db.insert(
                                    "every_circle.transactions_bounty", tx_bounty
                                )
                                print(
                                    "transaction_bounty post response: ",
                                    bounty_response,
                                )

                                if bounty_response.get("code") == 200:
                                    bounty_count += 1

                                    print("bounty_count: ", bounty_count)

                                    bounty_amount = tx_bounty["tb_amount"]
                                    wallet_result = credit_bounty_to_wallet(
                                        db,
                                        participant_id,
                                        bounty_amount,
                                        in_escrow=in_escrow,
                                    )
                                    print("wallet_result: ", wallet_result)
                                    if wallet_result.get("code") != 200:
                                        print(
                                            f"Warning: Failed to update wallet for "
                                            f"network participant {participant_id}: {wallet_result}"
                                        )

                                else:
                                    print(
                                        f"Warning: Failed to insert bounty for network participant {participant_id}: {bounty_response}"
                                    )
                            except Exception as e:
                                print(
                                    f"Error processing bounty for network participant {participant_id}: {str(e)}"
                                )
                                continue

                response["transaction_items"] = items_count
                response["transaction_bounty_count"] = bounty_count
                response["message"] = "Transaction completed successfully"
                response["code"] = 200
                return response, 200

        except Exception as e:
            print(f"Error in Transactions POST: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500

    def put(self):
        print("In Transactions PUT")
        response = {}

        try:
            payload = request.get_json()
            if not payload:
                response["message"] = "Request body is required"
                response["code"] = 400
                return response, 400

            if not payload.get("transaction_uid"):
                response["message"] = "transaction_uid is required"
                response["code"] = 400
                return response, 400

            transaction_uid = payload.get("transaction_uid")
            delivery_items = payload.get("delivery_verification_items")

            if delivery_items is not None:
                return self._put_delivery_verification(
                    transaction_uid, payload, delivery_items
                )

            update_fields = {}

            if "transaction_in_escrow" in payload:
                update_fields["transaction_in_escrow"] = (
                    1 if payload.get("transaction_in_escrow") else 0
                )

            if "transaction_return_requested" in payload:
                update_fields["transaction_return_requested"] = (
                    1 if payload.get("transaction_return_requested") else 0
                )

            if "transaction_return_note" in payload:
                update_fields["transaction_return_note"] = payload.get(
                    "transaction_return_note"
                )

            if "transaction_return_status" in payload:
                update_fields["transaction_return_status"] = payload.get(
                    "transaction_return_status"
                )

            if not update_fields:
                response["message"] = "No valid fields to update"
                response["code"] = 400
                return response, 400

            with connect() as db:
                update_response = db.update(
                    "every_circle.transactions",
                    {"transaction_uid": transaction_uid},
                    update_fields,
                )

                if update_response.get("code") != 200:
                    response["message"] = update_response.get(
                        "message", "Failed to update transaction"
                    )
                    response["code"] = update_response.get("code", 500)
                    return response, response["code"]

                response["message"] = "Transaction updated successfully"
                response["code"] = 200
                response["transaction_uid"] = transaction_uid
                response.update(update_fields)
                return response, 200

        except Exception as e:
            print(f"Error in Transactions PUT: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500

    def _put_delivery_verification(
        self, transaction_uid, payload, delivery_items
    ):
        response = {}

        if not isinstance(delivery_items, list) or len(delivery_items) == 0:
            response["message"] = (
                "delivery_verification_items must be a non-empty list"
            )
            response["code"] = 400
            return response, 400

        if "transaction_in_escrow" not in payload:
            response["message"] = "transaction_in_escrow is required"
            response["code"] = 400
            return response, 400

        buyer_profile_id = _get_authenticated_profile_id()
        if not buyer_profile_id:
            response["message"] = "Authenticated buyer profile is required"
            response["code"] = 403
            return response, 403

        received_at = utc_now_str()
        updated_lines = []
        seen_ti = set()

        try:
            with connect() as db:
                tx_row_q = db.execute(
                    """
                    SELECT transaction_uid, transaction_profile_id, transaction_in_escrow
                    FROM every_circle.transactions
                    WHERE transaction_uid = %s
                    """,
                    (transaction_uid,),
                )
                tx_rows = tx_row_q.get("result") or []
                if not tx_rows:
                    response["message"] = "Transaction not found"
                    response["code"] = 404
                    return response, 404

                tx_row = tx_rows[0]
                if tx_row.get("transaction_profile_id") != buyer_profile_id:
                    response["message"] = (
                        "Caller is not the buyer on this transaction"
                    )
                    response["code"] = 403
                    return response, 403

                for entry in delivery_items:
                    item_uid = entry.get("transaction_item_uid")
                    try:
                        received_qty = int(entry.get("received_quantity"))
                    except (TypeError, ValueError):
                        received_qty = -1

                    if not item_uid:
                        response["message"] = (
                            "Each entry requires transaction_item_uid"
                        )
                        response["code"] = 400
                        return response, 400
                    if received_qty < 1:
                        response["message"] = (
                            f"Invalid received_quantity for item {item_uid}"
                        )
                        response["code"] = 400
                        return response, 400
                    if item_uid in seen_ti:
                        response["message"] = (
                            f"Duplicate transaction_item_uid: {item_uid}"
                        )
                        response["code"] = 400
                        return response, 400
                    seen_ti.add(item_uid)

                    ti_row = _resolve_transaction_item(
                        db, transaction_uid, item_uid
                    )
                    if not ti_row:
                        response["message"] = (
                            f"Transaction item not found on this sale: {item_uid}"
                        )
                        response["code"] = 400
                        return response, 400

                    ti_uid = ti_row.get("ti_uid")
                    order_qty = int(ti_row.get("ti_bs_qty") or 0)
                    current_received = int(ti_row.get("ti_received_qty") or 0)
                    remaining = order_qty - current_received

                    if order_qty <= 0:
                        response["message"] = (
                            f"Item {item_uid} is not eligible for delivery verification"
                        )
                        response["code"] = 400
                        return response, 400
                    if received_qty > remaining:
                        response["message"] = (
                            f"received_quantity exceeds remaining qty for {item_uid} "
                            f"(remaining: {remaining})"
                        )
                        response["code"] = 400
                        return response, 400

                    new_received = current_received + received_qty
                    ti_update = db.execute(
                        """
                        UPDATE every_circle.transactions_items
                        SET ti_received_qty = %s,
                            ti_received_at = %s
                        WHERE ti_uid = %s AND ti_transaction_id = %s
                        """,
                        (new_received, received_at, ti_uid, transaction_uid),
                        "post",
                    )
                    if ti_update.get("code") != 200:
                        response["message"] = ti_update.get(
                            "message", "Failed to update transaction item"
                        )
                        response["code"] = ti_update.get("code", 500)
                        return response, response["code"]

                    updated_lines.append(
                        {
                            "transaction_item_uid": ti_uid,
                            "ti_received_qty": new_received,
                            "ti_bs_qty": order_qty,
                        }
                    )

                all_received = _all_lines_fully_received(db, transaction_uid)
                escrow_release_result = None
                update_fields = {}

                if all_received and int(tx_row.get("transaction_in_escrow") or 0) == 1:
                    escrow_release_result = release_escrow_for_transaction(
                        db, transaction_uid, reason="buyer_confirmed"
                    )
                    if escrow_release_result.get("code") != 200:
                        response["message"] = escrow_release_result.get(
                            "message", "Failed to release escrow"
                        )
                        response["code"] = escrow_release_result.get("code", 500)
                        return response, response["code"]
                    update_fields["transaction_in_escrow"] = 0
                else:
                    update_fields["transaction_in_escrow"] = 0 if all_received else 1

                if "transaction_return_requested" in payload:
                    update_fields["transaction_return_requested"] = (
                        1 if payload.get("transaction_return_requested") else 0
                    )
                if "transaction_return_note" in payload:
                    update_fields["transaction_return_note"] = payload.get(
                        "transaction_return_note"
                    )
                if "transaction_return_status" in payload:
                    update_fields["transaction_return_status"] = payload.get(
                        "transaction_return_status"
                    )

                if update_fields:
                    update_response = db.update(
                        "every_circle.transactions",
                        {"transaction_uid": transaction_uid},
                        update_fields,
                    )
                    if update_response.get("code") != 200:
                        response["message"] = update_response.get(
                            "message", "Failed to update transaction"
                        )
                        response["code"] = update_response.get("code", 500)
                        return response, response["code"]

                response["message"] = "Transaction updated successfully"
                response["code"] = 200
                response["transaction_uid"] = transaction_uid
                response.update(update_fields)
                if escrow_release_result:
                    response["escrow_release"] = summarize_escrow_result(
                        escrow_release_result
                    )
                response["delivery_verification_items"] = updated_lines
                response["all_items_received"] = all_received
                return response, 200

        except Exception as e:
            print(f"Error in Transactions PUT (delivery verification): {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500


class SellerTransactions(Resource):

    def get(self, profile_id=None):
        print(f"In SellerTransactions GET with profile_id: {profile_id}")
        response = {}

        try:
            if not profile_id:
                response["message"] = "profile_id is required"
                response["code"] = 400
                return response, 400

            with connect() as db:
                # Execute query to get transactions
                query = """
                    SELECT
                        t.transaction_uid,
                        t.transaction_datetime,
                        t.transaction_total,
                        t.transaction_taxes,
                        t.transaction_fees,
                        t.transaction_business_id AS seller_id,
                        t.transaction_profile_id,
                        t.transaction_in_escrow,
                        t.transaction_return_requested,
                        t.transaction_return_note,
                        t.transaction_return_status,
                        
                        -- ti.*,
                        CASE
                            WHEN ti.ti_bs_id LIKE '250-%%' THEN biz.business_name
                            WHEN ti.ti_bs_id LIKE '150-%%' THEN
                                CONCAT(expertise_pp.profile_personal_first_name, ' ', expertise_pp.profile_personal_last_name)
                            WHEN ti.ti_bs_id LIKE '165-%%' THEN
                                CONCAT(wish_pp.profile_personal_first_name, ' ', wish_pp.profile_personal_last_name)
                            ELSE NULL
                        END AS business_name,
                        CASE
                            WHEN ti.ti_bs_id LIKE '250-%%' THEN 'Business'
                            WHEN ti.ti_bs_id LIKE '150-%%' THEN 'Offering'
                            WHEN ti.ti_bs_id LIKE '165-%%' THEN 'Seeking'
                            ELSE 'Unknown'
                        END AS purchase_type,
                        GROUP_CONCAT(
                            CASE
                                WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_service_name
                                WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_title
                                WHEN ti.ti_bs_id LIKE '165-%%' THEN pw.profile_wish_title
                                ELSE 'See Receipt'
                            END
                            ORDER BY ti.ti_uid
                            SEPARATOR ', '
                        ) AS purchased_item,
                        MIN(ti.ti_bs_id) AS ti_bs_id,
                        SUM(ti.ti_bs_qty) AS ti_bs_qty,
                        MIN(ti.ti_uid) AS ti_uid,
                        MIN(ti.ti_bs_cost) AS unit_price,
                        MIN(buyer_pp.profile_personal_first_name) AS buyer_first_name,
                        MIN(buyer_pp.profile_personal_last_name) AS buyer_last_name,
                        MIN(buyer_u.user_email_id) AS buyer_email,
                        MIN(buyer_pp.profile_personal_email_is_public) AS buyer_email_is_public,
                        MIN(buyer_pp.profile_personal_phone_number) AS buyer_phone,
                        MIN(buyer_pp.profile_personal_phone_number_is_public) AS buyer_phone_is_public,
                        MIN(buyer_pp.profile_personal_city) AS buyer_city,
                        MIN(buyer_pp.profile_personal_state) AS buyer_state,
                        MIN(buyer_pp.profile_personal_location_is_public) AS buyer_location_is_public
                    FROM every_circle.transactions t
                    LEFT JOIN every_circle.transactions_items ti
                    ON t.transaction_uid = ti.ti_transaction_id
                    LEFT JOIN every_circle.business_services bs
                    ON ti.ti_bs_id = bs.bs_uid
                    LEFT JOIN every_circle.business biz
                    ON bs.bs_business_id = biz.business_uid
                    LEFT JOIN every_circle.profile_personal buyer_pp
                    ON t.transaction_profile_id = buyer_pp.profile_personal_uid
                    LEFT JOIN every_circle.users buyer_u
                    ON buyer_pp.profile_personal_user_id = buyer_u.user_uid
                    LEFT JOIN every_circle.profile_personal seller_pp
                    ON t.transaction_business_id = seller_pp.profile_personal_user_id
                    LEFT JOIN every_circle.profile_expertise pe
                    ON ti.ti_bs_id = pe.profile_expertise_uid
                    LEFT JOIN every_circle.profile_personal expertise_pp
                    ON pe.profile_expertise_profile_personal_id = expertise_pp.profile_personal_uid
                    LEFT JOIN every_circle.wish_response wr
                    ON ti.ti_bs_id = wr.wish_response_uid
                    LEFT JOIN every_circle.profile_wish pw
                    ON wr.wr_profile_wish_id = pw.profile_wish_uid
                    LEFT JOIN every_circle.profile_personal wish_pp
                    ON pw.profile_wish_profile_personal_id = wish_pp.profile_personal_uid
                    WHERE t.transaction_business_id = %s
                    -- WHERE t.transaction_business_id = '110-000014'
                    GROUP BY
                    t.transaction_uid,
                    t.transaction_datetime,
                    t.transaction_total,
                    t.transaction_profile_id,
                    seller_id,
                    business_name,
                    purchase_type
                    ORDER BY t.transaction_datetime DESC, ti_uid ASC
               """

                print(f"Executing seller query for profile_id: {profile_id}")
                result = db.execute(query, (profile_id,))
                # print(f"Seller query result: {result}")

                if result.get("code") == 200:
                    rows = _enrich_transaction_rows(result.get("result", []))
                    response["message"] = "Seller transactions retrieved successfully"
                    response["code"] = 200
                    response["data"] = rows
                    response["count"] = len(rows)
                    if _request_timezone():
                        response["timezone"] = _request_timezone()
                else:
                    response["message"] = "Query execution failed"
                    response["code"] = result.get("code", 500)
                    response["error"] = result.get("error", "Unknown error")
                    return response, response["code"]

                return response, 200

        except Exception as e:
            print(f"Error in SellerTransactions GET: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500
        

class DeclinedReturns(Resource):

    def get(self):
        print("In DeclinedReturns GET")
        response = {}

        try:
            with connect() as db:
                query = """
                    SELECT 
                        t.transaction_uid,
                        t.transaction_profile_id,
                        t.transaction_business_id,
                        t.transaction_return_note,
                        t.transaction_return_status,
                        t.transaction_return_seller_note,
                        t.transaction_datetime,
                        CONCAT(p.profile_personal_first_name, ' ', p.profile_personal_last_name) AS buyer_name,
                        b.business_name AS seller_name
                    FROM every_circle.transactions t
                    LEFT JOIN every_circle.profile_personal p 
                        ON p.profile_personal_uid = t.transaction_profile_id
                    LEFT JOIN every_circle.business b 
                        ON b.business_uid = t.transaction_business_id
                    WHERE t.transaction_return_status = 'declined'
                    ORDER BY t.transaction_datetime DESC
                """
                result = db.execute(query)
                print("DeclinedReturns query result:", result)

                if result.get("code") == 200:
                    response["message"] = "Declined returns retrieved successfully"
                    response["code"] = 200
                    response["data"] = result.get("result", [])
                else:
                    response["message"] = "Query execution failed"
                    response["code"] = result.get("code", 500)
                    return response, response["code"]

                return response, 200

        except Exception as e:
            print(f"Error in DeclinedReturns GET: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500
        
    def put(self):
        print("In DeclinedReturns PUT")
        response = {}

 
        try:
            data = request.get_json()
            transaction_uid = data.get("transaction_uid")
            seller_note = data.get("transaction_return_seller_note", "")
 
            if not transaction_uid:
                response["message"] = "transaction_uid is required"
                response["code"] = 400
                return response, 400
 
            action = data.get("action", "decline")

            with connect() as db:
                if action == "resolve":
                    favor = data.get("resolved_in_favor_of", "seller")
                    # buyer wins = treat same as accepted (triggers refund display)
                    # seller wins = resolved (clears red highlight)
                    status = "accepted" if favor == "buyer" else "resolved"
                    update_fields = {"transaction_return_status": status}
                else:
                    update_fields = {
                        "transaction_return_status": "declined",
                        "transaction_return_seller_note": seller_note,
                    }

                print("DeclinedReturns PUT update_fields:", update_fields)
                result = db.update(
                    "every_circle.transactions",
                    {"transaction_uid": transaction_uid},
                    update_fields,
                )
                print("DeclinedReturns PUT result:", result)

                if result.get("code") == 200:
                    response["message"] = "Return declined successfully" if action == "decline" else "Return resolved successfully"
                    response["code"] = 200
                else:
                    response["message"] = "Failed to update transaction"
                    response["code"] = result.get("code", 500)
                    return response, response["code"]

                return response, 200

        except Exception as e:
            print(f"Error in DeclinedReturns PUT: {str(e)}")
            print(traceback.format_exc())
            response["message"] = f"An error occurred: {str(e)}"
            response["code"] = 500
            return response, 500
   