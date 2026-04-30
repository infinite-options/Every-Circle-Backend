from aiohttp import payload
from flask_restful import Resource
from datetime import datetime
import traceback
from flask import request, jsonify
import json


from data_ec import connect, processImage
from user_path_connection import ConnectionsPath


def _strip_currency(value):
    """Remove $ and commas from currency values before storing."""
    if value is None:
        return None
    if isinstance(value, str):
        return value.replace("$", "").replace(",", "").strip()
    return value


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
                       t.transaction_profile_id,
                       t.transaction_in_escrow,
                       t.transaction_return_requested,
                       t.transaction_return_note,
                       t.transaction_return_status,
                       CASE
                           WHEN ti.ti_bs_id LIKE '250-%%' THEN biz.business_uid
                           WHEN ti.ti_bs_id LIKE '150-%%' THEN expertise_pp.profile_personal_uid
                           ELSE NULL
                       END AS seller_id,
                       CASE
                           WHEN ti.ti_bs_id LIKE '250-%%' THEN biz.business_name
                           WHEN ti.ti_bs_id LIKE '150-%%' THEN
                               CONCAT(expertise_pp.profile_personal_first_name, ' ', expertise_pp.profile_personal_last_name)
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
                       ON biz.`business_user_id-DNU` = seller_pp.profile_personal_user_id
                   LEFT JOIN every_circle.profile_expertise pe
                       ON ti.ti_bs_id = pe.profile_expertise_uid
                   LEFT JOIN every_circle.profile_personal expertise_pp
                       ON pe.profile_expertise_profile_personal_id = expertise_pp.profile_personal_uid
                   LEFT JOIN every_circle.wish_response wr
                       ON ti.ti_bs_id = wr.wish_response_uid
                   LEFT JOIN every_circle.profile_wish pw
                       ON wr.wr_profile_wish_id = pw.profile_wish_uid
                   WHERE t.transaction_profile_id = %s
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
                print(f"Query result: {result}")

                if result.get("code") == 200:
                    response["message"] = "Transactions retrieved successfully"
                    response["code"] = 200
                    response["data"] = result.get("result", [])
                    response["count"] = len(result.get("result", []))
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

                new_transaction_uid = transaction_stored_procedure_response["result"][
                    0
                ]["new_id"]
                transaction["transaction_uid"] = new_transaction_uid
                transactions_datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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
                        transaction_item_stored_procedure_response["result"][0][
                            "new_id"
                        ]
                    )
                    print(
                        "new_transaction_item_uid: ",
                        new_transaction_item_uid,
                        type(new_transaction_item_uid),
                    )

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
                    item_bounty_type = "per_item"

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
                                f"Expertise not found: {item.get('expertise_uid')}"
                            )
                            response["code"] = 404
                            return response, 404

                        bs_data = bs_response["result"][0]
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
                        print("tx_item: ", tx_item)

                    elif ti_bs_id and str(ti_bs_id).startswith("165"):
                        print("ti_bs_id is a wish")
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
                        print("tx_item: ", tx_item)

                    else:
                        print("ti_bs_id is not a valid ID")
                        continue

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

                    # Process bounty if applicable
                    bounty_amount = item.get("bounty", 0)
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
                            # print("Warning: No recommender_profile_id provided, skipping bounty processing")
                            # continue
                            print("Warning: No recommender_profile_id provided")
                            recommender_profile_id = payload.get("profile_id")

                        # Find connection path between buyer and recommender
                        try:
                            connections_path = ConnectionsPath()
                            network_response, network_status = connections_path.get(
                                payload.get("profile_id"), recommender_profile_id
                            )

                            if network_status != 200 or not network_response.get(
                                "combined_path"
                            ):
                                print(
                                    f"Warning: Could not find connection path. Status: {network_status}, Response: {network_response}"
                                )
                                # Continue without network path, but still process known participants
                                combined_path = None
                            else:
                                combined_path = network_response["combined_path"]
                                print("network combined_path: ", combined_path)
                        except Exception as e:
                            print(f"Error getting connection path: {str(e)}")
                            combined_path = None

                        # Deduplicate known_participants while preserving order
                        # e.g. buyer == recommender would otherwise create two tb records for the same person
                        seen = set()
                        known_participants = []
                        for p in [
                            payload.get("profile_id"),
                            recommender_profile_id,
                            "every-circle",
                        ]:
                            if p and p not in seen:
                                seen.add(p)
                                known_participants.append(p)

                        # Process network path if available
                        network_result = []
                        network_percentage = 0
                        if combined_path:
                            try:
                                uids = combined_path.split(",")
                                # Extract middle elements (excluding the first and last)
                                # Also exclude anyone already in known_participants to avoid double-paying
                                middle = uids[1:-1] if len(uids) > 2 else []
                                network_result = [u for u in middle if u not in seen]
                                if len(network_result) < 2:
                                    if "charity" not in seen:
                                        network_result.append("charity")
                                print("network_result: ", network_result)

                                if len(network_result) > 0:
                                    network_percentage = 0.40 / len(network_result)
                            except Exception as e:
                                print(f"Error processing network path: {str(e)}")
                                network_result = []

                        # Process known participants (buyer, recommender, every-circle)
                        for participant in known_participants:
                            if not participant:
                                continue

                            print(f"Processing known participant: {participant}")

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
                                        f"Warning: Failed to generate bounty UID for participant: {participant}"
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
                                    "tb_profile_id": participant,
                                    "tb_percentage": "0.2",
                                    "tb_amount": round(0.20 * effective_bounty, 4),
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
                                else:
                                    print(
                                        f"Warning: Failed to insert bounty for participant {participant}: {bounty_response}"
                                    )
                            except Exception as e:
                                print(
                                    f"Error processing bounty for participant {participant}: {str(e)}"
                                )
                                continue

                        # Process network participants
                        for participant in network_result:
                            if not participant:
                                continue

                            print(f"Processing network participant: {participant}")

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
                                        f"Warning: Failed to generate bounty UID for network participant: {participant}"
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
                                    "tb_profile_id": participant,
                                    "tb_percentage": str(network_percentage),
                                    "tb_amount": round(
                                        network_percentage * effective_bounty, 4
                                    ),
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
                                else:
                                    print(
                                        f"Warning: Failed to insert bounty for network participant {participant}: {bounty_response}"
                                    )
                            except Exception as e:
                                print(
                                    f"Error processing bounty for network participant {participant}: {str(e)}"
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
            update_fields = {}

            if "transaction_in_escrow" in payload:
                update_fields["transaction_in_escrow"] = 1 if payload.get("transaction_in_escrow") else 0

            if "transaction_return_requested" in payload:
                update_fields["transaction_return_requested"] = 1 if payload.get("transaction_return_requested") else 0

            if "transaction_return_note" in payload:
                update_fields["transaction_return_note"] = payload.get("transaction_return_note")

            if "transaction_return_status" in payload:
                update_fields["transaction_return_status"] = payload.get("transaction_return_status")    

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
                       t.transaction_business_id,
                       t.transaction_profile_id,
                       t.transaction_in_escrow,
                       t.transaction_return_requested,
                       t.transaction_return_note,
                       ti.ti_uid,
                       ti.ti_uid,
                       ti.ti_bs_id,
                       ti.ti_bs_qty,
                       ti.ti_bs_cost
                   FROM every_circle.transactions t
                   LEFT JOIN every_circle.transactions_items ti ON ti.ti_transaction_id = t.transaction_uid
                   -- WHERE t.transaction_profile_id = '110-000014'
                   WHERE t.transaction_business_id = %s
                   ORDER BY t.transaction_datetime DESC
               """

                print(f"Executing seller query for profile_id: {profile_id}")
                result = db.execute(query, (profile_id,))
                print(f"Seller query result: {result}")

                if result.get("code") == 200:
                    response["message"] = "Seller transactions retrieved successfully"
                    response["code"] = 200
                    response["data"] = result.get("result", [])
                    response["count"] = len(result.get("result", []))
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

   