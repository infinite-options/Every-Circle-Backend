from flask_restful import Resource
from flask import request
from datetime import datetime
import traceback


from data_ec import connect


class BountyResults(Resource):
    def get(self, profile_id):
        print(f"In BountyResults GET for profile_id: {profile_id}")
        response = {}

        try:
            with connect() as db:
                # Query to get bounty results for the specific profile_id
                # bounty_query = f"""
                #     SELECT
                #         transaction_uid,
                #         transaction_datetime,
                #         SUM(tb_amount) AS bounty_earned,
                #         transaction_profile_id,
                #         transaction_business_id,
                #         t.transaction_in_escrow
                #     FROM (
                #         SELECT *
                #         FROM every_circle.transactions_bounty
                #         LEFT JOIN every_circle.transactions_items ON tb_ti_id = ti_uid
                #         LEFT JOIN every_circle.transactions ON ti_transaction_id = transaction_uid
                #         LEFT JOIN every_circle.business ON ti_bs_id = business_uid
                #         -- WHERE tb_profile_id = '110-000014'
                #         WHERE tb_profile_id = '{profile_id}'
                #     ) AS t
                #     GROUP BY t.transaction_uid
                #     ORDER BY t.transaction_datetime DESC
                # """
                bounty_query = f""" 
                SELECT *,
                        SUM(tb_amount) AS bounty_earned
                    FROM (
                        SELECT 
                            tb.*,
                            ti.*,
                            t.transaction_datetime, t.transaction_profile_id, t.transaction_business_id, 
                            IF (t.transaction_in_escrow = 1,1,0) AS in_escrow,
                            CONCAT(p.profile_personal_first_name, ' ', p.profile_personal_last_name) AS purchaser_name,
                            IF(
                                t.transaction_business_id LIKE '110%%',
                                CONCAT(pp.profile_personal_first_name, ' ', pp.profile_personal_last_name),
                                b.business_name
                            ) AS display_name

                        FROM every_circle.transactions_bounty tb
                        LEFT JOIN every_circle.transactions_items ti ON tb_ti_id = ti_uid
                        LEFT JOIN every_circle.transactions t ON ti.ti_transaction_id = t.transaction_uid
                        LEFT JOIN every_circle.business b ON t.transaction_business_id = b.business_uid
                        LEFT JOIN every_circle.profile_personal pp ON t.transaction_business_id = pp.profile_personal_uid
                        LEFT JOIN every_circle.profile_personal p ON t.transaction_profile_id = p.profile_personal_uid
                        WHERE tb_profile_id = %s
                    ) AS bounty_results
                    GROUP BY bounty_results.ti_transaction_id, bounty_results.transaction_business_id
                    ORDER BY bounty_results.transaction_datetime DESC
                """
                
                bounty_response = db.execute(bounty_query, (profile_id,))

                if bounty_response["code"] == 200:
                    response["code"] = 200
                    response["message"] = "Bounty results retrieved successfully"
                    response["data"] = bounty_response["result"]
                    response["total_bounties"] = len(bounty_response["result"])

                    # Calculate total bounty earned
                    total_bounty = sum(
                        float(bounty["bounty_earned"])
                        for bounty in bounty_response["result"]
                    )
                    response["total_bounty_earned"] = total_bounty

                    return response, 200
                else:
                    response["code"] = 500
                    response["message"] = "Error retrieving bounty results"
                    return response, 500

        except Exception as e:
            print(f"Error in BountyResults GET: {str(e)}")
            print(traceback.format_exc())
            response["code"] = 500
            response["message"] = f"An error occurred: {str(e)}"
            return response, 500


class BusinessBountyResults(Resource):
    def get(self, business_id):
        print(f"In BusinessBountyResults GET for business_id: {business_id}")
        response = {}

        try:
            with connect() as db:
                # Query to get bounty results for transactions where this business was the seller

                bounty_query = """
                   SELECT
                       t.transaction_uid,
                       t.transaction_datetime,
                       t.transaction_taxes,
                       t.transaction_profile_id,
                       t.transaction_business_id,
                       t.bs_uid,
                       t.bs_service_name,
                       t.bs_cost,
                       t.bs_bounty,
                       t.bs_bounty_type,
                       t.ti_bs_qty,
                       SUM(t.tb_amount) AS bounty_earned,
                       CASE
                           WHEN t.bs_bounty_type = 'total' THEN t.bs_bounty
                           ELSE (t.bs_bounty * t.ti_bs_qty)
                       END AS bounty_paid
                   FROM (
                       SELECT *
                       FROM every_circle.transactions_bounty
                       LEFT JOIN every_circle.transactions_items ON tb_ti_id = ti_uid
                       LEFT JOIN every_circle.transactions ON ti_transaction_id = transaction_uid
                       LEFT JOIN every_circle.business_services ON ti_bs_id = bs_uid
                       WHERE transaction_business_id = %s
                   ) AS t
                   GROUP BY
                       t.transaction_uid,
                       t.transaction_datetime,
                       t.transaction_taxes,
                       t.transaction_profile_id,
                       t.transaction_business_id,
                       t.bs_uid,
                       t.bs_service_name,
                       t.bs_cost,
                       t.bs_bounty,
                       t.bs_bounty_type,
                       t.ti_bs_qty
                   ORDER BY t.transaction_datetime DESC
               """

                bounty_response = db.execute(bounty_query, (business_id,))

                if bounty_response["code"] == 200:
                    response["code"] = 200
                    response["message"] = (
                        "Business bounty results retrieved successfully"
                    )
                    response["data"] = bounty_response["result"]
                    response["total_bounties"] = len(bounty_response["result"])

                    # Calculate total bounty paid out by business
                    total_bounty = sum(
                        float(bounty["bounty_earned"])
                        for bounty in bounty_response["result"]
                    )
                    response["total_bounty_earned"] = total_bounty

                    return response, 200
                else:
                    response["code"] = 500
                    response["message"] = "Error retrieving business bounty results"
                    return response, 500

        except Exception as e:
            print(f"Error in BusinessBountyResults GET: {str(e)}")
            print(traceback.format_exc())
            response["code"] = 500
            response["message"] = f"An error occurred: {str(e)}"
            return response, 500
