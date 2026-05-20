from flask_restful import Resource
from flask import request
from data_ec import connect



class TransactionReceipt(Resource):
    def get(self, profile_id, transaction_uid):
        print(f"In TransactionReceipt GET for profile_id: {profile_id}, transaction_uid: {transaction_uid}")
        response = {}
        seller_id = request.args.get('seller_id')

        try:
            with connect() as db:
                query = """
                    SELECT
                        t.transaction_uid,
                        t.transaction_profile_id,
                        t.transaction_datetime,
                        t.transaction_total,
                        t.transaction_amount,
                        t.transaction_taxes,
                        t.transaction_fees,
                        t.transaction_in_escrow,
                        ti.ti_uid,
                        ti.ti_bs_id,
                        ti.ti_bs_qty,
                        ti.ti_bs_cost,
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
                    LEFT JOIN every_circle.transactions_items ti
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

                params = [profile_id, transaction_uid]

                if seller_id:
                    if seller_id.startswith('150-'):
                        query += " AND ti.ti_bs_id = %s"
                        params.append(seller_id)
                    elif seller_id.startswith('165-'):
                        query += " AND ti.ti_bs_id = %s"
                        params.append(seller_id)
                    else:
                        query += " AND bs.bs_business_id = %s"
                        params.append(seller_id)

                result = db.execute(query, tuple(params))

                if result.get('code') == 200:
                    rows = result.get('result', [])
                    # Parse available_options JSON string if returned as string
                    import json as _json
                    for row in rows:
                        raw = row.get('available_options')
                        if isinstance(raw, str):
                            try:
                                row['available_options'] = _json.loads(raw)
                            except Exception:
                                row['available_options'] = []
                        elif raw is None:
                            row['available_options'] = []

                    response['message'] = 'Transaction receipt retrieved successfully'
                    response['code'] = 200
                    response['data'] = rows
                    return response, 200
                else:
                    response['message'] = result.get('message', 'Error retrieving transaction receipt')
                    response['code'] = result.get('code', 500)
                    return response, response['code']

        except Exception as e:
            print(f"Error in TransactionReceipt GET: {str(e)}")
            response['message'] = f'An error occurred: {str(e)}'
            response['code'] = 500
            return response, 500