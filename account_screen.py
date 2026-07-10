"""
Aggregated payloads for the mobile Account screen.

Two routes reduce parallel fan-out:
  - Personal: purchases + bounty results + seller-side line items (same IDs as legacy).
  - Business: seller transactions + business bounty results + business info.

Mutations (PUT transactions, decline returns) stay on existing endpoints.
"""

from flask_restful import Resource
from flask import request

from transactions import Transactions, SellerTransactions
from bounty_results import BountyResults, BusinessBountyResults
from business_info import BusinessInfo
from datetime_utils import enrich_datetime_fields


def _request_timezone():
    return request.args.get("timezone") or request.args.get("tz")


def _merge_body_status(body, status):
    """Ensure each subsection is a dict with a numeric code for the client."""
    if not isinstance(body, dict):
        return {"code": status, "data": body}
    out = dict(body)
    out.setdefault("code", status if status is not None else out.get("code"))
    return out


def _enrich_section_datetimes(body, field="transaction_datetime"):
    """Ensure nested account-screen lists expose UTC (+ optional local) datetimes."""
    if not isinstance(body, dict):
        return body

    tz_name = _request_timezone()
    data = body.get("data")
    if not isinstance(data, list):
        return body

    enriched = []
    for row in data:
        if isinstance(row, dict):
            enriched.append(enrich_datetime_fields(dict(row), field, tz_name))
        else:
            enriched.append(row)

    body = dict(body)
    body["data"] = enriched
    if tz_name:
        body["timezone"] = tz_name
    body["datetime_storage"] = "UTC"
    return body


class AccountScreenPersonal(Resource):
    """
    GET /api/v1/account-screen/personal/<profile_id>

    Combines:
      - GET /api/v1/transactions/<profile_id>  (purchases)
      - GET /api/bountyresults/<profile_id>
      - GET /api/v1/transactions/seller/<profile_id>  (seller / expertise lines)
    """

    def get(self, profile_id):
        if not profile_id:
            return {"code": 400, "message": "profile_id is required"}, 400

        purchases_body, purchases_status = Transactions().get(profile_id)
        bounty_body, bounty_status = BountyResults().get(profile_id)
        seller_body, seller_status = SellerTransactions().get(profile_id)

        purchases_body = _enrich_section_datetimes(purchases_body)
        bounty_body = _enrich_section_datetimes(bounty_body)
        seller_body = _enrich_section_datetimes(seller_body)

        return (
            {
                "code": 200,
                "purchases": _merge_body_status(purchases_body, purchases_status),
                "bounty_results": _merge_body_status(bounty_body, bounty_status),
                "seller_transactions": _merge_body_status(seller_body, seller_status),
            },
            200,
        )


class AccountScreenBusiness(Resource):
    """
    GET /api/v1/account-screen/business/<business_uid>

    Combines:
      - GET /api/v1/transactions/seller/<business_uid>
      - GET /api/business-bountyresults/<business_uid>
      - GET /api/v1/businessinfo/<business_uid>
    """

    def get(self, business_uid):
        if not business_uid:
            return {"code": 400, "message": "business_uid is required"}, 400

        seller_body, seller_status = SellerTransactions().get(business_uid)
        bounty_body, bounty_status = BusinessBountyResults().get(business_uid)
        info_body, info_status = BusinessInfo().get(business_uid)

        seller_body = _enrich_section_datetimes(seller_body)
        bounty_body = _enrich_section_datetimes(bounty_body)

        return (
            {
                "code": 200,
                "seller_transactions": _merge_body_status(seller_body, seller_status),
                "business_bounty_results": _merge_body_status(
                    bounty_body, bounty_status
                ),
                "business_info": _merge_body_status(info_body, info_status),
            },
            200,
        )
