"""
Aggregated payloads for the mobile Account screen.

Two routes reduce parallel fan-out:
  - Personal: purchases + bounty results + seller-side line items (same IDs as legacy).
  - Business: seller transactions + business bounty results + business info.

Mutations (PUT transactions, decline returns) stay on existing endpoints.
"""

from flask_restful import Resource

from transactions import Transactions, SellerTransactions
from bounty_results import BountyResults, BusinessBountyResults
from business_info import BusinessInfo


def _merge_body_status(body, status):
    """Ensure each subsection is a dict with a numeric code for the client."""
    if not isinstance(body, dict):
        return {"code": status, "data": body}
    out = dict(body)
    out.setdefault("code", status if status is not None else out.get("code"))
    return out


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
