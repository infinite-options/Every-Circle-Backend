"""
Aggregated payloads for the mobile Account screen.

Two routes reduce parallel fan-out:
  - Personal: purchases + bounty results + seller-side line items (same IDs as legacy).
  - Business: seller transactions + business bounty results + business info.

Mutations (PUT transactions, decline returns) stay on existing endpoints.
"""

from flask_restful import Resource
from flask import request

from data_ec import connect
from transactions import Transactions, SellerTransactions
from bounty_results import BountyResults, BusinessBountyResults
from business_info import BusinessInfo
from datetime_utils import enrich_datetime_fields
from account_screen_purchases_v2 import build_purchases_v2_rows
from account_screen_seller_v2 import build_seller_transactions_v2_rows
from account_screen_v2_contract import (
    build_purchases_v2_section,
    finalize_account_screen_rows,
)
from user_profile_info import build_account_screen_profile
from wallet_service import build_wallet_summary
from wallet_transactions_service import resolve_seller_wallet_profile_id


def _load_seller_offerings(db, business_uid):
    """Personal offerings (150-*) for the seller profile tied to this business account."""
    profile_id = resolve_seller_wallet_profile_id(db, business_uid)
    if not profile_id:
        return []
    rows = db.execute(
        """
        SELECT profile_expertise_uid, profile_expertise_title,
               profile_expertise_quantity, profile_expertise_cost,
               profile_expertise_bounty, profile_expertise_sku
        FROM every_circle.profile_expertise
        WHERE profile_expertise_profile_personal_id = %s
        ORDER BY profile_expertise_title
        """,
        (profile_id,),
    )
    return (rows or {}).get("result") or []


def _request_timezone():
    return request.args.get("timezone") or request.args.get("tz")


def _merge_body_status(body, status):
    """Ensure each subsection is a dict with a numeric code for the client."""
    if not isinstance(body, dict):
        return {"code": status, "data": body}
    out = dict(body)
    out.setdefault("code", status if status is not None else out.get("code"))
    return out


def _enrich_rows_datetimes(rows, field="transaction_datetime"):
    """Enrich v2 row datetimes in place."""
    if not rows:
        return rows
    tz_name = _request_timezone()
    enriched = []
    for row in rows:
        if isinstance(row, dict):
            enriched.append(enrich_datetime_fields(dict(row), field, tz_name))
        else:
            enriched.append(row)
    return enriched


def _enrich_section_datetimes_legacy(body, field="transaction_datetime"):
    """Datetime enrichment for bounty / seller subsections that still use data[]."""
    if not isinstance(body, dict):
        return body

    tz_name = _request_timezone()
    out = dict(body)
    data = body.get("data")
    if isinstance(data, list):
        enriched = []
        for row in data:
            if isinstance(row, dict):
                enriched.append(enrich_datetime_fields(dict(row), field, tz_name))
            else:
                enriched.append(row)
        out["data"] = enriched
    if tz_name:
        out["timezone"] = tz_name
    out["datetime_storage"] = "UTC"
    return out


class AccountScreenPersonal(Resource):
    """
    GET /api/v1/account-screen/personal/<profile_id>

    Schema v2 personal payload. FE reads purchases.rows[] only (no purchases.data[],
    no order_list_hydration). Each row includes units + display chips.

    Combines legacy fetches for purchases, bounty, and seller-side lines, then
    emits v2 rows for purchases and seller_transactions.
    """

    def get(self, profile_id):
        if not profile_id:
            return {"code": 400, "message": "profile_id is required"}, 400

        purchases_body, purchases_status = Transactions().get(profile_id)
        bounty_body, bounty_status = BountyResults().get(profile_id)
        seller_body, seller_status = SellerTransactions().get(profile_id)

        bounty_body = _enrich_section_datetimes_legacy(bounty_body)
        seller_body = _enrich_section_datetimes_legacy(seller_body)

        tz_name = _request_timezone()
        response = {
            "code": 200,
            "schema_version": 2,
            "purchases": None,
            "bounty_results": _merge_body_status(bounty_body, bounty_status),
            "seller_transactions": _merge_body_status(seller_body, seller_status),
            "profile": None,
        }
        if tz_name:
            response["timezone"] = tz_name
        response["datetime_storage"] = "UTC"

        with connect() as db:
            purchase_rows = (purchases_body or {}).get("rows")
            if not purchase_rows:
                legacy = (purchases_body or {}).get("data") or []
                purchase_rows = build_purchases_v2_rows(db, legacy)
            purchase_rows = _enrich_rows_datetimes(purchase_rows)
            response["purchases"] = build_purchases_v2_section(
                code=purchases_status,
                message=(purchases_body or {}).get("message"),
                rows=purchase_rows,
            )

            seller_legacy = (response.get("seller_transactions") or {}).get("data") or []
            seller_v2_rows = finalize_account_screen_rows(
                build_seller_transactions_v2_rows(db, seller_legacy)
            )
            seller_v2_rows = _enrich_rows_datetimes(seller_v2_rows)
            if isinstance(response.get("seller_transactions"), dict):
                response["seller_transactions"]["data"] = seller_v2_rows
                response["seller_transactions"]["count"] = len(seller_v2_rows)
                response["seller_transactions"].pop("rows", None)

            response["wallet"] = build_wallet_summary(db, profile_id)
            response["profile"] = build_account_screen_profile(db, profile_id)

        return (response, 200)


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

        seller_body = _enrich_section_datetimes_legacy(seller_body)
        bounty_body = _enrich_section_datetimes_legacy(bounty_body)

        response = {
            "code": 200,
            "schema_version": 2,
            "seller_transactions": _merge_body_status(seller_body, seller_status),
            "business_bounty_results": _merge_body_status(
                bounty_body, bounty_status
            ),
            "business_info": _merge_body_status(info_body, info_status),
        }
        tz_name = _request_timezone()
        if tz_name:
            response["timezone"] = tz_name
        response["datetime_storage"] = "UTC"

        with connect() as db:
            seller_legacy = (response.get("seller_transactions") or {}).get("data") or []
            seller_v2_rows = finalize_account_screen_rows(
                build_seller_transactions_v2_rows(db, seller_legacy)
            )
            seller_v2_rows = _enrich_rows_datetimes(seller_v2_rows)
            if isinstance(response.get("seller_transactions"), dict):
                response["seller_transactions"]["data"] = seller_v2_rows
                response["seller_transactions"]["count"] = len(seller_v2_rows)

            wallet_profile_id = resolve_seller_wallet_profile_id(db, business_uid)
            if wallet_profile_id:
                response["wallet"] = build_wallet_summary(db, wallet_profile_id)
            offerings = _load_seller_offerings(db, business_uid)
            if offerings:
                response["offerings"] = offerings
                if isinstance(response.get("business_info"), dict):
                    response["business_info"] = dict(response["business_info"])
                    response["business_info"]["offerings"] = offerings

        return (response, 200)
