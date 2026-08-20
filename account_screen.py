"""
Aggregated payloads for the mobile Account screen (schema v3).

One GET renders wallet, earnings, ledger (first page), purchases, sales,
bounty results, and profile — no separate wallet_ledger fetch on initial load.

Mutations stay on existing endpoints; after success the client refreshes
account-screen only.
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
from account_screen_v2_contract import finalize_account_screen_rows
from wallet_transactions_service import resolve_seller_wallet_profile_id

from account_screen_v3 import (
    _parse_ledger_pagination,
    build_bounty_results_v3,
    build_earnings_v3,
    build_profile_v3_business,
    build_profile_v3_personal,
    build_purchases_v3,
    build_sales_products_v3,
    build_sales_v3,
    build_wallet_ledger_v3,
    build_wallet_v3,
)


def _load_personal_offerings(db, profile_id):
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


def _load_business_products(db, business_uid):
    rows = db.execute(
        """
        SELECT bs_uid, bs_service_name, bs_quantity, bs_cost, bs_bounty, bs_sku
        FROM every_circle.business_services
        WHERE bs_business_id = %s
        ORDER BY bs_service_name
        """,
        (business_uid,),
    )
    return (rows or {}).get("result") or []


def _request_timezone():
    return request.args.get("timezone") or request.args.get("tz")


def _enrich_rows_datetimes(rows, field="transaction_datetime"):
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


class AccountScreenPersonal(Resource):
    """
    GET /api/v1/account-screen/personal/<profile_id>

    Schema v3: single authoritative payload for Purchases, Sales, Earnings,
    Bounty table, Wallet summary, and embedded wallet_ledger (paginated).

    Query params:
      timezone / tz — profile timezone for display labels and earnings chart
      ledger_offset, ledger_limit — ledger pagination (default 0, 50)
    """

    def get(self, profile_id):
        if not profile_id:
            return {"code": 400, "message": "profile_id is required"}, 400

        from auth import require_actor_or_admin

        _, error = require_actor_or_admin(profile_id, allow_business=True)
        if error:
            return error, error["code"]

        tz_name = _request_timezone()
        ledger_limit, ledger_offset = _parse_ledger_pagination(request.args)

        purchases_body, purchases_status = Transactions().get(profile_id)
        bounty_body, bounty_status = BountyResults().get(profile_id)
        seller_body, seller_status = SellerTransactions().get(profile_id)

        if purchases_status not in (200, None):
            return purchases_body, purchases_status

        response = {
            "code": 200,
            "schema_version": 3,
            "account_screen_type": "personal",
            "account_screen_id": profile_id,
            "wallet": None,
            "earnings": None,
            "wallet_ledger": None,
            "purchases": {"rows": []},
            "sales": {"offerings": [], "transactions": []},
            "bounty_results": {"rows": []},
            "profile": None,
        }
        if tz_name:
            response["timezone"] = tz_name
        response["datetime_storage"] = "UTC"

        with connect() as db:
            source_rows = (purchases_body or {}).get("data") or []
            if (purchases_body or {}).get("schema_version") == 2:
                purchase_rows = _enrich_rows_datetimes(source_rows)
            else:
                purchase_rows = build_purchases_v2_rows(db, source_rows)
                purchase_rows = _enrich_rows_datetimes(purchase_rows)
            response["purchases"] = build_purchases_v3(db, purchase_rows, tz_name=tz_name)

            seller_legacy = (seller_body or {}).get("data") or []
            seller_v2_rows = finalize_account_screen_rows(
                build_seller_transactions_v2_rows(db, seller_legacy)
            )
            seller_v2_rows = _enrich_rows_datetimes(seller_v2_rows)
            offerings = _load_personal_offerings(db, profile_id)
            response["sales"] = build_sales_v3(
                db,
                profile_id,
                seller_v2_rows,
                tz_name=tz_name,
                offerings_source=offerings,
            )

            bounty_legacy = (bounty_body or {}).get("data") or []
            bounty_enriched = _enrich_rows_datetimes(bounty_legacy)
            response["bounty_results"] = build_bounty_results_v3(
                db, bounty_enriched, tz_name=tz_name
            )

            response["wallet"] = build_wallet_v3(db, profile_id)
            response["earnings"] = build_earnings_v3(db, profile_id, tz_name)
            response["wallet_ledger"] = build_wallet_ledger_v3(
                db,
                profile_id,
                offset=ledger_offset,
                limit=ledger_limit,
                tz_name=tz_name,
            )
            response["profile"] = build_profile_v3_personal(db, profile_id)

        return response, 200


class AccountScreenBusiness(Resource):
    """
    GET /api/v1/account-screen/business/<business_uid>

    Same v3 envelope as personal; sales.products[] for business inventory catalog.
    Wallet/ledger omitted when no seller wallet profile exists.
    """

    def get(self, business_uid):
        if not business_uid:
            return {"code": 400, "message": "business_uid is required"}, 400

        from auth import require_actor_or_admin

        _, error = require_actor_or_admin(business_uid, allow_business=True)
        if error:
            return error, error["code"]

        tz_name = _request_timezone()
        ledger_limit, ledger_offset = _parse_ledger_pagination(request.args)

        seller_body, seller_status = SellerTransactions().get(business_uid)
        bounty_body, bounty_status = BusinessBountyResults().get(business_uid)
        info_body, info_status = BusinessInfo().get(business_uid)

        response = {
            "code": 200,
            "schema_version": 3,
            "account_screen_type": "business",
            "account_screen_id": business_uid,
            "wallet": None,
            "earnings": None,
            "wallet_ledger": None,
            "purchases": {"rows": []},
            "sales": {"products": [], "transactions": []},
            "bounty_results": {"rows": []},
            "profile": None,
        }
        if tz_name:
            response["timezone"] = tz_name
        response["datetime_storage"] = "UTC"

        with connect() as db:
            seller_legacy = (seller_body or {}).get("data") or []
            seller_v2_rows = finalize_account_screen_rows(
                build_seller_transactions_v2_rows(db, seller_legacy)
            )
            seller_v2_rows = _enrich_rows_datetimes(seller_v2_rows)

            products = _load_business_products(db, business_uid)
            response["sales"] = build_sales_products_v3(
                db,
                business_uid,
                seller_v2_rows,
                tz_name=tz_name,
                products_source=products,
            )

            bounty_legacy = (bounty_body or {}).get("data") or []
            bounty_enriched = _enrich_rows_datetimes(bounty_legacy)
            response["bounty_results"] = build_bounty_results_v3(
                db, bounty_enriched, tz_name=tz_name
            )

            wallet_profile_id = resolve_seller_wallet_profile_id(db, business_uid)
            if wallet_profile_id:
                response["wallet"] = build_wallet_v3(db, wallet_profile_id)
                response["earnings"] = build_earnings_v3(
                    db, wallet_profile_id, tz_name
                )
                response["wallet_ledger"] = build_wallet_ledger_v3(
                    db,
                    wallet_profile_id,
                    offset=ledger_offset,
                    limit=ledger_limit,
                    tz_name=tz_name,
                )

            response["profile"] = build_profile_v3_business(info_body)

        return response, 200
