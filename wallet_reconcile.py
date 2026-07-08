from flask_restful import Resource

from data_ec import connect
from wallet_service import reconcile_all_profile_wallets, reconcile_profile_wallet


class WalletReconcile(Resource):
    """GET /api/v1/wallet_reconcile/<profile_id> — fix one profile's wallet from bounty ledger."""

    def get(self, profile_id):
        if not profile_id:
            return {"code": 400, "message": "profile_id is required"}, 400

        try:
            with connect() as db:
                result = reconcile_profile_wallet(db, profile_id)
                status = result.get("code", 500)
                return result, status
        except Exception as e:
            return {"code": 500, "message": str(e)}, 500


class WalletReconcileAll(Resource):
    """GET /api/v1/wallet_reconcile — fix all profiles that appear in transactions_bounty."""

    def get(self):
        try:
            with connect() as db:
                result = reconcile_all_profile_wallets(db)
                status = 200 if result.get("code") == 200 else 500
                return result, status
        except Exception as e:
            return {"code": 500, "message": str(e)}, 500
