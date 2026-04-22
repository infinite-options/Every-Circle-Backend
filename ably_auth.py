import asyncio
import os

from flask import request
from flask_restful import Resource


class AblyToken(Resource):
    """GET /api/v1/ably/token?client_id=<uid>
    Returns a short-lived Ably token request for client-side auth.
    """

    def get(self):
        api_key = os.getenv("ABLY_API_KEY", "")
        if not api_key:
            return {"message": "ABLY_API_KEY not configured on server", "code": 500}, 500

        client_id = request.args.get("client_id") or request.args.get("profile_uid") or "anonymous-client"

        async def _make_token_request():
            import ably

            async with ably.AblyRest(api_key) as client:
                token_request = await client.auth.create_token_request(
                    token_params={
                        "client_id": client_id,
                        "ttl": 60 * 60 * 1000,  # 1 hour
                    }
                )
                return token_request.to_dict()

        try:
            token_request = asyncio.run(_make_token_request())
            return {"message": "Success", "code": 200, "result": token_request}, 200
        except Exception as e:
            print(f"Error creating Ably token request: {e}")
            return {"message": "Failed to create Ably token request", "code": 500}, 500

