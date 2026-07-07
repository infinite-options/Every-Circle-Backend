"""Platform wallet id and legacy bounty profile_id mapping."""

EC_WALLET_ID = "ec-wallet"

# Historical bounty rows used these before wallet PK was standardized.
_LEGACY_PLATFORM_BOUNTY_IDS = frozenset({"every-circle", "every-circ"})


def resolve_wallet_profile_id(bounty_profile_id):
    """Map transactions_bounty.tb_profile_id to wallet.wallet_profile_id."""
    if bounty_profile_id in _LEGACY_PLATFORM_BOUNTY_IDS:
        return EC_WALLET_ID
    return bounty_profile_id
