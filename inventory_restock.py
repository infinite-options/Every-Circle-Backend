"""Shared return-linked restock validation (cumulative per trr_uid)."""


def sum_restocked_for_trr(
    db,
    *,
    table,
    qty_column,
    trr_column,
    listing_column,
    trr_uid,
    listing_uid,
):
    """Total units already restocked for this return request + listing."""
    if not trr_uid:
        return 0
    result = db.execute(
        f"""
        SELECT COALESCE(SUM({qty_column}), 0) AS restocked_total
        FROM {table}
        WHERE {trr_column} = %s AND {listing_column} = %s
        """,
        (trr_uid, listing_uid),
    )
    rows = result.get("result") or []
    if not rows:
        return 0
    try:
        return int(rows[0].get("restocked_total") or 0)
    except (TypeError, ValueError):
        return 0


def load_trr_return_quantity(db, trr_uid):
    """Return qty cap from transaction_return_requests.trr_return_quantity."""
    if not trr_uid:
        return None
    result = db.execute(
        """
        SELECT trr_return_quantity
        FROM every_circle.transaction_return_requests
        WHERE trr_uid = %s
        LIMIT 1
        """,
        (trr_uid,),
    )
    rows = result.get("result") or []
    if not rows:
        return None
    try:
        return int(rows[0].get("trr_return_quantity") or 0)
    except (TypeError, ValueError):
        return 0


def validate_trr_restock_capacity(db, trr_uid, quantity, *, already_restocked):
    """
    Ensure cumulative restock for trr_uid does not exceed trr_return_quantity.

    Returns (allowed: bool, error_response_or_None, trr_return_quantity).
    """
    trr_return_qty = load_trr_return_quantity(db, trr_uid)
    if trr_return_qty is None:
        return False, {
            "message": "Return request not found for trr_uid",
            "code": 404,
            "trr_uid": trr_uid,
        }, None

    restocked = int(already_restocked or 0)
    remaining_restockable = max(trr_return_qty - restocked, 0)

    if restocked >= trr_return_qty:
        return False, {
            "message": "Return quantity already fully restocked for this trr_uid",
            "code": 409,
            "trr_uid": trr_uid,
            "trr_return_quantity": trr_return_qty,
            "restocked_total": restocked,
            "remaining_restockable": 0,
        }, trr_return_qty

    if restocked + quantity > trr_return_qty:
        return False, {
            "message": (
                f"Restock quantity exceeds remaining return allowance "
                f"(restocked {restocked}, requested {quantity}, "
                f"return qty {trr_return_qty})"
            ),
            "code": 400,
            "trr_uid": trr_uid,
            "trr_return_quantity": trr_return_qty,
            "restocked_total": restocked,
            "remaining_restockable": remaining_restockable,
        }, trr_return_qty

    return True, None, trr_return_qty


def restock_audit_insert_succeeded(insert_response):
    return isinstance(insert_response, dict) and insert_response.get("code") == 200


def restock_audit_insert_error(insert_response):
    """Build a 500 response when the audit insert fails."""
    if restock_audit_insert_succeeded(insert_response):
        return None
    detail = ""
    if isinstance(insert_response, dict):
        detail = str(
            insert_response.get("message")
            or insert_response.get("error")
            or ""
        )
    message = "Failed to record restock audit row"
    if "Duplicate" in detail or "duplicate" in detail.lower():
        message = (
            "Restock audit could not be recorded: only one row per return is "
            "allowed by the database (drop uq_per_trr_offering / uq_bsr_trr_bs)"
        )
    elif detail:
        message = f"{message}: {detail}"
    return {"message": message, "code": 500}
