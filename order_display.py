"""
Human-readable display chip builders for v2 order and return surfaces.

Product vocabulary (acceptance table):
  Order (sale) rows     — Delivered/Received = shipping + buyer verification
  Return rows           — Delivered/Received = return/cancel logistics + refund

Chip terms:
  Returning   — units in an open return request
  Cancelling  — units in an open pre-ship cancel request
  Returned    — return confirmed (ledger)
  Cancelled   — pre-ship cancel confirmed (ledger); drives active_qty with Cancelling
  Pending / Refunded / Rejected — refund column on return rows

active_qty = purchased − completed pre-ship cancels − in-progress pre-ship cancels (Cancelling).
"""

# Em dash — pickup/virtual have no delivery step (instant verification).
DELIVERY_NOT_APPLICABLE = "—"

REFUND_PENDING = "pending"
REFUND_REFUNDED = "refunded"
REFUND_REJECTED = "rejected"
REFUND_STRIPE_FAIL = ("stripe_fail", "stripe_failed")

RETURN_STATUS_RETURNING = "returning"
RETURN_STATUS_RETURNED = "returned"
RETURN_STATUS_CANCELLED = "cancelled"


def _fulfillment_method(row):
    method = row.get("fulfillment_method") or row.get("ti_fulfillment_method") or "ship"
    return str(method).strip().lower()


def _requires_shipping(row):
    if row.get("requires_shipping") is not None:
        return bool(row.get("requires_shipping"))
    return _fulfillment_method(row) not in ("pickup", "virtual", "not_required")


def _shippable_total(units):
    active = int(units.get("active_qty") or 0)
    purchased = int(units.get("purchased_qty") or 0)
    return active if active > 0 else purchased


# ---------------------------------------------------------------------------
# Sale row chips (row_kind = order / sale)
# ---------------------------------------------------------------------------


def _is_pickup_or_virtual(row):
    return _fulfillment_method(row) in ("pickup", "virtual") or not _requires_shipping(row)


def sale_delivered_label(row, units):
    """Left chip on sale rows: shipping / fulfillment progress."""
    shipped = int(units.get("shipped_qty") or 0)
    total = _shippable_total(units)

    if _is_pickup_or_virtual(row):
        return DELIVERY_NOT_APPLICABLE

    if shipped <= 0:
        return "Not Shipped"
    if shipped < total:
        return f"{shipped}/{total}"
    return "Shipped"


def sale_received_label(row, units, *, audience="buyer"):
    """Right chip on sale rows: buyer verification (seller sees status, not Verify)."""
    active = int(units.get("active_qty") or 0)
    shipped = int(units.get("shipped_qty") or 0)
    verified = int(units.get("verified_qty") or 0)
    returned = int(units.get("returned_shipped_completed_qty") or 0) + int(
        units.get("returned_unshipped_completed_qty") or 0
    )
    returned_shipped = int(units.get("returned_shipped_completed_qty") or 0)
    return_in_progress_shipped = int(units.get("return_in_progress_shipped_qty") or 0)
    verifiable = int(units.get("verifiable_remaining_qty") or 0)
    if verifiable <= 0 and audience == "buyer":
        from units_ledger import (
            compute_pickup_verifiable_remaining,
            compute_verifiable_remaining,
        )

        if _is_pickup_or_virtual(row):
            purchased = int(units.get("purchased_qty") or active or 0)
            verifiable = compute_pickup_verifiable_remaining(
                purchased=purchased,
                verified=verified,
                cancelled_pre_ship=units.get("cancelled_pre_ship_qty"),
                cancelled_pre_ship_in_progress=units.get(
                    "cancelled_pre_ship_in_progress_qty"
                ),
                return_in_progress_shipped=return_in_progress_shipped,
                returned_shipped=returned_shipped,
            )
        else:
            verifiable = compute_verifiable_remaining(
                shipped=shipped,
                verified=verified,
                returned_shipped=returned_shipped,
                return_in_progress_shipped=return_in_progress_shipped,
            )

    # ti_received_qty is gross (never decremented on return). Net kept + returned
    # covers active when every active unit is either still verified or returned.
    net_verified = max(0, verified - returned_shipped)
    resolved = net_verified + returned
    if resolved >= active and active > 0 and verifiable <= 0:
        return "Yes", None

    if verified >= active and active > 0 and verifiable <= 0:
        return "Yes", None

    if audience == "buyer" and verifiable > 0:
        return "Verify", "verify"

    if verified <= 0:
        return "No", None

    if active > 0:
        return f"{verified}/{active}", None
    return "Partial", None


def build_sale_display(row, units, *, audience="buyer", include_qty=True):
    """Full display block for a sale / order list row."""
    delivered = sale_delivered_label(row, units)
    received, received_action = sale_received_label(row, units, audience=audience)
    total = _shippable_total(units)

    display = {
        "delivered_label": delivered,
        "received_label": received,
    }
    if received_action:
        display["received_action"] = received_action
    elif audience == "buyer":
        display["received_action"] = "status"
    if audience == "seller":
        display["received_action"] = "status"
    if include_qty:
        display["qty"] = int(units.get("purchased_qty") or 0) if audience == "seller" else total

    open_returns = row.get("open_returns") or []
    if open_returns and audience == "buyer":
        display["order_return_summary"] = open_returns[0].get("display_status")

    return display


# ---------------------------------------------------------------------------
# Return request chips (row_kind = return, includes pending TRR)
# ---------------------------------------------------------------------------


def _refund_status(req):
    fs = (
        req.get("refund_status")
        or req.get("trr_refund_status")
        or req.get("transaction_refund_status")
        or ""
    )
    return str(fs).strip().lower()


def _return_status(req):
    rs = req.get("return_status") or req.get("trr_return_status") or req.get("trr_status") or ""
    return str(rs).strip().lower()


def is_cancel_request(req):
    """Pre-ship cancel (not a physical return)."""
    if not req:
        return False
    if req.get("cancel_unshipped") or req.get("pre_ship_cancel") or req.get("is_cancel_before_ship"):
        return True
    if req.get("trr_cancel_unshipped") in (1, "1", True, "true"):
        return True
    return _return_status(req) == RETURN_STATUS_CANCELLED


def is_awaiting_seller(req):
    """Open TRR with no return ledger transaction yet."""
    if not req or req.get("trr_return_transaction_uid"):
        return False
    rs = _return_status(req)
    fs = _refund_status(req) or REFUND_PENDING
    return (rs, fs) in (
        (RETURN_STATUS_RETURNING, REFUND_PENDING),
        (RETURN_STATUS_RETURNED, REFUND_PENDING),
        (RETURN_STATUS_CANCELLED, REFUND_PENDING),
    )


def is_refund_rejected(req):
    fs = _refund_status(req)
    return fs in (REFUND_REJECTED,) + REFUND_STRIPE_FAIL


def return_request_delivered_chip(req):
    """
    Left chip on return / pending_return rows.

    Cancelling — cancel request awaiting seller or denied
    Cancelled  — cancel confirmed
    Returning  — return request awaiting seller or denied
    Returned   — return confirmed
    """
    cancel = is_cancel_request(req)
    awaiting = is_awaiting_seller(req)
    rejected = is_refund_rejected(req)

    if cancel:
        if awaiting or rejected:
            return "Cancelling"
        return "Cancelled"

    if awaiting or rejected:
        return "Returning"
    return "Returned"


def return_request_received_chip(req):
    """Right chip on return / pending_return rows."""
    fs = _refund_status(req)
    if fs == REFUND_REFUNDED:
        return "Refunded"
    if fs in (REFUND_REJECTED,) + REFUND_STRIPE_FAIL:
        return "Rejected"
    return "Pending"


def return_request_display_status(req):
    """Human summary, e.g. 'Cancelling - Pending'."""
    return f"{return_request_delivered_chip(req)} - {return_request_received_chip(req)}"


def api_return_status(req):
    """Machine return_status for API (FE confirm flow)."""
    cancel = is_cancel_request(req)
    awaiting = is_awaiting_seller(req)
    stored = _return_status(req)

    if awaiting and cancel:
        return RETURN_STATUS_RETURNING
    return stored or RETURN_STATUS_RETURNING


def build_return_request_display(req, *, qty=None):
    """
    Status + display for one TRR (pending_return, open_returns[], list rows).

    Single source of truth across orders/:uid, purchases.rows[], seller_transactions[].
    """
    if not req:
        return {}

    fs = _refund_status(req) or REFUND_PENDING
    api_rs = api_return_status(req)
    delivered = return_request_delivered_chip(req)
    received = return_request_received_chip(req)

    out = {
        "return_status": api_rs,
        "refund_status": fs,
        "display_status": return_request_display_status(req),
        "display": {
            "delivered_label": delivered,
            "received_label": received,
        },
    }
    if qty is not None:
        out["display"]["qty"] = qty
    if is_cancel_request(req):
        out["cancel_unshipped"] = True
        out["pre_ship_cancel"] = True
        out["is_cancel_before_ship"] = True
    return out


# ---------------------------------------------------------------------------
# Completed return ledger row (row_kind = return, has transaction_uid)
# ---------------------------------------------------------------------------


def build_return_ledger_display(row, *, qty=None):
    """Display for a completed return transaction list row."""
    cancel = bool(row.get("cancel_unshipped") or row.get("pre_ship_cancel"))
    fs = _refund_status(row)

    delivered = "Cancelled" if cancel else "Returned"
    if fs == REFUND_REFUNDED:
        received = "Refunded"
    elif fs in (REFUND_REJECTED,) + REFUND_STRIPE_FAIL:
        received = "Rejected"
    else:
        received = "Pending"

    display = {
        "delivered_label": delivered,
        "received_label": received,
    }
    if qty is not None:
        display["qty"] = qty
    return display
