"""
Per-line commerce fields shared by order detail, account-screen, and wallet ledger.

Shipping: exactly one of ti_shipping_amount_per_unit | ti_shipping_amount_per_line.
Bounty: line_bounty_paid (seller pool) + optional buyer tb_* on the line.
"""

from transactions import (
    _catalog_bounty_unit_and_type,
    _line_bounty_totals,
    _normalize_shipping_refundable,
    _refund_shipping_for_line,
    _tax_amount_for_line,
    _to_float,
    _bounty_to_reclaim_for_line,
)
from wallet_transactions_service import _parse_unit_cost


def round_money(value):
    return round(_to_float(value), 2)


def is_per_unit_shipping_model(row):
    """True when checkout used a per-unit shipping rate (Buyer Fixed listing)."""
    listing = (row.get("ti_listing_shipping") or "").strip().lower()
    return listing in ("buyer fixed", "buyer_fixed")


def line_shipping_charge(row):
    """Total shipping charged for this sale line at checkout."""
    if not isinstance(row, dict):
        return 0.0
    qty = max(int(row.get("ti_bs_qty") or 0), 1)
    line_total = row.get("ti_line_shipping_amount")
    if line_total is not None and line_total != "":
        return round_money(line_total)
    per_unit = _to_float(row.get("ti_shipping_amount"))
    return round_money(per_unit * qty)


def line_shipping_api_fields(row):
    """
    Exclusive shipping fields for API payloads.

    Per-unit model → ti_shipping_amount_per_unit only.
    Flat line model → ti_shipping_amount_per_line only (0.00 when free).
    """
    if not isinstance(row, dict):
        return {"ti_shipping_amount_per_line": 0.0}

    qty = max(int(row.get("ti_bs_qty") or 0), 1)
    per_unit = round_money(row.get("ti_shipping_amount"))
    line_total = line_shipping_charge(row)

    if is_per_unit_shipping_model(row):
        return {"ti_shipping_amount_per_unit": per_unit}

    return {"ti_shipping_amount_per_line": line_total}


def line_shipping_refundable_api(row):
    """Boolean ti_bs_shipping_refundable for FE."""
    raw = row.get("ti_shipping_refundable")
    if raw is None:
        raw = row.get("ti_bs_shipping_refundable")
    return bool(_normalize_shipping_refundable(raw, default=0))


def _strip_legacy_line_shipping_fields(line):
    for key in (
        "ti_shipping_amount",
        "ti_line_shipping_amount",
        "ti_listing_shipping",
        "ti_shipping_refundable",
    ):
        line.pop(key, None)


def batch_buyer_bounty_by_line(db, ti_uids, buyer_profile_id):
    """Buyer earned bounty per ti_uid: {ti_uid: {tb_amount, tb_percentage, bounty_earned}}."""
    uids = [u for u in (ti_uids or []) if u]
    if not uids or not buyer_profile_id:
        return {}

    placeholders = ", ".join(["%s"] * len(uids))
    q = db.execute(
        f"""
        SELECT tb_ti_id, tb_percentage, tb_amount
        FROM every_circle.transactions_bounty
        WHERE tb_ti_id IN ({placeholders})
          AND tb_profile_id = %s
          AND tb_amount > 0.0001
        """,
        tuple(uids) + (buyer_profile_id,),
    )
    out = {}
    for row in q.get("result") or []:
        ti_uid = row.get("tb_ti_id")
        if not ti_uid:
            continue
        amt = round_money(row.get("tb_amount"))
        pct_raw = row.get("tb_percentage")
        try:
            pct = float(pct_raw)
        except (TypeError, ValueError):
            pct = pct_raw
        entry = out.setdefault(
            ti_uid,
            {"tb_amount": 0.0, "tb_percentage": pct, "bounty_earned": 0.0},
        )
        entry["tb_amount"] = round_money(entry["tb_amount"] + amt)
        entry["bounty_earned"] = entry["tb_amount"]
        if pct_raw is not None and entry.get("tb_percentage") is None:
            entry["tb_percentage"] = pct
    return out


def attach_line_commerce_fields(
    line,
    *,
    line_bounty_paid=0.0,
    buyer_bounty=None,
):
    """Apply shipping + bounty fields to one sale line dict (mutates in place)."""
    if not isinstance(line, dict):
        return line

    line["line_bounty_paid"] = round_money(line_bounty_paid)
    line.update(line_shipping_api_fields(line))
    line["ti_bs_shipping_refundable"] = line_shipping_refundable_api(line)

    unit, bounty_type = _catalog_bounty_unit_and_type(line)
    if unit > 0:
        line["bs_bounty"] = round_money(unit)
        line["bs_bounty_type"] = bounty_type
    else:
        line.pop("bs_bounty", None)
        line.pop("bs_bounty_type", None)
        line.pop("profile_expertise_bounty", None)
        line.pop("profile_expertise_bounty_type", None)

    if buyer_bounty:
        if buyer_bounty.get("tb_amount") is not None:
            line["tb_amount"] = buyer_bounty["tb_amount"]
            line["bounty_earned"] = buyer_bounty.get(
                "bounty_earned", buyer_bounty["tb_amount"]
            )
        if buyer_bounty.get("tb_percentage") is not None:
            line["tb_percentage"] = buyer_bounty["tb_percentage"]

    _strip_legacy_line_shipping_fields(line)
    return line


def attach_sale_lines_commerce(db, lines, *, buyer_profile_id=None):
    """Batch attach commerce fields to sale.lines[] or account-screen line rows."""
    if not lines:
        return lines

    ti_uids = [line.get("ti_uid") for line in lines if isinstance(line, dict) and line.get("ti_uid")]
    bounty_map = _line_bounty_totals(db, ti_uids)
    buyer_map = batch_buyer_bounty_by_line(db, ti_uids, buyer_profile_id)

    for line in lines:
        if not isinstance(line, dict):
            continue
        ti_uid = line.get("ti_uid")
        attach_line_commerce_fields(
            line,
            line_bounty_paid=bounty_map.get(ti_uid, 0.0),
            buyer_bounty=buyer_map.get(ti_uid),
        )
    return lines


def compute_line_proceeds_breakdown(db, order_uid, line_row):
    """
    Seller proceeds breakdown for one sale line (wallet ledger lines[]).

    net_amount = merchandise + tax + shipping + bounty_amount (bounty negative).
    """
    if not isinstance(line_row, dict):
        return None

    ti_uid = line_row.get("ti_uid")
    qty = int(line_row.get("ti_bs_qty") or 0)
    if qty <= 0:
        return None

    unit = _parse_unit_cost(line_row.get("ti_bs_cost"))
    merchandise = round_money(unit * qty)
    tax_per_unit = _tax_amount_for_line(
        unit,
        line_row.get("ti_bs_is_taxable"),
        line_row.get("ti_bs_tax_rate"),
    )
    tax_amount = round_money(tax_per_unit * qty)

    ship_fields = line_shipping_api_fields(line_row)
    if "ti_shipping_amount_per_unit" in ship_fields:
        shipping_amount = round_money(ship_fields["ti_shipping_amount_per_unit"] * qty)
    else:
        shipping_amount = round_money(ship_fields.get("ti_shipping_amount_per_line", 0))

    line_bounty = round_money(
        _line_bounty_totals(db, [ti_uid]).get(ti_uid, 0.0) if ti_uid else 0.0
    )
    bounty_amount = round_money(-line_bounty)
    net_amount = round_money(merchandise + tax_amount + shipping_amount + bounty_amount)

    return {
        "ti_uid": ti_uid,
        "ti_bs_id": line_row.get("ti_bs_id"),
        "ti_bs_qty": qty,
        "merchandise_amount": merchandise,
        "tax_amount": tax_amount,
        "shipping_amount": shipping_amount,
        "bounty_amount": bounty_amount,
        "net_amount": net_amount,
    }


def load_commerce_sale_line(db, order_uid, ti_uid):
    """One sale line row with commerce joins, or None."""
    if not order_uid or not ti_uid:
        return None
    for row in _load_commerce_sale_lines(db, order_uid):
        if row.get("ti_uid") == ti_uid:
            return row
    return None


def compute_line_event_proceeds_breakdown(
    db,
    order_uid,
    line_row,
    *,
    return_shipped_qty=0,
    cancel_unshipped_qty=0,
    verified_qty=0,
    line_bounty_ledger=None,
):
    """
    Exact seller-proceeds component amounts for one ledger event on a sale line.

    No order-level proration — uses stored line snapshots and return split fields.
    Returns signed-ready positive magnitudes; caller applies sign for reversals.
    """
    if not isinstance(line_row, dict):
        return None

    ti_uid = line_row.get("ti_uid")
    try:
        return_shipped_qty = int(return_shipped_qty or 0)
        cancel_unshipped_qty = int(cancel_unshipped_qty or 0)
        verified_qty = int(verified_qty or 0)
    except (TypeError, ValueError):
        return None

    event_qty = return_shipped_qty + cancel_unshipped_qty + verified_qty
    if event_qty <= 0:
        return None

    from transactions import (
        _refund_shipping_for_line,
        _seller_bounty_to_reclaim_for_line,
        _tax_amount_for_line,
    )

    if line_bounty_ledger is None and ti_uid:
        line_bounty_ledger = _line_bounty_totals(db, [ti_uid]).get(ti_uid, 0.0)

    unit_merch = _parse_unit_cost(line_row.get("ti_bs_cost"))
    if unit_merch <= 0:
        from order_quantity_context import wallet_ledger_data_issue

        wallet_ledger_data_issue(
            "line missing ti_bs_cost for proceeds breakdown",
            order_uid=order_uid,
            ti_uid=ti_uid,
        )
    unit_tax = _tax_amount_for_line(
        unit_merch,
        line_row.get("ti_bs_is_taxable"),
        line_row.get("ti_bs_tax_rate"),
    )
    line_qty = max(int(line_row.get("ti_bs_qty") or 0), 1)

    return_qty = return_shipped_qty + cancel_unshipped_qty
    if return_qty > 0 and verified_qty > 0:
        return None

    if return_qty > 0:
        merchandise = round_money(unit_merch * return_qty)
        sales_tax = round_money(unit_tax * return_qty)
        shipping = round_money(
            _refund_shipping_for_line(
                line_row,
                return_qty,
                return_shipped_qty=return_shipped_qty,
                cancel_unshipped_qty=cancel_unshipped_qty,
            )
        )
        bounty_paid = round_money(
            _seller_bounty_to_reclaim_for_line(
                line_row,
                return_qty,
                line_bounty_ledger=line_bounty_ledger or 0.0,
            )
        )
        event_qty = return_qty
    elif verified_qty > 0:
        merchandise = round_money(unit_merch * verified_qty)
        sales_tax = round_money(unit_tax * verified_qty)
        if is_per_unit_shipping_model(line_row):
            shipping = round_money(
                _to_float(line_row.get("ti_shipping_amount")) * verified_qty
            )
        elif verified_qty >= line_qty:
            shipping = round_money(line_shipping_charge(line_row))
        else:
            from order_quantity_context import wallet_ledger_data_issue

            wallet_ledger_data_issue(
                "partial verify on non-per-unit shipping line: shipping unavailable",
                order_uid=order_uid,
                ti_uid=ti_uid,
                verified_qty=verified_qty,
                line_qty=line_qty,
            )
            shipping = 0.0
        bounty_paid = round_money(
            _seller_bounty_to_reclaim_for_line(
                line_row,
                verified_qty,
                line_bounty_ledger=line_bounty_ledger or 0.0,
            )
        )
        event_qty = verified_qty
    else:
        return None

    bounty_amount = round_money(-bounty_paid)
    amount = round_money(merchandise + sales_tax + shipping + bounty_amount)

    return {
        "merchandise_amount": merchandise,
        "sales_tax_amount": sales_tax,
        "shipping_amount": shipping,
        "bounty_amount": bounty_amount,
        "amount": amount,
        "purchased_qty": event_qty,
        "ti_uid": ti_uid,
        "ti_bs_id": line_row.get("ti_bs_id"),
    }


def build_order_proceeds_line_breakdowns(db, order_uid, *, buyer_profile_id=None, lines=None):
    """lines[] for wallet ledger sale_proceeds header."""
    if lines is None:
        lines = _load_commerce_sale_lines(db, order_uid)
    attach_sale_lines_commerce(db, lines, buyer_profile_id=buyer_profile_id)
    breakdowns = []
    for line in lines:
        entry = compute_line_proceeds_breakdown(db, order_uid, line)
        if entry:
            breakdowns.append(entry)
    return breakdowns


def _load_commerce_sale_lines(db, order_uid):
    """Minimal sale line rows for proceeds / ledger breakdown."""
    if not order_uid:
        return []
    q = db.execute(
        """
        SELECT
            ti.ti_uid,
            ti.ti_bs_id,
            ti.ti_bs_qty,
            ti.ti_bs_cost,
            ti.ti_bs_is_taxable,
            ti.ti_bs_tax_rate,
            ti.ti_shipping_amount,
            ti.ti_line_shipping_amount,
            ti.ti_listing_shipping,
            ti.ti_shipping_refundable,
            bs.bs_bounty,
            bs.bs_bounty_type,
            pe.profile_expertise_bounty,
            pe.profile_expertise_bounty_type
        FROM every_circle.transactions_items ti
        LEFT JOIN every_circle.business_services bs ON ti.ti_bs_id = bs.bs_uid
        LEFT JOIN every_circle.profile_expertise pe ON ti.ti_bs_id = pe.profile_expertise_uid
        WHERE ti.ti_transaction_id = %s
        ORDER BY ti.ti_uid ASC
        """,
        (order_uid,),
    )
    return q.get("result") or []


def pending_return_item_commerce_fields(db, order_uid, item, *, ti_row=None):
    """line_bounty_reclaim + line_shipping_refund for pending_returns.items[]."""
    if not isinstance(item, dict):
        return {}

    ti_uid = item.get("transaction_item_uid") or item.get("ti_uid")
    if not ti_uid:
        return {}

    try:
        rq = int(item.get("return_quantity") or 0)
    except (TypeError, ValueError):
        rq = 0

    return_shipped = int(item.get("return_shipped_qty") or 0)
    cancel_unshipped = int(item.get("cancel_unshipped_qty") or 0)

    if ti_row is None:
        from transactions import _fetch_ti_row_for_bounty

        ti_row = _fetch_ti_row_for_bounty(db, ti_uid, order_uid)

    out = {}
    if ti_row and rq > 0:
        reclaim = _bounty_to_reclaim_for_line(
            db, order_uid, ti_uid, rq, ti_row=ti_row
        )
        if reclaim:
            out["line_bounty_reclaim"] = round_money(reclaim)

    if ti_row:
        ship_refund = _refund_shipping_for_line(
            ti_row,
            rq,
            return_shipped_qty=return_shipped,
            cancel_unshipped_qty=cancel_unshipped,
        )
        out["line_shipping_refund"] = round_money(ship_refund)

    return out
