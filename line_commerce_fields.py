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
        unit = round_money(unit)
        line["bs_bounty"] = unit
        line["ti_bs_bounty"] = unit
        line["bounty_amount"] = unit
        line["item_bounty"] = unit
        line["bs_bounty_type"] = bounty_type
        line["ti_bs_bounty_type"] = bounty_type
    else:
        line.pop("bs_bounty", None)
        line.pop("ti_bs_bounty", None)
        line.pop("bounty_amount", None)
        line.pop("item_bounty", None)
        line.pop("bs_bounty_type", None)
        line.pop("ti_bs_bounty_type", None)
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


def attach_bounty_pool_fields(line, *, catalog_row=None):
    """
    Seller line bounty pool fields for FE Total column (unit × qty semantics).

    Same snapshot as receipt / order-detail sale lines. bounty_earned / tb_amount
    remain the buyer share; pool fields are the unit catalog bounty at purchase.
    """
    if not isinstance(line, dict):
        return line

    source = catalog_row if isinstance(catalog_row, dict) else line
    try:
        qty = int(source.get("ti_bs_qty") or line.get("ti_bs_qty") or 0)
    except (TypeError, ValueError):
        qty = 0
    if qty > 0:
        line["ti_bs_qty"] = qty

    unit, bounty_type = _catalog_bounty_unit_and_type(source)
    if unit > 0:
        unit = round_money(unit)
        line["bs_bounty"] = unit
        line["ti_bs_bounty"] = unit
        line["bounty_amount"] = unit
        line["item_bounty"] = unit
        line["bs_bounty_type"] = bounty_type
        line["ti_bs_bounty_type"] = bounty_type
    return line


def _batch_catalog_rows_for_bounty(db, ti_uids):
    uids = [u for u in (ti_uids or []) if u]
    if not uids:
        return {}
    placeholders = ", ".join(["%s"] * len(uids))
    q = db.execute(
        f"""
        SELECT
            ti.ti_uid,
            ti.ti_bs_id,
            ti.ti_bs_qty,
            bs.bs_bounty,
            bs.bs_bounty_type,
            pe.profile_expertise_bounty,
            pe.profile_expertise_bounty_type
        FROM every_circle.transactions_items ti
        LEFT JOIN every_circle.business_services bs
            ON ti.ti_bs_id = bs.bs_uid
        LEFT JOIN every_circle.profile_expertise pe
            ON ti.ti_bs_id = pe.profile_expertise_uid
        WHERE ti.ti_uid IN ({placeholders})
        """,
        tuple(uids),
    )
    return {
        row.get("ti_uid"): row
        for row in (q.get("result") or [])
        if row.get("ti_uid")
    }


def enrich_bounty_result_rows(db, rows):
    """Attach line bounty pool fields to bounty_results.data[] rows."""
    if not rows:
        return rows

    catalog_map = _batch_catalog_rows_for_bounty(
        db,
        [row.get("ti_uid") for row in rows if isinstance(row, dict)],
    )
    enriched = []
    for row in rows:
        if not isinstance(row, dict):
            enriched.append(row)
            continue
        out = dict(row)
        ti_uid = out.get("ti_uid")
        attach_bounty_pool_fields(out, catalog_row=catalog_map.get(ti_uid))
        enriched.append(out)
    return enriched


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

    merchandise = line_merchandise_total_from_row(line_row)
    if merchandise is None:
        return None
    tax_amount = _line_tax_snapshot(line_row)
    if tax_amount is None:
        tax_amount = round_money(
            _tax_amount_for_line(
                merchandise,
                line_row.get("ti_bs_is_taxable"),
                line_row.get("ti_bs_tax_rate"),
            )
        )

    shipping_amount = _line_shipping_snapshot(line_row)
    if shipping_amount is None:
        shipping_amount = round_money(line_shipping_charge(line_row))

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
        merchandise = _line_merchandise_for_qty(line_row, return_qty) or 0.0
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
        merchandise = _line_merchandise_for_qty(line_row, verified_qty) or 0.0
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
            ti.ti_line_tax_amount,
            ti.ti_tax_amount,
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


def _return_line_split_specs(*, return_shipped_qty=0, cancel_unshipped_qty=0, cancel_only=False):
    """Split one hybrid return line into per-kind API rows."""
    try:
        shipped = int(return_shipped_qty or 0)
        cancel = int(cancel_unshipped_qty or 0)
    except (TypeError, ValueError):
        shipped = cancel = 0

    if shipped > 0 and cancel > 0:
        return [
            {
                "return_kind": "return",
                "return_quantity": shipped,
                "return_shipped_qty": shipped,
                "cancel_unshipped_qty": 0,
            },
            {
                "return_kind": "cancel",
                "return_quantity": cancel,
                "return_shipped_qty": 0,
                "cancel_unshipped_qty": cancel,
            },
        ]
    if cancel > 0 or cancel_only:
        qty = cancel if cancel > 0 else 0
        if qty <= 0:
            return []
        return [
            {
                "return_kind": "cancel",
                "return_quantity": qty,
                "return_shipped_qty": 0,
                "cancel_unshipped_qty": qty,
            }
        ]
    if shipped > 0:
        return [
            {
                "return_kind": "return",
                "return_quantity": shipped,
                "return_shipped_qty": shipped,
                "cancel_unshipped_qty": 0,
            }
        ]
    return []


def _split_row_refund_money(
    db,
    order_uid,
    ti_row,
    *,
    return_qty,
    return_shipped_qty=0,
    cancel_unshipped_qty=0,
    line_bounty_ledger=None,
):
    """Exact refund components for one return/cancel split row."""
    try:
        rq = int(return_qty or 0)
    except (TypeError, ValueError):
        rq = 0
    if rq <= 0 or not isinstance(ti_row, dict):
        return {
            "line_merchandise_refund": 0.0,
            "line_tax_refund": 0.0,
            "line_shipping_refund": 0.0,
            "line_bounty_reclaim": 0.0,
        }

    line_merchandise_refund = _line_merchandise_for_qty(ti_row, rq) or 0.0
    orig_qty = int(ti_row.get("ti_bs_qty") or 0)

    stored_line_tax = _line_tax_snapshot(ti_row)
    if stored_line_tax is not None and orig_qty > 0:
        line_tax_refund = round_money(_to_float(stored_line_tax) * rq / orig_qty)
    else:
        line_tax_refund = round_money(
            _tax_amount_for_line(
                line_merchandise_refund,
                ti_row.get("ti_bs_is_taxable"),
                ti_row.get("ti_bs_tax_rate"),
            )
        )

    line_shipping_refund = round_money(
        _refund_shipping_for_line(
            ti_row,
            rq,
            return_shipped_qty=return_shipped_qty,
            cancel_unshipped_qty=cancel_unshipped_qty,
        )
    )

    ti_uid = ti_row.get("ti_uid")
    line_bounty_reclaim = round_money(
        _bounty_to_reclaim_for_line(
            db,
            order_uid,
            ti_uid,
            rq,
            ti_row=ti_row,
            line_bounty_ledger=line_bounty_ledger,
        )
    )

    return {
        "line_merchandise_refund": line_merchandise_refund,
        "line_tax_refund": line_tax_refund,
        "line_shipping_refund": line_shipping_refund,
        "line_bounty_reclaim": line_bounty_reclaim,
    }


def _attach_split_row_money(row, money):
    row.update(money)
    row["money"] = {
        "merchandise": money["line_merchandise_refund"],
        "tax": money["line_tax_refund"],
        "shipping": money["line_shipping_refund"],
        "bounty": money["line_bounty_reclaim"],
    }
    return row


def expand_return_line_splits(db, order_uid, line, *, ti_row=None, cancel_only=False):
    """
    Expand one return line into 1+ API rows with per-split qty and money.

    Hybrid lines (shipped + cancel on the same ti_uid) become two rows so the FE
    can render Return · must receive and Cancellation · not shipped without
    client-side proration.
    """
    if not isinstance(line, dict):
        return []

    out = dict(line)
    ti_uid = out.get("transaction_item_uid") or out.get("ti_uid")
    if ti_uid:
        out.setdefault("transaction_item_uid", ti_uid)
        out.setdefault("ti_uid", ti_uid)

    has_split = (
        out.get("return_shipped_qty") is not None
        or out.get("cancel_unshipped_qty") is not None
    )
    try:
        shipped = int(out.get("return_shipped_qty") or 0) if has_split else 0
        cancel = int(out.get("cancel_unshipped_qty") or 0) if has_split else 0
        rq = int(out.get("return_quantity") or 0)
    except (TypeError, ValueError):
        shipped = cancel = rq = 0

    if not has_split:
        if cancel_only:
            shipped, cancel = 0, rq
        else:
            shipped, cancel = rq, 0

    specs = _return_line_split_specs(
        return_shipped_qty=shipped,
        cancel_unshipped_qty=cancel,
        cancel_only=cancel_only and cancel <= 0 and shipped <= 0,
    )
    if not specs:
        return []

    if ti_row is None and ti_uid:
        from transactions import _fetch_ti_row_for_bounty

        ti_row = _fetch_ti_row_for_bounty(db, ti_uid, order_uid)

    line_bounty_ledger = None
    if ti_uid:
        line_bounty_ledger = _line_bounty_totals(db, [ti_uid]).get(ti_uid, 0.0)

    results = []
    for spec in specs:
        row = dict(out)
        row.update(spec)
        money = _split_row_refund_money(
            db,
            order_uid,
            ti_row,
            return_qty=spec["return_quantity"],
            return_shipped_qty=spec["return_shipped_qty"],
            cancel_unshipped_qty=spec["cancel_unshipped_qty"],
            line_bounty_ledger=line_bounty_ledger,
        )
        results.append(_attach_split_row_money(row, money))
    return results


def collapse_return_lines_for_list_row(lines):
    """
    One return line per product for account-screen list rows.

    Merges hybrid return/cancel API splits (same ti_uid, different return_kind)
    back into a single line with combined qty and money fields.
    """
    if not lines:
        return []

    def _group_key(line):
        return (
            line.get("transaction_item_uid")
            or line.get("ti_original_ti_uid")
            or line.get("ti_uid")
            or line.get("ti_bs_id")
        )

    groups = {}
    order = []
    for line in lines:
        if not isinstance(line, dict):
            continue
        key = _group_key(line)
        if key is None:
            order.append(id(line))
            groups[id(line)] = dict(line)
            continue
        if key not in groups:
            merged = dict(line)
            merged.pop("return_kind", None)
            groups[key] = merged
            order.append(key)
            continue
        merged = groups[key]
        for qty_field in ("return_shipped_qty", "cancel_unshipped_qty", "return_quantity"):
            try:
                merged[qty_field] = int(merged.get(qty_field) or 0) + int(
                    line.get(qty_field) or 0
                )
            except (TypeError, ValueError):
                pass
        for money_field in (
            "line_merchandise_refund",
            "line_tax_refund",
            "line_shipping_refund",
            "line_bounty_reclaim",
        ):
            if money_field in line or money_field in merged:
                merged[money_field] = round_money(
                    _to_float(merged.get(money_field))
                    + _to_float(line.get(money_field))
                )
        m0 = merged.get("money") if isinstance(merged.get("money"), dict) else {}
        m1 = line.get("money") if isinstance(line.get("money"), dict) else {}
        if m0 or m1:
            merged["money"] = {
                k: round_money(_to_float(m0.get(k)) + _to_float(m1.get(k)))
                for k in ("merchandise", "tax", "shipping", "bounty")
                if k in m0 or k in m1
            }
        merged.pop("return_kind", None)
    return [groups[k] for k in order]


def expand_return_lines_list(db, order_uid, lines, *, cancel_only=False):
    """Expand and attach per-split money for each return line."""
    expanded = []
    ti_cache = {}
    for line in lines or []:
        ti_uid = (line or {}).get("transaction_item_uid") or (line or {}).get("ti_uid")
        ti_row = ti_cache.get(ti_uid) if ti_uid else None
        if ti_row is None and ti_uid:
            from transactions import _fetch_ti_row_for_bounty

            ti_row = _fetch_ti_row_for_bounty(db, ti_uid, order_uid)
            ti_cache[ti_uid] = ti_row
        expanded.extend(
            expand_return_line_splits(
                db,
                order_uid,
                line,
                ti_row=ti_row,
                cancel_only=cancel_only,
            )
        )
    return expanded


def pending_return_item_commerce_fields(db, order_uid, item, *, ti_row=None):
    """Legacy single-row commerce helper — use expand_return_line_splits for hybrids."""
    if not isinstance(item, dict):
        return {}

    try:
        shipped = int(item.get("return_shipped_qty") or 0)
        cancel = int(item.get("cancel_unshipped_qty") or 0)
    except (TypeError, ValueError):
        shipped = cancel = 0
    if shipped > 0 and cancel > 0:
        return {}

    splits = expand_return_line_splits(db, order_uid, item, ti_row=ti_row)
    if len(splits) != 1:
        return {}
    split = splits[0]
    return {
        k: split[k]
        for k in (
            "line_merchandise_refund",
            "line_tax_refund",
            "line_shipping_refund",
            "line_bounty_reclaim",
            "money",
            "return_kind",
        )
        if k in split
    }


_LINE_TAX_AMOUNT_COLUMN_READY = False
_LINE_TAX_AMOUNT_V2_COLUMN_READY = False


def ensure_line_tax_amount_column(db):
    """Add ti_line_tax_amount (+ legacy ti_tax_amount) checkout snapshot columns."""
    global _LINE_TAX_AMOUNT_COLUMN_READY, _LINE_TAX_AMOUNT_V2_COLUMN_READY
    if not _LINE_TAX_AMOUNT_COLUMN_READY:
        db.execute(
            "ALTER TABLE every_circle.transactions_items "
            "ADD COLUMN ti_line_tax_amount DECIMAL(12,2) NULL",
            cmd="post",
        )
        _LINE_TAX_AMOUNT_COLUMN_READY = True
    if not _LINE_TAX_AMOUNT_V2_COLUMN_READY:
        db.execute(
            "ALTER TABLE every_circle.transactions_items "
            "ADD COLUMN ti_tax_amount DECIMAL(12,2) NULL",
            cmd="post",
        )
        _LINE_TAX_AMOUNT_V2_COLUMN_READY = True
        # Backfill preferred column from legacy name when present.
        db.execute(
            """
            UPDATE every_circle.transactions_items
            SET ti_line_tax_amount = ti_tax_amount
            WHERE ti_line_tax_amount IS NULL
              AND ti_tax_amount IS NOT NULL
            """,
            cmd="post",
        )


def _line_snapshot_qty(row):
    try:
        return int(row.get("ti_bs_qty") or (row.get("units") or {}).get("purchased_qty") or 0)
    except (TypeError, ValueError):
        return 0


def _line_choices_extra_total(row):
    """Total choice add-on dollars stored on the line (not per unit)."""
    if not isinstance(row, dict):
        return 0.0
    raw = row.get("ti_choices_extra_cost")
    if raw is None or raw == "":
        raw = row.get("choices_extra_cost")
    if raw is None or raw == "":
        return 0.0
    return round_money(_to_float(raw))


def line_merchandise_total_from_row(row):
    """
    Line merchandise from checkout snapshots: unit cost × qty + choice extras.

    ti_bs_cost is the base unit price at purchase; ti_choices_extra_cost is the
    total add-on for the line when business options were selected.
    """
    if not isinstance(row, dict):
        return None
    qty = _line_snapshot_qty(row)
    return _line_merchandise_for_qty(row, qty)


def _line_merchandise_for_qty(row, qty):
    """Merchandise dollars for qty units of a sale line (prorates choice extras)."""
    if not isinstance(row, dict):
        return None
    try:
        qty = int(qty or 0)
    except (TypeError, ValueError):
        return None
    if qty <= 0:
        return None
    unit = _parse_unit_cost(row.get("ti_bs_cost") or row.get("unit_price"))
    if unit <= 0:
        return None
    base = round_money(unit * qty)
    extra = _line_choices_extra_total(row)
    orig_qty = _line_snapshot_qty(row)
    if extra > 0 and orig_qty > 0:
        return round_money(base + extra * qty / orig_qty)
    return base


def _snapshot_present(value):
    return value is not None and value != ""


def _line_tax_snapshot(row):
    """
    Resolve line tax from stored snapshots only.

    Priority: ti_line_tax_amount → ti_tax_amount → rate × line merchandise.
    Never allocates from order-level transaction_taxes.
    """
    if not isinstance(row, dict):
        return None
    for key in ("ti_line_tax_amount", "line_tax_amount", "ti_tax_amount"):
        if _snapshot_present(row.get(key)):
            return round_money(row.get(key))

    qty = _line_snapshot_qty(row)
    merchandise = line_merchandise_total_from_row(row)
    if merchandise is None:
        return None
    rate = row.get("ti_bs_tax_rate") or row.get("ti_tax_rate")
    if not _snapshot_present(rate):
        return None
    tax = round_money(
        _tax_amount_for_line(merchandise, row.get("ti_bs_is_taxable"), rate)
    )
    return tax if tax >= 0 else None


def _line_shipping_snapshot(row):
    """
    Resolve line shipping from stored snapshots only.

    Priority: ti_line_shipping_amount → ti_shipping_amount_per_unit × qty.
    Never allocates from order-level transaction_shipping.
    """
    if not isinstance(row, dict):
        return None
    line_total = row.get("ti_line_shipping_amount") or row.get("line_shipping_amount")
    if _snapshot_present(line_total):
        return round_money(line_total)

    qty = _line_snapshot_qty(row)
    if qty <= 0:
        return None
    per_unit = row.get("ti_shipping_amount_per_unit")
    if not _snapshot_present(per_unit):
        per_unit = row.get("ti_shipping_amount")
    if _snapshot_present(per_unit):
        return round_money(_to_float(per_unit) * qty)
    return None


def line_snapshot_api_fields(row):
    """Normalized snapshot fields for account-screen rows."""
    if not isinstance(row, dict):
        return {}
    qty = _line_snapshot_qty(row)
    unit = row.get("ti_bs_cost") or row.get("unit_price")
    out = {}
    if _snapshot_present(unit):
        out["ti_bs_cost"] = _normalize_stored_cost_value(unit)
    if qty > 0:
        out["ti_bs_qty"] = qty
    rate = row.get("ti_bs_tax_rate") or row.get("ti_tax_rate")
    if _snapshot_present(rate):
        out["ti_bs_tax_rate"] = rate
    tax = _line_tax_snapshot(row)
    if tax is not None:
        out["ti_line_tax_amount"] = tax
    per_unit = row.get("ti_shipping_amount_per_unit")
    if not _snapshot_present(per_unit):
        per_unit = row.get("ti_shipping_amount")
    if _snapshot_present(per_unit):
        out["ti_shipping_amount_per_unit"] = round_money(per_unit)
        out["ti_shipping_amount"] = round_money(per_unit)
    ship = _line_shipping_snapshot(row)
    if ship is not None:
        out["ti_line_shipping_amount"] = ship
    merch_total = line_merchandise_total_from_row(row)
    if merch_total is not None:
        out["line_merchandise_total"] = merch_total
    choices_extra = _line_choices_extra_total(row)
    if choices_extra > 0:
        out["ti_choices_extra_cost"] = choices_extra
    return out


def _normalize_stored_cost_value(value):
    try:
        return str(round(_parse_unit_cost(value), 2)).rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return value


def format_offering_rate_display(unit_cost):
    """Purchase/line display rate — always '$X/each'."""
    unit = _parse_unit_cost(unit_cost)
    if unit <= 0:
        return None
    if unit == int(unit):
        return f"${int(unit)}/each"
    return f"${unit:.2f}/each"


def format_profile_expertise_cost_label(unit_cost):
    """Catalog-style unit rate without currency prefix — 'X/each'."""
    unit = _parse_unit_cost(unit_cost)
    if unit <= 0:
        return None
    if unit == int(unit):
        return f"{int(unit)}/each"
    return f"{unit:.2f}/each"


def build_purchase_line_v3_entry(line_row):
    """Enriched purchases.rows[].lines[] element from persisted line snapshots."""
    if not isinstance(line_row, dict):
        return {}
    unit = _parse_unit_cost(line_row.get("ti_bs_cost"))
    item_name = line_row.get("item_name") or line_row.get("purchased_item")
    merch_total = line_merchandise_total_from_row(line_row)
    entry = {
        "ti_uid": line_row.get("ti_uid"),
        "ti_bs_id": line_row.get("ti_bs_id"),
        "item_name": item_name,
        "purchased_item": item_name,
        "ti_bs_qty": int(line_row.get("ti_bs_qty") or 0),
        "unit_price": unit if unit > 0 else None,
        "money": order_money_from_line_snapshots(line_row),
    }
    if merch_total is not None:
        entry["line_merchandise_total"] = merch_total
    cost_str = _normalize_stored_cost_value(line_row.get("ti_bs_cost"))
    if cost_str is not None:
        entry["ti_bs_cost"] = cost_str
    if unit > 0:
        entry["profile_expertise_cost"] = format_profile_expertise_cost_label(unit)
        entry["offering_rate_display"] = format_offering_rate_display(unit)
    tax = _line_tax_snapshot(line_row)
    if tax is not None:
        entry["ti_line_tax_amount"] = tax
    ship = _line_shipping_snapshot(line_row)
    if ship is not None:
        entry["ti_line_shipping_amount"] = ship
    received = line_row.get("ti_received_qty")
    if received is not None:
        entry["ti_received_qty"] = int(received or 0)
    shipped = line_row.get("ti_shipped_qty")
    if shipped is not None:
        entry["ti_shipped_qty"] = int(shipped or 0)
    return entry


def build_purchase_order_lines_v3(db, order_uid):
    """Load and enrich all sale lines for a buyer purchase order row."""
    from account_screen_line_rows import load_order_sale_lines

    if not db or not order_uid:
        return []
    return [
        build_purchase_line_v3_entry(line)
        for line in load_order_sale_lines(db, order_uid)
    ]


def order_money_from_line_snapshots(row):
    """
    Build v3 order money from stored checkout snapshots only.

    No order-level allocation or rate recomputation on read.
    """
    merchandise = line_merchandise_total_from_row(row)
    tax_amt = _line_tax_snapshot(row)
    ship_amt = _line_shipping_snapshot(row)

    if merchandise is None or tax_amt is None or ship_amt is None:
        return {
            "merchandise": None,
            "tax": None,
            "shipping": None,
            "customer_total": None,
            "customer_credit": None,
            "known": False,
        }

    total = round_money(merchandise + tax_amt + ship_amt)

    return {
        "merchandise": merchandise,
        "tax": tax_amt,
        "shipping": ship_amt,
        "customer_total": total,
        "customer_credit": None,
        "known": True,
    }


def aggregate_order_customer_money(db, order_uid, *, order_row=None):
    """
    Sum persisted line snapshots for a multi-item buyer order row.

    customer_total = merchandise + tax + shipping (excludes card fees).
    Exposes money.fees and money.customer_total_with_fees when order fees exist.
    """
    if not order_uid:
        return {
            "merchandise": None,
            "tax": None,
            "shipping": None,
            "customer_total": None,
            "customer_credit": None,
            "known": False,
        }

    q = db.execute(
        """
        SELECT ti_uid
        FROM every_circle.transactions_items
        WHERE ti_transaction_id = %s
        ORDER BY ti_uid ASC
        """,
        (order_uid,),
    )
    ti_uids = [r.get("ti_uid") for r in (q.get("result") or []) if r.get("ti_uid")]
    if not ti_uids:
        return {
            "merchandise": None,
            "tax": None,
            "shipping": None,
            "customer_total": None,
            "customer_credit": None,
            "known": False,
        }

    snap_map = batch_line_checkout_snapshots(db, ti_uids)
    total_merch = total_tax = total_ship = 0.0
    for ti_uid in ti_uids:
        line_money = order_money_from_line_snapshots(snap_map.get(ti_uid) or {})
        if not line_money.get("known"):
            return {
                "merchandise": None,
                "tax": None,
                "shipping": None,
                "customer_total": None,
                "customer_credit": None,
                "known": False,
            }
        total_merch += _to_float(line_money.get("merchandise"))
        total_tax += _to_float(line_money.get("tax"))
        total_ship += _to_float(line_money.get("shipping"))

    customer_total = round_money(total_merch + total_tax + total_ship)
    out = {
        "merchandise": round_money(total_merch),
        "tax": round_money(total_tax),
        "shipping": round_money(total_ship),
        "customer_total": customer_total,
        "customer_credit": None,
        "known": True,
    }
    fees = round_money((order_row or {}).get("transaction_fees"))
    if fees > 0:
        out["fees"] = fees
        out["customer_total_with_fees"] = round_money(customer_total + fees)
    return out


def is_consolidated_purchase_order_row(row):
    """True for buyer list rows aggregated at order level (not per-line sale_line)."""
    if not isinstance(row, dict):
        return False
    kind = row.get("row_kind")
    if kind == "sale_line":
        return False
    if kind in ("return", "pending_return"):
        return False
    return bool(row.get("order_uid") or row.get("transaction_uid"))


def return_money_from_line_snapshots(row, *, sale_line=None):
    """
    Build v3 return money by prorating original sale line snapshots.

    Uses per-unit economics from checkout — not order-level totals.
    """
    source = sale_line if isinstance(sale_line, dict) else row
    units = row.get("units") or {}
    return_qty = int(units.get("return_shipped_qty") or 0) + int(
        units.get("cancel_unshipped_qty") or units.get("return_unshipped_qty") or 0
    )
    if return_qty <= 0:
        return_qty = int(units.get("purchased_qty") or row.get("return_quantity_total") or 0)
    if return_qty <= 0:
        try:
            return_qty = int(row.get("ti_bs_qty") or 0)
        except (TypeError, ValueError):
            return_qty = 0

    orig_qty = _line_snapshot_qty(source)
    unit = _parse_unit_cost(source.get("ti_bs_cost") or source.get("unit_price"))
    line_tax = _line_tax_snapshot(source)

    if return_qty <= 0 or orig_qty <= 0 or unit <= 0 or line_tax is None:
        pending = row.get("pending_return") or {}
        estimated = pending.get("estimated_refund") or row.get("estimated_refund") or {}
        if estimated:
            merchandise = -abs(round_money(estimated.get("subtotal")))
            tax = -abs(round_money(estimated.get("taxes")))
            shipping = -abs(
                round_money(estimated.get("shipping_refund") or estimated.get("shipping"))
            )
            credit = -abs(
                round_money(
                    estimated.get("total_customer_credit") or estimated.get("total")
                )
            )
            if credit == 0 and (merchandise or tax or shipping):
                credit = round_money(merchandise + tax + shipping)
            if credit:
                return {
                    "merchandise": merchandise,
                    "tax": tax,
                    "shipping": shipping,
                    "customer_total": None,
                    "customer_credit": credit,
                    "known": True,
                }
        return {
            "merchandise": None,
            "tax": None,
            "shipping": None,
            "customer_total": None,
            "customer_credit": None,
            "known": False,
        }

    merchandise = round_money(unit * return_qty)
    tax_amt = round_money(_to_float(line_tax) * return_qty / orig_qty)
    return_shipped = int(units.get("return_shipped_qty") or 0)
    cancel_unshipped = int(
        units.get("cancel_unshipped_qty") or units.get("return_unshipped_qty") or 0
    )
    shipping_credit = round_money(
        _refund_shipping_for_line(
            source,
            return_qty,
            return_shipped_qty=return_shipped,
            cancel_unshipped_qty=cancel_unshipped,
        )
    )
    credit = round_money(-(merchandise + tax_amt + shipping_credit))

    return {
        "merchandise": round_money(-merchandise),
        "tax": round_money(-tax_amt),
        "shipping": round_money(-shipping_credit),
        "customer_total": None,
        "customer_credit": credit,
        "known": True,
    }


def batch_line_checkout_snapshots(db, ti_uids):
    """Load persisted checkout snapshots for account-screen money blocks."""
    uids = [u for u in (ti_uids or []) if u]
    if not uids:
        return {}
    ensure_line_tax_amount_column(db)
    placeholders = ", ".join(["%s"] * len(uids))
    q = db.execute(
        f"""
        SELECT
            ti_uid,
            ti_bs_id,
            ti_bs_cost,
            ti_bs_qty,
            ti_bs_is_taxable,
            ti_bs_tax_rate,
            ti_line_tax_amount,
            ti_tax_amount,
            ti_shipping_amount,
            ti_line_shipping_amount,
            ti_shipping_refundable,
            ti_listing_shipping
        FROM every_circle.transactions_items
        WHERE ti_uid IN ({placeholders})
        """,
        tuple(uids),
    )
    return {
        row.get("ti_uid"): row
        for row in (q.get("result") or [])
        if row.get("ti_uid")
    }


def attach_line_snapshots_to_rows(db, rows):
    """Merge stored line snapshots onto account-screen rows (in place copy)."""
    ti_uids = [r.get("ti_uid") for r in (rows or []) if isinstance(r, dict) and r.get("ti_uid")]
    snap_map = batch_line_checkout_snapshots(db, ti_uids)
    enriched = []
    for row in rows or []:
        if not isinstance(row, dict):
            enriched.append(row)
            continue
        out = dict(row)
        snap = snap_map.get(out.get("ti_uid"))
        if snap:
            skip_qty = out.get("row_kind") in ("return", "pending_return")
            for key in (
                "ti_bs_id",
                "ti_bs_cost",
                "ti_bs_qty",
                "ti_bs_is_taxable",
                "ti_bs_tax_rate",
                "ti_line_tax_amount",
                "ti_tax_amount",
                "ti_shipping_amount",
                "ti_line_shipping_amount",
                "ti_shipping_refundable",
                "ti_listing_shipping",
            ):
                if skip_qty and key == "ti_bs_qty":
                    continue
                if out.get(key) is None and snap.get(key) is not None:
                    out[key] = snap.get(key)
            if out.get("ti_line_tax_amount") is None and snap.get("ti_tax_amount") is not None:
                out["ti_line_tax_amount"] = snap.get("ti_tax_amount")
        enriched.append(out)
    return enriched
