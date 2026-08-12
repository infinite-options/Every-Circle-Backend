"""
Account Screen API v3 payload builders.

Transforms finalized v2 rows into the v3 envelope and embeds wallet + ledger data
so the frontend can render the Account page from a single GET.
"""

from collections import defaultdict

from datetime_utils import enrich_datetime_fields, parse_stored_datetime
from line_commerce_fields import (
    attach_line_snapshots_to_rows,
    aggregate_order_customer_money,
    batch_line_checkout_snapshots,
    build_purchase_order_lines_v3,
    enrich_bounty_result_rows,
    format_offering_rate_display,
    is_consolidated_purchase_order_row,
    line_merchandise_total_from_row,
    line_snapshot_api_fields,
    order_money_from_line_snapshots,
    return_money_from_line_snapshots,
    round_money,
)
from transactions import _to_float
from wallet_ledger import apply_ledger_entry_display, get_wallet_ledger
from wallet_service import build_wallet_summary, compute_wallet_from_bounty_ledger

from account_screen_v3_contract import (
    attention_level_for_row,
    attention_priority,
    build_return_logistics,
    build_row_money,
    enrich_purchase_row_money,
    build_v3_actions,
    build_v3_display,
    build_v3_units,
    format_date_label,
    format_money_label,
    format_tb_percent_label,
    map_row_kind_v3,
    normalize_tb_percentage_display,
    strip_v2_row_fields,
)


def _parse_ledger_pagination(args):
    try:
        limit = int(args.get("ledger_limit", 50))
    except (TypeError, ValueError):
        limit = 50
    try:
        offset = int(args.get("ledger_offset", 0))
    except (TypeError, ValueError):
        offset = 0
    limit = max(1, min(limit, 500))
    offset = max(0, offset)
    return limit, offset


def build_wallet_v3(db, profile_id):
    summary = build_wallet_summary(db, profile_id)
    if not summary:
        return None
    useable = round_money(summary.get("wallet_useable_balance"))
    actual = round_money(summary.get("wallet_actual_balance"))
    # All non-useable funds (pending bounty + held sale proceeds).
    pending = round_money(actual - useable)
    return {
        "useable_balance": useable,
        "pending_balance": pending,
        "actual_balance": actual,
        "currency": "USD",
    }


def _bounty_chart_series(db, profile_id, tz_name):
    q = db.execute(
        """
        SELECT t.transaction_datetime, tb.tb_amount
        FROM every_circle.transactions_bounty tb
        INNER JOIN every_circle.transactions_items ti ON tb.tb_ti_id = ti.ti_uid
        INNER JOIN every_circle.transactions t ON ti.ti_transaction_id = t.transaction_uid
        WHERE tb.tb_profile_id = %s
          AND tb.tb_amount > 0.0001
        ORDER BY t.transaction_datetime ASC
        """,
        (profile_id,),
    )
    rows = (q or {}).get("result") or []
    daily = defaultdict(float)
    for row in rows:
        dt = parse_stored_datetime(row.get("transaction_datetime"))
        if dt is None:
            continue
        if tz_name:
            try:
                from zoneinfo import ZoneInfo

                dt = dt.astimezone(ZoneInfo(tz_name))
            except Exception:
                pass
        day_key = dt.date().isoformat()
        daily[day_key] += _to_float(row.get("tb_amount"))

    cumulative = 0.0
    series = []
    for day in sorted(daily.keys()):
        daily_amt = round_money(daily[day])
        cumulative = round_money(cumulative + daily_amt)
        series.append(
            {
                "date": day,
                "daily": daily_amt,
                "cumulative": cumulative,
            }
        )
    return {"granularity": "day", "series": series}


def build_earnings_v3(db, profile_id, tz_name=None):
    computed = compute_wallet_from_bounty_ledger(db, profile_id)
    return {
        "bounty_total_earned": round_money(computed.get("bounty_total")),
        "bounty_useable": round_money(computed.get("bounty_useable")),
        "bounty_pending": round_money(computed.get("bounty_pending")),
        "chart": _bounty_chart_series(db, profile_id, tz_name),
    }


def _ledger_money_keys():
    return (
        "amount",
        "merchandise_amount",
        "sales_tax_amount",
        "shipping_amount",
        "bounty_amount",
        "fees",
        "net_amount",
        "per_unit_proceeds",
        "per_unit_merchandise",
        "per_unit_sales_tax",
        "per_unit_shipping",
        "per_unit_bounty",
        "useable_delta",
        "pending_delta",
        "balance_after",
        "useable_balance_after",
        "merchandise",
        "tax",
        "shipping",
        "bounty",
    )


def _round_ledger_money_mapping(mapping):
    if not isinstance(mapping, dict):
        return mapping
    out = dict(mapping)
    for key in _ledger_money_keys():
        if key in out and out[key] is not None:
            out[key] = round_money(out[key])
    return out


def _round_ledger_entry(entry):
    """Round wallet-ledger monetary fields to cents at v3 serialization."""
    if not isinstance(entry, dict):
        return entry
    out = _round_ledger_money_mapping(entry)
    breakdown = out.get("breakdown")
    if isinstance(breakdown, dict):
        out["breakdown"] = _round_ledger_money_mapping(breakdown)
    lines = out.get("lines")
    if isinstance(lines, list):
        out["lines"] = [_round_ledger_money_mapping(line) for line in lines]
    return out


def _ledger_entry_breakdown(entry):
    entry_type = entry.get("entry_type") or ""
    amount = round_money(entry.get("amount"))

    if entry_type.startswith("bounty"):
        return {
            "merchandise": 0.0,
            "tax": 0.0,
            "shipping": 0.0,
            "bounty": amount,
            "fees": 0.0,
        }

    merchandise = round_money(entry.get("merchandise_amount"))
    tax = round_money(entry.get("sales_tax_amount"))
    shipping = round_money(entry.get("shipping_amount"))
    bounty = round_money(entry.get("bounty_amount"))
    fees = 0.0
    if not any((merchandise, tax, shipping, bounty)):
        return {
            "merchandise": amount if entry_type in ("wallet_payment", "wallet_refund") else 0.0,
            "tax": 0.0,
            "shipping": 0.0,
            "bounty": 0.0,
            "fees": 0.0,
        }
    return {
        "merchandise": merchandise,
        "tax": tax,
        "shipping": shipping,
        "bounty": bounty,
        "fees": fees,
    }


def build_wallet_ledger_v3(db, profile_id, *, offset=0, limit=50, tz_name=None):
    raw = get_wallet_ledger(db, profile_id, limit=limit, offset=offset)
    entries = []
    for row in raw.get("data") or []:
        if not isinstance(row, dict):
            continue
        entry = dict(row)
        entry["ledger_entry_uid"] = entry.pop("entry_id", None) or entry.get("wt_uid")
        entry["order_uid"] = (
            entry.get("order_uid")
            or entry.get("transaction_original_uid")
            or entry.get("transaction_uid")
        )
        if not entry.get("transaction_uid") and entry.get("order_uid"):
            entry["transaction_uid"] = entry["order_uid"]
        entry = _round_ledger_entry(entry)
        entry["breakdown"] = _ledger_entry_breakdown(entry)
        entry = apply_ledger_entry_display(entry, tz_name)
        if tz_name and entry.get("entry_datetime"):
            enriched = enrich_datetime_fields(entry, "entry_datetime", tz_name)
            entry["entry_datetime_local"] = enriched.get("entry_datetime_local")
        entries.append(entry)

    return {
        "total_entries": raw.get("total_entries") or 0,
        "offset": offset,
        "limit": limit,
        "entries": entries,
    }


def _purchase_type(row):
    bs_id = str(row.get("ti_bs_id") or "")
    if bs_id.startswith("250-"):
        return "service"
    if bs_id.startswith("165-"):
        return "wish"
    return "offering"


def _attach_row_snapshot_fields(v3, row):
    """Emit persisted checkout snapshots on v3 list rows."""
    v3.update(line_snapshot_api_fields(row))
    return v3


def transform_purchase_row_v3(row, *, db=None, tz_name=None, sale_line=None):
    is_consolidated = is_consolidated_purchase_order_row(row)
    if is_consolidated and db:
        order_uid = row.get("order_uid") or row.get("transaction_uid")
        money = aggregate_order_customer_money(db, order_uid, order_row=row)
    else:
        money = build_row_money(row, sale_line=sale_line)
    money = enrich_purchase_row_money(row, money)
    v3 = strip_v2_row_fields(row)
    v3["row_kind"] = map_row_kind_v3(row.get("row_kind"))
    v3["units"] = build_v3_units(row)
    v3["money"] = money
    v3["display"] = build_v3_display(row, money, audience="buyer", tz_name=tz_name)
    v3["return_logistics"] = build_return_logistics(row)
    v3["actions"] = build_v3_actions(row)
    v3["purchase_type"] = _normalize_purchase_type(row)
    v3.setdefault("trr_uid", None)
    if not v3.get("purchased_item") and v3.get("item_name"):
        v3["purchased_item"] = v3.pop("item_name")
    if is_consolidated:
        v3["ti_uid"] = None
        v3["ti_bs_id"] = None
        v3["ti_bs_cost"] = None
        for key in (
            "ti_line_tax_amount",
            "ti_tax_amount",
            "ti_shipping_amount",
            "ti_shipping_amount_per_unit",
            "ti_line_shipping_amount",
            "ti_bs_tax_rate",
            "offering_rate_display",
            "profile_expertise_cost",
        ):
            v3.pop(key, None)
        if db and v3.get("row_kind") == "order":
            order_uid = row.get("order_uid") or row.get("transaction_uid")
            lines = build_purchase_order_lines_v3(db, order_uid)
            if lines:
                v3["lines"] = lines
                if len(lines) == 1:
                    line0 = lines[0]
                    for key in (
                        "offering_rate_display",
                        "profile_expertise_cost",
                        "ti_bs_cost",
                        "unit_price",
                    ):
                        if line0.get(key) is not None:
                            v3[key] = line0[key]
    else:
        _attach_row_snapshot_fields(v3, row)
        if sale_line and v3["row_kind"] == "return":
            _attach_row_snapshot_fields(v3, _return_snapshot_view(row, sale_line))
        unit_cost = v3.get("ti_bs_cost")
        if unit_cost is not None and str(unit_cost).strip():
            v3["offering_rate_display"] = format_offering_rate_display(unit_cost)
    return v3


def _normalize_purchase_type(row):
    """Lowercase v3 purchase_type enum."""
    raw = str(row.get("purchase_type") or "").strip().lower()
    if raw in ("service", "wish", "business"):
        return "service" if raw in ("service", "business") else raw
    if raw in ("offering", "expertise", "product"):
        return "offering"
    return _purchase_type(row)


def _seller_line_bounty_pool(row, *, db=None, sale_line=None, allow_order_fallback=False):
    """Seller bounty pool for one sale line (not whole-order aggregate)."""
    line_bounty = round_money(row.get("line_bounty_paid"))
    if line_bounty:
        return line_bounty
    ti_uid = row.get("ti_uid")
    if db and ti_uid:
        from transactions import _line_bounty_totals, _seller_bounty_pool_for_line_row

        ledger_pool = round_money(_line_bounty_totals(db, [ti_uid]).get(ti_uid, 0.0))
        if ledger_pool:
            return ledger_pool
        if sale_line:
            catalog_pool = round_money(_seller_bounty_pool_for_line_row(sale_line))
            if catalog_pool:
                return catalog_pool
    if allow_order_fallback:
        return round_money(row.get("order_bounty_paid"))
    return 0.0


def _seller_return_bounty_reclaim(row, *, db=None, sale_line=None):
    stored = row.get("bounty_to_reclaim")
    if stored is not None and str(stored).strip() != "":
        reclaim = round_money(stored)
        return reclaim if reclaim else None
    if not db or not row.get("ti_uid"):
        return None
    from transactions import _bounty_to_reclaim_for_line

    units = build_v3_units(row)
    return_qty = int(units.get("return_shipped_qty") or 0) + int(
        units.get("cancel_unshipped_qty") or 0
    )
    if return_qty <= 0:
        return None
    order_uid = (
        row.get("order_uid")
        or row.get("trr_transaction_uid")
        or row.get("transaction_original_uid")
    )
    reclaim = _bounty_to_reclaim_for_line(
        db,
        order_uid,
        row.get("ti_uid"),
        return_qty,
        ti_row=sale_line,
    )
    reclaim = round_money(reclaim)
    return reclaim if reclaim else None


def _seller_bounty_block(row, *, db=None, sale_line=None):
    row_kind = map_row_kind_v3(row.get("row_kind"))
    is_return = row_kind == "return"
    line_bounty = _seller_line_bounty_pool(
        row,
        db=db,
        sale_line=sale_line,
        allow_order_fallback=not is_return,
    )
    reclaim_amt = _seller_return_bounty_reclaim(row, db=db, sale_line=sale_line) if is_return else None
    return {
        "order_bounty_paid": line_bounty if line_bounty else None,
        "bounty_to_reclaim": reclaim_amt,
    }


def _return_snapshot_view(row, sale_line):
    """Snapshot fields scoped to returned/cancelled qty for return rows."""
    from line_commerce_fields import (
        _line_tax_snapshot,
        _refund_shipping_for_line,
    )

    view = dict(sale_line or row)
    units = build_v3_units(row)
    return_qty = int(units.get("return_shipped_qty") or 0) + int(
        units.get("cancel_unshipped_qty") or 0
    )
    if return_qty <= 0:
        return view
    orig_qty = int(sale_line.get("ti_bs_qty") or 0) if sale_line else 0
    view["ti_bs_qty"] = return_qty
    unit = view.get("ti_bs_cost")
    line_tax = _line_tax_snapshot(sale_line or row)
    if line_tax is not None and orig_qty > 0:
        view["ti_line_tax_amount"] = round_money(_to_float(line_tax) * return_qty / orig_qty)
    return_shipped = int(units.get("return_shipped_qty") or 0)
    cancel_unshipped = int(units.get("cancel_unshipped_qty") or 0)
    ship_refund = _refund_shipping_for_line(
        sale_line or row,
        return_qty,
        return_shipped_qty=return_shipped,
        cancel_unshipped_qty=cancel_unshipped,
    )
    view["ti_line_shipping_amount"] = round_money(ship_refund)
    per_unit = (sale_line or row).get("ti_shipping_amount")
    if per_unit is not None:
        view["ti_shipping_amount_per_unit"] = round_money(per_unit)
    return view


def transform_sale_row_v3(row, *, db=None, tz_name=None, sale_line=None):
    money = build_row_money(row, sale_line=sale_line)
    bounty_block = _seller_bounty_block(row, db=db, sale_line=sale_line)
    order_bounty = bounty_block.get("order_bounty_paid") or 0
    reclaim = bounty_block.get("bounty_to_reclaim")
    row_for_display = dict(row)
    row_for_display["order_bounty_paid"] = order_bounty
    row_for_display["line_bounty_paid"] = order_bounty
    row_for_display["bounty_to_reclaim"] = reclaim
    row_for_display["bounty"] = bounty_block
    v3 = strip_v2_row_fields(row)
    v3["row_kind"] = map_row_kind_v3(row.get("row_kind"))
    v3["units"] = build_v3_units(row)
    v3["money"] = money
    v3["order_bounty_paid"] = order_bounty if order_bounty else None
    v3["bounty_paid"] = order_bounty if order_bounty else None
    v3["bounty"] = bounty_block
    v3["purchase_type"] = _normalize_purchase_type(row)
    v3["display"] = build_v3_display(
        row_for_display, money, audience="seller", tz_name=tz_name
    )
    v3["attention_level"] = attention_level_for_row(row)
    v3["return_logistics"] = build_return_logistics(row)
    v3["actions"] = build_v3_actions(row)
    v3.setdefault("trr_uid", None)
    v3.setdefault("placed_by_uid", row.get("transaction_profile_id"))
    if not v3.get("item_name") and v3.get("purchased_item"):
        v3["item_name"] = v3["purchased_item"]
    _attach_row_snapshot_fields(v3, row)
    if sale_line and v3["row_kind"] == "return":
        _attach_row_snapshot_fields(v3, _return_snapshot_view(row, sale_line))
    unit_cost = v3.get("ti_bs_cost")
    if unit_cost is not None and str(unit_cost).strip():
        v3["offering_rate_display"] = format_offering_rate_display(unit_cost)
    return v3


def _sale_line_for_row(row, snap_map):
    ti_uid = row.get("ti_uid")
    if not ti_uid:
        return None
    if row.get("row_kind") in ("return", "pending_return"):
        return snap_map.get(ti_uid)
    return None


def build_buyer_purchase_row_v3(db, profile_id, order_uid, *, tz_name=None):
    """
    One account-screen v3 purchases.rows[] element after a buyer purchase mutation.

    Clears qty caches and reloads from DB so verification fields match post-write state.
    """
    from account_screen_purchases_v2 import build_purchases_v2_rows
    from datetime_utils import enrich_datetime_fields
    from order_quantity_context import clear_ledger_quantity_caches
    from transactions import fetch_buyer_purchase_list_row

    clear_ledger_quantity_caches()

    raw = fetch_buyer_purchase_list_row(db, profile_id, order_uid)
    if not raw:
        return None
    row = enrich_datetime_fields(dict(raw), "transaction_datetime", tz_name)
    v2_rows = build_purchases_v2_rows(db, [row])
    if not v2_rows:
        return None
    return transform_purchase_row_v3(v2_rows[0], db=db, tz_name=tz_name)


def build_purchases_v3(db, rows, *, tz_name=None):
    from order_quantity_context import clear_ledger_quantity_caches

    clear_ledger_quantity_caches()
    enriched = attach_line_snapshots_to_rows(db, rows)
    snap_map = batch_line_checkout_snapshots(
        db, [r.get("ti_uid") for r in enriched if isinstance(r, dict)]
    )
    transformed = [
        transform_purchase_row_v3(
            r,
            db=db,
            tz_name=tz_name,
            sale_line=_sale_line_for_row(r, snap_map),
        )
        for r in enriched
    ]
    return {"rows": transformed}


def _gross_order_qty_for_sold(raw, units):
    """Original units ordered — never net of in-progress returns/cancels."""
    line_units = raw.get("units") or {}
    return int(
        line_units.get("purchased_qty")
        or raw.get("ti_bs_qty")
        or units.get("purchased_qty")
        or 0
    )


def _net_quantity_sold_by_offering(enriched, transactions):
    """Net lifetime units sold per offering (orders minus completed returns/cancels)."""
    sold = defaultdict(int)
    for raw, tx in zip(enriched or [], transactions or []):
        offering_uid = raw.get("ti_bs_id") or raw.get("offering_uid")
        if not offering_uid:
            continue
        raw_kind = raw.get("row_kind")
        if raw.get("is_pending_return") or raw_kind == "pending_return":
            continue
        units = tx.get("units") or build_v3_units(raw)
        if tx.get("row_kind") == "order":
            sold[offering_uid] += _gross_order_qty_for_sold(raw, units)
        elif tx.get("row_kind") == "return" and raw_kind == "return":
            sold[offering_uid] -= int(units.get("return_shipped_qty") or 0)
            sold[offering_uid] -= int(units.get("cancel_unshipped_qty") or 0)
    return sold


def _net_product_sales_aggregates(enriched, transactions, snap_map):
    """
    Net quantity, merchandise revenue, and bounty paid per business product (250-*).

    Revenue is sum of line_merchandise_total on sale lines minus return merchandise
    credits. Omits products with no tracked sale/return activity.
    """
    agg = defaultdict(
        lambda: {
            "qty": 0,
            "revenue": 0.0,
            "bounty_paid": 0.0,
            "has_activity": False,
            "title": None,
        }
    )
    for raw, tx in zip(enriched or [], transactions or []):
        product_uid = raw.get("ti_bs_id") or raw.get("offering_uid")
        if not product_uid or not str(product_uid).startswith("250-"):
            continue
        raw_kind = raw.get("row_kind")
        if raw.get("is_pending_return") or raw_kind == "pending_return":
            continue

        bucket = agg[product_uid]
        name = raw.get("purchased_item") or raw.get("item_name") or raw.get("bs_service_name")
        if name and str(name).strip() and not bucket["title"]:
            bucket["title"] = str(name).strip()

        row_kind = tx.get("row_kind")
        if row_kind == "order":
            bucket["has_activity"] = True
            units = tx.get("units") or build_v3_units(raw)
            bucket["qty"] += _gross_order_qty_for_sold(raw, units)
            merch = line_merchandise_total_from_row(raw)
            if merch is not None:
                bucket["revenue"] = round_money(bucket["revenue"] + merch)
            bounty = round_money(
                raw.get("line_bounty_paid") or raw.get("order_bounty_paid") or 0
            )
            if bounty:
                bucket["bounty_paid"] = round_money(bucket["bounty_paid"] + bounty)
        elif row_kind == "return" and raw_kind == "return":
            bucket["has_activity"] = True
            units = tx.get("units") or build_v3_units(raw)
            ret_qty = int(units.get("return_shipped_qty") or 0) + int(
                units.get("cancel_unshipped_qty") or 0
            )
            bucket["qty"] -= ret_qty
            sale_line = snap_map.get(raw.get("ti_uid"))
            ret_money = return_money_from_line_snapshots(raw, sale_line=sale_line)
            if ret_money.get("known") and ret_money.get("merchandise") is not None:
                bucket["revenue"] = round_money(
                    bucket["revenue"] + ret_money["merchandise"]
                )
            reclaim = round_money(
                raw.get("bounty_to_reclaim")
                or (tx.get("bounty") or {}).get("bounty_to_reclaim")
                or 0
            )
            if reclaim:
                bucket["bounty_paid"] = round_money(bucket["bounty_paid"] - reclaim)
    return agg


def _build_products_v3(enriched, transactions, products_source, snap_map):
    """Authoritative Product Sales summary — one row per catalog product with explicit zeros."""
    aggregates = _net_product_sales_aggregates(enriched, transactions, snap_map)
    attention_by_product = {}
    for raw, tx in zip(enriched or [], transactions or []):
        product_uid = raw.get("ti_bs_id") or raw.get("offering_uid")
        if not product_uid:
            continue
        level = attention_level_for_row(raw)
        if level:
            prev = attention_by_product.get(product_uid)
            if not prev or attention_priority(level) > attention_priority(prev):
                attention_by_product[product_uid] = level

    catalog_items = [
        item for item in (products_source or []) if isinstance(item, dict)
    ]
    from transactions import _parse_limited_quantity

    products = []
    for item in catalog_items:
        uid = item.get("bs_uid") or item.get("product_uid")
        if not uid:
            continue

        stats = aggregates.get(uid) or {}
        title = (
            item.get("bs_service_name")
            or item.get("title")
            or stats.get("title")
            or uid
        )

        quantity_sold = max(0, int(stats.get("qty") or 0))
        revenue = max(0.0, round_money(stats.get("revenue") or 0))
        bounty_paid = max(0.0, round_money(stats.get("bounty_paid") or 0))

        entry = {
            "product_uid": uid,
            "title": str(title).strip(),
            "quantity_sold": quantity_sold,
            "revenue": revenue,
            "bounty_paid": bounty_paid,
        }

        if quantity_sold > 0 or revenue > 0 or bounty_paid > 0:
            entry["money"] = {
                "merchandise": revenue,
                "bounty_paid": bounty_paid,
            }

        raw_qty = item.get("bs_quantity")
        if raw_qty is None or raw_qty == "":
            raw_qty = item.get("quantity_available")
        parsed_qty = _parse_limited_quantity(raw_qty)
        if parsed_qty is None:
            entry["quantity_available_label"] = "Unlimited"
        else:
            entry["quantity_available"] = parsed_qty
            entry["quantity_available_label"] = str(parsed_qty)

        unit_price = round_money(item.get("bs_cost") or item.get("unit_price"))
        if unit_price:
            entry["unit_price"] = unit_price
        catalog_bounty = round_money(item.get("bs_bounty") or item.get("bounty"))
        if catalog_bounty:
            entry["bounty"] = catalog_bounty

        level = attention_by_product.get(uid)
        if level:
            entry["attention_level"] = level
        products.append(entry)
    return products


def _build_offerings_v3(enriched, transactions, offerings_source=None):
    sold_by_offering = _net_quantity_sold_by_offering(enriched, transactions)
    attention_by_offering = {}
    for raw, tx in zip(enriched or [], transactions or []):
        offering_uid = raw.get("ti_bs_id") or raw.get("offering_uid")
        if not offering_uid:
            continue
        level = attention_level_for_row(raw)
        if level:
            prev = attention_by_offering.get(offering_uid)
            if not prev or attention_priority(level) > attention_priority(prev):
                attention_by_offering[offering_uid] = level

    catalog = {
        (item.get("profile_expertise_uid") or item.get("offering_uid") or item.get("bs_uid")): item
        for item in (offerings_source or [])
        if isinstance(item, dict)
    }
    offering_uids = sorted(set(sold_by_offering.keys()) | set(catalog.keys()))
    offerings = []
    for uid in offering_uids:
        if not uid:
            continue
        item = catalog.get(uid) or {}
        net_sold = max(0, int(sold_by_offering.get(uid) or 0))
        if net_sold <= 0 and uid not in catalog:
            continue
        try:
            qty_avail = item.get("profile_expertise_quantity")
            if qty_avail is None:
                qty_avail = item.get("quantity_available")
            qty_int = int(qty_avail) if qty_avail is not None else None
        except (TypeError, ValueError):
            qty_int = None
        unlimited = qty_int is None or qty_int <= 0
        offerings.append(
            {
                "offering_uid": uid,
                "profile_expertise_uid": uid,
                "title": item.get("profile_expertise_title")
                or item.get("title")
                or item.get("bs_service_name"),
                "unit_price": round_money(
                    item.get("profile_expertise_cost") or item.get("unit_price")
                )
                if item
                else None,
                "bounty": round_money(
                    item.get("profile_expertise_bounty") or item.get("bounty")
                )
                if item
                else None,
                "quantity_available": None if unlimited else qty_int,
                "quantity_available_label": "∞" if unlimited else str(qty_int),
                "quantity_sold": net_sold,
                "attention_level": attention_by_offering.get(uid),
            }
        )
    return offerings


def build_sales_v3(db, profile_id, seller_rows, *, tz_name=None, offerings_source=None):
    enriched = attach_line_snapshots_to_rows(db, seller_rows)
    snap_map = batch_line_checkout_snapshots(
        db, [r.get("ti_uid") for r in enriched if isinstance(r, dict)]
    )
    transactions = [
        transform_sale_row_v3(
            r,
            db=db,
            tz_name=tz_name,
            sale_line=_sale_line_for_row(r, snap_map),
        )
        for r in enriched
    ]
    offerings = _build_offerings_v3(enriched, transactions, offerings_source)
    return {"offerings": offerings, "transactions": transactions}


def build_sales_products_v3(db, business_uid, seller_rows, *, tz_name=None, products_source=None):
    """Business account: authoritative sales.products[] Product Sales summary."""
    enriched = attach_line_snapshots_to_rows(db, seller_rows)
    snap_map = batch_line_checkout_snapshots(
        db, [r.get("ti_uid") for r in enriched if isinstance(r, dict)]
    )
    transactions = [
        transform_sale_row_v3(
            r,
            db=db,
            tz_name=tz_name,
            sale_line=_sale_line_for_row(r, snap_map),
        )
        for r in enriched
    ]
    products = _build_products_v3(enriched, transactions, products_source, snap_map)
    return {
        "products": products,
        "transactions": transactions,
    }


def _bounty_proceeds_status(row, db):
    in_escrow = row.get("in_escrow")
    released = row.get("ti_bounty_released_at") or row.get("bounty_released_at")
    if released:
        return "useable"
    if in_escrow in (1, "1", True):
        return "pending"
    ti_uid = row.get("ti_uid")
    if ti_uid:
        q = db.execute(
            """
            SELECT ti_bounty_released_at
            FROM every_circle.transactions_items
            WHERE ti_uid = %s
            LIMIT 1
            """,
            (ti_uid,),
        )
        rows = (q or {}).get("result") or []
        if rows and rows[0].get("ti_bounty_released_at"):
            return "useable"
    return "pending"


def _batch_bounty_line_catalog_fields(db, ti_uids):
    """Product name and ids for bounty_results rows (business + offering lines)."""
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
            bs.bs_uid,
            bs.bs_service_name,
            bs.bs_service_desc,
            pe.profile_expertise_title,
            CASE
                WHEN ti.ti_bs_id LIKE '250-%%' THEN bs.bs_service_name
                WHEN ti.ti_bs_id LIKE '150-%%' THEN pe.profile_expertise_title
                ELSE NULL
            END AS item_name
        FROM every_circle.transactions_items ti
        LEFT JOIN every_circle.business_services bs ON ti.ti_bs_id = bs.bs_uid
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


def _attach_bounty_row_catalog_fields(db, rows):
    """Merge product display fields onto bounty result rows."""
    ti_uids = [r.get("ti_uid") for r in (rows or []) if isinstance(r, dict) and r.get("ti_uid")]
    catalog_map = _batch_bounty_line_catalog_fields(db, ti_uids)
    enriched = []
    for row in rows or []:
        if not isinstance(row, dict):
            enriched.append(row)
            continue
        out = dict(row)
        catalog = catalog_map.get(out.get("ti_uid")) or {}
        for key in (
            "ti_bs_id",
            "ti_bs_qty",
            "bs_uid",
            "bs_service_name",
            "bs_service_desc",
            "profile_expertise_title",
            "item_name",
        ):
            if out.get(key) is None and catalog.get(key) is not None:
                out[key] = catalog.get(key)
        enriched.append(out)
    return enriched


def _bounty_product_display_name(row):
    for key in (
        "bs_service_name",
        "profile_expertise_title",
        "item_name",
        "purchased_item",
        "title",
    ):
        val = row.get(key)
        if val is not None and str(val).strip():
            return str(val).strip()
    return None


def _bounty_row_product_fields(row, *, sale_line=None):
    """Identity, qty, and revenue fields for bounty chart / receipt rows."""
    if not isinstance(row, dict):
        return {}

    is_return = row.get("is_return") in (True, 1, "1", "true", "True")
    ti_bs_id = row.get("ti_bs_id") or row.get("bs_uid")
    product_name = _bounty_product_display_name(row)
    fields = {}

    if ti_bs_id:
        fields["ti_bs_id"] = ti_bs_id
        if str(ti_bs_id).startswith("250-"):
            fields["bs_uid"] = ti_bs_id

    if product_name:
        fields["bs_service_name"] = product_name
        fields["item_name"] = product_name
        fields["purchased_item"] = product_name

    desc = row.get("bs_service_desc")
    if desc is not None and str(desc).strip():
        fields["bs_service_desc"] = str(desc).strip()

    money = {}

    if is_return:
        units = build_v3_units(row)
        ret_qty = int(units.get("return_shipped_qty") or 0) + int(
            units.get("cancel_unshipped_qty") or 0
        )
        if ret_qty > 0:
            fields["ti_bs_qty"] = -ret_qty
        ret_money = return_money_from_line_snapshots(row, sale_line=sale_line)
        if ret_money.get("known") and ret_money.get("merchandise") is not None:
            fields["line_merchandise_total"] = ret_money["merchandise"]
            money["merchandise"] = ret_money["merchandise"]
    else:
        qty = row.get("ti_bs_qty")
        if qty is not None and str(qty).strip() != "":
            try:
                parsed_qty = int(qty)
                if parsed_qty != 0:
                    fields["ti_bs_qty"] = parsed_qty
            except (TypeError, ValueError):
                pass
        merch_total = line_merchandise_total_from_row(row)
        if merch_total is not None:
            fields["line_merchandise_total"] = merch_total
            money["merchandise"] = merch_total
        elif not money:
            order_money = order_money_from_line_snapshots(row)
            if order_money.get("known") and order_money.get("merchandise") is not None:
                fields["line_merchandise_total"] = order_money["merchandise"]
                money["merchandise"] = order_money["merchandise"]

    bounty_paid = round_money(row.get("bounty_paid") or row.get("bounty_earned") or 0)
    if bounty_paid:
        fields["bounty_paid"] = bounty_paid
        money["bounty_paid"] = bounty_paid

    if money:
        fields["money"] = money

    for key, val in line_snapshot_api_fields(row).items():
        if val is not None and fields.get(key) is None:
            fields[key] = val
    return fields


def build_bounty_results_v3(db, rows, *, tz_name=None):
    enriched = enrich_bounty_result_rows(db, rows or [])
    enriched = attach_line_snapshots_to_rows(db, enriched)
    enriched = _attach_bounty_row_catalog_fields(db, enriched)
    snap_map = batch_line_checkout_snapshots(
        db, [r.get("ti_uid") for r in enriched if isinstance(r, dict) and r.get("ti_uid")]
    )
    v3_rows = []
    for row in enriched:
        if not isinstance(row, dict):
            continue
        proceeds_status = _bounty_proceeds_status(row, db)
        earned = round_money(row.get("bounty_earned") or row.get("tb_amount"))
        pool = round_money(row.get("bs_bounty") or row.get("tb_amount"))
        pct = normalize_tb_percentage_display(row.get("tb_percentage"))
        pct_label = format_tb_percent_label(row.get("tb_percentage"))

        entry_dt = row.get("transaction_datetime") or row.get("entry_datetime")
        order_uid = (
            row.get("transaction_uid")
            or row.get("ti_transaction_id")
            or row.get("order_uid")
        )
        ti_uid = row.get("ti_uid")
        sale_line = snap_map.get(ti_uid) if row.get("is_return") in (True, 1, "1") else None
        entry = {
            "bounty_line_uid": f"br-{ti_uid or order_uid}",
            "transaction_uid": order_uid,
            "order_uid": order_uid,
            "ti_uid": ti_uid,
            "entry_datetime": entry_dt,
            "entry_datetime_local": enrich_datetime_fields(
                {"entry_datetime": entry_dt}, "entry_datetime", tz_name
            ).get("entry_datetime_local")
            if entry_dt
            else None,
            "purchaser_profile_id": row.get("transaction_profile_id"),
            "purchaser_name": row.get("purchaser_name"),
            "transaction_business_id": row.get("transaction_business_id"),
            "business_name": row.get("display_name") or row.get("business_name"),
            "bs_bounty": pool,
            "bounty_earned": earned,
            "tb_percentage": pct,
            "proceeds_status": proceeds_status,
            "bounty_released_at": row.get("ti_bounty_released_at"),
            "display": {
                "date_label": format_date_label(entry_dt, tz_name) or "—",
                "status_label": "Useable" if proceeds_status == "useable" else "Pending",
                "pool_label": format_money_label(pool),
                "earned_label": format_money_label(earned),
                "percent_label": pct_label,
            },
        }
        if row.get("is_return") in (True, 1, "1"):
            entry["is_return"] = True
        entry.update(_bounty_row_product_fields(row, sale_line=sale_line))
        v3_rows.append(entry)
    return {"rows": v3_rows}


def build_profile_v3_personal(db, profile_id):
    from user_profile_info import build_account_screen_profile

    profile = build_account_screen_profile(db, profile_id)
    if not profile:
        return {"personal_info": None, "expertise_info": [], "user_email": None}
    return profile


def build_profile_v3_business(info_body):
    if not isinstance(info_body, dict):
        return {"business_info": None, "services": []}
    data = info_body.get("data") if "data" in info_body else info_body
    if isinstance(data, dict):
        services = data.get("services") or data.get("business_services") or []
        business_info = {k: v for k, v in data.items() if k not in ("services", "business_services")}
        return {
            "business_info": business_info,
            "services": services,
        }
    return {"business_info": data, "services": []}
