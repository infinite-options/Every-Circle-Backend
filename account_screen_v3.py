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
    line_snapshot_api_fields,
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
    from transactions import fetch_buyer_purchase_list_row

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
    """Business account: sales.products[] instead of offerings[]."""
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
    sold_by_product = _net_quantity_sold_by_offering(enriched, transactions)
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

    products = []
    for item in products_source or []:
        uid = item.get("bs_uid") or item.get("product_uid")
        if not uid:
            continue
        raw_qty = item.get("bs_quantity")
        if raw_qty is None or raw_qty == "":
            raw_qty = item.get("quantity_available")
        from transactions import _parse_limited_quantity

        parsed_qty = _parse_limited_quantity(raw_qty)
        unlimited = parsed_qty is None
        qty_avail = parsed_qty if parsed_qty is not None else 0
        products.append(
            {
                "product_uid": uid,
                "title": item.get("bs_service_name") or item.get("title"),
                "unit_price": round_money(item.get("bs_cost") or item.get("unit_price")),
                "bounty": round_money(item.get("bs_bounty") or item.get("bounty")),
                "quantity_available": None if unlimited else qty_avail,
                "quantity_available_label": "∞" if unlimited else str(qty_avail),
                "quantity_sold": max(0, int(sold_by_product.get(uid) or 0)),
                "attention_level": attention_by_product.get(uid),
            }
        )
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


def build_bounty_results_v3(db, rows, *, tz_name=None):
    enriched = enrich_bounty_result_rows(db, rows or [])
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
        v3_rows.append(
            {
                "bounty_line_uid": f"br-{row.get('ti_uid') or order_uid}",
                "transaction_uid": order_uid,
                "order_uid": order_uid,
                "ti_uid": row.get("ti_uid"),
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
        )
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
