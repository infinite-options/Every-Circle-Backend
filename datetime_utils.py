"""UTC storage and API serialization for transaction timestamps."""

from datetime import datetime, timezone

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    from backports.zoneinfo import ZoneInfo  # type: ignore


_DATETIME_FMT = "%Y-%m-%d %H:%M:%S"


def utc_now_str():
    """Naive UTC timestamp string for DB columns (interpreted as GMT)."""
    return datetime.now(timezone.utc).strftime(_DATETIME_FMT)


def parse_stored_datetime(value):
    """
    Parse DB/API datetime as UTC.
    Legacy rows without timezone are treated as UTC.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    text = str(value).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(text.replace(" ", "T", 1))
    except ValueError:
        try:
            dt = datetime.strptime(text[:19], _DATETIME_FMT)
        except ValueError:
            return None

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def format_utc_iso(value):
    """Serialize a stored timestamp as ISO-8601 UTC (Z suffix)."""
    dt = parse_stored_datetime(value)
    if dt is None:
        return None
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def format_local_datetime(value, tz_name):
    """Convert a stored UTC timestamp to a timezone-aware local ISO string."""
    dt = parse_stored_datetime(value)
    if dt is None or not tz_name:
        return None
    try:
        local_dt = dt.astimezone(ZoneInfo(tz_name))
    except Exception:
        return None
    return local_dt.isoformat(timespec="seconds")


def enrich_datetime_fields(row, field="transaction_datetime", tz_name=None):
    """Add UTC ISO (+ optional local) fields alongside the legacy column."""
    if not isinstance(row, dict):
        return row

    raw = row.get(field)
    if not raw:
        return row

    row[field] = format_utc_iso(raw) or raw
    row[f"{field}_utc"] = row[field]
    if tz_name:
        local_value = format_local_datetime(raw, tz_name)
        if local_value:
            row[f"{field}_local"] = local_value
    return row
