from __future__ import annotations

from datetime import datetime, timezone

from dateutil.parser import parse as parse_dt
from dateutil import tz


def parse_utc(ts_utc: str) -> datetime:
    dt = parse_dt(ts_utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_unix_seconds(ts_utc: str) -> int:
    return int(parse_utc(ts_utc).timestamp())


def reinterpret_as_local_then_to_utc(ts: str, export_timezone: str) -> str:
    """
    Use when an external source produced ISO timestamps but they are actually in the exporter's local timezone.
    Example: "2026-01-18T06:57:00Z" that is really America/New_York local time.
    """
    local_tz = tz.gettz(export_timezone)
    if local_tz is None:
        raise ValueError(f"Unknown timezone: {export_timezone}")
    dt = parse_dt(ts)
    # Drop any timezone info and reinterpret as local.
    dt = dt.replace(tzinfo=local_tz)
    return dt.astimezone(tz.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
