from __future__ import annotations

from datetime import datetime, timezone

from dateutil.parser import parse as parse_dt


def parse_utc(ts_utc: str) -> datetime:
    dt = parse_dt(ts_utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_unix_seconds(ts_utc: str) -> int:
    return int(parse_utc(ts_utc).timestamp())

