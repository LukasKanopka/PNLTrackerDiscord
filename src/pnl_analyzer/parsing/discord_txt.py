from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime

from dateutil import tz
from dateutil.parser import parse as parse_dt


@dataclass(frozen=True)
class RawMessage:
    author: str
    timestamp_utc: str
    text: str


_HEADER_PATTERNS: list[re.Pattern[str]] = [
    # [3/18/2026 10:15 PM] user: message
    re.compile(r"^\[(?P<ts>.+?)\]\s+(?P<author>[^:]+):\s*(?P<text>.*)$"),
    # 3/18/2026 10:15 PM - user: message
    re.compile(r"^(?P<ts>\d{1,2}/\d{1,2}/\d{2,4}.*?)\s*[-–]\s*(?P<author>[^:]+):\s*(?P<text>.*)$"),
    # user — 03/18/2026 10:15 PM
    re.compile(r"^(?P<author>.+?)\s+—\s+(?P<ts>.+?)$"),
]


def _to_utc_iso(ts: str, export_timezone: str) -> str:
    local_tz = tz.gettz(export_timezone)
    if local_tz is None:
        raise ValueError(f"Unknown timezone: {export_timezone}")
    dt = parse_dt(ts, fuzzy=True)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=local_tz)
    return dt.astimezone(tz.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_discord_txt(content: str, export_timezone: str) -> list[dict]:
    """
    Best-effort parser for common Discord-export `.txt` formats.

    Produces normalized message objects:
      {author, timestamp_utc, text}
    """
    lines = content.splitlines()
    messages: list[RawMessage] = []

    pending_author: str | None = None
    pending_ts_utc: str | None = None
    pending_text_lines: list[str] = []

    def flush() -> None:
        nonlocal pending_author, pending_ts_utc, pending_text_lines
        if pending_author and pending_ts_utc:
            text = "\n".join(pending_text_lines).strip()
            if text:
                messages.append(RawMessage(author=pending_author, timestamp_utc=pending_ts_utc, text=text))
        pending_author = None
        pending_ts_utc = None
        pending_text_lines = []

    i = 0
    while i < len(lines):
        line = lines[i].rstrip("\n")

        # Pattern 1: one-line header containing timestamp, author, and optional text
        m = _HEADER_PATTERNS[0].match(line) or _HEADER_PATTERNS[1].match(line)
        if m:
            flush()
            pending_ts_utc = _to_utc_iso(m.group("ts"), export_timezone)
            pending_author = m.group("author").strip()
            first_text = m.group("text").strip()
            pending_text_lines = [first_text] if first_text else []
            i += 1
            continue

        # Pattern 2: header line "author — timestamp" followed by message text lines until next header
        m = _HEADER_PATTERNS[2].match(line)
        if m:
            flush()
            pending_author = m.group("author").strip()
            pending_ts_utc = _to_utc_iso(m.group("ts"), export_timezone)
            i += 1
            # next line(s) are message content
            while i < len(lines):
                nxt = lines[i]
                if _HEADER_PATTERNS[0].match(nxt) or _HEADER_PATTERNS[1].match(nxt) or _HEADER_PATTERNS[2].match(nxt):
                    break
                pending_text_lines.append(nxt)
                i += 1
            continue

        # Continuation line (multi-line message)
        if pending_author and pending_ts_utc:
            pending_text_lines.append(line)
        i += 1

    flush()

    # Filter obvious attachment-only noise
    out: list[dict] = []
    for msg in messages:
        stripped = msg.text.strip()
        if not stripped:
            continue
        if re.fullmatch(r"https?://\S+", stripped):
            continue
        if stripped.lower().startswith("attachment") or stripped.lower().startswith("uploaded"):
            continue
        out.append({"author": msg.author, "timestamp_utc": msg.timestamp_utc, "text": stripped})
    return out
