from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from pathlib import Path


def _safe_name(name: str) -> str:
    name = (name or "").strip()
    if not name:
        return "upload.txt"
    name = os.path.basename(name)
    name = re.sub(r"[^a-zA-Z0-9._-]+", "_", name)
    return name[:180] or "upload.txt"


@dataclass(frozen=True)
class StoredUpload:
    sha256: str
    byte_size: int
    storage_path: str


def store_upload_bytes(*, upload_store_dir: str, original_filename: str | None, content: bytes) -> StoredUpload:
    sha = hashlib.sha256(content).hexdigest()
    byte_size = len(content)

    base = Path(upload_store_dir)
    base.mkdir(parents=True, exist_ok=True)

    safe = _safe_name(original_filename or "upload.txt")
    # keep it deterministic but readable; collisions are practically impossible with sha prefix
    out_name = f"{sha[:12]}_{safe}"
    out_path = base / out_name
    out_path.write_bytes(content)
    return StoredUpload(sha256=sha, byte_size=byte_size, storage_path=str(out_path))

