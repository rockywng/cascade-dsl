"""Small shared helpers (hashing, SQL normalization)."""
from __future__ import annotations

import hashlib
import json

from .sqlscan import tokenize


def h(*parts) -> str:
    return hashlib.sha256(json.dumps(parts, sort_keys=True, default=str).encode()).hexdigest()[:16]


def normalize_sql(sql: str) -> str:
    """Comment/whitespace/keyword-case insensitive form used for logic hashes."""
    return " ".join(t.up if t.kind == "ident" else t.text for t in tokenize(sql)).rstrip(" ;")
