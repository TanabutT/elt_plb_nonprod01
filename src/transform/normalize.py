"""Tiny transformation example functions.

Keep functions single-responsibility and short so the code stays readable.
"""
from typing import Iterable, Dict, Any, List


def normalize_customer_records(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize a sequence of customer record dicts.

    This function demonstrates a simple pure transform: producing consistent keys and types.
    """
    out = []

    for r in rows:
        normalized = {
            "customer_id": r.get("customer_id") or r.get("id"),
            "name": (r.get("name") or "").strip(),
            "email": (r.get("email") or "").lower(),
        }
        out.append(normalized)

    return out
