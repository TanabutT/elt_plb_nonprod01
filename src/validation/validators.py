"""Validation helpers for rowcounts and aggregates.

These functions are small and return a dictionary describing the result.
"""
from typing import Any, Dict


def compare_counts(source_count: int, target_count: int) -> Dict[str, Any]:
    """Compare two row counts and return a validation result dict.

    Returns: {"status": "SUCCESS" | "FAILURE", "difference": int}
    """
    diff = source_count - target_count
    status = "SUCCESS" if diff == 0 else "FAILURE"
    return {"status": status, "difference": diff}


def write_validation_log(job_id: str, step: str, source_value: Any, target_value: Any, status: str, details: str = "") -> Dict[str, Any]:
    """Create a small validation log entry dict that could be written to BQ.

    This keeps logs structured and simple for tests & examples.
    """
    return {
        "job_id": job_id,
        "step": step,
        "source_value": source_value,
        "target_value": target_value,
        "status": status,
        "details": details,
    }
