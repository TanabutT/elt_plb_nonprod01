"""Tiny GCS helper stubs used by the extract/load functions.

These are intentionally minimal and easy to test; they avoid external dependencies in this starter.
"""
from typing import Iterable, List


def upload_file_to_gcs(local_path: str, bucket: str, dest_path: str) -> dict:
    """Upload a local file to GCS (placeholder function) and return metadata-like dict.

    Real implementation should use google-cloud-storage.
    """
    return {"local_path": local_path, "bucket": bucket, "dest_path": dest_path, "status": "SIMULATED_UPLOAD"}


def list_gcs_objects(bucket: str, prefix: str = "") -> Iterable[str]:
    """Return an iterable of object paths under a prefix (placeholder).

    Use in tests by patching or replacing this implementation.
    """
    # Simulate an empty list - real implementation would return object URIs.
    return []
