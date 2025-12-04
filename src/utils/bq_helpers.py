"""Tiny BigQuery helper stubs.

These are dummies used to keep the code testable and free of heavy GCP imports for now.
"""
from typing import Dict, Any


def run_query(sql: str) -> Dict[str, Any]:
    """Execute a SQL statement (placeholder) and return a result structure.

    Real implementation would use google-cloud-bigquery and return rows.
    """
    return {"sql": sql, "status": "SIMULATED_OK", "rows": []}


def load_table_from_uri(gcs_uri: str, dataset: str, table: str) -> Dict[str, Any]:
    """Simulate loading a table from a GCS URI.

    Returns a small dictionary to denote status.
    """
    return {"gcs_uri": gcs_uri, "dataset": dataset, "table": table, "status": "SIMULATED_LOAD"}
