"""Function to load Parquet files from GCS into BigQuery.

This is intentionally a small wrapper function for the load operation.
"""
from typing import Optional


def load_parquet_to_bq(gcs_uri: str, dataset: str, table_name: str, write_disposition: str = "WRITE_TRUNCATE", partition_field: Optional[str] = None) -> dict:
    """Load a parquet file at `gcs_uri` into BigQuery `dataset.table_name`.

    Returns a dict-like result placeholder so tests and DAGs can inspect it.
    """
    # Production: call google-cloud-bigquery client and run job config
    result = {
        "gcs_uri": gcs_uri,
        "dataset": dataset,
        "table_name": table_name,
        "status": "SIMULATED_SUCCESS",
    }

    if partition_field:
        result["partition_field"] = partition_field

    return result
