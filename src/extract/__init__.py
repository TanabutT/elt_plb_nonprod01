"""extract - functions that pull data from source systems (mssql) and write to GCS as parquet.
All modules must export function(s) and avoid classes.
"""

__all__ = ["mssql_to_parquet"]
