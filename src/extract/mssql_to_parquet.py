"""Simple, function-only utilities to extract data from MSSQL and write Parquet files.

These are intentionally minimal and side-effect free where possible so they are easy to test.
"""
from pathlib import Path
from typing import Iterable, Optional


def extract_table_to_parquet(connection_string: str, table_name: str, output_dir: str, columns: Optional[Iterable[str]] = None, where: Optional[str] = None) -> str:
    """Extract `table_name` from the MSSQL `connection_string` and write a parquet file to `output_dir`.

    This is a placeholder implementation used for wiring and tests.

    Returns the path of the written parquet file.
    """
    # Implementation note: In production this function should stream rows, convert to parquet
    # using fastparquet/pyarrow, and write the file to the output directory or upload to GCS.

    Path(output_dir).mkdir(parents=True, exist_ok=True)
    output_path = Path(output_dir) / f"{table_name}.parquet"

    # Placeholder: create an empty parquet-like marker file
    output_path.write_text("parquet-placeholder")

    return str(output_path)
