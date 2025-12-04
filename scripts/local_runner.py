"""Small local runner used by developers to execute functions locally for quick smoke tests.

All runner actions should call functions-only modules in src/.
"""
from src.extract.mssql_to_parquet import extract_table_to_parquet


def run_example_extract():
    """Run an example extract locally. Returns the output path.

    This function is intentionally tiny and easy to call from CLI or tests.
    """
    return extract_table_to_parquet("DRIVER={ODBC};SERVER=127.0.0.1;UID=u;PWD=p;", "sample_table", "/tmp/plb_extracted")


if __name__ == "__main__":
    print("Running local extract example: ", run_example_extract())
