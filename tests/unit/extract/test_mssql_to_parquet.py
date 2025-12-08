import os
from src.extract.mssql_to_parquet import extract_table_to_parquet



def test_extract_creates_parquet_file(tmp_path):
    out_dir = tmp_path / "out"
    out_path = extract_table_to_parquet("fake-conn", "my_table", str(out_dir))

    assert os.path.exists(out_path)
    with open(out_path, "r") as fh:
        assert "parquet-placeholder" in fh.read()
