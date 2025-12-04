from src.load.gcs_to_bq import load_parquet_to_bq


def test_load_parquet_to_bq_returns_status_key():
    res = load_parquet_to_bq("gs://bucket/test.parquet", "ds", "t")
    assert res.get("status") == "SIMULATED_SUCCESS"
