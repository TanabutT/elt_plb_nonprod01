from src.validation.validators import compare_counts, write_validation_log


def test_compare_counts_success():
    res = compare_counts(100, 100)
    assert res["status"] == "SUCCESS"
    assert res["difference"] == 0


def test_write_validation_log_contains_expected_keys():
    r = write_validation_log("job-1", "step", 1, 1, "SUCCESS", "ok")
    assert r["job_id"] == "job-1"
    assert r["status"] == "SUCCESS"
