"""Race / duplicate handling (unit-only, mocked Spark).

- duplicate-settlement diagnostic never trips the drift gate
- file-registry redelivery is detected via content hash
- reconciliation MERGE is keyed on transaction_id (idempotent rerun)
"""

import pytest

from src.pipeline import check_batch_drift
from src.validation.file_registry import load_registry, record_file

pytestmark = pytest.mark.unit


def test_drift_ignores_duplicate_diagnostic():
    check_batch_drift(
        {
            "settlement_rows_evaluated": 3,
            "batch_MATCHED": 2,
            "batch_EXCEPTION_MISSING_WEBHOOK": 1,
            "batch_EXCEPTION_DUPLICATE_SETTLEMENT": 99,
        }
    )


def test_drift_still_trips_on_real_tiling_gap():
    with pytest.raises(RuntimeError, match="Batch drift"):
        check_batch_drift({"settlement_rows_evaluated": 3, "batch_MATCHED": 1})


def test_registry_redelivery_detected(tmp_path):
    a = tmp_path / "a.csv"
    b = tmp_path / "b.csv"
    a.write_text("tx,amount\n1,100\n")
    b.write_bytes(a.read_bytes())
    data_dir = str(tmp_path)
    first = record_file(data_dir, str(a), "2025-04-02")
    assert first["seen_before"] is False
    second = record_file(data_dir, str(b), "2025-04-02")
    assert second["seen_before"] is True
    assert first["sha256"] == second["sha256"]
    assert first["sha256"] in load_registry(data_dir)


def test_merge_keyed_on_transaction_id():
    import inspect

    from src.processing import reconcile

    src = inspect.getsource(reconcile.run_reconciliation)
    assert "ON t.transaction_id = s.transaction_id" in src
    # rerun must UPDATE matched rows, never duplicate-insert them
    assert src.count("WHEN NOT MATCHED") == 1
