import os
import sys

import pytest


@pytest.fixture(autouse=True)
def _reset_singletons(monkeypatch):
    """Prevent Settings/FeeEngine singletons leaking between tests (order independence)."""
    import src.common.settings as settings_mod
    import src.processing.fee_engine as fee_mod

    monkeypatch.setattr(settings_mod, "_settings", None)
    monkeypatch.setattr(fee_mod, "_fee_engine", None)
    yield
    settings_mod._settings = None
    fee_mod._fee_engine = None


@pytest.fixture()
def canonical_df():
    import pandas as pd

    return pd.DataFrame(
        [
            {
                "transaction_id": "tx_abcdef123456",
                "bank_ref_id": "bnk_001",
                "settled_amount_paise": 97640,
                "settlement_date": "2025-04-02",
                "instrument_type": "CREDIT_CARD",
                "merchant_id": "UNKNOWN",
            }
        ]
    )


@pytest.fixture(scope="session")
def spark():
    # Skip only when Spark is actually unavailable (the old blanket Windows
    # skip hid real MERGE/dedup coverage on dev boxes that run it fine).
    try:
        from pyspark.sql import SparkSession

        old_spark_home = os.environ.pop("SPARK_HOME", None)
        old_python = os.environ.get("PYSPARK_PYTHON")
        old_driver = os.environ.get("PYSPARK_DRIVER_PYTHON")
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

        session = (
            SparkSession.builder.master("local[1]")
            .config("spark.python.worker.reuse", "true")
            .config("spark.sql.shuffle.partitions", "1")
            .appName("TxRecon-Tests")
            .getOrCreate()
        )
    except Exception as exc:
        pytest.skip(f"Spark unavailable: {exc}")
        return
    yield session
    session.stop()
    # restore caller env (never leak test config into other tests)
    if old_spark_home is not None:
        os.environ["SPARK_HOME"] = old_spark_home
    if old_python is None:
        os.environ.pop("PYSPARK_PYTHON", None)
    else:
        os.environ["PYSPARK_PYTHON"] = old_python
    if old_driver is None:
        os.environ.pop("PYSPARK_DRIVER_PYTHON", None)
    else:
        os.environ["PYSPARK_DRIVER_PYTHON"] = old_driver
