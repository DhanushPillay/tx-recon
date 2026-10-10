import os
import sys

import pytest


@pytest.fixture(autouse=True)
def _reset_singletons(monkeypatch):
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
    except (FileNotFoundError, OSError) as exc:
        pytest.skip(
            f"Spark unavailable (JVM launch): {exc}. Set a valid JAVA_HOME and "
            "unset SPARK_HOME if it points at a deleted install."
        )
    yield session
    session.stop()
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
