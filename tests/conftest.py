import os
import platform
import sys

import pytest

skip_pyspark = platform.system() == "Windows"


@pytest.fixture(scope="session")
def spark():
    if skip_pyspark:
        pytest.skip("PySpark Python worker crashes on Windows with Python 3.13")

    from pyspark.sql import SparkSession

    if "SPARK_HOME" in os.environ:
        del os.environ["SPARK_HOME"]
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.python.worker.reuse", "true")
        .config("spark.sql.shuffle.partitions", "1")
        .appName("TxRecon-Tests")
        .getOrCreate()
    )
    yield session
    session.stop()
