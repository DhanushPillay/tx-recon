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
