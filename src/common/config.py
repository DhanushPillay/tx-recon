import os

from pyspark.sql import SparkSession

from src.common.settings import get_settings


def _check_java_home() -> None:
    """Drop a dangling JAVA_HOME so the JVM launch fails clearly, not cryptically.

    A stale JAVA_HOME (e.g. pointing at a deleted JDK) makes PySpark die with a
    bare FileNotFoundError from the gateway launcher. Warn and fall back to PATH.
    """
    import logging
    import shutil

    java_home = os.environ.get("JAVA_HOME")
    if not java_home:
        return
    exe = os.path.join(java_home, "bin", "java.exe" if os.name == "nt" else "java")
    if not os.path.isfile(exe):
        logging.getLogger(__name__).warning(
            "Ignoring dangling JAVA_HOME=%s (no java binary at %s), falling back to PATH=%s",
            java_home,
            exe,
            shutil.which("java"),
        )
        os.environ.pop("JAVA_HOME", None)


def get_spark_session(app_name: str = "TxRecon") -> SparkSession:
    """Build the Iceberg+Nessie+S3A Spark session. Reads all config from Settings."""
    settings = get_settings()

    # PySpark ships its own Hadoop; a stale SPARK_HOME breaks worker classpath.
    os.environ.pop("SPARK_HOME", None)
    _check_java_home()

    packages = settings.spark_jar_packages.split(",")

    spark = (
        SparkSession.builder.appName(app_name)
        .master(settings.spark_master)
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.driver.memory", settings.spark_driver_memory)
        .config("spark.executor.memory", settings.spark_executor_memory)
        # ponytail: Temurin 17.0.20 C2 segfault; set SPARK_TIERED_STOP=1 to re-enable cap, else native JIT
        .config(
            "spark.driver.extraJavaOptions",
            "-XX:TieredStopAtLevel=1" if os.environ.get("SPARK_TIERED_STOP") == "1" else "",
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-XX:TieredStopAtLevel=1" if os.environ.get("SPARK_TIERED_STOP") == "1" else "",
        )
        .config("spark.executor.cores", str(settings.spark_executor_cores))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.nessie.uri",
            f"http://{settings.nessie_host}:{settings.nessie_port}/api/v1",
        )
        .config("spark.sql.catalog.nessie.ref", settings.nessie_ref)
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.nessie.warehouse", settings.iceberg_warehouse)
        .config("spark.sql.catalog.nessie.s3.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", str(settings.spark_shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.hadoop.fs.s3a.committer.name", "directory")
        .config("spark.sql.streaming.checkpoint.compress", "true")
    )

    if settings.spark_master.startswith("spark://"):
        # ponytail: host driver + Docker workers only; local[*] must not force a host.
        builder = (
            spark.config("spark.driver.host", "host.docker.internal")
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.pyspark.python", "python3")
        )
        return builder.getOrCreate()

    return spark.getOrCreate()
