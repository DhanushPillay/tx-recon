import os

from pyspark.sql import SparkSession

from src.common.settings import get_settings


def get_spark_session(app_name="TxRecon"):
    settings = get_settings()

    if "SPARK_HOME" in os.environ:
        del os.environ["SPARK_HOME"]

    packages = settings.spark_jar_packages.split(",")

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
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
        .getOrCreate()
    )

    return spark
