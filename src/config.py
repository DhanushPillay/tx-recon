import os
from pyspark.sql import SparkSession

is_airflow = os.environ.get("AIRFLOW_HOME") is not None
nessie_host = "nessie" if is_airflow else "localhost"
minio_host = "minio" if is_airflow else "localhost"
redpanda_host = "redpanda" if is_airflow else "localhost"


def get_spark_session(app_name="TxRecon"):
    # Clear invalid global SPARK_HOME if it exists to allow pip-installed pyspark to work
    if "SPARK_HOME" in os.environ:
        del os.environ["SPARK_HOME"]

    # Iceberg requires Nessie and AWS S3 SDK for MinIO
    packages = [
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        "org.apache.spark:spark-avro_2.12:3.5.1",
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    ]

    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", ",".join(packages))
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.uri", f"http://{nessie_host}:19120/api/v1")
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.authentication.type", "NONE")
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.nessie.warehouse", "s3a://lakehouse/warehouse")
        .config("spark.sql.catalog.nessie.s3.endpoint", f"http://{minio_host}:9000")
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{minio_host}:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    return spark
