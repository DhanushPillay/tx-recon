import os

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Project
    project_root: str = os.environ.get("PROJECT_ROOT", os.getcwd())

    # MinIO / S3
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = "admin"
    minio_secret_key: str = "password"

    # Nessie
    nessie_host: str = "localhost"
    nessie_port: int = 19120
    nessie_ref: str = "main"

    # Redpanda / Kafka
    redpanda_host: str = "localhost"
    kafka_broker: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:8081"
    topic_name: str = "gateway_webhooks"

    # Spark
    spark_shuffle_partitions: int = 8
    spark_jar_packages: str = (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "org.apache.spark:spark-avro_2.12:3.5.1,"
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.107.1,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    # Iceberg
    iceberg_warehouse: str = "s3a://lakehouse/warehouse"
    webhook_table: str = "nessie.db.webhooks"
    dlq_table: str = "nessie.db.webhooks_dlq"

    # Fee engine
    fee_rate_config: str = "config/fee_rates.yaml"
    default_mdr_rate: float = 0.015

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    @classmethod
    def for_airflow(cls) -> "Settings":
        return cls(
            nessie_host="nessie",
            minio_endpoint="http://minio:9000",
            redpanda_host="redpanda",
            kafka_broker="redpanda:9092",
            schema_registry_url="http://redpanda:8081",
        )


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        is_airflow = os.environ.get("AIRFLOW_HOME") is not None
        _settings = Settings.for_airflow() if is_airflow else Settings()
    return _settings
