import os

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Central typed config. All infra URLs come from env/.env; code never hardcodes hosts."""

    # Project
    project_root: str = os.environ.get("PROJECT_ROOT", os.getcwd())

    # MinIO / S3
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""

    # Nessie
    nessie_host: str = "localhost"
    nessie_port: int = Field(default=19120, gt=0, lt=65536)
    nessie_ref: str = "main"

    # Redpanda / Kafka
    redpanda_host: str = "localhost"
    kafka_broker: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:8081"
    topic_name: str = "gateway_webhooks"

    # Spark
    spark_mode: str = "local"
    load_csv_on_driver: bool = False
    spark_shuffle_partitions: int = 200
    spark_master: str = "local[*]"
    spark_driver_memory: str = "2g"
    spark_executor_memory: str = "2g"
    spark_executor_cores: int = 2
    spark_jar_packages: str = (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
        "org.apache.spark:spark-avro_2.12:3.5.5,"
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.107.9,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    # Iceberg
    iceberg_warehouse: str = "s3a://lakehouse/warehouse"
    webhook_table: str = "nessie.db.webhooks"
    dlq_table: str = "nessie.db.webhooks_dlq"
    # Cloud targeting without fork: TABLE_PREFIX=glue rewrites nessie.db.* to
    # glue.db.* (Glue catalog), so run_reconciliation() runs unchanged on AWS.
    table_prefix: str = ""

    # Fee engine
    fee_rate_config: str = "config/fee_rates.yaml"
    default_mdr_rate: float = 0.015

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    @model_validator(mode="after")
    def _apply_table_prefix(self) -> "Settings":
        if self.table_prefix:
            prefix = self.table_prefix.rstrip(".")
            for attr in ("webhook_table", "dlq_table"):
                name = getattr(self, attr)
                if "." in name:
                    _, rest = name.split(".", 1)
                    object.__setattr__(self, attr, f"{prefix}.{rest}")
        return self

    @classmethod
    def for_airflow(cls, _base: "Settings | None" = None) -> "Settings":
        base = _base.model_dump() if _base else {}
        return cls(
            **{
                **base,
                "nessie_host": "nessie",
                "minio_endpoint": "http://minio:9000",
                "redpanda_host": "redpanda",
                "kafka_broker": "redpanda:9092",
                "schema_registry_url": "http://redpanda:8081",
            }
        )

    @classmethod
    def for_cluster(cls, _base: "Settings | None" = None) -> "Settings":
        """Free local multinode: driver on host, 1 master + 2 workers in compose.

        Requires hosts entries (minio/nessie/redpanda/spark-master -> 127.0.0.1)
        and SPARK_MODE=cluster. See docker-compose.spark.yml header.
        """
        base = _base.model_dump() if _base else {}
        return cls(
            **{
                **base,
                "nessie_host": "nessie",
                "minio_endpoint": "http://minio:9000",
                "redpanda_host": "redpanda",
                "kafka_broker": "redpanda:9092",
                "schema_registry_url": "http://redpanda:8081",
                "spark_master": "spark://spark-master:7077",
                "spark_shuffle_partitions": 200,
                "spark_executor_cores": 2,
                "load_csv_on_driver": False,
            }
        )


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        base = Settings()
        is_airflow = os.environ.get("AIRFLOW_HOME") is not None
        if base.spark_mode == "cluster":
            _settings = Settings.for_cluster(base)
        else:
            _settings = Settings.for_airflow(base) if is_airflow else base
    return _settings


def reset_settings() -> None:
    """Test helper: clear the module singleton (use in fixtures, not prod code)."""
    global _settings
    _settings = None
