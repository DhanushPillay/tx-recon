import os

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Central typed config. All infra URLs come from env/.env; code never hardcodes hosts."""

    # Project
    project_root: str = os.environ.get("PROJECT_ROOT", os.getcwd())

    # MinIO / S3
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = Field(default="", min_length=0)
    minio_secret_key: str = Field(default="", min_length=0)
    webhook_secret: str = ""

    # Nessie
    nessie_host: str = "localhost"
    nessie_port: int = Field(default=19120, gt=0, lt=65536)
    nessie_ref: str = "main"

    # Redpanda / Kafka
    redpanda_host: str = "localhost"
    kafka_broker: str = "localhost:19092"
    schema_registry_url: str = "http://localhost:8081"
    topic_name: str = "gateway_webhooks"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_sasl_mechanism: str = ""
    kafka_sasl_username: str = ""
    kafka_sasl_password: str = ""
    # Late-data policy: event-time bound for dropDuplicates state + late-row alerts.
    stream_watermark_delay: str = "1 day"

    # Spark
    spark_mode: str = "local"
    load_csv_on_driver: bool = False
    spark_shuffle_partitions: int = 32
    spark_master: str = "local[*]"
    spark_driver_memory: str = "2g"
    spark_executor_memory: str = "2g"
    spark_executor_cores: int = 2
    spark_jar_packages: str = (
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
        "org.apache.spark:spark-avro_2.12:3.5.5,"
        "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.107.9,"
        "org.apache.iceberg:iceberg-aws-bundle:1.11.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )

    # Iceberg
    iceberg_warehouse: str = "s3a://lakehouse/warehouse"
    nessie_api_version: str = "v2"
    webhook_table: str = "nessie.db.webhooks"
    dlq_table: str = "nessie.db.webhooks_dlq"
    # Cloud targeting without fork: TABLE_PREFIX=glue rewrites nessie.db.* to
    # glue.db.* (Glue catalog), so run_reconciliation() runs unchanged on AWS.
    table_prefix: str = ""

    # Fee engine
    fee_rate_config: str = "config/fee_rates.yaml"

    # Fail-closed switches. Defaults are strict: destructive seeding and
    # silent fallbacks must be opted into, never inherited.
    strict_slo: bool = True
    allow_destructive_seed: bool = False
    require_schema_registry: bool = False
    require_kafka_sasl: bool = False

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8", "extra": "ignore"}

    @model_validator(mode="after")
    def _resolve_secret_files(self) -> "Settings":
        # Docker/K8s secret convention: *_FILE points at a mounted file whose
        # contents are the secret. Explicit env value wins over the file.
        for field in ("minio_secret_key", "webhook_secret", "kafka_sasl_password"):
            if not getattr(self, field):
                path = os.environ.get(field.upper() + "_FILE", "")
                if path:
                    try:
                        with open(path, encoding="utf-8") as fh:
                            object.__setattr__(self, field, fh.read().strip())
                    except OSError as exc:
                        raise ValueError(f"cannot read secret file {path}: {exc}") from exc
        return self

    @model_validator(mode="after")
    def _apply_table_prefix(self) -> "Settings":
        # An empty env override (e.g. SPARK_JAR_PACKAGES=) must not wipe the
        # default jar list — that silently breaks the Spark catalog at runtime.
        if not self.spark_jar_packages.strip():
            object.__setattr__(
                self,
                "spark_jar_packages",
                Settings.model_fields["spark_jar_packages"].default,
            )
        if self.table_prefix:
            prefix = self.table_prefix.rstrip(".")
            for attr in ("webhook_table", "dlq_table"):
                name = getattr(self, attr)
                if "." in name:
                    _, rest = name.split(".", 1)
                    object.__setattr__(self, attr, f"{prefix}.{rest}")
        return self

    @classmethod
    def _docker_common(cls, base: dict) -> dict:
        base.update(
            nessie_host="nessie",
            minio_endpoint="http://minio:9000",
            redpanda_host="redpanda",
            kafka_broker="redpanda:9092",
            schema_registry_url="http://redpanda:8081",
        )
        return base

    @classmethod
    def for_airflow(cls, _base: "Settings | None" = None) -> "Settings":
        base = _base.model_dump() if _base else {}
        return cls(**cls._docker_common(base))

    @classmethod
    def for_local(cls, _base: "Settings | None" = None) -> "Settings":
        """Single-node bench: driver-heavy, file warehouse, low shuffle.

        BENCH_MODE=local -> file:///tmp/tx-recon-warehouse + HadoopFileIO,
        bypassing MinIO S3 round-trip (30-40% saving on local NVMe). Prod
        stays s3a://lakehouse. 32 partitions = cores*2 for 28c host.
        """
        base = _base.model_dump() if _base else {}
        bench_local = os.environ.get("BENCH_MODE") == "local"
        if bench_local:
            tmp = os.path.join(os.environ.get("TEMP", "/tmp"), "tx-recon-warehouse")  # noqa: S108 — local bench warehouse only
            # file:// needs triple slash; normalize Windows backslashes
            wh = "file:///" + tmp.replace("\\", "/").lstrip("/")
            base.update(
                spark_master="local[12]",
                spark_shuffle_partitions=32,
                spark_driver_memory="12g",
                spark_executor_memory="4g",
                iceberg_warehouse=wh,
            )
        else:
            # Even without file://, bench benefits from lower shuffle + more driver mem
            base.update(
                spark_shuffle_partitions=32,
                spark_driver_memory="8g",
            )
        return cls(**base)


_settings: Settings | None = None


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        base = Settings()
        is_airflow = os.environ.get("AIRFLOW_HOME") is not None
        bench_local = os.environ.get("BENCH_MODE") == "local"
        # Single node only: local file warehouse or default s3a.
        if bench_local:
            _settings = Settings.for_local(base)
        else:
            _settings = Settings.for_airflow(base) if is_airflow else base
            # Apply bench shuffle/memory tuning even on default local[*] when BENCH_MODE not set?
            # Only when explicitly requested via env to keep prod 200
            if os.environ.get("BENCH_SHUFFLE") == "1":
                _settings = Settings.for_local(_settings)
        # Allow explicit file warehouse override (e.g. BENCH_WAREHOUSE=file:///tmp/...)
        # Loud on purpose: a stray override silently redirects the warehouse.
        wh_override = os.environ.get("BENCH_WAREHOUSE")
        if wh_override:
            import logging as _logging

            _logging.getLogger(__name__).warning(
                f"BENCH_WAREHOUSE override active: warehouse={wh_override}"
            )
            object.__setattr__(_settings, "iceberg_warehouse", wh_override)
    return _settings
