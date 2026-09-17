#!/usr/bin/env bash
# Spark on YARN submit helpers — zero-cost local Hadoop.
# Two modes: client (driver on host, interactive) and cluster (driver inside YARN AM).
# Prerequisites: docker-compose.hadoop.yml up -d, scripts/hdfs_init.sh run once,
#   hosts entries: 127.0.0.1 namenode resourcemanager datanode1 datanode2
#   Windows firewall: allow Java on private network (driver callback host.docker.internal)
set -euo pipefail

MODE="${1:-client}"   # client | cluster
APP="${2:-src/pipeline.py}"
shift 2 2>/dev/null || true

if [[ "$MODE" != "client" && "$MODE" != "cluster" ]]; then
  echo "usage: $0 [client|cluster] [app.py] [app args...]" >&2; exit 2
fi

# HADOOP_CONF_DIR for --master yarn must contain core-site.xml/hdfs-site.xml/yarn-site.xml
# apache/hadoop:3.3.6 generates them from env in container; for host submit we export
# a minimal dir or run submit inside a hadoop container. Easiest: use docker exec.
# Option A: host submit (needs hadoop conf on host)
if [[ -z "${HADOOP_CONF_DIR:-}" ]]; then
  # generate minimal conf on the fly in /tmp/hadoop-conf
  CONF_DIR="/tmp/hadoop-conf"
  mkdir -p "$CONF_DIR"
  cat > "$CONF_DIR/core-site.xml" <<'XML'
<configuration><property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property></configuration>
XML
  cat > "$CONF_DIR/hdfs-site.xml" <<'XML'
<configuration>
  <property><name>dfs.client.use.datanode.hostname</name><value>true</value></property>
  <property><name>dfs.replication</name><value>1</value></property>
</configuration>
XML
  cat > "$CONF_DIR/yarn-site.xml" <<'XML'
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>resourcemanager</value></property>
  <property><name>yarn.resourcemanager.address</name><value>resourcemanager:8032</value></property>
</configuration>
XML
  export HADOOP_CONF_DIR="$CONF_DIR"
fi

export SPARK_YARN_DEPLOY_MODE="$MODE"
export SPARK_MODE=yarn
export SPARK_MASTER=yarn

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.11.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.107.9,org.apache.iceberg:iceberg-aws-bundle:1.11.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,org.apache.spark:spark-avro_2.12:3.5.5"

echo "== HADOOP_CONF_DIR=$HADOOP_CONF_DIR MODE=$MODE APP=$APP PACKAGES=1.11.0/0.107.9 =="

# Run via python (PySpark) honoring SPARK_MODE=yarn in settings.py
# For pure spark-submit jar path, use commented block below.
if [[ "$APP" == *.py ]]; then
  if [[ "$MODE" == "cluster" ]]; then
    echo "NOTE: cluster mode via spark-submit (driver inside YARN AM needs src/ + deps shipped)." >&2
    spark-submit \
      --master yarn --deploy-mode cluster \
      --packages "$PACKAGES" \
      --py-files src.zip \
      --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
      --conf spark.yarn.stagingDir=hdfs://namenode:8020/tmp/spark-staging \
      --conf spark.sql.catalog.nessie.warehouse=s3a://lakehouse/warehouse \
      --conf spark.sql.catalog.nessie_hdfs.warehouse=hdfs://namenode:8020/warehouse \
      "$APP" "$@"
  else
    # Pass through remaining args to the app
    python "$APP" "$@"
  fi
else
  spark-submit \
    --master yarn --deploy-mode "$MODE" \
    --packages "$PACKAGES" \
    --conf spark.hadoop.fs.defaultFS=hdfs://namenode:8020 \
    --conf spark.hadoop.dfs.client.use.datanode.hostname=true \
    --conf spark.yarn.stagingDir=hdfs://namenode:8020/tmp/spark-staging \
    --conf spark.yarn.access.hadoopFileSystems=hdfs://namenode:8020,s3a://lakehouse/ \
    --conf spark.sql.catalog.nessie.warehouse=s3a://lakehouse/warehouse \
    --conf spark.sql.catalog.nessie_hdfs.warehouse=hdfs://namenode:8020/warehouse \
    "$APP" "$@"
fi

# Pure spark-submit example (cluster mode jar):
# spark-submit --master yarn --deploy-mode cluster --packages $PACKAGES \
#   --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v2 \
#   --conf spark.sql.catalog.nessie_hdfs.warehouse=hdfs://namenode:8020/warehouse \
#   local:///opt/app/app.jar
