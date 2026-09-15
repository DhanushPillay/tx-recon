#!/usr/bin/env bash
# Init HDFS for yarn mode — zero-cost local Hadoop.
# Requires docker-compose.hadoop.yml up -d (namenode :8020, resourcemanager :8088)
set -euo pipefail

HDFS="hdfs://namenode:8020"
# run from host via docker exec; works on Windows with docker desktop
exec_hdfs() { docker exec namenode hdfs dfs "$@"; }

echo "== waiting for namenode :9870 =="
for i in {1..30}; do curl -sf http://localhost:9870/ >/dev/null && break || sleep 2; done
curl -sf http://localhost:9870/ >/dev/null || { echo "namenode not up"; exit 1; }

echo "== waiting for hdfs rpc $HDFS =="
for i in {1..30}; do docker exec namenode hdfs dfs -ls / >/dev/null 2>&1 && break || sleep 2; done

echo "== mkdirs =="
exec_hdfs -mkdir -p /warehouse /tmp/spark-staging /user/spark /data
exec_hdfs -chmod -R 777 /warehouse /tmp /user
exec_hdfs -chmod 777 /data || true

echo "== put sample data if present =="
if ls data/*.csv >/dev/null 2>&1; then
  docker cp data/. namenode:/tmp/data_init/
  exec_hdfs -put -f /tmp/data_init/*.csv /data/ || true
fi

exec_hdfs -ls -R /warehouse || true
exec_hdfs -ls /data || true
echo "done — try: docker exec namenode hdfs dfs -ls hdfs://namenode:8020/"
