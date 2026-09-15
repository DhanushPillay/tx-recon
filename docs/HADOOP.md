# Hadoop YARN+HDFS — zero-cost distributed mode

Local 7-service Hadoop 3.3.6 that proves `SPARK_MASTER=yarn` without paying for EMR. Primary warehouse stays `s3a://lakehouse/warehouse` (MinIO, catalog `nessie`), secondary HDFS warehouse `hdfs://namenode:8020/warehouse` (catalog `nessie_hdfs`) proves HDFS.

## Services

`namenode :9870/:8020`, `datanode1/2 :9864`, `resourcemanager :8088`, `nodemanager1 :8042` / `nodemanager2 :8043`, `historyserver :19888`. Image `apache/hadoop:3.3.6` (`platform: linux/amd64`). Replication `1`, NM `4096 MB / 4 vcores`, tuned for 12GB Docker.

## One-time hosts fix

Add to `C:\Windows\System32\drivers\etc\hosts` (admin notepad):

```
127.0.0.1  namenode resourcemanager datanode1 datanode2 minio nessie redpanda spark-master
```

Windows firewall: allow `java` on private network once (driver `host.docker.internal` callback).

## Up

```bash
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml up -d --wait
bash scripts/hdfs_init.sh   # or: docker exec namenode hdfs dfs -ls hdfs://namenode:8020/
curl http://localhost:9870/          # HDFS UI
curl http://localhost:8088/cluster   # YARN UI — wait for READY
curl http://localhost:19888/         # History
docker exec namenode hdfs dfs -ls hdfs://namenode:8020/warehouse
docker exec namenode hdfs dfs -df -h
```

Include screenshots of those 4 URLs in PR (`docs/screenshots/`).

## Run on YARN (client vs cluster)

```bash
# client — driver on host, interactive, needs HADOOP_CONF_DIR + host callback
SPARK_MODE=yarn SPARK_YARN_DEPLOY_MODE=client bash scripts/spark_submit_yarn.sh client src/pipeline.py

# cluster — driver inside YARN AM, client can exit
SPARK_MODE=yarn SPARK_YARN_DEPLOY_MODE=cluster bash scripts/spark_submit_yarn.sh cluster src/pipeline.py

# direct python also works (settings.py maps SPARK_MODE=yarn -> spark_master=yarn)
SPARK_MODE=yarn python src/pipeline.py
SPARK_MODE=yarn python src/processing/reconcile.py
```

YARN UI should show `RUNNING` -> `FINISHED`, Spark History shows executors across 2 NMs. Compare single-node `BENCHMARKS.md` vs YARN.

## Dual catalog

```sql
-- primary s3a (existing): nessie.db.webhooks, nessie.db.webhooks_dlq
-- secondary hdfs (yarn proof):
SHOW NAMESPACES IN nessie_hdfs;
SELECT count(*) FROM nessie_hdfs.db.webhooks;
-- Trino can read both; point Trino at HDFS by mounting hadoop conf if needed
```

## Down

```bash
docker compose -f docker-compose.yml -f docker-compose.hadoop.yml down
# persist volumes namenode_data/datanode*_data; add -v to wipe warehouse
```

## Pitfalls fixed in this branch

- `HADOOP_CONF_DIR must be set` -> `Dockerfile.spark` sets `/opt/spark/conf` and `scripts/spark_submit_yarn.sh` generates `/tmp/hadoop-conf` if empty.
- `spark.yarn.stagingDir must be hdfs://` -> `settings.py spark_yarn_staging_dir=hdfs://namenode:8020/tmp/spark-staging`.
- `fs.defaultFS` + `dfs.client.use.datanode.hostname=true` for Docker bridge.
- `spark.yarn.access.hadoopFileSystems` for S3A+HDFS dual FS.
- Nessie api `v2` for `0.107.5`; Iceberg `1.11.0` matrix correct.

## Cloud mapping

Same Terraform + `spark-submit --master yarn` runs on EMR by swapping `var.aws_region` and warehouse `s3://tx-recon-lakehouse/warehouse` — see `infra/terraform/local` with LocalStack for zero-cost `terraform plan` proof.
