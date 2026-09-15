#!/bin/bash
set -ex
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
export PATH=$JAVA_HOME/bin:/opt/python/cpython-3.11.16-linux-x86_64-gnu/bin:$PATH
export HADOOP_CONF_DIR=/opt/spark/conf
mkdir -p /opt/spark/conf
cat > /opt/spark/conf/core-site.xml <<'XML'
<configuration><property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property></configuration>
XML
cat > /opt/spark/conf/hdfs-site.xml <<'XML'
<configuration><property><name>dfs.client.use.datanode.hostname</name><value>true</value></property><property><name>dfs.replication</name><value>1</value></property></configuration>
XML
cat > /opt/spark/conf/yarn-site.xml <<'XML'
<configuration><property><name>yarn.resourcemanager.hostname</name><value>resourcemanager</value></property><property><name>yarn.resourcemanager.address</name><value>resourcemanager:8032</value></property></configuration>
XML
pip install --break-system-packages -e /opt/tx-recon
echo "pip done"
