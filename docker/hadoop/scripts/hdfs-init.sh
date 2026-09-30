#!/usr/bin/env bash
# Однократная подготовка HDFS: каталоги слоёв и архив jar-ов Spark для YARN.
set -euo pipefail

until hdfs dfsadmin -safemode get 2>/dev/null | grep -q "OFF"; do
  echo "жду выхода NameNode из safe mode"; sleep 3
done

hdfs dfs -mkdir -p /spark/events /data/raw /data/export /warehouse /logs/yarn /tmp /tmp/hive
hdfs dfs -chmod 1777 /tmp /tmp/hive /logs/yarn
# архив перезаливается, только если его нет: он меняется вместе с образом
hdfs dfs -test -e /spark/spark-libs.zip || hdfs dfs -put /opt/spark/spark-libs.zip /spark/spark-libs.zip
hdfs dfs -ls /
