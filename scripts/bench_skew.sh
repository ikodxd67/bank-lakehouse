#!/usr/bin/env bash
# Замер стратегий соединения с ТСП на полной выгрузке фактов (55 млн строк).
# Каждая стратегия пишет в свой каталог и не трогает водяной знак выгрузки.
set -euo pipefail
export MSYS_NO_PATHCONV=1
JAR=/opt/bank/spark/scala/target/scala-2.12/bank-fact_2.12-0.1.0.jar

for s in ${STRATEGIES:-none aqe salt broadcast}; do
  docker compose exec -T nodemanager hdfs dfs -rm -r -f -skipTrash /data/bench/skew/$s > /dev/null
  docker compose exec -T nodemanager spark-submit --deploy-mode cluster --class bank.FactCardTxn "$JAR" \
    --strategy "$s" --full true --record false --out hdfs://namenode:8020/data/bench/skew/$s 2>&1 \
    | grep -oE "application_[0-9]+_[0-9]+" | tail -1 | sed "s/^/$s /"
  docker compose exec -T nodemanager hdfs dfs -rm -r -f -skipTrash /data/bench/skew/$s > /dev/null
done
