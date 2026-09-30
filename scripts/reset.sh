#!/usr/bin/env bash
# Полный сброс CDC и озера перед новой генерацией источника.
#
# Частичный сброс опасен: после перегенерации txn_id начинаются заново, и старое
# событие из Kafka с большей версией перезаписало бы в ods новую строку.
set -euo pipefail
export MSYS_NO_PATHCONV=1
CONNECT=${BANK_CONNECT_URL:-http://localhost:8083}

echo "== коннектор: стоп, сброс смещений, удаление"
if curl -sf "$CONNECT/connectors/core-cdc" > /dev/null; then
  curl -sf -X PUT "$CONNECT/connectors/core-cdc/stop" > /dev/null
  sleep 3
  curl -sf -X DELETE "$CONNECT/connectors/core-cdc/offsets" > /dev/null || true
  curl -sf -X DELETE "$CONNECT/connectors/core-cdc" > /dev/null
fi

echo "== слот репликации"
docker compose exec -T source-db psql -U bank -d core -tAc \
  "select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = 'bank_cdc'"

echo "== топики CDC"
for t in $(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list | grep -E '^(core\.|__debezium-heartbeat)'); do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic "$t"
done

echo "== raw, ods и checkpoint"
docker compose exec -T nodemanager spark-sql --deploy-mode client -e "
  DROP TABLE IF EXISTS raw.cdc_events;
  DROP TABLE IF EXISTS ods._load_state PURGE;
  DROP TABLE IF EXISTS ods.clients PURGE;
  DROP TABLE IF EXISTS ods.accounts PURGE;
  DROP TABLE IF EXISTS ods.cards PURGE;
  DROP TABLE IF EXISTS ods.merchants PURGE;
  DROP TABLE IF EXISTS ods.card_transactions PURGE;
  DROP TABLE IF EXISTS ods.fx_rates PURGE;
  DROP TABLE IF EXISTS ods._export_state PURGE;" 2>/dev/null
docker compose exec -T namenode hdfs dfs -rm -r -f -skipTrash /checkpoints /data/raw/cdc_events /data/export
docker compose exec -T namenode hdfs dfs -mkdir -p /data/export
echo "готово"
