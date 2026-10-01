# Runbook

Команды для Git Bash на Windows; на Linux и macOS `MSYS_NO_PATHCONV` не нужен
(он отключает подмену путей вида `/opt/...` в аргументах `docker exec`).

```bash
export MSYS_NO_PATHCONV=1
NM="docker compose exec -T nodemanager"
```

## Первый запуск

Перед стартом проверь место на диске: источнику нужно 12 ГБ, HDFS и Greenplum —
ещё около 10, плюс образы. Когда диск хоста кончился, виртуальная машина Docker
упала вместе со всеми базами (см. «Сбои»).

1. Источник и история:
   ```bash
   docker compose up -d && bank migrate && bank generate
   ```
2. CDC и Hadoop. **Коннектор регистрируется до начальной выгрузки**: слот
   репликации должен существовать раньше, иначе изменения, сделанные во время
   выгрузки, потеряются.
   ```bash
   docker compose --profile cdc --profile hadoop up -d
   bank cdc-register && bank cdc-status
   ```
3. Сборка задачи на Scala:
   ```bash
   docker run --rm -v "$PWD/spark/scala":/work -v bank-sbt-cache:/root/.cache -v bank-ivy:/root/.ivy2 \
     -w /work sbtscala/scala-sbt:eclipse-temurin-17.0.20_8_1.13.0_2.12.21 sbt -batch package
   ```
4. Greenplum и Airflow, затем DAG `bank_initial_load` в интерфейсе Airflow
   (http://localhost:8080) и включить `bank_hourly`.

## Цикл руками

Те же шаги, что делает `bank_hourly`, только в режиме client (вывод драйвера
сразу в консоли):

```bash
$NM bash -c 'cd /opt/bank/spark/pyspark && spark-submit --deploy-mode client cdc_to_raw.py'
$NM bash -c 'cd /opt/bank/spark/pyspark && spark-submit --deploy-mode client --py-files tables.py raw_to_ods.py'
$NM spark-submit --deploy-mode client --class bank.FactCardTxn \
    /opt/bank/spark/scala/target/scala-2.12/bank-fact_2.12-0.1.0.jar
$NM bash -c 'cd /opt/bank/spark/pyspark && spark-submit --deploy-mode client export_dims.py'
bank gp-load
dbt build --project-dir dbt --profiles-dir dbt
bank reconcile
```

## Где смотреть

| Что | Где |
|---|---|
| HDFS | http://localhost:9870 |
| YARN, очереди и приложения | http://localhost:8088 |
| Spark History Server | http://localhost:18080 (из скриптов — `127.0.0.1`) |
| Логи драйвера после режима cluster | `docker compose exec resourcemanager yarn logs -applicationId <id> -log_files stdout` |
| Отставание слота репликации | `select slot_name, active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) from pg_replication_slots` в источнике |
| Коннектор | `bank cdc-status` |
| Перекос по стадиям Spark | `python scripts/spark_stages.py <application_id>` |
| Сверка с источником | `bank reconcile` (дни «в пути» — последние 8, там ещё возможны изменения) |

## Сбои и что с ними делать

**Коннектор упал или стоит.** `bank cdc-status`. Пока коннектор стоит, слот
держит WAL в источнике — следить за отставанием слота. После починки коннектор
продолжит с подтверждённой позиции слота, ничего не потеряв; дубли, если будут,
отсечёт правило версий в ods.

**raw_to_ods упал между MERGE и записью водяного знака.** Ничего делать не
надо: следующий запуск применит тот же пакет повторно, и MERGE его пропустит
(версии не больше текущих).

**Загрузка в Greenplum упала.** Выгрузка грузится одной транзакцией вместе с
записью в `meta.loaded_exports`; при ошибке она откатывается целиком и
подхватится следующим запуском `bank gp-load`.

**Нужно начать заново (новая генерация источника).** Только полностью:
```bash
bash scripts/reset.sh
```
Частичный сброс опасен: после перегенерации `txn_id` начинаются с 1, и старое
событие из Kafka с большей версией перезаписало бы новую строку в ods.

## Что уже ломалось

**Диск хоста заполнился.** Виртуальный диск Docker (`docker_data.vhdx`) растёт
и сам не сжимается. Когда на C: осталось 0,2 ГБ, упали PostgreSQL, Greenplum и
containerd, движок Docker отвечал 500. Восстановление: освободить место на хосте,
закрыть Docker Desktop, `wsl --shutdown`, запустить снова; диски смонтировались
без форматирования, данные целы. После этого — проверить слот, HDFS
(`hdfs dfsadmin -report`: Missing blocks = 0) и Greenplum.

**Kafka писала журналы мимо тома.** Образ `apache/kafka` по умолчанию хранит
журналы в `/tmp` контейнера; нужно `KAFKA_LOG_DIRS=/var/lib/kafka/data`. Перенос
без потерь: выгрузить всё из Kafka в raw (`cdc_to_raw`), пересоздать брокер,
сбросить checkpoint `cdc_to_raw` (смещения в новых топиках начинаются с нуля).
Debezium продолжит со слота.

**Kafka Connect не стартует: `_connect_offsets ... cleanup.policy=compact`.**
Служебный топик создался автоматически брокером с политикой `delete` (так было,
когда Kafka пересоздали, а старый воркер продолжал писать). Остановить Connect,
удалить три топика `_connect_*`, запустить — воркер создаст их правильно.

**Greenplum в цикле перезапусков.** При рестарте контейнера `pxf cluster start`
подключается к базе по TCP без пароля и падает, а с ним и контейнер. Лечится
переменной `PGPASSWORD` в окружении контейнера.

**Hadoop-контейнеры падают с `too many values to unpack` в envtoconf.py.**
Скрипт образа превращает переменные `<имя>_<формат>_<ключ>` в файлы конфигурации;
`YARN_CONF_DIR` он принял за файл `yarn.conf`. Не задавать такие переменные.
