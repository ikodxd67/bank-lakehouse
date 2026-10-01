# bank-lakehouse

Хранилище для процессинга банковских карт на стеке, который чаще всего
встречается в банковских вакансиях: Hadoop (HDFS, YARN), Hive, Spark на YARN
(PySpark и Scala), Iceberg, Greenplum и dbt. Изменения из OLTP-базы забираются
через CDC (Debezium), а не по водяному знаку, поэтому хранилище видит и
удаления.

```
core (PostgreSQL) ──Debezium──> Kafka ──Spark──> raw (Hive, Parquet в HDFS)
      │                                              │
      └── начальная выгрузка: Spark JDBC ──> ods (Iceberg, MERGE по версии строки)
                                                     │
                                  Spark на Scala: факты с курсом на дату
                                                     │
                     Greenplum: PXF ──> stg ──dbt──> звезда (SCD2) ──> витрины
```

Оркестрация — Airflow 3: Spark уходит на YARN в режиме `cluster`.

Подробнее: [дизайн](docs/design.md) · [решения и компромиссы](docs/decisions.md) ·
[замеры](docs/performance.md) · [runbook](docs/runbook.md)

## Что внутри

| | |
|---|---|
| **Источник** | PostgreSQL с процессингом карт: 200 тыс. клиентов, 260 тыс. карт, **55,6 млн операций** за 21 месяц. Авторизации подтверждаются через сутки, неподтверждённые удаляются через 7 дней. Симулятор даёт поток вставок, изменений и удалений |
| **CDC** | Debezium 3.7 (`pgoutput`, `snapshot.mode=no_data`) → Kafka 4.2. История — параллельная JDBC-выгрузка Spark. Стык потока и выгрузки безопасен за счёт версии строки (`row_version`) |
| **Hadoop** | HDFS и YARN 3.5 с двумя очередями (`etl`, `adhoc`), Hive Metastore 3.1, HiveServer2, Spark History Server |
| **raw** | события CDC как есть в Hive-таблице Parquet с партициями `tbl/load_dt`; Structured Streaming с `availableNow` по расписанию; ежедневное сжатие мелких файлов |
| **ods** | Iceberg (merge-on-read) в HDFS, каталог — Hive Metastore. MERGE применяет событие, только если его версия больше текущей; удаление — флаг. Отсечение партиций в MERGE: **36 с → 7 с** |
| **Spark на Scala** | выгрузка фактов: пять соединений, курс на дату с переносом на выходные, выбор стратегии против перекоса (AQE, соль, broadcast); тесты на ScalaTest |
| **Greenplum** | Greengage 7.5 (открытый форк Greenplum 7). Загрузка через **PXF**: сегменты читают Parquet из HDFS параллельно, 55,6 млн строк за 200 с |
| **dbt** | dbt-postgres со своим макросом для Greenplum: `DISTRIBUTED BY`, колоночное хранение zstd, помесячные партиции. SCD2 по клиентам и картам по времени изменения в источнике, инкрементальный факт `delete+insert`, три инкрементальные витрины (пересчёт только затронутых дней и месяцев), тесты |
| **Качество** | сверка источника с хранилищем по дням (`bank reconcile`), тесты dbt, проверка непрерывности SCD2, отсутствие персональных данных в Greenplum |
| **Airflow 3** | почасовой цикл от CDC до сверки, ежедневное обслуживание (Iceberg, raw, VACUUM), ручная начальная загрузка. При повторе задачи Spark-приложение не отправляется заново |
| **CI** | линтер и юнит-тесты, Scala (sbt), MERGE на настоящем Iceberg в образе кластера, DAG-и в образе Airflow, dbt на настоящем Greengage в два прохода |

## Замеры

Подробности и выводы — в [performance.md](docs/performance.md).

| | |
|---|---|
| Перекос соединения с ТСП (четверть операций у одного ТСП) | самая долгая задача / медиана: 9,2 без защиты, 1,7 с AQE или солью; broadcast: −25 % времени |
| MERGE в Iceberg на 55,7 млн строк | 36,2 с → 7,1 с за счёт отсечения партиций |
| Начальная выгрузка JDBC → Iceberg | 55,6 млн строк за 196 с |
| HDFS → Greenplum через PXF | 55,6 млн строк за 200 с |
| Почасовой цикл от CDC до сверки | 12,5 мин → 6 мин после перевода витрин на инкремент |
| Ключ распределения факта в Greenplum | по ключу соединения −16–18 % на запросе; ключ с перекосом (60/40 по сегментам) +14–16 % |

## Запуск

Нужен Docker с 8 ГБ памяти и **не меньше 40 ГБ свободного места** (источник
занимает 12 ГБ, HDFS и Greenplum ещё около 10), Python 3.12.

```bash
python -m venv .venv && .venv/Scripts/activate      # Linux/macOS: source .venv/bin/activate
pip install -e ".[dev,dbt]"
cp .env.example .env

docker compose up -d                                       # источник
bank migrate && bank generate                              # история, ~6 минут
docker compose --profile cdc --profile hadoop up -d        # Kafka, Debezium, HDFS, YARN, Metastore
bank cdc-register                                          # слот репликации — до выгрузки!
docker compose --profile gp --profile airflow up -d        # Greenplum, Airflow (http://localhost:8080)
```

Дальше в Airflow: `bank_initial_load` один раз, потом включить `bank_hourly`.
Как то же самое сделать руками и что делать, если что-то упало, — в
[runbook](docs/runbook.md).

Задача на Scala собирается так:

```bash
cd spark/scala && sbt package        # или в контейнере sbtscala/scala-sbt, см. runbook
```

Тесты:

```bash
pytest -m "not integration" --ignore=tests/spark    # юнит-тесты
```

Spark-тесты, тесты DAG-ов и dbt на Greengage запускаются в контейнерах — команды
в [.github/workflows/ci.yml](.github/workflows/ci.yml).

## Структура

```
sql/source/        схема источника и генератор истории (SQL)
src/bank_lakehouse генератор, симулятор, коннектор, загрузчик в Greenplum, сверка
spark/pyspark/     cdc_to_raw, raw_to_ods, initial_load, export_dims, maintenance
spark/scala/       выгрузка фактов на Scala (sbt, ScalaTest)
dbt/               звезда и витрины в Greenplum, макрос для Greenplum
dags/              DAG-и Airflow
docker/            образы: Hadoop+Spark, Hive, Kafka Connect, Airflow; конфиги
scripts/           сброс стенда, замеры
tests/             юнит, Spark (Iceberg), DAG-и, dbt на Greengage
```

## Чего здесь нет

Кластер одномашинный: одна DataNode с репликацией 1, одна NodeManager, Greenplum
с двумя сегментами без зеркал. Нет Kerberos и Ranger — права в HDFS выключены.
Нет Schema Registry: события в JSON, схема задана в коде. HiveServer2 3.1 не
читает Iceberg-таблицы ods (их читает Spark), причины — в
[decisions.md](docs/decisions.md).
