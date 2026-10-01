#!/usr/bin/env bash
# Ключ распределения факта в Greenplum: перекос по сегментам и время трёх запросов.
# Копии без партиций, чтобы сравнивать только распределение. Каждая копия
# удаляется после замера (1,6 ГБ на копию).
set -euo pipefail
export MSYS_NO_PATHCONV=1
psql_gp() { docker compose exec -T -u gpadmin greenplum bash -lc "psql -d dwh -v ON_ERROR_STOP=1 -X -q $*"; }

Q_TURNOVER="select f.txn_date, f.mcc, coalesce(c.segment, 'unknown'), count(*), count(distinct f.client_id), sum(f.amount_kzt)
  from bench.fact f left join dds.dim_client c on c.client_id = f.client_id and f.txn_ts >= c.valid_from and f.txn_ts < c.valid_to
  where f.status = 'posted' and not f.is_deleted group by 1, 2, 3"
Q_CLIENT="select date_trunc('month', txn_date), client_id, count(*), sum(amount_kzt), count(distinct mcc)
  from bench.fact where status = 'posted' and not is_deleted group by 1, 2"
Q_CARD="select c.product, count(*), sum(f.amount_kzt) from bench.fact f join dds.dim_card c using (card_id)
  where c.is_current group by 1"

psql_gp -c "'create schema if not exists bench'"
for key in ${KEYS:-txn_id client_id card_id merchant_id}; do
  psql_gp -c "'drop table if exists bench.fact'"
  t0=$(date +%s)
  psql_gp -c "'create table bench.fact with (appendoptimized = true, orientation = column, compresstype = zstd, compresslevel = 1)
    as select * from dds.fact_card_txn distributed by ($key)'" -c "'analyze bench.fact'"
  echo "== $key: создание $(( $(date +%s) - t0 )) с"
  psql_gp -tA -c "\"select string_agg(n::text, ' / ' order by seg) || '  max/avg = ' || round(max(n) / avg(n), 3)
    from (select gp_segment_id seg, count(*) n from bench.fact group by 1) s\""
  for q in TURNOVER CLIENT CARD; do
    case $q in TURNOVER) sql=$Q_TURNOVER ;; CLIENT) sql=$Q_CLIENT ;; CARD) sql=$Q_CARD ;; esac
    motions=$(psql_gp -tA -c "\"explain $sql\"" | grep -oE "(Redistribute|Broadcast|Gather) Motion" | sort | uniq -c | tr -s ' ' | tr '\n' ';')
    s=$(date +%s%N); psql_gp -c "\"select count(*) from ($sql) q\"" > /dev/null; e=$(date +%s%N)
    echo "   $q: $(( (e - s) / 1000000 )) мс; $motions"
  done
done
psql_gp -c "'drop schema bench cascade'"
