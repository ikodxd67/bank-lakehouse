{#
  Факт операций. Каждая выгрузка несёт операции, изменившиеся в источнике; в факт
  попадает последняя версия. Инкремент — delete+insert по txn_id: и факт, и временная
  таблица распределены по txn_id, поэтому удаление идёт на каждом сегменте локально.

  Чтобы DELETE не сканировал все партиции, дата самой ранней изменённой операции
  подставляется условием на целевую таблицу (incremental_predicates).
#}
{%- set min_date = none -%}
{%- if execute and is_incremental() -%}
  {%- set q -%}
    select min(txn_date) from {{ source('stg', 'fact_card_txn_delta') }}
    where export_id > (select coalesce(max(export_id), 0) from {{ this }})
  {%- endset -%}
  {%- set min_date = run_query(q).columns[0].values()[0] -%}
{%- endif %}

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='txn_id',
    distributed_by=var('fact_distribution', ['txn_id']),
    storage={'appendoptimized': 'true', 'orientation': 'column', 'compresstype': 'zstd', 'compresslevel': 1},
    partition_by_range={'column': 'txn_date', 'start': "date '2025-01-01'", 'end': "date '2027-01-01'", 'every': "interval '1 month'"},
    incremental_predicates=(["DBT_INTERNAL_DEST.txn_date >= date '" ~ min_date ~ "'"] if min_date else []),
    post_hook="analyze {{ this }}",
) }}
{# без статистики планировщик GPORCA не знает размеров партиций и ошибается с перемещениями данных #}

with delta as (
    select *
    from {{ source('stg', 'fact_card_txn_delta') }}
    {% if is_incremental() %}
    where export_id > (select coalesce(max(export_id), 0) from {{ this }})
    {% endif %}
),
latest as (
    select *, row_number() over (partition by txn_id order by version desc, export_id desc) as rn
    from delta
)
select
    txn_id, txn_ts, txn_date, card_id, account_id, client_id, product,
    merchant_id, mcc, merchant_country, amount, currency, fx_rate, amount_kzt,
    status, is_deleted, version, export_id
from latest
where rn = 1
