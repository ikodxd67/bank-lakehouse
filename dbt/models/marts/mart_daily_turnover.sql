{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='txn_date', distributed_by='randomly') }}
{#
  Оборот по дням, MCC и сегменту клиента. Сегмент берётся на момент операции
  (соединение с SCD2 по интервалу), а не текущий: клиент, ставший affluent в
  сентябре, не переписывает оборот mass за август.
#}
select
    f.txn_date,
    f.mcc,
    coalesce(c.segment, 'unknown')            as segment,
    count(*)                                  as txn_count,
    count(distinct f.client_id)               as client_count,
    sum(f.amount_kzt)                         as turnover_kzt,
    (select max(export_id) from {{ ref('fact_card_txn') }}) as src_export_id
from {{ ref('fact_card_txn') }} f
left join {{ ref('dim_client') }} c
    on c.client_id = f.client_id
   and f.txn_ts >= c.valid_from and f.txn_ts < c.valid_to
where f.status = 'posted' and not f.is_deleted
{% if is_incremental() %}
  and f.txn_date in ({{ changed_periods('txn_date') }})
{% endif %}
group by 1, 2, 3
