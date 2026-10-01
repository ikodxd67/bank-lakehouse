{{ config(materialized='incremental', incremental_strategy='delete+insert', unique_key='month', distributed_by=['client_id']) }}
{# клиент × месяц; распределение по client_id совпадает с фактом по клиенту, если такое понадобится #}
select
    date_trunc('month', f.txn_date)::date                                   as month,
    f.client_id,
    count(*)                                                                as txn_count,
    sum(f.amount_kzt)                                                       as spend_kzt,
    round(sum(f.amount_kzt) filter (where f.merchant_country <> 'KZ') / nullif(sum(f.amount_kzt), 0), 4)
                                                                            as foreign_share,
    count(distinct f.mcc)                                                   as mcc_count,
    (select max(export_id) from {{ ref('fact_card_txn') }})                 as src_export_id
from {{ ref('fact_card_txn') }} f
where f.status = 'posted' and not f.is_deleted
{% if is_incremental() %}
  and date_trunc('month', f.txn_date)::date in ({{ changed_periods("date_trunc('month', txn_date)::date") }})
{% endif %}
group by 1, 2
