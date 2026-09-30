{{ config(distributed_by=['merchant_id']) }}
select
    date_trunc('month', f.txn_date)::date                         as month,
    f.merchant_id,
    m.mcc,
    m.is_foreign,
    count(*) filter (where f.status = 'posted')                   as posted_count,
    count(*) filter (where f.status = 'reversed')                 as reversed_count,
    sum(f.amount_kzt) filter (where f.status = 'posted')          as turnover_kzt,
    round(avg(f.amount_kzt) filter (where f.status = 'posted'), 2) as avg_ticket_kzt
from {{ ref('fact_card_txn') }} f
join {{ ref('dim_merchant') }} m using (merchant_id)
where not f.is_deleted
group by 1, 2, 3, 4
