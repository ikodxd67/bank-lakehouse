-- в факте по каждой операции лежит последняя загруженная версия
with latest as (
    select txn_id, max(version) as version
    from {{ source('stg', 'fact_card_txn_delta') }}
    group by txn_id
)
select l.txn_id, l.version as expected, f.version as actual
from latest l
left join {{ ref('fact_card_txn') }} f using (txn_id)
where f.version is distinct from l.version
