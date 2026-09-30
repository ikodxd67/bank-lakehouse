{{ config(distributed_by='replicated') }}
{# 20 тыс. строк: реплицировать дешевле, чем перераспределять факт при каждом соединении #}
select merchant_id, name, mcc, city, country, country <> 'KZ' as is_foreign
from {{ ref('stg_merchants') }}
