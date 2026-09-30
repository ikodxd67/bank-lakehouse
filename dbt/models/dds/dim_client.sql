{{ config(distributed_by=['client_id']) }}
select
    client_id,
    birth_year,
    city,
    segment,
    -- История до первой загрузки неизвестна: первую версию открываем с начала
    -- времён, иначе операции до её updated_at не нашли бы версию в соединении по интервалу
    case when row_number() over (partition by client_id order by dbt_valid_from) = 1
         then timestamptz '1900-01-01' else dbt_valid_from end        as valid_from,
    coalesce(dbt_valid_to, timestamptz '9999-12-31')       as valid_to,
    dbt_valid_to is null                                   as is_current
from {{ ref('snap_clients') }}
