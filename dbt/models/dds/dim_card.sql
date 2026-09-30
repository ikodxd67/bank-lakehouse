{{ config(distributed_by=['card_id']) }}
select
    card_id,
    account_id,
    client_id,
    product,
    status,
    issued_at,
    expires_at,
    -- История до первой загрузки неизвестна: первую версию открываем с начала
    -- времён, иначе операции до её updated_at не нашли бы версию в соединении по интервалу
    case when row_number() over (partition by card_id order by dbt_valid_from) = 1
         then timestamptz '1900-01-01' else dbt_valid_from end        as valid_from,
    coalesce(dbt_valid_to, timestamptz '9999-12-31')       as valid_to,
    dbt_valid_to is null                                   as is_current
from {{ ref('snap_cards') }}
