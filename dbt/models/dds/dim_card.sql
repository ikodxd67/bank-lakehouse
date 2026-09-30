{{ config(distributed_by=['card_id']) }}
select
    card_id,
    account_id,
    client_id,
    product,
    status,
    issued_at,
    expires_at,
    dbt_valid_from                                         as valid_from,
    coalesce(dbt_valid_to, timestamptz '9999-12-31')       as valid_to,
    dbt_valid_to is null                                   as is_current
from {{ ref('snap_cards') }}
