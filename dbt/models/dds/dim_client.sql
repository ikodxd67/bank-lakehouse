{{ config(distributed_by=['client_id']) }}
select
    client_id,
    birth_year,
    city,
    segment,
    dbt_valid_from                                         as valid_from,
    coalesce(dbt_valid_to, timestamptz '9999-12-31')       as valid_to,
    dbt_valid_to is null                                   as is_current
from {{ ref('snap_clients') }}
