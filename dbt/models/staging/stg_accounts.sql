select account_id, client_id, currency, status, opened_at, closed_at, updated_at, is_deleted
from {{ source('stg', 'ext_accounts') }}
