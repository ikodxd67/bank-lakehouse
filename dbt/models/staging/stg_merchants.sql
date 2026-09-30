select merchant_id, name, mcc, city, country, updated_at, is_deleted
from {{ source('stg', 'ext_merchants') }}
