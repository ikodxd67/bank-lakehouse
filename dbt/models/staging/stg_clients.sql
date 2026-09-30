select client_id, birth_year, city, segment, created_at, updated_at, is_deleted
from {{ source('stg', 'ext_clients') }}
