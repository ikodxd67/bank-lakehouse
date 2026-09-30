select c.card_id, c.account_id, a.client_id, c.product, c.status, c.issued_at, c.expires_at, c.updated_at, c.is_deleted
from {{ source('stg', 'ext_cards') }} c
left join {{ source('stg', 'ext_accounts') }} a using (account_id)
