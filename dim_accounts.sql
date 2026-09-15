select
    a.account_id,
    a.customer_id,
    a.account_type,
    a.account_status,
    a.balance,
    a.currency,
    a.last_updated_at
from {{ ref('stg_accounts') }} a
