select
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    phone,
    city,
    state,
    country,
    last_updated_at
from {{ ref('stg_customers') }}
