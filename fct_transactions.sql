{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge'
    )
}}

select
    t.transaction_id,
    t.account_id,
    a.customer_id,
    t.transaction_type,
    t.amount,
    t.merchant,
    t.status,
    t.transaction_ts,
    t.ingested_at
from {{ ref('stg_transactions') }} t
left join {{ ref('stg_accounts') }} a on t.account_id = a.account_id

{% if is_incremental() %}
where t.ingested_at > (select coalesce(max(ingested_at), '1900-01-01') from {{ this }})
{% endif %}
