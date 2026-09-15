with source as (
    select
        after:account_id::int                as account_id,
        after:customer_id::int                as customer_id,
        after:account_type::string            as account_type,
        after:account_status::string          as account_status,
        after:balance::float                  as balance,
        after:currency::string                as currency,
        op                                     as cdc_op,
        source_ts_ms                           as source_ts_ms,
        row_number() over (
            partition by after:account_id::int
            order by source_ts_ms desc
        ) as rn
    from {{ source('bronze', 'accounts_raw') }}
    where op != 'd'
)

select
    account_id,
    customer_id,
    account_type,
    account_status,
    balance,
    currency,
    to_timestamp_ntz(source_ts_ms / 1000) as last_updated_at
from source
where rn = 1
