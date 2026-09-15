select
    after:transaction_id::int              as transaction_id,
    after:account_id::int                   as account_id,
    after:transaction_type::string          as transaction_type,
    after:amount::float                     as amount,
    after:merchant::string                  as merchant,
    after:status::string                    as status,
    to_timestamp_ntz(after:transaction_ts::string) as transaction_ts,
    to_timestamp_ntz(source_ts_ms / 1000)   as ingested_at
from {{ source('bronze', 'transactions_raw') }}
where op != 'd'
