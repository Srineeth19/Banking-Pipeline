-- Flattens raw CDC JSON payloads into a clean, deduplicated customers view.
-- Keeps only the latest event per customer_id (last write wins).

with source as (
    select
        after:customer_id::int              as customer_id,
        after:first_name::string             as first_name,
        after:last_name::string              as last_name,
        after:email::string                  as email,
        after:phone::string                  as phone,
        after:city::string                   as city,
        after:state::string                  as state,
        after:country::string                as country,
        op                                    as cdc_op,
        source_ts_ms                          as source_ts_ms,
        row_number() over (
            partition by after:customer_id::int
            order by source_ts_ms desc
        ) as rn
    from {{ source('bronze', 'customers_raw') }}
    where op != 'd'
)

select
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state,
    country,
    to_timestamp_ntz(source_ts_ms / 1000) as last_updated_at
from source
where rn = 1
