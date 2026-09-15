{% snapshot accounts_snapshot %}

{{
    config(
        target_schema='gold',
        unique_key='account_id',
        strategy='timestamp',
        updated_at='last_updated_at',
    )
}}

select * from {{ ref('stg_accounts') }}

{% endsnapshot %}
