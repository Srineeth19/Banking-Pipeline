{% snapshot customers_snapshot %}

{{
    config(
        target_schema='gold',
        unique_key='customer_id',
        strategy='timestamp',
        updated_at='last_updated_at',
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
