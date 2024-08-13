with source as (
      select * from {{ source('staging', 'customers') }}
),
renamed as (
    select
        {{ adapter.quote("customer_id") }} as customer_order_id,
        {{ adapter.quote("customer_unique_id") }} as customer_id,
        {{ adapter.quote("customer_zip_code_prefix") }} as customer_zip_code,
        {{ adapter.quote("customer_city") }},
        {{ adapter.quote("customer_state") }}

    from source
)
select * from renamed
  