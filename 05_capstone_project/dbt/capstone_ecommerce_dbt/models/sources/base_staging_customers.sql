with source as (
      select * from {{ source('staging', 'customers') }}
),
renamed as (
    select
        {{ adapter.quote("customer_id") }},
        {{ adapter.quote("customer_unique_id") }},
        {{ adapter.quote("customer_zip_code_prefix") }},
        {{ adapter.quote("customer_city") }},
        {{ adapter.quote("customer_state") }}

    from source
)
select * from renamed
  