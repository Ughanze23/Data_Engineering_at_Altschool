with source as (
      select * from {{ source('staging', 'sellers') }}
),
renamed as (
    select
        {{ adapter.quote("seller_id") }},
        {{ adapter.quote("seller_zip_code_prefix") }},
        {{ adapter.quote("seller_city") }},
        {{ adapter.quote("seller_state") }}

    from source
)
select * from renamed
  