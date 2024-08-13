with source as (
      select * from {{ source('staging', 'product_category') }}
),
renamed as (
    select
        {{ adapter.quote("string_field_0") }} as category_id,
        REPLACE({{ adapter.quote("string_field_1") }},'_', ' ') as product_category_name,
        REPLACE({{ adapter.quote("string_field_2") }}, '_', ' ') as product_category

    from source
)
select * from renamed
  