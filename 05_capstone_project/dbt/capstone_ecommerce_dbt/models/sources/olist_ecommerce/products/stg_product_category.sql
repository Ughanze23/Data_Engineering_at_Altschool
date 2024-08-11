with source as (
      select * from {{ source('staging', 'product_category') }}
),
renamed as (
    select
        {{ adapter.quote("string_field_0") }} as category_id,
        {{ adapter.quote("string_field_1") }} as product_category_name,
        {{ adapter.quote("string_field_2") }} as product_category

    from source
)
select * from renamed
  