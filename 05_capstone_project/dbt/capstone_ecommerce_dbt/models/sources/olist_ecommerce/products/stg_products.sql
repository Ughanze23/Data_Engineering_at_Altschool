with source as (
      select * from {{ source('staging', 'products') }}
),
renamed as (
    select
        {{ adapter.quote("product_id") }},
        {{ adapter.quote("category_id") }},
        {{ adapter.quote("product_name_lenght") }} as product_name_length,
        {{ adapter.quote("product_description_lenght") }} as product_description_length,
        {{ adapter.quote("product_photos_qty") }},
        {{ adapter.quote("product_weight_g") }},
        {{ adapter.quote("product_length_cm") }},
        {{ adapter.quote("product_height_cm") }},
        {{ adapter.quote("product_width_cm") }}

    from source
)
select * from renamed
  