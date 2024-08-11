with source as (
      select * from {{ source('staging', 'order_items') }}
),
renamed as (
    select
        {{ adapter.quote("order_id") }},
        {{ adapter.quote("order_item_id") }},
        {{ adapter.quote("product_id") }},
        {{ adapter.quote("seller_id") }},
        {{ adapter.quote("shipping_limit_date") }},
        {{ adapter.quote("price") }},
        {{ adapter.quote("freight_value") }}

    from source
)
select * from renamed
  