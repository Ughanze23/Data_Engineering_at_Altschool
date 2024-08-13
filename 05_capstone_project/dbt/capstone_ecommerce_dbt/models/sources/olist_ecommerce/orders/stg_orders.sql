with source as (
      select * from {{ source('staging', 'orders') }}
),
renamed as (
    select
        {{ adapter.quote("order_id") }},
        {{ adapter.quote("customer_id") }} as customer_order_id,
        {{ adapter.quote("order_status") }},
        {{ adapter.quote("order_purchase_timestamp") }},
        {{ adapter.quote("order_approved_at") }},
        {{ adapter.quote("order_delivered_carrier_date") }},
        {{ adapter.quote("order_delivered_customer_date") }},
        {{ adapter.quote("order_estimated_delivery_date") }}

    from source
)
select * from renamed
  