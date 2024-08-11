with source as (
      select * from {{ source('staging', 'order_payments') }}
),
renamed as (
    select
        {{ adapter.quote("order_id") }},
        {{ adapter.quote("payment_sequential") }},
        {{ adapter.quote("payment_type") }},
        {{ adapter.quote("payment_installments") }},
        {{ adapter.quote("payment_value") }}

    from source
)
select * from renamed
  