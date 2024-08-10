with source as (
      select * from {{ source('staging', 'geolocation') }}
),
renamed as (
    select
        {{ adapter.quote("geolocation_zip_code_prefix") }},
        {{ adapter.quote("geolocation_lat") }},
        {{ adapter.quote("geolocation_lng") }},
        {{ adapter.quote("geolocation_city") }},
        {{ adapter.quote("geolocation_state") }}

    from source
)
select * from renamed
  