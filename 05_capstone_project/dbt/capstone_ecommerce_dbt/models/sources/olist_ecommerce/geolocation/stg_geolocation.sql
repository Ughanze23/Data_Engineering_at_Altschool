with source as (
      select * from {{ source('staging', 'geolocation') }}
),
renamed as (
    select
        {{ adapter.quote("geolocation_zip_code_prefix") }} as zip_code,
        {{ adapter.quote("geolocation_lat") }} as lat,
        {{ adapter.quote("geolocation_lng") }} as lng,
        {{ adapter.quote("geolocation_city") }} as city,
        {{ adapter.quote("geolocation_state") }} as state

    from source
)
select * from renamed
  