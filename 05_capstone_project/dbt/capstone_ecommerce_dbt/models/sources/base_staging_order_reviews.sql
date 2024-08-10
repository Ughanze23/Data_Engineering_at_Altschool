with source as (
      select * from {{ source('staging', 'order_reviews') }}
),
renamed as (
    select
        {{ adapter.quote("order_id") }},
        {{ adapter.quote("review_creation_date") }},
        {{ adapter.quote("review_comment_title") }},
        {{ adapter.quote("review_score") }},
        {{ adapter.quote("review_id") }},
        {{ adapter.quote("review_comment_message") }},
        {{ adapter.quote("review_answer_timestamp") }}

    from source
)
select * from renamed
  