
with source as (
    select * from {{ source('raw_api_dump', 'healthcare_staff') }}
),

filtered as (
    select
    date,
    type,
    staff,
    state
    from source
    where type in ('doctor', 'nurse', 'nurse_community')
    and state not in ('Malaysia')
)

select * from filtered