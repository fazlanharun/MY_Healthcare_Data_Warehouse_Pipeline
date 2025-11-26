
with source as (
    select * from {{ source('raw_api_dump', 'hospital_beds') }}
),

filtered as (
    select
    beds/2 AS beds, --to match total state with Malaysia
    date,
    type,
    state
    from source
    where 1=1
    and state not in ('Malaysia')
    and type in ('all')
)

select * from filtered

