
with source as (
    select * from {{ source('raw_api_dump', 'population_state') }}
),

filtered as (
    select
    extract(year from date) AS year,
    state,
    sex,
    age,
    ethnicity,
    population * 1000 as population,
    from source
    where 1=1
    and state not in ('Malaysia')
    and sex not in ('both')
    and ethnicity not in ('overall','bumi','other')
    and age not in ('overall')
    and extract(year from date) > 1990
)

select * from filtered

