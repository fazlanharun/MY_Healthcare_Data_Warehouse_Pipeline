{{ config(
    cluster_by=['state_id']
) }}

with source AS (
    SELECT *
        FROM {{ ref('stg_population_state')}}
)

SELECT 
    ds.state_id,
    year,
    sex,
    age,
    ethnicity,
    population
FROM source
JOIN {{ref('dim_state')}}  ds
ON source.state = ds.state_name
