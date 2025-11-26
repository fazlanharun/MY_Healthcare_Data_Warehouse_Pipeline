{{ config(
    cluster_by=['state_id']
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_hospital_beds') }}
)


SELECT
    ds.state_id,
    EXTRACT(YEAR FROM date) AS year,
    SUM(beds) AS total_beds
 FROM source s
    JOIN {{ ref('dim_state') }} ds
      ON s.state = ds.state_name
    GROUP BY year, ds.state_id
