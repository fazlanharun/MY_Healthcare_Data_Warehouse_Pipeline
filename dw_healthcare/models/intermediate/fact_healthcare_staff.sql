{{ config(
    cluster_by=['state_id']
) }}

WITH source AS (
    SELECT *
    FROM {{ ref('stg_healthcare_staff') }}
)

SELECT
    ds.state_id,
    EXTRACT(YEAR FROM date) AS year,
    SUM(CASE WHEN type = 'doctor' THEN staff ELSE 0 END) AS doctor_count,
    SUM(CASE WHEN type IN ('nurse', 'nurse_community') THEN staff ELSE 0 END) AS nurse_count
FROM source s
JOIN {{ ref('dim_state') }} ds
  ON s.state = ds.state_name
GROUP BY ds.state_id, year