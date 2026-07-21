{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Filter_221 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Filter_221')}}

),

Summarize_239 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((latitude IS NULL) OR (CAST(latitude AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS Count_lat,
    COUNT(
      (
        CASE
          WHEN ((longitude IS NULL) OR (CAST(longitude AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS Count_long,
    latitude1 AS latitude1,
    longitude1 AS longitude1
  
  FROM Filter_221 AS in0
  
  GROUP BY 
    latitude1, longitude1

)

SELECT *

FROM Summarize_239
