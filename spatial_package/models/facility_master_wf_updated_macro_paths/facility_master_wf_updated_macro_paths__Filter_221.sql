{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH MultiFieldFormula_399 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__MultiFieldFormula_399')}}

),

AlteryxSelect_53 AS (

  SELECT 
    contact_id AS contact_id,
    city_town_village AS city_town_village,
    latitude1 AS latitude1,
    longitude1 AS longitude1,
    latitude AS latitude,
    longitude AS longitude,
    org_name AS org_name
  
  FROM MultiFieldFormula_399 AS in0

),

Filter_221 AS (

  SELECT * 
  
  FROM AlteryxSelect_53 AS in0
  
  WHERE (NOT(latitude IS NULL))

)

SELECT *

FROM Filter_221
