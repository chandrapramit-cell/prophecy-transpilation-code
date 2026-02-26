{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH DynamicInput_39 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_39') }}

),

Union_482 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Union_482')}}

),

Join_494_left AS (

  SELECT in0.*
  
  FROM Union_482 AS in0
  ANTI JOIN DynamicInput_39 AS in1
     ON (in0.v_TZAC = in1.TZAC)

),

Filter_500 AS (

  SELECT * 
  
  FROM Join_494_left AS in0
  
  WHERE (
          NOT(
            (LENGTH(v_TZAC)) = 0)
        )

),

AlteryxSelect_496 AS (

  SELECT 
    v_TZAC AS v_TZAC,
    CAST(NULL AS STRING) AS Filename
  
  FROM Filter_500 AS in0

),

Unique_495 AS (

  SELECT * 
  
  FROM AlteryxSelect_496 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY v_TZAC ORDER BY v_TZAC) = 1

)

SELECT *

FROM Unique_495
