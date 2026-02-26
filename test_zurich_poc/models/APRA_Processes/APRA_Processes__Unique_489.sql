{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_482 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Union_482')}}

),

DynamicInput_464 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_464') }}

),

Join_477_left AS (

  SELECT in0.*
  
  FROM Union_482 AS in0
  ANTI JOIN DynamicInput_464 AS in1
     ON (in0.v_Reinsurer = in1.Reinsurer)

),

Filter_501 AS (

  SELECT * 
  
  FROM Join_477_left AS in0
  
  WHERE (
          NOT(
            (LENGTH(v_Reinsurer)) = 0)
        )

),

AlteryxSelect_490 AS (

  SELECT 
    v_Reinsurer AS v_Reinsurer,
    CAST(NULL AS STRING) AS Filename
  
  FROM Filter_501 AS in0

),

Unique_489 AS (

  SELECT * 
  
  FROM AlteryxSelect_490 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY v_Reinsurer ORDER BY v_Reinsurer) = 1

)

SELECT *

FROM Unique_489
