{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH AlteryxSelect_338 AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__AlteryxSelect_338')}}

),

Summarize_367 AS (

  SELECT 
    COUNT((
      CASE
        WHEN ((OD IS NULL) OR (CAST(OD AS string) = ''))
          THEN NULL
        ELSE 1
      END
    )) AS `Count`,
    Support AS Support,
    variableTYPE AS variableTYPE
  
  FROM AlteryxSelect_338 AS in0
  
  GROUP BY 
    Support, variableTYPE

)

SELECT *

FROM Summarize_367
