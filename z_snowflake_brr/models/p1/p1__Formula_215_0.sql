{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Cleanse_214 AS (

  SELECT *
  
  FROM {{ ref('p1__Cleanse_214')}}

),

Formula_215_0 AS (

  {#VisualGroup: INPUTITEMMASTER#}
  SELECT 
    (
      CASE
        WHEN ((SKU_STANDARD IS NULL) OR ((LENGTH(SKU_STANDARD)) = 0))
          THEN SOURCE_SKU
        ELSE SKU_STANDARD
      END
    ) AS SKU_STANDARD,
    * EXCLUDE ("SKU_STANDARD")
  
  FROM Cleanse_214 AS in0

)

SELECT *

FROM Formula_215_0
