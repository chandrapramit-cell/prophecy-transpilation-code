{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_89 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Configuration_t_89_ref') }}

),

AlteryxSelect_226 AS (

  SELECT 
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(variableDate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(variableDate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(variableDate AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(variableDate AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(variableDate AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS variableDate,
    * EXCEPT (`variableDate`)
  
  FROM Configuration_t_89 AS in0

)

SELECT *

FROM AlteryxSelect_226
