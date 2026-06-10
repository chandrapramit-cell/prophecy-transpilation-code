{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH TextInput_26 AS (

  SELECT * 
  
  FROM {{ ref('seed_perpetual_inventory_details_26')}}

),

TextInput_26_cast AS (

  SELECT 
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(StartDate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(StartDate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(StartDate AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(StartDate AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(StartDate AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS StartDate,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(EndDate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(EndDate AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(EndDate AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(EndDate AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(EndDate AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS EndDate
  
  FROM TextInput_26 AS in0

),

Formula_27_0 AS (

  SELECT 
    (DATE_ADD(EndDate, CAST(1 AS INTEGER))) AS EndDate,
    * EXCEPT (`enddate`)
  
  FROM TextInput_26_cast AS in0

)

SELECT *

FROM Formula_27_0
