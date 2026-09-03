{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_3233 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3233')}}

),

AlteryxSelect_3256 AS (

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
    CAST(Revenue AS DOUBLE) AS Revenue,
    * EXCEPT (`variableDate`, `Revenue`)
  
  FROM AlteryxSelect_3233 AS in0

)

SELECT *

FROM AlteryxSelect_3256
