{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH selectsum_csv_1667 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'selectsum_csv_1667_ref') }}

),

AlteryxSelect_860 AS (

  SELECT 
    CAST(MBR_INDV_BE_KEY AS STRING) AS MBR_INDV_BE_KEY,
    * EXCEPT (`MBR_INDV_BE_KEY`)
  
  FROM selectsum_csv_1667 AS in0

)

SELECT *

FROM AlteryxSelect_860
