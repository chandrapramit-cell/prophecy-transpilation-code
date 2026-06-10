{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH selectsum_csv_1667 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('intervention', 'selectsum_csv_1667') }}

),

AlteryxSelect_860 AS (

  SELECT 
    CAST(MBR_INDV_BE_KEY AS string) AS MBR_INDV_BE_KEY,
    * EXCEPT (`MBR_INDV_BE_KEY`)
  
  FROM selectsum_csv_1667 AS in0

)

SELECT *

FROM AlteryxSelect_860
