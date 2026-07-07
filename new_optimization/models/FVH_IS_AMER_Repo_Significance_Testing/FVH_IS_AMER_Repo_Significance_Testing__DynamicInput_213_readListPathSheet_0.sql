{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_204_1 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Formula_204_1')}}

),

DynamicInput_213_readListPathSheet_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((coalesce((CONTAINS(LOWER(Charges), LOWER('|||'))), FALSE)) AS BOOLEAN)
          THEN CAST((ELEMENT_AT((SPLIT(Charges, '[|||]')), 1)) AS string)
        ELSE Charges
      END
    ) AS string) AS file_path,
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (
              CASE
                WHEN CAST((coalesce((CONTAINS(LOWER(Charges), LOWER('|||'))), FALSE)) AS BOOLEAN)
                  THEN CAST((ELEMENT_AT((SPLIT(Charges, '[|||]')), 2)) AS string)
                ELSE 'Sheet1'
              END
            ), 
            '"', 
            '')
        ), 
        '$', 
        '')
    ) AS string) AS sheet_name,
    *
  
  FROM Formula_204_1 AS in0

)

SELECT *

FROM DynamicInput_213_readListPathSheet_0
