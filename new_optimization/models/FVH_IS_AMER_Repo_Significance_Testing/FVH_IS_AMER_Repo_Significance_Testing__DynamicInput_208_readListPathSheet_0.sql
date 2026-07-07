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

DynamicInput_208_readListPathSheet_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((coalesce((CONTAINS(LOWER(`Fvo trades vcg`), LOWER('|||'))), FALSE)) AS BOOLEAN)
          THEN CAST((ELEMENT_AT((SPLIT(`Fvo trades vcg`, '[|||]')), 1)) AS string)
        ELSE `Fvo trades vcg`
      END
    ) AS string) AS file_path,
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (
              CASE
                WHEN CAST((coalesce((CONTAINS(LOWER(`Fvo trades vcg`), LOWER('|||'))), FALSE)) AS BOOLEAN)
                  THEN CAST((ELEMENT_AT((SPLIT(`Fvo trades vcg`, '[|||]')), 2)) AS string)
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

FROM DynamicInput_208_readListPathSheet_0
