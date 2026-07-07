{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_204_1 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testingneww__Formula_204_1')}}

),

DynamicInput_212_readListPathSheet_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((coalesce((CONTAINS(LOWER(CurveMap), LOWER('|||'))), FALSE)) AS BOOLEAN)
          THEN CAST((ELEMENT_AT((SPLIT(CurveMap, '[|||]')), 1)) AS string)
        ELSE CurveMap
      END
    ) AS string) AS file_path,
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (
              CASE
                WHEN CAST((coalesce((CONTAINS(LOWER(CurveMap), LOWER('|||'))), FALSE)) AS BOOLEAN)
                  THEN CAST((ELEMENT_AT((SPLIT(CurveMap, '[|||]')), 2)) AS string)
                ELSE 'CurveMap'
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

FROM DynamicInput_212_readListPathSheet_0
