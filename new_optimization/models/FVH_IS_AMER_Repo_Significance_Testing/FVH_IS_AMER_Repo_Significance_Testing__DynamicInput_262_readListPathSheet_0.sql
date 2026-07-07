{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_235_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Formula_235_0')}}

),

DynamicInput_262_readListPathSheet_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((coalesce((CONTAINS(LOWER(Overrides), LOWER('|||'))), FALSE)) AS BOOLEAN)
          THEN CAST((ELEMENT_AT((SPLIT(Overrides, '[|||]')), 1)) AS string)
        ELSE Overrides
      END
    ) AS string) AS file_path,
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (
              CASE
                WHEN CAST((coalesce((CONTAINS(LOWER(Overrides), LOWER('|||'))), FALSE)) AS BOOLEAN)
                  THEN CAST((ELEMENT_AT((SPLIT(Overrides, '[|||]')), 2)) AS string)
                ELSE 'Overrides'
              END
            ), 
            '"', 
            '')
        ), 
        '$', 
        '')
    ) AS string) AS sheet_name,
    *
  
  FROM Formula_235_0 AS in0

)

SELECT *

FROM DynamicInput_262_readListPathSheet_0
