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

DynamicInput_221_readListPathSheet_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((coalesce((CONTAINS(LOWER(PLATO_UCN_Data), LOWER('|||'))), FALSE)) AS BOOLEAN)
          THEN CAST((ELEMENT_AT((SPLIT(PLATO_UCN_Data, '[|||]')), 1)) AS string)
        ELSE PLATO_UCN_Data
      END
    ) AS string) AS file_path,
    CAST((
      REGEXP_REPLACE(
        (
          REGEXP_REPLACE(
            (
              CASE
                WHEN CAST((coalesce((CONTAINS(LOWER(PLATO_UCN_Data), LOWER('|||'))), FALSE)) AS BOOLEAN)
                  THEN CAST((ELEMENT_AT((SPLIT(PLATO_UCN_Data, '[|||]')), 2)) AS string)
                ELSE 'PLATO_UCN_Data'
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

FROM DynamicInput_221_readListPathSheet_0
