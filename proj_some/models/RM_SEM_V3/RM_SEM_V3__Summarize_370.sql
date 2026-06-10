{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Database__LOADI_280 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_280') }}

),

Summarize_370 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((DPTR_DATE IS NULL) OR (CAST(DPTR_DATE AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    NDOD AS NDOD
  
  FROM Database__LOADI_280 AS in0
  
  GROUP BY NDOD

)

SELECT *

FROM Summarize_370
