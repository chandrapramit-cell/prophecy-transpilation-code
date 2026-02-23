{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Database__repor_1 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_1_ref') }}

),

Filter_109_reject AS (

  SELECT * 
  
  FROM Database__repor_1 AS in0
  
  WHERE (
          (
            NOT(
              ((PARSE_DATE('%Y-%m-%d', DT)) > (PARSE_DATE('%Y-%m-%d', (DATE_ADD(CAST(CURRENT_DATE AS STRING), INTERVAL CAST(-98 AS INTEGER) DAY)))))
              OR ((PARSE_DATE('%Y-%m-%d', DT)) >= (PARSE_DATE('%Y-%m-%d', (DATE_ADD(DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL CAST(0 AS INTEGER) DAY))))))
          )
          OR (
               (
                 ((PARSE_DATE('%Y-%m-%d', DT)) > (PARSE_DATE('%Y-%m-%d', (DATE_ADD(CAST(CURRENT_DATE AS STRING), INTERVAL CAST(-98 AS INTEGER) DAY)))))
                 OR ((PARSE_DATE('%Y-%m-%d', DT)) >= (PARSE_DATE('%Y-%m-%d', (DATE_ADD(DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL CAST(0 AS INTEGER) DAY)))))
               ) IS NULL
             )
        )

),

Formula_111_0 AS (

  SELECT 
    (PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', (DATE_ADD(DT, INTERVAL 12 MONTH)))) AS DT,
    * EXCEPT (`dt`)
  
  FROM Filter_109_reject AS in0

),

Formula_111_1 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(DT, MONTH)))) AS MONTH,
    *
  
  FROM Formula_111_0 AS in0

),

Summarize_110 AS (

  SELECT 
    (SUM(ANC_REV) OVER (PARTITION BY KPI, MONTH ORDER BY 1 NULLS FIRST)) AS MTH_ANC_REV,
    *
  
  FROM Formula_111_1 AS in0

),

Join_113_inner_formula_to_Formula_114_0 AS (

  select *  REPLACE( MONTH as `MONTH` ,  DT as `DT` ,  KPI as `KPI` ,  ANC_REV as `ANC_REV` ,  MTH_ANC_REV as `MTH_ANC_REV` ) from Summarize_110

),

Join_113_inner_formula_to_Formula_114_1 AS (

  SELECT 
    CAST((CAST(ANC_REV AS NUMERIC) / CAST(MTH_ANC_REV AS NUMERIC)) AS FLOAT64) AS ANC_PCT,
    *
  
  FROM Join_113_inner_formula_to_Formula_114_0 AS in0

),

CrossTab_115_0 AS (

  SELECT 
    (
      CASE
        WHEN (KPI IS NULL)
          THEN '_Null_'
        ELSE KPI
      END
    ) AS KPI,
    * EXCEPT (`kpi`)
  
  FROM Join_113_inner_formula_to_Formula_114_1 AS in0

),

CrossTab_115_1 AS (

  SELECT 
    (REGEXP_REPLACE(KPI, '[\\s!@#$%^&*(),.?":{}|<>\\[\\]=;/\\-+]', '_')) AS KPI,
    * EXCEPT (`kpi`)
  
  FROM CrossTab_115_0 AS in0

)

SELECT *

FROM CrossTab_115_1
