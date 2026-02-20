{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_159_0 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Formula_159_0')}}

),

Union_166_reformat_1 AS (

  SELECT 
    Bucket AS Bucket,
    MBR_AGE_RANGE AS MBR_AGE_RANGE,
    CAST(MBR_INDV_BE_KEY AS STRING) AS MBR_INDV_BE_KEY,
    CAST(YearMonth AS STRING) AS YearMonth
  
  FROM Formula_159_0 AS in0

),

Join_146_inner_formula_to_Formula_148_1 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Join_146_inner_formula_to_Formula_148_1')}}

),

Union_166_reformat_0 AS (

  SELECT 
    Avg_Avg_TotalRVU AS Avg_Avg_TotalRVU,
    Avg_TotalRVU AS Avg_TotalRVU,
    Bucket AS Bucket,
    MBR_AGE_RANGE AS MBR_AGE_RANGE,
    CAST(MBR_INDV_BE_KEY AS STRING) AS MBR_INDV_BE_KEY,
    CAST(`Standardized RVU` AS FLOAT64) AS `Standardized RVU`,
    StdDev_Avg_TotalRVU AS StdDev_Avg_TotalRVU,
    CAST(YearMonth AS STRING) AS YearMonth
  
  FROM Join_146_inner_formula_to_Formula_148_1 AS in0

),

Union_166 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_166_reformat_0', 'Union_166_reformat_1'], 
      [
        '[{"name": "Bucket", "dataType": "String"}, {"name": "MBR_INDV_BE_KEY", "dataType": "Decimal"}, {"name": "YearMonth", "dataType": "Integer"}, {"name": "Avg_Avg_TotalRVU", "dataType": "Double"}, {"name": "StdDev_Avg_TotalRVU", "dataType": "Double"}, {"name": "Avg_TotalRVU", "dataType": "Double"}, {"name": "Standardized RVU", "dataType": "Decimal"}, {"name": "MBR_AGE_RANGE", "dataType": "String"}]', 
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "YearMonth", "dataType": "String"}, {"name": "MBR_AGE_RANGE", "dataType": "String"}, {"name": "Bucket", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_167 AS (

  SELECT 
    DISTINCT MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    YearMonth AS YearMonth,
    Bucket AS Bucket
  
  FROM Union_166 AS in0

),

Sort_168 AS (

  SELECT * 
  
  FROM Summarize_167 AS in0
  
  ORDER BY MBR_INDV_BE_KEY ASC, YearMonth ASC

),

Filter_189 AS (

  SELECT * 
  
  FROM Sort_168 AS in0
  
  WHERE (
          YearMonth = (
            coalesce(
              CAST((
                REGEXP_REPLACE(
                  (REGEXP_REPLACE((REGEXP_REPLACE((SUBSTRING(CAST(CAST(CURRENT_DATE AS STRING) AS STRING), 1, 7)), '-', '')), ',', '')), 
                  '.', 
                  '.')
              ) AS DOUBLE), 
              0)
          )
        )

)

SELECT *

FROM Filter_189
