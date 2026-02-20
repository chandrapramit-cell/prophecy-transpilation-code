{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_146_inner_formula_to_Formula_148_1 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Join_146_inner_formula_to_Formula_148_1')}}

),

Summarize_149 AS (

  SELECT 
    COUNT(DISTINCT MBR_INDV_BE_KEY) AS CountDistinct_MBR_INDV_BE_KEY,
    MBR_AGE_RANGE AS MBR_AGE_RANGE,
    YearMonth AS YearMonth,
    Bucket AS Bucket
  
  FROM Join_146_inner_formula_to_Formula_148_1 AS in0
  
  GROUP BY 
    MBR_AGE_RANGE, YearMonth, Bucket

),

Sort_150 AS (

  SELECT * 
  
  FROM Summarize_149 AS in0
  
  ORDER BY YearMonth ASC, MBR_AGE_RANGE ASC, Bucket ASC

),

Union_163_reformat_0 AS (

  SELECT 
    Bucket AS Bucket,
    CountDistinct_MBR_INDV_BE_KEY AS CountDistinct_MBR_INDV_BE_KEY,
    MBR_AGE_RANGE AS MBR_AGE_RANGE,
    CAST(YearMonth AS STRING) AS YearMonth
  
  FROM Sort_150 AS in0

),

Formula_159_0 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Formula_159_0')}}

),

Summarize_164 AS (

  SELECT 
    COUNT(DISTINCT MBR_INDV_BE_KEY) AS CountDistinct_MBR_INDV_BE_KEY,
    YearMonth AS YearMonth,
    MBR_AGE_RANGE AS MBR_AGE_RANGE,
    Bucket AS Bucket
  
  FROM Formula_159_0 AS in0
  
  GROUP BY 
    YearMonth, MBR_AGE_RANGE, Bucket

),

Union_163_reformat_1 AS (

  SELECT 
    Bucket AS Bucket,
    CountDistinct_MBR_INDV_BE_KEY AS CountDistinct_MBR_INDV_BE_KEY,
    MBR_AGE_RANGE AS MBR_AGE_RANGE,
    CAST(YearMonth AS STRING) AS YearMonth
  
  FROM Summarize_164 AS in0

),

Union_163 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_163_reformat_0', 'Union_163_reformat_1'], 
      [
        '[{"name": "MBR_AGE_RANGE", "dataType": "String"}, {"name": "YearMonth", "dataType": "Integer"}, {"name": "Bucket", "dataType": "String"}, {"name": "CountDistinct_MBR_INDV_BE_KEY", "dataType": "Double"}]', 
        '[{"name": "YearMonth", "dataType": "String"}, {"name": "MBR_AGE_RANGE", "dataType": "String"}, {"name": "Bucket", "dataType": "String"}, {"name": "CountDistinct_MBR_INDV_BE_KEY", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

CrossTab_151 AS (

  SELECT *
  
  FROM (
    SELECT 
      MBR_AGE_RANGE,
      YearMonth,
      Bucket,
      COUNTDISTINCT_MBR_INDV_BE_KEY
    
    FROM Union_163 AS in0
  )
  PIVOT (
    SUM(COUNTDISTINCT_MBR_INDV_BE_KEY) AS Sum
    FOR Bucket
    IN (
      '4__Very_High', '2__Moderate', '5__Unknown', '1__Low', '3__High'
    )
  )

)

SELECT *

FROM CrossTab_151
