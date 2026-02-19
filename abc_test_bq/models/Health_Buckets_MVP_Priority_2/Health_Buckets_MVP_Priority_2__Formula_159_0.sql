{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sample_143 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Sample_143')}}

),

Sample_142 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Sample_142')}}

),

Join_155_left AS (

  SELECT in0.*
  
  FROM Sample_142 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.MBR_INDV_BE_KEY
    
    FROM Sample_143 AS in1
    
    WHERE in1.MBR_INDV_BE_KEY IS NOT NULL
  ) AS in1_keys
     ON (in0.MBR_INDV_BE_KEY = in1_keys.MBR_INDV_BE_KEY)
  
  WHERE (in1_keys.MBR_INDV_BE_KEY IS NULL)

),

Summarize_156 AS (

  SELECT 
    DISTINCT MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    YearMonth AS YearMonth,
    MBR_AGE_RANGE AS MBR_AGE_RANGE
  
  FROM Join_155_left AS in0

),

Union_158_reformat_0 AS (

  SELECT 
    MBR_AGE_RANGE AS MBR_AGE_RANGE,
    CAST(MBR_INDV_BE_KEY AS STRING) AS MBR_INDV_BE_KEY,
    CAST(YearMonth AS STRING) AS YearMonth
  
  FROM Summarize_156 AS in0

),

Filter_99_reject AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Filter_99_reject')}}

),

Summarize_161 AS (

  SELECT 
    DISTINCT MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    YearMonth AS YearMonth,
    MBR_AGE_RANGE AS MBR_AGE_RANGE
  
  FROM Filter_99_reject AS in0

),

Union_158_reformat_1 AS (

  SELECT 
    MBR_AGE_RANGE AS MBR_AGE_RANGE,
    CAST(MBR_INDV_BE_KEY AS STRING) AS MBR_INDV_BE_KEY,
    CAST(YearMonth AS STRING) AS YearMonth
  
  FROM Summarize_161 AS in0

),

Union_158 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_158_reformat_0', 'Union_158_reformat_1'], 
      [
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "Decimal"}, {"name": "YearMonth", "dataType": "Integer"}, {"name": "MBR_AGE_RANGE", "dataType": "String"}]', 
        '[{"name": "MBR_INDV_BE_KEY", "dataType": "String"}, {"name": "YearMonth", "dataType": "String"}, {"name": "MBR_AGE_RANGE", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_160 AS (

  SELECT 
    DISTINCT MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    YearMonth AS YearMonth,
    MBR_AGE_RANGE AS MBR_AGE_RANGE
  
  FROM Union_158 AS in0

),

Formula_159_0 AS (

  SELECT 
    '5. Unknown' AS Bucket,
    *
  
  FROM Summarize_160 AS in0

)

SELECT *

FROM Formula_159_0
