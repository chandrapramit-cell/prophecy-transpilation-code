{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Filter_3315 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_3315')}}

),

Summarize_3298 AS (

  SELECT 
    SUM(ARR) AS ARR,
    `Expected Year Renewals` AS `Expected Year Renewals`,
    Stage AS Stage
  
  FROM Filter_3315 AS in0
  
  GROUP BY 
    `Expected Year Renewals`, Stage

),

Formula_3299_0 AS (

  SELECT 
    CAST('Total' AS string) AS Product,
    *
  
  FROM Summarize_3298 AS in0

),

Summarize_3301 AS (

  SELECT 
    SUM(ARR) AS ARR,
    `Expected Year Renewals` AS `Expected Year Renewals`,
    Product AS Product,
    Stage AS Stage
  
  FROM Filter_3315 AS in0
  
  GROUP BY 
    `Expected Year Renewals`, Product, Stage

),

Union_3302 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_3299_0', 'Summarize_3301'], 
      [
        '[{"name": "Expected Year Renewals", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "ARR", "dataType": "Double"}, {"name": "Product", "dataType": "String"}]', 
        '[{"name": "Expected Year Renewals", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "ARR", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

CrossTab_3266 AS (

  SELECT *
  
  FROM (
    SELECT 
      Stage,
      Product,
      `Expected Year Renewals`,
      ARR
    
    FROM Union_3302 AS in0
  )
  PIVOT (
    SUM(ARR) AS Sum
    FOR `Expected Year Renewals`
    IN (
      '2024', '2025', '2023', '2026'
    )
  )

)

SELECT *

FROM CrossTab_3266
