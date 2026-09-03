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

AlteryxSelect_3272 AS (

  SELECT *
  
  FROM Filter_3315 AS in0

),

Summarize_3295 AS (

  SELECT 
    COUNT(DISTINCT `Mas90 Customer Number`) AS `CountDistinct_Mas90 Customer Number`,
    `Expected Year Renewals` AS `Expected Year Renewals`,
    Stage AS Stage
  
  FROM AlteryxSelect_3272 AS in0
  
  GROUP BY 
    `Expected Year Renewals`, Stage

),

Formula_3296_0 AS (

  SELECT 
    CAST('Total' AS string) AS Product,
    *
  
  FROM Summarize_3295 AS in0

),

Summarize_3270 AS (

  SELECT 
    COUNT(DISTINCT `Mas90 Customer Number`) AS `CountDistinct_Mas90 Customer Number`,
    `Expected Year Renewals` AS `Expected Year Renewals`,
    Product AS Product,
    Stage AS Stage
  
  FROM AlteryxSelect_3272 AS in0
  
  GROUP BY 
    `Expected Year Renewals`, Product, Stage

),

Union_3297 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_3270', 'Formula_3296_0'], 
      [
        '[{"name": "Expected Year Renewals", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "CountDistinct_Mas90 Customer Number", "dataType": "Double"}]', 
        '[{"name": "Expected Year Renewals", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "CountDistinct_Mas90 Customer Number", "dataType": "Double"}, {"name": "Product", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

CrossTab_3267 AS (

  SELECT *
  
  FROM (
    SELECT 
      Product,
      Stage,
      `Expected Year Renewals`,
      `CountDistinct_Mas90 Customer Number`
    
    FROM Union_3297 AS in0
  )
  PIVOT (
    SUM(`CountDistinct_Mas90 Customer Number`) AS Sum
    FOR `Expected Year Renewals`
    IN (
      '2024', '2025', '2023', '2026'
    )
  )

)

SELECT *

FROM CrossTab_3267
