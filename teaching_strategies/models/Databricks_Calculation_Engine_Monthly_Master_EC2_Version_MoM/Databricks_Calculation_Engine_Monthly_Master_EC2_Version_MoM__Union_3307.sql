{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH DynamicRename_3277 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'DynamicRename_3277'
    )
  }}

),

DynamicRename_3273 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'DynamicRename_3273'
    )
  }}

),

Join_3268_right_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.Stage = in1.Stage) AND (in0.Product = in1.Product))
          THEN NULL
        ELSE in1.Stage
      END
    ) AS Stage,
    (
      CASE
        WHEN ((in0.Stage = in1.Stage) AND (in0.Product = in1.Product))
          THEN NULL
        ELSE in1.Product
      END
    ) AS Product,
    in0.* EXCEPT (`Stage`, `Product`),
    in1.* EXCEPT (`Product`, `Stage`)
  
  FROM DynamicRename_3273 AS in0
  RIGHT JOIN DynamicRename_3277 AS in1
     ON ((in0.Stage = in1.Stage) AND (in0.Product = in1.Product))

),

Filter_3269 AS (

  SELECT * 
  
  FROM Join_3268_right_UnionRightOuter AS in0
  
  WHERE (NOT(Stage IS NULL))

),

Transpose_3281_cast_to_string AS (

  SELECT 
    CAST(`2023 Amount` AS STRING) AS `2023 Amount`,
    CAST(`2023 Count` AS STRING) AS `2023 Count`,
    CAST(`2024 Amount` AS STRING) AS `2024 Amount`,
    CAST(`2024 Count` AS STRING) AS `2024 Count`,
    CAST(`2025 Amount` AS STRING) AS `2025 Amount`,
    CAST(`2025 Count` AS STRING) AS `2025 Count`,
    CAST(`2026 Amount` AS STRING) AS `2026 Amount`,
    CAST(`2026 Count` AS STRING) AS `2026 Count`,
    Product AS Product,
    Stage AS Stage
  
  FROM Filter_3269 AS in0

),

Transpose_3281 AS (

  SELECT 
    Product,
    Stage,
    Name,
    Value
  
  FROM Transpose_3281_cast_to_string AS in0
  UNPIVOT INCLUDE NULLS (
    Value
    FOR Name IN (
      `2023 Amount`, 
      `2024 Amount`, 
      `2026 Amount`, 
      `2026 Count`, 
      `2025 Amount`, 
      `2025 Count`, 
      `2023 Count`, 
      `2024 Count`
    )
  )

),

Formula_3282 AS (

  SELECT *
  
  FROM Transpose_3281 AS in0

),

AlteryxSelect_3283 AS (

  SELECT *
  
  FROM Formula_3282 AS in0

),

CrossTab_3284_sanitize_0 AS (

  SELECT 
    (REGEXP_REPLACE(Name, '[\\s!@#$%^&*(),.?":{}|<>\\[\\]=;/\\-+]', '_')) AS Name,
    * EXCEPT (`name`)
  
  FROM AlteryxSelect_3283 AS in0

),

CrossTab_3284 AS (

  SELECT *
  
  FROM (
    SELECT 
      Product,
      Stage,
      Name,
      VALUE
    
    FROM CrossTab_3284_sanitize_0 AS in0
  )
  PIVOT (
    SUM(VALUE) AS Sum
    FOR Name
    IN (
      '2023_Amount', 
      '2023_Count', 
      '2024_Amount', 
      '2026_Count', 
      '2026_Amount', 
      '2025_Amount', 
      '2025_Count', 
      '2024_Count'
    )
  )

),

Transpose_3285_cast_to_string AS (

  SELECT 
    CAST(`2023_Amount` AS STRING) AS `2023_Amount`,
    CAST(`2023_Count` AS STRING) AS `2023_Count`,
    CAST(`2024_Amount` AS STRING) AS `2024_Amount`,
    CAST(`2024_Count` AS STRING) AS `2024_Count`,
    CAST(`2025_Amount` AS STRING) AS `2025_Amount`,
    CAST(`2025_Count` AS STRING) AS `2025_Count`,
    CAST(`2026_Amount` AS STRING) AS `2026_Amount`,
    CAST(`2026_Count` AS STRING) AS `2026_Count`,
    CAST(variableDate AS string) AS variableDate,
    Product AS Product,
    Stage AS Stage
  
  FROM CrossTab_3284 AS in0

),

Transpose_3285 AS (

  SELECT 
    Product,
    Stage,
    variableDate,
    Name,
    Value
  
  FROM Transpose_3285_cast_to_string AS in0
  UNPIVOT INCLUDE NULLS (
    Value
    FOR Name IN (
      variableDate, 
      `2023_Amount`, 
      `2023_Count`, 
      `2024_Amount`, 
      `2024_Count`, 
      `2026_Count`, 
      `2026_Amount`, 
      `2025_Amount`, 
      `2025_Count`
    )
  )

),

Filter_3286 AS (

  SELECT * 
  
  FROM Transpose_3285 AS in0
  
  WHERE not(isnull(Value))

),

Formula_3287_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((coalesce((CONTAINS(LOWER(Name), LOWER('Amount'))), FALSE)) AS BOOLEAN)
          THEN 'ARR Amount'
        WHEN CAST((coalesce((CONTAINS(LOWER(Name), LOWER('Count'))), FALSE)) AS BOOLEAN)
          THEN 'Count'
        ELSE ''
      END
    ) AS string) AS `New Name`,
    *
  
  FROM Filter_3286 AS in0

),

AlteryxSelect_3294 AS (

  SELECT 
    CAST(Name AS string) AS Name,
    CAST(Value AS DOUBLE) AS `Value`,
    * EXCEPT (`Name`, `Value`)
  
  FROM Formula_3287_0 AS in0

),

CrossTab_3289_sanitize_0 AS (

  SELECT 
    (REGEXP_REPLACE(`New Name`, '[\\s!@#$%^&*(),.?":{}|<>\\[\\]=;/\\-+]', '_')) AS `New Name`,
    * EXCEPT (`new name`)
  
  FROM AlteryxSelect_3294 AS in0

),

CrossTab_3289 AS (

  SELECT *
  
  FROM (
    SELECT 
      Product,
      Stage,
      Date,
      `New Name`,
      VALUE
    
    FROM CrossTab_3289_sanitize_0 AS in0
  )
  PIVOT (
    SUM(VALUE) AS Sum
    FOR `New Name`
    IN (
      'Count', 'ARR_Amount'
    )
  )

),

AlteryxSelect_3288 AS (

  SELECT 
    ARR_Amount AS `ARR Amount`,
    * EXCEPT (`ARR_Amount`)
  
  FROM CrossTab_3289 AS in0

),

Summarize_3303 AS (

  SELECT 
    SUM(`ARR Amount`) AS `ARR Amount`,
    SUM(Count) AS `Count`,
    Product AS Product,
    variableDate AS variableDate
  
  FROM AlteryxSelect_3288 AS in0
  
  GROUP BY 
    Product, variableDate

),

Formula_3327_0 AS (

  SELECT 
    CAST('Total' AS string) AS Stage,
    *
  
  FROM Summarize_3303 AS in0

),

Union_3326 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_3288', 'Formula_3327_0'], 
      [
        '[{"name": "variableDate", "dataType": "Date"}, {"name": "ARR Amount", "dataType": "Double"}, {"name": "Count", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}]', 
        '[{"name": "variableDate", "dataType": "Date"}, {"name": "ARR Amount", "dataType": "Double"}, {"name": "Count", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_3265 AS (

  SELECT * 
  
  FROM Union_3326 AS in0
  
  WHERE (NOT (isnull(`ARR Amount`)) OR NOT (isnull(Count)))

),

TextInput_3324 AS (

  SELECT * 
  
  FROM {{ ref('seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3324')}}

),

TextInput_3324_cast AS (

  SELECT 
    CAST(Product AS string) AS Product,
    CAST(`Clean Product` AS string) AS `Clean Product`
  
  FROM TextInput_3324 AS in0

),

AlteryxSelect_3306 AS (

  SELECT 
    Stage AS Stage,
    Product AS Product,
    variableDate AS variableDate,
    `ARR Amount` AS `ARR Amount`,
    Count AS `Count`,
    * EXCEPT (`Stage`, `Product`, `variableDate`, `ARR Amount`, `Count`)
  
  FROM Filter_3265 AS in0

),

Join_3264_inner AS (

  SELECT 
    in1.`Clean Product` AS Product,
    in0.variableDate AS variableDate,
    in0.Stage AS Stage,
    in0.`ARR Amount` AS `ARR Amount`,
    in0.Count AS `Count`,
    in0.* EXCEPT (`Product`, `variableDate`, `Stage`, `ARR Amount`, `Count`),
    in1.* EXCEPT (`Clean Product`, `Product`)
  
  FROM AlteryxSelect_3306 AS in0
  INNER JOIN TextInput_3324_cast AS in1
     ON (in0.Product = in1.Product)

),

Filter_3325 AS (

  SELECT * 
  
  FROM Join_3264_inner AS in0
  
  WHERE (variableDate > to_date('2023-11-30'))

),

HistoricalYetTo_3323 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'HistoricalYetTo_3323'
    )
  }}

),

Join_3320_inner AS (

  SELECT 
    in0.variableDate AS variableDate,
    in1.`Clean Product` AS Product,
    in0.Stage AS Stage,
    in0.`ARR Amount` AS `ARR Amount`,
    in0.Count AS `Count`,
    in0.* EXCEPT (`Product`, `variableDate`, `Stage`, `ARR Amount`, `Count`),
    in1.* EXCEPT (`Product`, `Clean Product`)
  
  FROM HistoricalYetTo_3323 AS in0
  INNER JOIN TextInput_3324_cast AS in1
     ON (in0.Product = in1.Product)

),

Formula_3319_0 AS (

  SELECT 
    CAST((LAST_DAY(CAST((TO_TIMESTAMP((REGEXP_REPLACE(variableDate, '\\.\\d+', '')), 'MMM-yyyy')) AS DATE))) AS string) AS `Clean Date`,
    *
  
  FROM Join_3320_inner AS in0

),

AlteryxSelect_3318 AS (

  SELECT 
    Product AS Product,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Clean Date` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Clean Date` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Clean Date` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Clean Date` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Clean Date` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS variableDate,
    Stage AS Stage,
    `ARR Amount` AS `ARR Amount`,
    Count AS `Count`,
    * EXCEPT (`variableDate`, `Product`, `Stage`, `ARR Amount`, `Count`, `Clean Date`)
  
  FROM Formula_3319_0 AS in0

),

Formula_3317_0 AS (

  SELECT 
    CAST('Current' AS string) AS Origin,
    *
  
  FROM Filter_3325 AS in0

),

Formula_3322_0 AS (

  SELECT 
    CAST('Historical' AS string) AS Origin,
    *
  
  FROM AlteryxSelect_3318 AS in0

),

Union_3307 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_3317_0', 'Formula_3322_0'], 
      [
        '[{"name": "variableDate", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ARR Amount", "dataType": "Double"}, {"name": "Count", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}]', 
        '[{"name": "variableDate", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ARR Amount", "dataType": "Double"}, {"name": "Count", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_3307
