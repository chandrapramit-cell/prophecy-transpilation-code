{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH FieldInfo_15 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DQ_Macro', 'FieldInfo_15') }}

),

FieldInfo_14 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DQ_Macro', 'FieldInfo_14') }}

),

Join_16_inner AS (

  SELECT * 
  
  FROM FieldInfo_14 AS in0
  INNER JOIN FieldInfo_15 AS in1
     ON (in0.Name = in1.Name)

),

Formula_23_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST(((STRPOS((coalesce(LOWER(CAST(VARIABLETYPE AS string)), '')), LOWER('String'))) > 0) AS BOOLEAN)
          THEN 'String'
        WHEN CAST(((STRPOS((coalesce(LOWER(CAST(VARIABLETYPE AS string)), '')), LOWER('Int'))) > 0) AS BOOLEAN)
          THEN 'Integer'
        ELSE VARIABLETYPE
      END
    ) AS string) AS `Expected Group Type`,
    CAST((
      CASE
        WHEN CAST(((STRPOS((coalesce(LOWER(CAST(RIGHT_TYPE AS string)), '')), LOWER('String'))) > 0) AS BOOLEAN)
          THEN 'String'
        WHEN CAST(((STRPOS((coalesce(LOWER(CAST(RIGHT_TYPE AS string)), '')), LOWER('Int'))) > 0) AS BOOLEAN)
          THEN 'Integer'
        ELSE RIGHT_TYPE
      END
    ) AS string) AS `Actual Group Type`,
    *
  
  FROM Join_16_inner

),

Formula_23_1 AS (

  SELECT 
    (`Expected Group Type` = `Actual Group Type`) AS `Type Test`,
    *
  
  FROM Formula_23_0 AS in0

)

SELECT *

FROM Formula_23_1
