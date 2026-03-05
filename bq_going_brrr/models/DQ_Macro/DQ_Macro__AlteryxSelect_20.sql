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

Join_16_right AS (

  SELECT * 
  
  FROM FieldInfo_14 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM FieldInfo_15 AS in1
    
    WHERE in1.Name IS NOT NULL
  ) AS in1_keys
     ON (in0.Name = in1_keys.Name)
  
  WHERE (in1_keys.Name IS NULL)

),

AlteryxSelect_20 AS (

  SELECT 
    NAME AS `Extra Field`,
    VARIABLETYPE AS `Extra Field Type`
  
  FROM Join_16_right AS in0

)

SELECT *

FROM AlteryxSelect_20
