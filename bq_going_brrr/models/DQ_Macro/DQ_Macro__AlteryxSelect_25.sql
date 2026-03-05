{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_23_1 AS (

  SELECT *
  
  FROM {{ ref('DQ_Macro__Formula_23_1')}}

),

Filter_24_reject AS (

  SELECT * 
  
  FROM Formula_23_1 AS in0
  
  WHERE (NOT CAST(`Type Test` AS BOOLEAN))

),

AlteryxSelect_25 AS (

  SELECT 
    NAME AS `Expected Field Name`,
    VARIABLETYPE AS `Expected Type`,
    RIGHT_NAME AS `Actual Field Name`,
    RIGHT_TYPE AS `Actual Field Type`
  
  FROM Filter_24_reject AS in0

)

SELECT *

FROM AlteryxSelect_25
