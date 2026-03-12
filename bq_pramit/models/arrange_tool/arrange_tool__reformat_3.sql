{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Table_1 AS (

  SELECT * 
  
  FROM {{ ref('array')}}

),

reformat_2 AS (

  SELECT 
    SPLIT(nums, ',') AS arrr_col,
    *
  
  FROM Table_1 AS in0

),

reformat_3 AS (

  SELECT ARRAY_REDUCE(
           arrr_col, 
           "start", 
           (acc, col) -> CONCAT(acc, col))
  
  FROM reformat_2 AS in0

)

SELECT *

FROM reformat_3
