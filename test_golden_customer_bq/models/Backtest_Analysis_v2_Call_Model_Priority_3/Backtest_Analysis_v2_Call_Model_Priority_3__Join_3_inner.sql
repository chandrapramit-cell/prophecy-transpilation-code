{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH OverallTestFilt_90 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'OverallTestFilt_90_ref') }}

),

Join_3_inner_rightRecordPosition AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin`
  
  FROM OverallTestFilt_90

),

OverallSystemTe_1 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'OverallSystemTe_1_ref') }}

),

Join_3_inner_leftRecordPosition AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `recordPositionForJoin`
  
  FROM OverallSystemTe_1

),

Join_3_inner AS (

  SELECT 
    in0.* EXCEPT (`recordPositionForJoin`),
    in1.* EXCEPT (`recordPositionForJoin`)
  
  FROM Join_3_inner_leftRecordPosition AS in0
  INNER JOIN Join_3_inner_rightRecordPosition AS in1
     ON (in0.recordPositionForJoin = in1.recordPositionForJoin)

)

SELECT *

FROM Join_3_inner
