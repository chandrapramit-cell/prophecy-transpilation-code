{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Source_File__ex_1 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Source_File__ex_1_ref') }}

),

LockInFilter_128 AS (

  SELECT * 
  
  FROM Source_File__ex_1 AS in0
  
  WHERE (ItemPrimaryVendorID = '123')

)

SELECT *

FROM LockInFilter_128
