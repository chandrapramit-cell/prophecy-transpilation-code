{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Source_File__ex_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('4PL_Inventory_Report_1_', 'Source_File__ex_1') }}

),

LockInFilter_128 AS (

  SELECT * 
  
  FROM Source_File__ex_1 AS in0
  
  WHERE (ITEMPRIMARYVENDORID = '123')

)

SELECT *

FROM LockInFilter_128
