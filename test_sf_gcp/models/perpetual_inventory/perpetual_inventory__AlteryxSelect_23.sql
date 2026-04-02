{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH SamtecFacilitie_179 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'SamtecFacilitie_179') }}

),

AlteryxSelect_173 AS (

  SELECT VALUE AS FACILITY
  
  FROM SamtecFacilitie_179 AS in0

),

Filter_12 AS (

  SELECT * 
  
  FROM AlteryxSelect_173 AS in0
  
  WHERE (FACILITY = 'SAMTEC USA')

),

AlteryxSelect_23 AS (

  SELECT FACILITY AS FACILITY
  
  FROM Filter_12 AS in0

)

SELECT *

FROM AlteryxSelect_23
