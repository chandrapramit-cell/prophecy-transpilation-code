{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH SamtecFacilitie_179 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('perpetual_inventory', 'SamtecFacilitie_179') }}

),

AlteryxSelect_173 AS (

  SELECT 
    VALUE AS Facility,
    * EXCEPT (`NAME`, `VALUE`)
  
  FROM SamtecFacilitie_179 AS in0

),

Filter_12 AS (

  SELECT * 
  
  FROM AlteryxSelect_173 AS in0
  
  WHERE {{ var('variable12_Expression') }}

),

AlteryxSelect_23 AS (

  SELECT Facility AS Facility
  
  FROM Filter_12 AS in0

)

SELECT *

FROM AlteryxSelect_23
