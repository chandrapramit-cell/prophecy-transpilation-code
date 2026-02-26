{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_482 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Union_482')}}

),

DynamicInput_39 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_39') }}

),

Join_494_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Union_482 AS in0
  INNER JOIN DynamicInput_39 AS in1
     ON (in0.v_TZAC = in1.TZAC)

)

SELECT *

FROM Join_494_inner
