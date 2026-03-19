{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Database__P0000_204 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('1_CRIS_Get_Source_Data', 'Database__P0000_204') }}

),

Formula_207_0 AS (

  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS LOADED_DATE,
    *
  
  FROM Database__P0000_204 AS in0

)

SELECT *

FROM Formula_207_0
