{{
  config({    
    "materialized": "table",
    "alias": "aka_GPD_UDDL_Wr_205",
    "database": "rohit",
    "schema": "default"
  })
}}

WITH Formula_207_0 AS (

  SELECT *
  
  FROM {{ ref('1_CRIS_Get_Source_Data__Formula_207_0')}}

)

SELECT *

FROM Formula_207_0
