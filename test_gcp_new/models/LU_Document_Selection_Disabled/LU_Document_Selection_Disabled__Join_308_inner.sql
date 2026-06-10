{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH aka_GPDIP_EDLUD_298 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Document_Selection_Disabled', 'aka_GPDIP_EDLUD_298') }}

),

aka_GPDIP_EDLUD_307 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Document_Selection_Disabled', 'aka_GPDIP_EDLUD_307') }}

),

Join_308_inner AS (

  SELECT 
    in0.sub_country_id AS pfleet_subcountry_row_id,
    in1.r_object_id AS r_object_id
  
  FROM aka_GPDIP_EDLUD_307 AS in0
  INNER JOIN aka_GPDIP_EDLUD_298 AS in1
     ON (in0.i_chronicle_id = in1.i_chronicle_id)

)

SELECT *

FROM Join_308_inner
