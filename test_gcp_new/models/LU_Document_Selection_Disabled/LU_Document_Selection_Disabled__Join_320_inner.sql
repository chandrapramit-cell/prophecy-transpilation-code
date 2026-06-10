{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH aka_GPDIP_EDLUD_312 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Document_Selection_Disabled', 'aka_GPDIP_EDLUD_312') }}

),

aka_GPDIP_EDLUD_319 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('LU_Document_Selection_Disabled', 'aka_GPDIP_EDLUD_319') }}

),

Join_320_inner AS (

  SELECT 
    in0.sub_country_id AS pfleet_subcountry_row_id,
    in1.r_object_id AS r_object_id
  
  FROM aka_GPDIP_EDLUD_319 AS in0
  INNER JOIN aka_GPDIP_EDLUD_312 AS in1
     ON (in0.i_chronicle_id = in1.i_chronicle_id)

)

SELECT *

FROM Join_320_inner
