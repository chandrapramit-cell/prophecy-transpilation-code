{{
  config({    
    "materialized": "table",
    "alias": "aka_GPD_UDDL_Wr_200",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Database__P0000_196 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('1_CRIS_Get_Source_Data', 'Database__P0000_196') }}

),

AlteryxSelect_201 AS (

  SELECT 
    CAST(EMPL_ID AS string) AS EMPL_ID,
    CAST(DOMAIN AS string) AS DOMAIN,
    CAST(NTID AS string) AS NTID,
    CAST(COST_CENTER AS string) AS COST_CENTER,
    CAST(MANUAL_PROFILE AS string) AS MANUAL_PROFILE,
    * EXCEPT (`RN`, `EMPL_ID`, `DOMAIN`, `NTID`, `COST_CENTER`, `MANUAL_PROFILE`)
  
  FROM Database__P0000_196 AS in0

),

Formula_202_0 AS (

  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS LOADED_DATE,
    *
  
  FROM AlteryxSelect_201 AS in0

)

SELECT *

FROM Formula_202_0
