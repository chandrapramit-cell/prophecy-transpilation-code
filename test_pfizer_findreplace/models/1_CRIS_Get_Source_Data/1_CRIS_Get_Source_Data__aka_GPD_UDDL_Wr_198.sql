{{
  config({    
    "materialized": "table",
    "alias": "aka_GPD_UDDL_Wr_198",
    "database": "rohit",
    "schema": "default"
  })
}}

WITH Database__P0000_197 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('1_CRIS_Get_Source_Data', 'Database__P0000_197') }}

),

AlteryxSelect_199 AS (

  SELECT 
    CAST(EMPL_ID AS string) AS EMPL_ID,
    CAST(DOMAIN AS string) AS DOMAIN,
    CAST(NTID AS string) AS NTID,
    CAST(RWT AS string) AS RWT,
    DESCRIPTION AS DESCRIPTION,
    CAST(`PRIMARY` AS string) AS PRIMARY,
    CAST(PERCENTAGE AS DOUBLE) AS PERCENTAGE,
    SKILL_ONB AS SKILL_ONB,
    CAST(MODIFIED_BY AS string) AS UPDATED_BY,
    * EXCEPT (`MODIFIELD_DATE`, 
    `EMPL_ID`, 
    `DOMAIN`, 
    `NTID`, 
    `RWT`, 
    `DESCRIPTION`, 
    `PRIMARY`, 
    `PERCENTAGE`, 
    `SKILL_ONB`, 
    `MODIFIED_BY`)
  
  FROM Database__P0000_197 AS in0

),

Formula_203_0 AS (

  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS LOADED_DATE,
    *
  
  FROM AlteryxSelect_199 AS in0

)

SELECT *

FROM Formula_203_0
