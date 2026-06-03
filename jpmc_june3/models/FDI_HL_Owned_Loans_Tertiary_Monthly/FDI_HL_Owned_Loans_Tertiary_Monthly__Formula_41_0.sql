{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Source__User_Db_42 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FDI_HL_Owned_Loans_Tertiary_Monthly', 'Source__User_Db_42') }}

),

Source__User_Db_45 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FDI_HL_Owned_Loans_Tertiary_Monthly', 'Source__User_Db_45') }}

),

Union_39 AS (

  {{
    prophecy_basics.UnionByName(
      ['Source__User_Db_42', 'Source__User_Db_45'], 
      ['[]', '[]'], 
      'allowMissingColumns'
    )
  }}

),

Formula_41_0 AS (

  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS Data_Refresh_Date,
    *
  
  FROM Union_39 AS in0

)

SELECT *

FROM Formula_41_0
