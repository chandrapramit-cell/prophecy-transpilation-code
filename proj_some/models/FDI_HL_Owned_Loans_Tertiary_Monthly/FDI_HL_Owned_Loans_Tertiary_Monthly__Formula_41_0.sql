{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Source__User_Db_45 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Source__User_Db_45_ref') }}

),

Source__User_Db_42 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Source__User_Db_42_ref') }}

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
