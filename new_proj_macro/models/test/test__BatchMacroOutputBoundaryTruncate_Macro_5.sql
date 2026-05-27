{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH `5_Input19_macro_ip` AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', '5_Input19_macro_ip') }}

),

BatchMacroOutputBoundaryTruncate_Macro_5 AS (

  -- Use the actual table name with the catalog and schema
  TRUNCATE TABLE 14_5_Output14_macro_op
  out0=5_Input19_macro_ip

)

SELECT *

FROM BatchMacroOutputBoundaryTruncate_Macro_5
