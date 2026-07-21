{{
  config({    
    "materialized": "table",
    "alias": "table_1070_Input12_macro_ip",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_897 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Union_897')}}

)

SELECT *

FROM Union_897
