{{
  config({    
    "materialized": "table",
    "alias": "table_1004_Input43_macro_ip",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_863_inner AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Join_863_inner')}}

)

SELECT *

FROM Join_863_inner
