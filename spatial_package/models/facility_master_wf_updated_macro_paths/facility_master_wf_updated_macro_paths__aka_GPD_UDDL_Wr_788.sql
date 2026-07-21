{{
  config({    
    "materialized": "table",
    "alias": "aka_GPD_UDDL_Wr_788",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH MultiFieldFormula_399 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__MultiFieldFormula_399')}}

)

SELECT *

FROM MultiFieldFormula_399
