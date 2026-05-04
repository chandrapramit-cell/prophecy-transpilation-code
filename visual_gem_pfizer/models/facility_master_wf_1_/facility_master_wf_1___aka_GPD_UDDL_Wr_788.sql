{{
  config({    
    "materialized": "table",
    "alias": "aka_GPD_UDDL_Wr_788",
    "database": "tanmay",
    "schema": "default2"
  })
}}

WITH MultiFieldFormula_399 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_1___MultiFieldFormula_399')}}

)

SELECT *

FROM MultiFieldFormula_399
