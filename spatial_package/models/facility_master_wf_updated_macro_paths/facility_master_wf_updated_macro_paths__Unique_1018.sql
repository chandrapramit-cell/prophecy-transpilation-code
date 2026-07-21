{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_1020 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Summarize_1020')}}

),

Filter_1022 AS (

  SELECT * 
  
  FROM Summarize_1020 AS in0
  
  WHERE (CountDistinctNonNull_new_org_name_alteryx > 1)

),

Unique_1018 AS (

  SELECT * 
  
  FROM Filter_1022 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY GroupID, CountDistinctNonNull_new_org_name_alteryx ORDER BY GroupID, CountDistinctNonNull_new_org_name_alteryx) = 1

)

SELECT *

FROM Unique_1018
