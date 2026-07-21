{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_897 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Union_897')}}

),

Unique_1021 AS (

  SELECT * 
  
  FROM Union_897 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY GroupID, new_org_name_alteryx ORDER BY GroupID, new_org_name_alteryx) = 1

),

Summarize_1020 AS (

  SELECT 
    COUNT(DISTINCT new_org_name_alteryx) AS CountDistinctNonNull_new_org_name_alteryx,
    GroupID AS GroupID
  
  FROM Unique_1021 AS in0
  
  GROUP BY GroupID

)

SELECT *

FROM Summarize_1020
