{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_866 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Summarize_866')}}

),

Filter_867 AS (

  SELECT * 
  
  FROM Summarize_866 AS in0
  
  WHERE (CountDistinctNonNull_organization_name > 1)

)

SELECT *

FROM Filter_867
