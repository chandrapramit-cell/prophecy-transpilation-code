{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_863_inner AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_updated_macro_paths__Join_863_inner')}}

),

Formula_864_0 AS (

  SELECT 
    CAST((
      CONCAT(
        '(', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(GroupID AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ')')
    ) AS string) AS EXP,
    *
  
  FROM Join_863_inner AS in0

),

Summarize_866 AS (

  SELECT 
    COUNT(DISTINCT organization_name) AS CountDistinctNonNull_organization_name,
    EXP AS EXP,
    GroupID AS GroupID
  
  FROM Formula_864_0 AS in0
  
  GROUP BY 
    EXP, GroupID

)

SELECT *

FROM Summarize_866
