{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_817 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_817')}}

),

Summarize_826 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    COUNT(DISTINCT `Member Individual Business Entity Key`) AS `CountDistinct_Member Individual Business Entity Key`,
    AdvancedCKD_Risk AS AdvancedCKD_Risk
  
  FROM Union_817 AS in0
  
  GROUP BY AdvancedCKD_Risk

)

SELECT *

FROM Summarize_826
