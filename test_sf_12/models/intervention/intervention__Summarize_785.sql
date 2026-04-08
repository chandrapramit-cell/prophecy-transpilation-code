{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_784 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_784')}}

),

Summarize_785 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    COUNT(DISTINCT `Member Individual Business Entity Key`) AS `CountDistinct_Member Individual Business Entity Key`,
    `ED Prediction Score` AS `ED Prediction Score`
  
  FROM Union_784 AS in0
  
  GROUP BY `ED Prediction Score`

)

SELECT *

FROM Summarize_785
