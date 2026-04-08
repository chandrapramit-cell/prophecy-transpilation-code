{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_782_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_782_0')}}

),

Summarize_786 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    COUNT(DISTINCT `Member Individual Business Entity Key`) AS `CountDistinct_Member Individual Business Entity Key`,
    `ED Prediction Score` AS `ED Prediction Score`
  
  FROM Formula_782_0 AS in0
  
  GROUP BY `ED Prediction Score`

)

SELECT *

FROM Summarize_786
