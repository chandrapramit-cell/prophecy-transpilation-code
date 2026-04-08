{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Join_776_left_UnionFullOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_776_left_UnionFullOuter')}}

),

Summarize_779 AS (

  {#VisualGroup: STEP1#}
  SELECT 
    COUNT(DISTINCT `Member Individual Business Entity Key`) AS `CountDistinct_Member Individual Business Entity Key`,
    `Prediction Score` AS `Prediction Score`
  
  FROM Join_776_left_UnionFullOuter AS in0
  
  GROUP BY `Prediction Score`

)

SELECT *

FROM Summarize_779
