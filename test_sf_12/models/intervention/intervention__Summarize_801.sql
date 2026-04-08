{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Join_805_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_805_left_UnionLeftOuter')}}

),

Summarize_801 AS (

  {#VisualGroup: STEP1#}
  SELECT DISTINCT `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`
  
  FROM Join_805_left_UnionLeftOuter AS in0

)

SELECT *

FROM Summarize_801
