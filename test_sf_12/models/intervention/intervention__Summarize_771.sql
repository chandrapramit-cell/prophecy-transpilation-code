{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH MultiFieldFormula_772 AS (

  SELECT *
  
  FROM {{ ref('intervention__MultiFieldFormula_772')}}

),

Summarize_771 AS (

  {#VisualGroup: STEP1#}
  SELECT DISTINCT `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`
  
  FROM MultiFieldFormula_772 AS in0

)

SELECT *

FROM Summarize_771
