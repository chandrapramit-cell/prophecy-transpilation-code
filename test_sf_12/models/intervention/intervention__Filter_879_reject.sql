{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Filter_816 AS (

  SELECT *
  
  FROM {{ ref('intervention__Filter_816')}}

),

Filter_879_reject AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM Filter_816 AS in0
  
  WHERE (
          (
            NOT(
              `Member Individual Business Entity Key` = '10000010052')
          )
          OR ((`Member Individual Business Entity Key` = '10000010052') IS NULL)
        )

)

SELECT *

FROM Filter_879_reject
