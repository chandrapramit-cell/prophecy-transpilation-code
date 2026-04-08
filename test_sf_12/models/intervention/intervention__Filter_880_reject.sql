{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Join_815_inner AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_815_inner')}}

),

Filter_880_reject AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM Join_815_inner AS in0
  
  WHERE (
          (
            (
              NOT(
                `Member Individual Business Entity Key` = '10000010052')
            )
            OR (`Member Individual Business Entity Key` IS NULL)
          )
          OR ((`Member Individual Business Entity Key` = '10000010052') IS NULL)
        )

)

SELECT *

FROM Filter_880_reject
