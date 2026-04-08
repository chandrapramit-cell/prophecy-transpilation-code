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

Filter_880 AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM Join_815_inner AS in0
  
  WHERE (`Member Individual Business Entity Key` = '10000010052')

)

SELECT *

FROM Filter_880
