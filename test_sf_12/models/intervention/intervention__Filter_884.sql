{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Join_864_inner AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_864_inner')}}

),

Filter_884 AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM Join_864_inner AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_884
