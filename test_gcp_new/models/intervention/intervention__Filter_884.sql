{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Join_864_inner AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_864_inner')}}

),

Filter_884 AS (

  SELECT * 
  
  FROM Join_864_inner AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_884
