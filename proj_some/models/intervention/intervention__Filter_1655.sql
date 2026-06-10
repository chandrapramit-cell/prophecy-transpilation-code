{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Filter_816 AS (

  SELECT *
  
  FROM {{ ref('intervention__Filter_816')}}

),

Filter_1655 AS (

  SELECT * 
  
  FROM Filter_816 AS in0
  
  WHERE (`Member Individual Business Entity Key` = '10000010052')

)

SELECT *

FROM Filter_1655
