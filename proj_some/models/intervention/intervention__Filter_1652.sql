{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_814_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_814_0')}}

),

Filter_1652 AS (

  SELECT * 
  
  FROM Formula_814_0 AS in0
  
  WHERE (`Member Individual Business Entity Key` = '10000010052')

)

SELECT *

FROM Filter_1652
