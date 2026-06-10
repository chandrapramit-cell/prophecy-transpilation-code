{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_805_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_805_left_UnionLeftOuter')}}

),

Filter_873 AS (

  SELECT * 
  
  FROM Join_805_left_UnionLeftOuter AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_873
