{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_798_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_798_left_UnionLeftOuter')}}

),

Filter_872 AS (

  SELECT * 
  
  FROM Join_798_left_UnionLeftOuter AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_872
