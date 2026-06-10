{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_822_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_822_left_UnionLeftOuter')}}

),

Filter_875_reject AS (

  SELECT * 
  
  FROM Join_822_left_UnionLeftOuter AS in0
  
  WHERE (
          (
            (
              NOT(
                SOURCE_ID = 'FEP')
            ) OR (SOURCE_ID IS NULL)
          )
          OR ((SOURCE_ID = 'FEP') IS NULL)
        )

)

SELECT *

FROM Filter_875_reject
