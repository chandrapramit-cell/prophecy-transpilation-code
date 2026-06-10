{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Join_776_left_UnionFullOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_776_left_UnionFullOuter')}}

),

Filter_869_reject AS (

  SELECT * 
  
  FROM Join_776_left_UnionFullOuter AS in0
  
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

FROM Filter_869_reject
