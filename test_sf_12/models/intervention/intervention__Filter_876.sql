{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Join_834_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_834_left_UnionLeftOuter')}}

),

Filter_876 AS (

  {#VisualGroup: STEP1#}
  SELECT * 
  
  FROM Join_834_left_UnionLeftOuter AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_876
