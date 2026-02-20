{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_850_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_850_left_UnionLeftOuter')}}

),

Filter_1649 AS (

  SELECT * 
  
  FROM Join_850_left_UnionLeftOuter AS in0
  
  WHERE (`Member Individual Business Entity Key` = '10000010052')

)

SELECT *

FROM Filter_1649
