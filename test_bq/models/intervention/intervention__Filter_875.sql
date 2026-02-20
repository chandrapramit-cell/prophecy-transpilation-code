{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_822_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_822_left_UnionLeftOuter')}}

),

Filter_875 AS (

  SELECT * 
  
  FROM Join_822_left_UnionLeftOuter AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_875
