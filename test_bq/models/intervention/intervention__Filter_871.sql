{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_792_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_792_left_UnionLeftOuter')}}

),

Filter_871 AS (

  SELECT * 
  
  FROM Join_792_left_UnionLeftOuter AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_871
