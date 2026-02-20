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

Filter_871_reject AS (

  SELECT * 
  
  FROM Join_792_left_UnionLeftOuter AS in0
  
  WHERE (((SOURCE_ID <> 'FEP') OR (SOURCE_ID IS NULL)) OR ((SOURCE_ID = 'FEP') IS NULL))

)

SELECT *

FROM Filter_871_reject
