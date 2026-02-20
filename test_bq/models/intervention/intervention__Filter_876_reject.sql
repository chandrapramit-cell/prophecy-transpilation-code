{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_834_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_834_left_UnionLeftOuter')}}

),

Filter_876_reject AS (

  SELECT * 
  
  FROM Join_834_left_UnionLeftOuter AS in0
  
  WHERE (((SOURCE_ID <> 'FEP') OR (SOURCE_ID IS NULL)) OR ((SOURCE_ID = 'FEP') IS NULL))

)

SELECT *

FROM Filter_876_reject
