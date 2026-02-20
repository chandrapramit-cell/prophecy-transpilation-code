{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_776_left_UnionFullOuter AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_776_left_UnionFullOuter')}}

),

Filter_869 AS (

  SELECT * 
  
  FROM Join_776_left_UnionFullOuter AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_869
