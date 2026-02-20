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

Summarize_777 AS (

  SELECT DISTINCT `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`
  
  FROM Join_776_left_UnionFullOuter AS in0

)

SELECT *

FROM Summarize_777
