{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_864_inner AS (

  SELECT *
  
  FROM {{ ref('intervention__Join_864_inner')}}

),

Filter_884_reject AS (

  SELECT * 
  
  FROM Join_864_inner AS in0
  
  WHERE (((SOURCE_ID <> 'FEP') OR (SOURCE_ID IS NULL)) OR ((SOURCE_ID = 'FEP') IS NULL))

)

SELECT *

FROM Filter_884_reject
