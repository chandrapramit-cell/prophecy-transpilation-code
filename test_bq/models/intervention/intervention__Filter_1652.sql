{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_814_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_814_0')}}

),

Filter_1652 AS (

  SELECT * 
  
  FROM Formula_814_0 AS in0
  
  WHERE (`Member Individual Business Entity Key` = '10000010052')

)

SELECT *

FROM Filter_1652
