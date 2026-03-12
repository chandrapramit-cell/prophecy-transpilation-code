{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_39_0 AS (

  SELECT *
  
  FROM {{ ref('32_2022_main__Formula_39_0')}}

),

Sort_46 AS (

  SELECT * 
  
  FROM Formula_39_0 AS in0
  
  ORDER BY Team ASC, Match ASC, mode ASC, GridX ASC, GridY ASC

)

SELECT *

FROM Sort_46
