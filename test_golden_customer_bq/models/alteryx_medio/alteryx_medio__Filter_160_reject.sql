{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Summarize_158 AS (

  SELECT *
  
  FROM {{ ref('alteryx_medio__Summarize_158')}}

),

Filter_160_reject AS (

  SELECT * 
  
  FROM Summarize_158 AS in0
  
  WHERE (
          (
            NOT(
              COUNT > CAST('1' AS FLOAT64))
          ) OR ((COUNT > CAST('1' AS FLOAT64)) IS NULL)
        )

)

SELECT *

FROM Filter_160_reject
