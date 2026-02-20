{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_1018 AS (

  SELECT *
  
  FROM {{ ref('intervention__Unique_1018')}}

),

Filter_1650_reject AS (

  SELECT * 
  
  FROM Unique_1018 AS in0
  
  WHERE (
          (`Member Individual Business Entity Key` <> '10000010052')
          OR ((`Member Individual Business Entity Key` = '10000010052') IS NULL)
        )

)

SELECT *

FROM Filter_1650_reject
