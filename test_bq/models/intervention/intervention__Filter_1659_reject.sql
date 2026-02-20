{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_1021_0 AS (

  SELECT *
  
  FROM {{ ref('intervention__Formula_1021_0')}}

),

Filter_1659_reject AS (

  SELECT * 
  
  FROM Formula_1021_0 AS in0
  
  WHERE (
          (`Member Individual Business Entity Key` <> '10000010052')
          OR ((`Member Individual Business Entity Key` = '10000010052') IS NULL)
        )

)

SELECT *

FROM Filter_1659_reject
