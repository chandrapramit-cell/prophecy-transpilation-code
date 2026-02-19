{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Formula_90_0 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Formula_90_0')}}

),

Filter_92 AS (

  SELECT * 
  
  FROM Formula_90_0 AS in0
  
  WHERE (CAST((SUBSTRING(PregnancyDiag, 1, 3)) AS STRING) = 'Z3A')

)

SELECT *

FROM Filter_92
