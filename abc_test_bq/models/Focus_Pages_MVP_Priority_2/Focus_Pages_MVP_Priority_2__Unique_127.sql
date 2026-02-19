{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_110 AS (

  SELECT *
  
  FROM {{ ref('Focus_Pages_MVP_Priority_2__Unique_110')}}

),

Unique_127 AS (

  SELECT * 
  
  FROM Unique_110 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY MBR_INDV_BE_KEY ORDER BY MBR_INDV_BE_KEY) = 1

)

SELECT *

FROM Unique_127
