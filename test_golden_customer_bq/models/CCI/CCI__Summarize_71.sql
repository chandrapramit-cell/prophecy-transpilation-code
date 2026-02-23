{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_78 AS (

  SELECT *
  
  FROM {{ ref('CCI__Unique_78')}}

),

Summarize_71 AS (

  SELECT 
    SUM(POINTS) AS POINTS,
    MemberId AS MemberId,
    YR_MO AS YR_MO
  
  FROM Unique_78 AS in0
  
  GROUP BY 
    MemberId, YR_MO

)

SELECT *

FROM Summarize_71
