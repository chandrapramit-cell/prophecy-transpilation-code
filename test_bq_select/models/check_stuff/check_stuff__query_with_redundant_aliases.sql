{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH random AS (

  SELECT * 
  
  FROM {{ ref('random')}}

),

query_with_redundant_aliases AS (

  SELECT * REPLACE(id+10 AS id) FROM random

)

SELECT *

FROM query_with_redundant_aliases
