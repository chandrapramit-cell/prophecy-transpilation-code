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

reformat_1 AS (

  SELECT 
    REPLACE(tag, 'h', '') AS tag2,
    * EXCEPT (tag)
  
  FROM random AS in0

)

SELECT *

FROM reformat_1
