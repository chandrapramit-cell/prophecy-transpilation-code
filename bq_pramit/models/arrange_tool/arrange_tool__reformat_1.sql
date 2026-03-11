{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH fib_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('arrange_tool', 'fib_1') }}

),

reformat_1 AS (

  SELECT *
  
  FROM fib_1 AS in0

)

SELECT *

FROM reformat_1
