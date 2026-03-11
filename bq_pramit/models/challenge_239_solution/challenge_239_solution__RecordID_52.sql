{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH GenerateRows_50 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('challenge_239_solution', 'GenerateRows_50') }}

),

Filter_51 AS (

  SELECT * 
  
  FROM GenerateRows_50 AS in0
  
  WHERE (((((((((s * 1000) + (e * 100)) + (n * 10)) + d) + (m * 1000)) + (o * 100)) + (r * 10)) + e) = (((((m * 10000) + (o * 1000)) + (n * 100)) + (e * 10)) + y))

),

RecordID_52 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM Filter_51

)

SELECT *

FROM RecordID_52
