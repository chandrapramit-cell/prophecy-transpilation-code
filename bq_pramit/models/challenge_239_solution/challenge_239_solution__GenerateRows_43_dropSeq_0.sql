{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH GenerateRows_43 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('challenge_239_solution', 'GenerateRows_43') }}

),

GenerateRows_43_dropSeq_0 AS (

  SELECT * EXCEPT (`seq`)
  
  FROM GenerateRows_43 AS in0

)

SELECT *

FROM GenerateRows_43_dropSeq_0
