{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH RecordID_52 AS (

  SELECT *
  
  FROM {{ ref('challenge_239_solution__RecordID_52')}}

),

Summarize_56 AS (

  SELECT *
  
  FROM {{ ref('challenge_239_solution__Summarize_56')}}

),

Join_57_inner AS (

  SELECT in0.* EXCEPT (`RecordID`)
  
  FROM RecordID_52 AS in0
  INNER JOIN Summarize_56 AS in1
     ON (in0.RecordID = in1.RecordID)

)

SELECT *

FROM Join_57_inner
