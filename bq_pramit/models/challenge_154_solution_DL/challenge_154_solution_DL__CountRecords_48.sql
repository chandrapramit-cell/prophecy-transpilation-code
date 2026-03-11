{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH RecordID_52 AS (

  SELECT *
  
  FROM {{ ref('challenge_154_solution_DL__RecordID_52')}}

),

CountRecords_48 AS (

  SELECT COUNT(*) AS `Count`
  
  FROM RecordID_52 AS in0

)

SELECT *

FROM CountRecords_48
