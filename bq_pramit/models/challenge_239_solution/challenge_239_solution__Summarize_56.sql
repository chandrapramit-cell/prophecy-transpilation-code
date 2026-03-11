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

Transpose_53 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_52'], 
      ['RecordID'], 
      ['s', 'e', 'n', 'd', 'm', 'o', 'r', 'y'], 
      'Name', 
      'Value', 
      ['e', 's', 'n', 'y', 'm', 'r', 'o', 'd', 'RecordID'], 
      true
    )
  }}

),

Unique_54_window AS (

  SELECT 
    (ROW_NUMBER() OVER (PARTITION BY RECORDID, VALUE)) AS row_number,
    *
  
  FROM Transpose_53 AS in0

),

Unique_54_filter AS (

  SELECT * 
  
  FROM Unique_54_window AS in0
  
  WHERE (row_number > 1)

),

Unique_54_drop_0 AS (

  SELECT * EXCEPT (`row_number`)
  
  FROM Unique_54_filter AS in0

),

Summarize_56 AS (

  SELECT DISTINCT RecordID AS RecordID
  
  FROM Unique_54_drop_0 AS in0

)

SELECT *

FROM Summarize_56
