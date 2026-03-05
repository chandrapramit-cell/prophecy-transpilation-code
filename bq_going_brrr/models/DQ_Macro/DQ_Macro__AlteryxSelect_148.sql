{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Query__Sheet1___144 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DQ_Macro', 'Query__Sheet1___144') }}

),

AlteryxSelect_148 AS (

  SELECT * 
  
  FROM Query__Sheet1___144 AS in0

)

SELECT *

FROM AlteryxSelect_148
