{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH FindReplace_3 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('findReplaceSample', 'FindReplace_3') }}

),

FindReplace_3_reorg_0 AS (

  SELECT * EXCEPT (`_rules`)
  
  FROM FindReplace_3 AS in0

)

SELECT *

FROM FindReplace_3_reorg_0
