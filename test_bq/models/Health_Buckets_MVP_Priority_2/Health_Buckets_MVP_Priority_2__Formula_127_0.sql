{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH MultiRowFormula_129_row_id_drop_0 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__MultiRowFormula_129_row_id_drop_0')}}

),

Formula_127_0 AS (

  SELECT 
    3 AS NumRows,
    *
  
  FROM MultiRowFormula_129_row_id_drop_0 AS in0

)

SELECT *

FROM Formula_127_0
