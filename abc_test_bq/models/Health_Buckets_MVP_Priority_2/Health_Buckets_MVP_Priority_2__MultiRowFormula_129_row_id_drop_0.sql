{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_99 AS (

  SELECT *
  
  FROM {{ ref('Health_Buckets_MVP_Priority_2__Filter_99')}}

),

MultiRowFormula_129_row_id_0 AS (

  SELECT 
    (ROW_NUMBER() OVER ()) AS prophecy_row_id,
    *
  
  FROM Filter_99 AS in0

),

MultiRowFormula_129 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_129_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_129 AS in0

)

SELECT *

FROM MultiRowFormula_129_row_id_drop_0
