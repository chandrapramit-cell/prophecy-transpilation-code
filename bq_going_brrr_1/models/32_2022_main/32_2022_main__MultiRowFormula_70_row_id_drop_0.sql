{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH data_2022_db_Qu_4 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('32_2022_main', 'data_2022_db_Qu_4') }}

),

global_db_Query_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('32_2022_main', 'global_db_Query_1') }}

),

Filter_2 AS (

  SELECT * 
  
  FROM global_db_Query_1 AS in0
  
  WHERE (variablekey = 'event')

),

AppendFields_3 AS (

  SELECT 
    in1.VALUE AS TargetEvent,
    in0.*
  
  FROM data_2022_db_Qu_4 AS in0
  INNER JOIN Filter_2 AS in1
     ON TRUE

),

Sort_9 AS (

  SELECT * 
  
  FROM AppendFields_3 AS in0
  
  ORDER BY Team ASC, Match ASC

),

MultiRowFormula_70_row_id_0 AS (

  SELECT 
    (ROW_NUMBER() OVER ()) AS prophecy_row_id,
    *
  
  FROM Sort_9 AS in0

),

MultiRowFormula_70 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_70_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_70 AS in0

)

SELECT *

FROM MultiRowFormula_70_row_id_drop_0
