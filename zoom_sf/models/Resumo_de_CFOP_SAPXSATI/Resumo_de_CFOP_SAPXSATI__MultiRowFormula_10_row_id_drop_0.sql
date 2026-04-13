{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH ENTRADASSAPF001_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Resumo_de_CFOP_SAPXSATI', 'ENTRADASSAPF001_1') }}

),

AlteryxSelect_3 AS (

  SELECT 
    F12 AS "COLUNA RESUMO",
    * EXCLUDE ("F12")
  
  FROM ENTRADASSAPF001_1 AS in0

),

MultiRowFormula_10_row_id_0 AS (

  SELECT 
    (SEQ8()) AS PROPHECY_ROW_ID,
    *
  
  FROM AlteryxSelect_3 AS in0

),

MultiRowFormula_10 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_10_row_id_drop_0 AS (

  SELECT * EXCLUDE ("PROPHECY_ROW_ID")
  
  FROM MultiRowFormula_10 AS in0

)

SELECT *

FROM MultiRowFormula_10_row_id_drop_0
