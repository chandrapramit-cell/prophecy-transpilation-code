{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH MultiRowFormula_10_row_id_drop_0 AS (

  SELECT *
  
  FROM {{ ref('Resumo_de_CFOP_SAPXSATI__MultiRowFormula_10_row_id_drop_0')}}

),

Filter_11_reject AS (

  SELECT * 
  
  FROM MultiRowFormula_10_row_id_drop_0 AS in0
  
  WHERE (
          (
            NOT(
              FLAG = '1')
          ) OR ((FLAG = '1') IS NULL)
        )

)

SELECT *

FROM Filter_11_reject
