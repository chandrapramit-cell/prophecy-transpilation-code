{{
  config({    
    "materialized": "table",
    "alias": "14_5_Output14_macro_op",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH `5_Input19_macro_ip` AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', '5_Input19_macro_ip') }}

),

Formula_11_5_0 AS (

  SELECT 
    {{ var('variable11_FormulaFields_FormulaField_expression') }} AS `Number of Boxes`,
    *
  
  FROM `5_Input19_macro_ip` AS in0

)

SELECT *

FROM Formula_11_5_0
