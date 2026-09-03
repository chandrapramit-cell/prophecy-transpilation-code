{{
  config({    
    "materialized": "incremental",
    "alias": "table_3124_Exit_macro_op",
    "database": "sony",
    "incremental_strategy": "append",
    "schema": "orch_test"
  })
}}

WITH Summarize_156_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_156_3124')}}

)

SELECT *

FROM Summarize_156_3124
