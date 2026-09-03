{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH table_3118_Input_macro_ip AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_3118_Input_macro_ip') }}

),

Summarize_848_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__Summarize_848_3118')}}

),

AppendFields_842_3118 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Summarize_848_3118 AS in0
  INNER JOIN table_3118_Input_macro_ip AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_842_3118
