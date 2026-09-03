{{
  config({    
    "materialized": "table",
    "alias": "FullStack_Stati_2305",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_1980 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_1980')}}

),

Formula_2306 AS (

  SELECT *
  
  FROM Union_1980 AS in0

)

SELECT *

FROM Formula_2306
