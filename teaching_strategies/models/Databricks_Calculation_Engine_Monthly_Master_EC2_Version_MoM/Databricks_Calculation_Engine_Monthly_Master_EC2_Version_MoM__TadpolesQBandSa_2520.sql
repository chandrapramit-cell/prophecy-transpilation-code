{{
  config({    
    "materialized": "table",
    "alias": "TadpolesQBandSa_2520",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Filter_2313 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2313')}}

)

SELECT *

FROM Filter_2313
