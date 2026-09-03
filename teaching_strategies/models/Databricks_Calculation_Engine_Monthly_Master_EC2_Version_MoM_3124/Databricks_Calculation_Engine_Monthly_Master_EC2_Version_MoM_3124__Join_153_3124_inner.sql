{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_25_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__AlteryxSelect_25_3124')}}

),

RecordID_148_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_148_3124')}}

),

Summarize_154_3124 AS (

  SELECT DISTINCT RecordIDExit AS RecordIDExit
  
  FROM AlteryxSelect_25_3124 AS in0

),

Join_153_3124_inner AS (

  SELECT 
    in0.* EXCEPT (`RecordIDExit`),
    in1.*
  
  FROM Summarize_154_3124 AS in0
  INNER JOIN RecordID_148_3124 AS in1
     ON (in0.RecordIDExit = in1.RecordIDExit)

)

SELECT *

FROM Join_153_3124_inner
