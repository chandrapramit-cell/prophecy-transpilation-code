{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH RecordID_110_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_110_3124')}}

),

Filter_107_3124 AS (

  SELECT * 
  
  FROM RecordID_110_3124 AS in0
  
  WHERE (ManualRecordID IS NULL)

)

SELECT *

FROM Filter_107_3124
