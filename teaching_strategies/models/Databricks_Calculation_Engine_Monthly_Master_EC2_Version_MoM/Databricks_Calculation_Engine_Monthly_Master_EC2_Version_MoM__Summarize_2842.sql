{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_2840 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2840')}}

),

Summarize_2842 AS (

  SELECT 
    MIN(RecordID) AS Min_RecordID,
    MAX(RecordID) AS Max_RecordID,
    MIN(CustIDMonthKey) AS Min_CustIDMonthKey,
    MAX(CustIDMonthKey) AS Max_CustIDMonthKey
  
  FROM AlteryxSelect_2840 AS in0

)

SELECT *

FROM Summarize_2842
