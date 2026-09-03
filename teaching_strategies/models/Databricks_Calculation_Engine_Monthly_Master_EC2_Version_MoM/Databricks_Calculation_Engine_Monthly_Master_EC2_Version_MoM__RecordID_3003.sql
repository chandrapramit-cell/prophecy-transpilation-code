{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Union_677 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_677')}}

),

Filter_3000 AS (

  SELECT * 
  
  FROM Union_677 AS in0
  
  WHERE ((EndDate_Annualization >= to_date('2024-01-01')) AND (EndDate_Annualization < to_date('2024-12-31')))

),

RecordID_3003 AS (

  {{
    prophecy_basics.RecordID(
      ['Filter_3000'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

)

SELECT *

FROM RecordID_3003
