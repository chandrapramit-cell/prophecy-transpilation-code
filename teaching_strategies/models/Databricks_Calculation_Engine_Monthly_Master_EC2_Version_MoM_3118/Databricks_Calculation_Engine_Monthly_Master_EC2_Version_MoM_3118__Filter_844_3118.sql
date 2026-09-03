{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_852_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__AlteryxSelect_852_3118')}}

),

Filter_844_3118 AS (

  SELECT * 
  
  FROM AlteryxSelect_852_3118 AS in0
  
  WHERE (`ARR Period` <= StaticHistoryYearEnd)

)

SELECT *

FROM Filter_844_3118
