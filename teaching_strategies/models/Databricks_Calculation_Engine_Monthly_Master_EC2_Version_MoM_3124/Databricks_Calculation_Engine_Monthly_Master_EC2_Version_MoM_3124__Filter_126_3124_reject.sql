{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH RecordID_129_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_129_3124')}}

),

Filter_126_3124_reject AS (

  SELECT * 
  
  FROM RecordID_129_3124 AS in0
  
  WHERE (
          (
            (
              NOT(
                UPPER(Origin) = UPPER('Manual Plug In'))
            ) OR (Origin IS NULL)
          )
          OR ((UPPER(Origin) = UPPER('Manual Plug In')) IS NULL)
        )

)

SELECT *

FROM Filter_126_3124_reject
