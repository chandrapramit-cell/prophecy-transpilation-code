{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AppendFields_842_3118 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3118__AppendFields_842_3118')}}

),

AlteryxSelect_852_3118 AS (

  SELECT * EXCEPT (`MaxIteration`)
  
  FROM AppendFields_842_3118 AS in0

)

SELECT *

FROM AlteryxSelect_852_3118
