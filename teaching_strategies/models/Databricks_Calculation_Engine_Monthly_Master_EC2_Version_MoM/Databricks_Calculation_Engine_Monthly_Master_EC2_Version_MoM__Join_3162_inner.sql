{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_3090 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3090')}}

),

Summarize_121 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_121')}}

),

Join_3162_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Loser Mas90 Customer Number`, `Winner Mas90 Customer Number`)
  
  FROM Summarize_121 AS in0
  INNER JOIN AlteryxSelect_3090 AS in1
     ON (in0.`Loser Mas90 Customer Number` = in1.`Loser Mas90 Customer Number`)

)

SELECT *

FROM Join_3162_inner
