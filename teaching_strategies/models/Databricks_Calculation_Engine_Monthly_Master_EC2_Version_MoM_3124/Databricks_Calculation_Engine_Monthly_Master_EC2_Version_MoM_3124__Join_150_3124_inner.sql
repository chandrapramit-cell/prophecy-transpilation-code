{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_145_3124_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Formula_145_3124_0')}}

),

Filter_146_3124_reject AS (

  SELECT * 
  
  FROM Formula_145_3124_0 AS in0
  
  WHERE (
          (
            (
              NOT(
                `Date Overlap Flag` = TRUE)
            ) OR (`Date Overlap Flag` IS NULL)
          )
          OR ((`Date Overlap Flag` = TRUE) IS NULL)
        )

),

Summarize_149_3124 AS (

  SELECT DISTINCT RecordIDExit AS RecordIDExit
  
  FROM Filter_146_3124_reject AS in0

),

RecordID_148_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__RecordID_148_3124')}}

),

Join_150_3124_inner AS (

  SELECT 
    in0.* EXCEPT (`RecordIDExit`),
    in1.*
  
  FROM Summarize_149_3124 AS in0
  INNER JOIN RecordID_148_3124 AS in1
     ON (in0.RecordIDExit = in1.RecordIDExit)

)

SELECT *

FROM Join_150_3124_inner
