{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH alpha_1__1__xls_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Alpha_workflow_update1', 'alpha_1__1__xls_1') }}

),

AlteryxSelect_515 AS (

  {#VisualGroup: RAProblems#}
  SELECT 
    CAST("JOB CODE" AS STRING) AS "JOB CODE",
    CAST(NULL AS STRING) AS "EMPLOYEE ID",
    * EXCLUDE ("JOB CODE")
  
  FROM alpha_1__1__xls_1 AS in0

)

SELECT *

FROM AlteryxSelect_515
