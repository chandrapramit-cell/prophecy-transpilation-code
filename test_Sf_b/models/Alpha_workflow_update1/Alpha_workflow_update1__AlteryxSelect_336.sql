{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_515 AS (

  SELECT *
  
  FROM {{ ref('Alpha_workflow_update1__AlteryxSelect_515')}}

),

Formula_344_0 AS (

  {#VisualGroup: WorkSchedule#}
  SELECT 
    CAST(CO AS STRING) AS "COMPANY CODE",
    *
  
  FROM AlteryxSelect_515 AS in0

),

AlteryxSelect_336 AS (

  {#VisualGroup: WorkSchedule#}
  SELECT 
    CAST(NULL AS STRING) AS COMPANY_CODE,
    *
  
  FROM Formula_344_0 AS in0

)

SELECT *

FROM AlteryxSelect_336
