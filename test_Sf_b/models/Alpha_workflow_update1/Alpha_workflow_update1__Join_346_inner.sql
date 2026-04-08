{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Filter_345_reject AS (

  SELECT *
  
  FROM {{ ref('Alpha_workflow_update1__Filter_345_reject')}}

),

Filter_347_reject AS (

  SELECT *
  
  FROM {{ ref('Alpha_workflow_update1__Filter_347_reject')}}

),

Join_346_inner AS (

  {#VisualGroup: WorkSchedule#}
  SELECT 
    in0.*,
    in1.*
  
  FROM Filter_347_reject AS in0
  INNER JOIN Filter_345_reject AS in1
     ON (in0."COMPANY CODE" = in1.COMPANY)

)

SELECT *

FROM Join_346_inner
