{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_336 AS (

  SELECT *
  
  FROM {{ ref('Alpha_workflow_update1__AlteryxSelect_336')}}

),

Filter_347_reject AS (

  {#VisualGroup: WorkSchedule#}
  SELECT * 
  
  FROM AlteryxSelect_336 AS in0
  
  WHERE (
          not(
            "COMPANY CODE" = '130')
        ) or isnull(
             "COMPANY CODE" = '130')

)

SELECT *

FROM Filter_347_reject
