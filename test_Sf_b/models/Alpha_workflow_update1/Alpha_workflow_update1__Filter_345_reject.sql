{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Work_ScheduleVa_335 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Alpha_workflow_update1', 'Work_ScheduleVa_335') }}

),

Filter_345_reject AS (

  {#VisualGroup: WorkSchedule#}
  SELECT * 
  
  FROM Work_ScheduleVa_335 AS in0
  
  WHERE (NOT (not(contains(DESCRIPTION, 'Any Value'))) OR isnull(not(contains(DESCRIPTION, 'Any Value'))))

)

SELECT *

FROM Filter_345_reject
