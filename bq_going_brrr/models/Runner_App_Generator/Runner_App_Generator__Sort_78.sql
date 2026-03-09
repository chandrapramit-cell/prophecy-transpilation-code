{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sort_50 AS (

  SELECT *
  
  FROM {{ ref('Runner_App_Generator__Sort_50')}}

),

Summarize_76 AS (

  SELECT 
    1 AS _dummy_,
    WorkflowFullPath AS WorkflowFullPath
  
  FROM Sort_50 AS in0
  
  GROUP BY WorkflowFullPath

),

Sort_78 AS (

  SELECT * 
  
  FROM Summarize_76 AS in0
  
  ORDER BY WorkflowFullPath ASC

)

SELECT *

FROM Sort_78
