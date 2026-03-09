{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH CReWOnlyUnique_16_count AS (

  SELECT *
  
  FROM {{ ref('143_common_workflows_Text_l_update_abbreviations__CReWOnlyUnique_16_count')}}

),

CReWOnlyUnique_16 AS (

  SELECT * 
  
  FROM CReWOnlyUnique_16_count AS in0
  
  WHERE (COUNT = 1)

)

SELECT *

FROM CReWOnlyUnique_16
