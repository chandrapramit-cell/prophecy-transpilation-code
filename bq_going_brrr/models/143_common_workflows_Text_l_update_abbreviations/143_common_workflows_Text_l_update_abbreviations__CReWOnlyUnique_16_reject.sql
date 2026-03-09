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

CReWOnlyUnique_16_reject AS (

  SELECT * 
  
  FROM CReWOnlyUnique_16_count AS in0
  
  WHERE (((COUNT <> 1) OR (COUNT IS NULL)) OR ((COUNT = 1) IS NULL))

)

SELECT *

FROM CReWOnlyUnique_16_reject
