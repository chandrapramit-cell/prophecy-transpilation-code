{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sort_15 AS (

  SELECT *
  
  FROM {{ ref('143_common_workflows_Text_l_update_abbreviations__Sort_15')}}

),

CReWOnlyUnique_16_count AS (

  SELECT DISTINCT abbreviation_codes AS abbreviation_codes
  
  FROM Sort_15 AS in0

)

SELECT *

FROM CReWOnlyUnique_16_count
