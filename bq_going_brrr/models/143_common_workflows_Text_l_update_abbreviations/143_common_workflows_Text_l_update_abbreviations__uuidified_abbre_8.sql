{{
  config({    
    "materialized": "table",
    "alias": "uuidified_abbre_8",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Sort_15 AS (

  SELECT *
  
  FROM {{ ref('143_common_workflows_Text_l_update_abbreviations__Sort_15')}}

)

SELECT *

FROM Sort_15
