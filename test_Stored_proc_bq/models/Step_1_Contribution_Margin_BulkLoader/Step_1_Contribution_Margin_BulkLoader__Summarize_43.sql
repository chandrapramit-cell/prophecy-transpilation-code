{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Source_File_C___15 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Step_1_Contribution_Margin_BulkLoader', 'Source_File_C___15') }}

),

Summarize_43 AS (

  SELECT DISTINCT reportingchannel AS reportingchannel
  
  FROM Source_File_C___15 AS in0

)

SELECT *

FROM Summarize_43
