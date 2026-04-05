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

Source_File_C___24 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Step_1_Contribution_Margin_BulkLoader', 'Source_File_C___24') }}

),

Join_26_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Source_File_C___24 AS in0
  INNER JOIN Source_File_C___15 AS in1
     ON (in0.districtcode = in1.hierarchycode)

)

SELECT *

FROM Join_26_inner
