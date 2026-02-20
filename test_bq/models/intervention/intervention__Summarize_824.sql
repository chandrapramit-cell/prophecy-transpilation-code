{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH CKD_Risk_List_c_1675 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('intervention', 'CKD_Risk_List_c_1675') }}

),

Summarize_824 AS (

  SELECT 
    COUNT(DISTINCT UNIVERSALMEMBERID) AS CountDistinct_UNIVERSALMEMBERID,
    AdvancedCKD_Risk AS AdvancedCKD_Risk
  
  FROM CKD_Risk_List_c_1675 AS in0
  
  GROUP BY AdvancedCKD_Risk

)

SELECT *

FROM Summarize_824
