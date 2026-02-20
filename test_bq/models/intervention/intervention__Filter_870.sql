{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Union_784 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_784')}}

),

Filter_870 AS (

  SELECT * 
  
  FROM Union_784 AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_870
