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

Filter_870_reject AS (

  SELECT * 
  
  FROM Union_784 AS in0
  
  WHERE (((SOURCE_ID <> 'FEP') OR (SOURCE_ID IS NULL)) OR ((SOURCE_ID = 'FEP') IS NULL))

)

SELECT *

FROM Filter_870_reject
