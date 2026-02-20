{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_890 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_890')}}

),

Filter_1020_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_890 AS in0
  
  WHERE (((SOURCE_ID <> 'FEP') OR (SOURCE_ID IS NULL)) OR ((SOURCE_ID = 'FEP') IS NULL))

)

SELECT *

FROM Filter_1020_reject
