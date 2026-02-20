{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_838 AS (

  SELECT *
  
  FROM {{ ref('intervention__AlteryxSelect_838')}}

),

Filter_878 AS (

  SELECT * 
  
  FROM AlteryxSelect_838 AS in0
  
  WHERE (SOURCE_ID = 'FEP')

)

SELECT *

FROM Filter_878
