{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_69 AS (

  SELECT *
  
  FROM {{ ref('CCI__AlteryxSelect_69')}}

),

Summarize_62 AS (

  SELECT DISTINCT EFF_DATE AS YR_MO
  
  FROM AlteryxSelect_69 AS in0

)

SELECT *

FROM Summarize_62
