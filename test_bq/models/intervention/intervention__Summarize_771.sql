{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH MultiFieldFormula_772 AS (

  SELECT *
  
  FROM {{ ref('intervention__MultiFieldFormula_772')}}

),

Summarize_771 AS (

  SELECT DISTINCT `Member Individual Business Entity Key` AS `Member Individual Business Entity Key`
  
  FROM MultiFieldFormula_772 AS in0

)

SELECT *

FROM Summarize_771
