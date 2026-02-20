{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Union_817 AS (

  SELECT *
  
  FROM {{ ref('intervention__Union_817')}}

),

Summarize_818 AS (

  SELECT 
    COUNT(DISTINCT `Member Individual Business Entity Key`) AS `CountDistinct_Member Individual Business Entity Key`,
    `Inflected Indicator CY` AS `Inflected Indicator CY`
  
  FROM Union_817 AS in0
  
  GROUP BY `Inflected Indicator CY`

)

SELECT *

FROM Summarize_818
