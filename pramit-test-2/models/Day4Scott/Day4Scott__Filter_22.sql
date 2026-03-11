{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH AlteryxSelect_11 AS (

  SELECT *
  
  FROM {{ ref('Day4Scott__AlteryxSelect_11')}}

),

Filter_22 AS (

  SELECT * 
  
  FROM AlteryxSelect_11 AS in0
  
  WHERE (VALUE = 'X')

)

SELECT *

FROM Filter_22
