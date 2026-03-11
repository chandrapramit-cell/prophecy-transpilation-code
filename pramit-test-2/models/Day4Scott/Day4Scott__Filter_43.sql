{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH AlteryxSelect_33 AS (

  SELECT *
  
  FROM {{ ref('Day4Scott__AlteryxSelect_33')}}

),

Filter_43 AS (

  SELECT * 
  
  FROM AlteryxSelect_33 AS in0
  
  WHERE (VALUE = 'X')

)

SELECT *

FROM Filter_43
