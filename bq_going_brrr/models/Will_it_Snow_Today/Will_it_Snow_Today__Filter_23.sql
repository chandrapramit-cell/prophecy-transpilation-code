{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH AlteryxSelect_15 AS (

  SELECT *
  
  FROM {{ ref('Will_it_Snow_Today__AlteryxSelect_15')}}

),

Filter_23 AS (

  SELECT * 
  
  FROM AlteryxSelect_15 AS in0
  
  WHERE (`Tag ampersand Monat` = '11-02')

)

SELECT *

FROM Filter_23
