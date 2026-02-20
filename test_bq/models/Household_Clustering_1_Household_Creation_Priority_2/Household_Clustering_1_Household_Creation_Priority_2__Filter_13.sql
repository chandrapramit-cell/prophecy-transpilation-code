{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Summarize_12 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Summarize_12')}}

),

Filter_13 AS (

  SELECT * 
  
  FROM Summarize_12 AS in0
  
  WHERE (CountDistinct_MBR_INDV_BE_KEY > CAST('8' AS FLOAT64))

)

SELECT *

FROM Filter_13
