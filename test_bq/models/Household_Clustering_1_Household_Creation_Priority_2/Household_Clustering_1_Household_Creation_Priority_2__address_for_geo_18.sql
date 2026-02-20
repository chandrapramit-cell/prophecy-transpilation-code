{{
  config({    
    "materialized": "table",
    "alias": "address_for_geo_18_ref",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_17 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Filter_17')}}

),

Formula_32_0 AS (

  SELECT 
    CAST((CONCAT('_', (SUBSTRING(CAST(CAST(CURRENT_DATE AS STRING) AS STRING), 1, 7)))) AS STRING) AS variabledate,
    *
  
  FROM Filter_17 AS in0

)

SELECT *

FROM Formula_32_0
