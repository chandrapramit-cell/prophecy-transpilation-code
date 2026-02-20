{{
  config({    
    "materialized": "table",
    "alias": "recast_cache_cs_2_ref",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_4 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Unique_4')}}

),

Formula_31_0 AS (

  SELECT 
    CAST((CONCAT('_', (SUBSTRING(CAST(CAST(CURRENT_DATE AS STRING) AS STRING), 1, 7)))) AS STRING) AS variabledate,
    *
  
  FROM Unique_4 AS in0

)

SELECT *

FROM Formula_31_0
