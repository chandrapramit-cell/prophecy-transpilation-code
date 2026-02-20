{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Join_14_left AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Join_14_left')}}

),

Summarize_15 AS (

  SELECT 
    DISTINCT MBR_HOME_ADDR_CITY_NM AS MBR_HOME_ADDR_CITY_NM,
    Address AS Address,
    MBR_HOME_ADDR_LN_2 AS MBR_HOME_ADDR_LN_2,
    MBR_HOME_ADDR_ZIP_CD_5 AS MBR_HOME_ADDR_ZIP_CD_5,
    MBR_HOME_ADDR_CNTY_NM AS MBR_HOME_ADDR_CNTY_NM,
    MBR_HOME_ADDR_LN_1 AS MBR_HOME_ADDR_LN_1,
    MBR_HOME_ADDR_ST_CD AS MBR_HOME_ADDR_ST_CD
  
  FROM Join_14_left AS in0

),

Filter_17 AS (

  SELECT * 
  
  FROM Summarize_15 AS in0
  
  WHERE (NOT CAST(((STRPOS(Address, 'po box')) > 0) AS BOOL))

)

SELECT *

FROM Filter_17
