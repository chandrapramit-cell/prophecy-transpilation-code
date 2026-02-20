{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Cleanse_11 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Cleanse_11')}}

),

Summarize_12 AS (

  SELECT 
    COUNT(DISTINCT MBR_INDV_BE_KEY) AS CountDistinct_MBR_INDV_BE_KEY,
    MBR_HOME_ADDR_CITY_NM AS MBR_HOME_ADDR_CITY_NM,
    Address AS Address,
    MBR_HOME_ADDR_ZIP_CD_5 AS MBR_HOME_ADDR_ZIP_CD_5,
    ACTVTY_YR_MO_SK AS ACTVTY_YR_MO_SK,
    MBR_HOME_ADDR_CNTY_NM AS MBR_HOME_ADDR_CNTY_NM,
    MBR_HOME_ADDR_ST_CD AS MBR_HOME_ADDR_ST_CD
  
  FROM Cleanse_11 AS in0
  
  GROUP BY 
    MBR_HOME_ADDR_CITY_NM, 
    Address, 
    MBR_HOME_ADDR_ZIP_CD_5, 
    ACTVTY_YR_MO_SK, 
    MBR_HOME_ADDR_CNTY_NM, 
    MBR_HOME_ADDR_ST_CD

)

SELECT *

FROM Summarize_12
