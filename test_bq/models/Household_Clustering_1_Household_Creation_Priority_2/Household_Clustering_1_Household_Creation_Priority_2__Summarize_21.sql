{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Filter_13 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Filter_13')}}

),

Cleanse_11 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Cleanse_11')}}

),

Join_14_inner AS (

  SELECT in0.*
  
  FROM Cleanse_11 AS in0
  INNER JOIN Filter_13 AS in1
     ON (
      (
        ((in0.Address = in1.Address) AND (in0.MBR_HOME_ADDR_CITY_NM = in1.MBR_HOME_ADDR_CITY_NM))
        AND (in0.MBR_HOME_ADDR_ST_CD = in1.MBR_HOME_ADDR_ST_CD)
      )
      AND (in0.MBR_HOME_ADDR_ZIP_CD_5 = in1.MBR_HOME_ADDR_ZIP_CD_5)
    )

),

Summarize_21 AS (

  SELECT COUNT(DISTINCT MBR_INDV_BE_KEY) AS CountDistinct_MBR_INDV_BE_KEY
  
  FROM Join_14_inner AS in0

)

SELECT *

FROM Summarize_21
