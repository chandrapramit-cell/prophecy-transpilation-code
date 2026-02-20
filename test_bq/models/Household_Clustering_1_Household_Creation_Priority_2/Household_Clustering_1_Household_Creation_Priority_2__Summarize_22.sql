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

Filter_17 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Filter_17')}}

),

Join_23_inner AS (

  SELECT 
    in1.MBR_HOME_ADDR_CITY_NM AS Right_MBR_HOME_ADDR_CITY_NM,
    in1.MBR_HOME_ADDR_ST_CD AS Right_MBR_HOME_ADDR_ST_CD,
    in1.MBR_HOME_ADDR_CNTY_NM AS Right_MBR_HOME_ADDR_CNTY_NM,
    in1.MBR_HOME_ADDR_ZIP_CD_5 AS Right_MBR_HOME_ADDR_ZIP_CD_5,
    in1.Address AS Right_Address,
    in1.MBR_HOME_ADDR_LN_1 AS Right_MBR_HOME_ADDR_LN_1,
    in1.MBR_HOME_ADDR_LN_2 AS Right_MBR_HOME_ADDR_LN_2,
    in0.*
  
  FROM Join_14_left AS in0
  INNER JOIN Filter_17 AS in1
     ON (
      (
        (
          ((in0.Address = in1.Address) AND (in0.MBR_HOME_ADDR_CITY_NM = in1.MBR_HOME_ADDR_CITY_NM))
          AND (in0.MBR_HOME_ADDR_ST_CD = in1.MBR_HOME_ADDR_ST_CD)
        )
        AND (in0.MBR_HOME_ADDR_CNTY_NM = in1.MBR_HOME_ADDR_CNTY_NM)
      )
      AND (in0.MBR_HOME_ADDR_ZIP_CD_5 = in1.MBR_HOME_ADDR_ZIP_CD_5)
    )

),

Summarize_22 AS (

  SELECT COUNT(DISTINCT MBR_INDV_BE_KEY) AS CountDistinct_MBR_INDV_BE_KEY
  
  FROM Join_23_inner AS in0

)

SELECT *

FROM Summarize_22
