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

Filter_13 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Filter_13')}}

),

Join_14_left AS (

  SELECT in0.*
  
  FROM Cleanse_11 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.Address,
      in1.MBR_HOME_ADDR_CITY_NM,
      in1.MBR_HOME_ADDR_ST_CD,
      in1.MBR_HOME_ADDR_ZIP_CD_5
    
    FROM Filter_13 AS in1
    
    WHERE in1.Address IS NOT NULL
          AND in1.MBR_HOME_ADDR_CITY_NM IS NOT NULL
          AND in1.MBR_HOME_ADDR_ST_CD IS NOT NULL
          AND in1.MBR_HOME_ADDR_ZIP_CD_5 IS NOT NULL
  ) AS in1_keys
     ON (
      (
        ((in0.Address = in1_keys.Address) AND (in0.MBR_HOME_ADDR_CITY_NM = in1_keys.MBR_HOME_ADDR_CITY_NM))
        AND (in0.MBR_HOME_ADDR_ST_CD = in1_keys.MBR_HOME_ADDR_ST_CD)
      )
      AND (in0.MBR_HOME_ADDR_ZIP_CD_5 = in1_keys.MBR_HOME_ADDR_ZIP_CD_5)
    )
  
  WHERE (in1_keys.Address IS NULL)

)

SELECT *

FROM Join_14_left
