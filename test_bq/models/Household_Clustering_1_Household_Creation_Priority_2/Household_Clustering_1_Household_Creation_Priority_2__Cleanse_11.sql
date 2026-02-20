{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Unique_4 AS (

  SELECT *
  
  FROM {{ ref('Household_Clustering_1_Household_Creation_Priority_2__Unique_4')}}

),

Filter_28 AS (

  SELECT * 
  
  FROM Unique_4 AS in0
  
  WHERE (MBR_RELSHP_CD <> 'DPNDT')

),

Summarize_9 AS (

  SELECT 
    DISTINCT MBR_INDV_BE_KEY AS MBR_INDV_BE_KEY,
    MBR_HOME_ADDR_CITY_NM AS MBR_HOME_ADDR_CITY_NM,
    MBR_HOME_ADDR_LN_2 AS MBR_HOME_ADDR_LN_2,
    MBR_HOME_ADDR_ZIP_CD_5 AS MBR_HOME_ADDR_ZIP_CD_5,
    ACTVTY_YR_MO_SK AS ACTVTY_YR_MO_SK,
    MBR_HOME_ADDR_CNTY_NM AS MBR_HOME_ADDR_CNTY_NM,
    MBR_HOME_ADDR_LN_1 AS MBR_HOME_ADDR_LN_1,
    MBR_HOME_ADDR_ST_CD AS MBR_HOME_ADDR_ST_CD
  
  FROM Filter_28 AS in0

),

Sort_8 AS (

  SELECT * 
  
  FROM Summarize_9 AS in0
  
  ORDER BY MBR_HOME_ADDR_LN_1 ASC, MBR_HOME_ADDR_LN_2 ASC, MBR_HOME_ADDR_CITY_NM ASC

),

Formula_10_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (MBR_HOME_ADDR_LN_1 IS NULL)
          THEN ''
        ELSE MBR_HOME_ADDR_LN_1
      END
    ) AS STRING) AS MBR_HOME_ADDR_LN_1,
    CAST((
      CASE
        WHEN (MBR_HOME_ADDR_LN_2 IS NULL)
          THEN ''
        ELSE MBR_HOME_ADDR_LN_2
      END
    ) AS STRING) AS MBR_HOME_ADDR_LN_2,
    * EXCEPT (`mbr_home_addr_ln_2`, `mbr_home_addr_ln_1`)
  
  FROM Sort_8 AS in0

),

Formula_10_1 AS (

  SELECT 
    CAST((CONCAT(MBR_HOME_ADDR_LN_1, ' ', MBR_HOME_ADDR_LN_2)) AS STRING) AS Address,
    *
  
  FROM Formula_10_0 AS in0

),

Cleanse_11 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_10_1'], 
      [
        { "name": "Address", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_LN_1", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_LN_2", "dataType": "String" }, 
        { "name": "MBR_INDV_BE_KEY", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_CITY_NM", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_ZIP_CD_5", "dataType": "String" }, 
        { "name": "ACTVTY_YR_MO_SK", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_CNTY_NM", "dataType": "String" }, 
        { "name": "MBR_HOME_ADDR_ST_CD", "dataType": "String" }
      ], 
      'keepOriginal', 
      [
        'MBR_HOME_ADDR_LN_1', 
        'MBR_HOME_ADDR_LN_2', 
        'MBR_HOME_ADDR_CITY_NM', 
        'MBR_HOME_ADDR_ST_CD', 
        'MBR_HOME_ADDR_CNTY_NM', 
        'MBR_HOME_ADDR_ZIP_CD_5', 
        'ACTVTY_YR_MO_SK', 
        'MBR_INDV_BE_KEY', 
        'Address'
      ], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

)

SELECT *

FROM Cleanse_11
