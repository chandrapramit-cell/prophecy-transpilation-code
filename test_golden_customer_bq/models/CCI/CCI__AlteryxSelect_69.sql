{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH dcm_phpdatabase_49 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'dcm_phpdatabase_49_ref') }}

),

AlteryxSelect_53 AS (

  SELECT 
    NULL AS Month,
    *
  
  FROM dcm_phpdatabase_49 AS in0

),

Unique_54 AS (

  SELECT * 
  
  FROM AlteryxSelect_53 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY EFF_DATE, CATEGORY ORDER BY EFF_DATE, CATEGORY) = 1

),

dcm_phpdatabase_47 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'dcm_phpdatabase_47_ref') }}

),

AlteryxSelect_52 AS (

  SELECT 
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(Birth_Date AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(Birth_Date AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(Birth_Date AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(Birth_Date AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(Birth_Date AS STRING)) AS DATE)
      END
    ) AS Birth_Date,
    * EXCEPT (`Birth_Date`)
  
  FROM dcm_phpdatabase_47 AS in0

),

AppendFields_55 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Unique_54 AS in0
  INNER JOIN AlteryxSelect_52 AS in1
     ON true

),

Filter_56 AS (

  SELECT * 
  
  FROM AppendFields_55 AS in0
  
  WHERE (Birth_Date < EFF_DATE)

),

Formula_60_0 AS (

  SELECT 
    (
      CASE
        WHEN (((DATE_DIFF((PARSE_DATE('%Y-%m-%d', EFF_DATE)), (PARSE_DATE('%Y-%m-%d', Birth_Date)), MONTH)) / 12) > 79)
          THEN 4
        WHEN (((DATE_DIFF((PARSE_DATE('%Y-%m-%d', EFF_DATE)), (PARSE_DATE('%Y-%m-%d', Birth_Date)), MONTH)) / 12) > 69)
          THEN 3
        WHEN (((DATE_DIFF((PARSE_DATE('%Y-%m-%d', EFF_DATE)), (PARSE_DATE('%Y-%m-%d', Birth_Date)), MONTH)) / 12) > 59)
          THEN 2
        WHEN (((DATE_DIFF((PARSE_DATE('%Y-%m-%d', EFF_DATE)), (PARSE_DATE('%Y-%m-%d', Birth_Date)), MONTH)) / 12) > 49)
          THEN 1
        ELSE 0
      END
    ) AS POINTS,
    *
  
  FROM Filter_56 AS in0

),

AlteryxSelect_69 AS (

  SELECT 
    Sys_Member_ID AS MemberId,
    * EXCEPT (`Birth_Date`, `Sys_Member_ID`)
  
  FROM Formula_60_0 AS in0

)

SELECT *

FROM AlteryxSelect_69
