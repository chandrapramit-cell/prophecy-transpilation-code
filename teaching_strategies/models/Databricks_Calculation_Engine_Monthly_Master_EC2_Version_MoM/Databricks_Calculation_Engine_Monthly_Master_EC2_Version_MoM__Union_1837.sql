{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH TextInput_2348 AS (

  SELECT * 
  
  FROM {{ ref('seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_2348')}}

),

TextInput_2348_cast AS (

  SELECT 
    CAST(`Mas90 Customer Number` AS string) AS `Mas90 Customer Number`,
    CAST(`Acquired ARR from ReadyRosie` AS DOUBLE) AS `Acquired ARR from ReadyRosie`,
    CAST(`Account Name` AS string) AS `Account Name`,
    CAST(`Acquired ARR from Quorum (QualityAssist)` AS INTEGER) AS `Acquired ARR from Quorum (QualityAssist)`
  
  FROM TextInput_2348 AS in0

),

Filter_2356 AS (

  SELECT * 
  
  FROM TextInput_2348_cast AS in0
  
  WHERE TRUE

),

AlteryxSelect_2349 AS (

  SELECT 
    CAST(`Mas90 Customer Number` AS string) AS `Mas90 Customer Number`,
    `Acquired ARR from ReadyRosie` AS `Acquired ARR from ReadyRosie`,
    CAST(`Acquired ARR from Quorum (QualityAssist)` AS DOUBLE) AS `Acquired ARR from Quorum (QualityAssist)`,
    `Account Name` AS `Account Name`,
    * EXCEPT (`Mas90 Customer Number`, 
    `Acquired ARR from ReadyRosie`, 
    `Acquired ARR from Quorum (QualityAssist)`, 
    `Account Name`)
  
  FROM Filter_2356 AS in0

),

Formula_2352_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          NOT(
            (`Acquired ARR from ReadyRosie` IS NULL)
            OR ((LENGTH(CAST(`Acquired ARR from ReadyRosie` AS string))) = 0))
        )
          THEN 'ReadyRosie'
        WHEN (
          NOT(
            (`Acquired ARR from Quorum (QualityAssist)` IS NULL)
            OR ((LENGTH(CAST(`Acquired ARR from Quorum (QualityAssist)` AS string))) = 0))
        )
          THEN 'Quorum'
        ELSE 'NA'
      END
    ) AS string) AS Product,
    *
  
  FROM AlteryxSelect_2349 AS in0

),

Formula_2352_1 AS (

  SELECT 
    (
      TO_DATE(
        (
          CASE
            WHEN (UPPER(Product) = UPPER('Quorum'))
              THEN '2022-03-01'
            ELSE '2019-06-01'
          END
        ), 
        'yyyy-MM-dd')
    ) AS StartDate_Annualization,
    *
  
  FROM Formula_2352_0 AS in0

),

Formula_2352_2 AS (

  SELECT 
    (
      TO_DATE(
        (
          CONCAT(
            (
              REGEXP_REPLACE(
                (REGEXP_REPLACE((FORMAT_NUMBER(CAST(EXTRACT(YEAR FROM StartDate_Annualization) AS DOUBLE), 0)), ',', '__THS__')), 
                '__THS__', 
                '')
            ), 
            '-12-31')
        ))
    ) AS EndDate_Annualization,
    CAST((
      CASE
        WHEN (UPPER(Product) = UPPER('Quorum'))
          THEN `Acquired ARR from Quorum (QualityAssist)`
        ELSE `Acquired ARR from ReadyRosie`
      END
    ) AS DOUBLE) AS TCV,
    CAST('Acquisition Renewal' AS string) AS Origin,
    (DATE_ADD((ADD_MONTHS(StartDate_Annualization, 12)), CAST(-1 AS INTEGER))) AS `Actual Closed Date`,
    CAST('Closed Lost' AS string) AS Stage,
    *
  
  FROM Formula_2352_1 AS in0

),

Filter_2350 AS (

  SELECT * 
  
  FROM Formula_2352_2 AS in0
  
  WHERE (
          (
            NOT(
              (LENGTH(Product)) = 0)
          ) AND ((LENGTH(`Mas90 Customer Number`)) = 0)
        )

),

Formula_2351_0 AS (

  SELECT 
    CAST(`Account Name` AS string) AS `Mas90 Customer Number`,
    * EXCEPT (`mas90 customer number`)
  
  FROM Filter_2350 AS in0

),

Filter_2350_reject AS (

  SELECT * 
  
  FROM Formula_2352_2 AS in0
  
  WHERE (
          (
            NOT(
              (LENGTH(Product)) = 0)
          )
          AND (
                (
                  NOT(
                    (LENGTH(`Mas90 Customer Number`)) = 0)
                )
                OR (((LENGTH(`Mas90 Customer Number`)) = 0) IS NULL)
              )
        )

),

Union_2354 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_2350_reject', 'Formula_2351_0'], 
      [
        '[{"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "TCV", "dataType": "Double"}, {"name": "Origin", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Product", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Acquired ARR from ReadyRosie", "dataType": "Double"}, {"name": "Acquired ARR from Quorum (QualityAssist)", "dataType": "Double"}, {"name": "Account Name", "dataType": "String"}]', 
        '[{"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "TCV", "dataType": "Double"}, {"name": "Origin", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Product", "dataType": "String"}, {"name": "Acquired ARR from ReadyRosie", "dataType": "Double"}, {"name": "Acquired ARR from Quorum (QualityAssist)", "dataType": "Double"}, {"name": "Account Name", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_2355 AS (

  SELECT * EXCEPT (`Acquired ARR from ReadyRosie`, `Acquired ARR from Quorum (QualityAssist)`, `Account Name`)
  
  FROM Union_2354 AS in0

),

Summarize_1067 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_1067')}}

),

Union_1837 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_1067', 'AlteryxSelect_2355'], 
      [
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "Date"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "Date"}]', 
        '[{"name": "Actual Closed Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_1837
