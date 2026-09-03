{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_1012_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1012_inner')}}

),

Formula_1013_to_Formula_1169_0 AS (

  SELECT 
    (TO_DATE((DATE_TRUNC('month', `Primary Quote: Start Date`)), 'yyyy-MM-dd')) AS StartDate_Annualization,
    to_date(
      CASE
        WHEN (substring(CAST(`Primary Quote: Start Date` AS STRING), 1, 7) = substring(CAST(`Primary Quote: End Date (Calculated)` AS STRING), 1, 7))
          THEN to_date(last_day(`Primary Quote: End Date (Calculated)`))
        WHEN (
          (to_date(date_trunc('month', `Primary Quote: Start Date`)) = `Primary Quote: Start Date`)
          AND (to_date(last_day(`Primary Quote: End Date (Calculated)`)) = `Primary Quote: End Date (Calculated)`)
        )
          THEN `Primary Quote: End Date (Calculated)`
        ELSE date_add(date_trunc('month', `Primary Quote: End Date (Calculated)`), CAST(-1 AS INT))
      END, 
      'yyyy-MM-dd') AS EndDate_Annualization,
    *
  
  FROM Join_1012_inner AS in0

),

Formula_1013_to_Formula_1169_1 AS (

  SELECT 
    to_date(
      CASE
        WHEN (Original_EndDate_Annualization > to_date(date_trunc('month', `Primary Quote: Start Date`)))
          THEN date_add(Original_EndDate_Annualization, CAST(1 AS INT))
        ELSE StartDate_Annualization
      END, 
      'yyyy-MM-dd') AS StartDate_Annualization,
    * EXCEPT (`startdate_annualization`)
  
  FROM Formula_1013_to_Formula_1169_0 AS in0

),

Formula_1013_to_Formula_1169_2 AS (

  SELECT 
    to_date(
      CASE
        WHEN (Original_EndDate_Annualization > to_date(date_trunc('month', `Primary Quote: Start Date`)))
          THEN date_add(
            date_trunc(
              'month', 
              date_add(
                StartDate_Annualization, 
                CAST((
                  CAST(datediff(to_date(`Primary Quote: End Date (Calculated)`), to_date(`Primary Quote: Start Date`)) AS INT)
                  + 1
                ) AS INT))), 
            CAST(-1 AS INT))
        ELSE EndDate_Annualization
      END, 
      'yyyy-MM-dd') AS EndDate_Annualization,
    CAST(`Orders ACV` AS DOUBLE) AS ACV,
    * EXCEPT (`enddate_annualization`)
  
  FROM Formula_1013_to_Formula_1169_1 AS in0

),

AlteryxSelect_1046 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_1046')}}

),

Formula_1013_to_Formula_1169_3 AS (

  SELECT 
    CAST(CASE
      WHEN (
        (StartDate_Annualization <= to_date('2016-02-28'))
        AND (EndDate_Annualization >= to_date('2016-02-29'))
      )
        THEN CAST(datediff(to_date(EndDate_Annualization), to_date(StartDate_Annualization)) AS INT)
      WHEN (
        (StartDate_Annualization <= to_date('2020-02-28'))
        AND (EndDate_Annualization >= to_date('2020-02-29'))
      )
        THEN CAST(datediff(to_date(EndDate_Annualization), to_date(StartDate_Annualization)) AS INT)
      WHEN (
        (StartDate_Annualization <= to_date('2024-02-28'))
        AND (EndDate_Annualization >= to_date('2024-02-29'))
      )
        THEN CAST(datediff(to_date(EndDate_Annualization), to_date(StartDate_Annualization)) AS INT)
      WHEN (
        (StartDate_Annualization <= to_date('2028-02-28'))
        AND (EndDate_Annualization >= to_date('2028-02-29'))
      )
        THEN CAST(datediff(to_date(EndDate_Annualization), to_date(StartDate_Annualization)) AS INT)
      ELSE (CAST(datediff(to_date(EndDate_Annualization), to_date(StartDate_Annualization)) AS INT) + 1)
    END AS DOUBLE) AS Engine_ContractDays,
    *
  
  FROM Formula_1013_to_Formula_1169_2 AS in0

),

Formula_1013_to_Formula_1169_4 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    CAST('Renewals' AS string) AS Origin,
    *
  
  FROM Formula_1013_to_Formula_1169_3 AS in0

),

Union_1045 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_1046', 'Formula_1013_to_Formula_1169_4'], 
      [
        '[{"name": "Orders Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Primary Quote: End Date (Calculated)", "dataType": "Date"}, {"name": "Right_Quantity", "dataType": "Double"}, {"name": "Renewed Contract: Order: Sales Order Number", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Primary Quote: Start Date", "dataType": "Date"}, {"name": "Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Left_Left_Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Renewed Contract: Contract Number", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Orders ACV", "dataType": "Double"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Opportunity Name", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}]', 
        '[{"name": "Orders Quantity", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Primary Quote: End Date (Calculated)", "dataType": "Date"}, {"name": "Renewed Contract: Order: Sales Order Number", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Primary Quote: Start Date", "dataType": "Date"}, {"name": "Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Left_Left_Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Renewed Contract: Contract Number", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Orders ACV", "dataType": "Double"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Opportunity Name", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_1067 AS (

  SELECT 
    SUM(ACV) AS ACV,
    SUM(TCV) AS TCV,
    SUM(Quantity) AS Quantity,
    `Actual Closed Date` AS `Actual Closed Date`,
    `Primary Quote: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Renewed Contract: Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Created Date` AS `Created Date`,
    StartDate_Annualization AS StartDate_Annualization,
    Origin AS Origin,
    `Primary Quote: Start Date` AS `Order: Start Date`,
    `Account Name: Mas90 Customer Number` AS `Mas90 Customer Number`,
    `Expected Renewal Date` AS `Expected Renewal Date`,
    `Product Code` AS `Product Code`,
    Product AS Product,
    Stage AS Stage,
    EndDate_Annualization AS EndDate_Annualization
  
  FROM Union_1045 AS in0
  
  GROUP BY 
    `Actual Closed Date`, 
    `Primary Quote: End Date (Calculated)`, 
    `Renewed Contract: Order: Sales Order Number`, 
    `Created Date`, 
    StartDate_Annualization, 
    Origin, 
    `Primary Quote: Start Date`, 
    `Account Name: Mas90 Customer Number`, 
    `Expected Renewal Date`, 
    `Product Code`, 
    Product, 
    Stage, 
    EndDate_Annualization

)

SELECT *

FROM Summarize_1067
