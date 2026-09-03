{{
  config({    
    "materialized": "table",
    "alias": "table_3124_Engine_Records_macro_ip",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_842 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_842')}}

),

Union_1837 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_1837')}}

),

AlteryxSelect_2993 AS (

  SELECT *
  
  FROM Union_1837 AS in0

),

Union_67_reformat_0 AS (

  SELECT 
    ACV AS ACV,
    `Actual Closed Date` AS `Actual Closed Date`,
    (TO_DATE(`Created Date`, 'yyyy-MM-dd')) AS `Created Date`,
    EndDate_Annualization AS EndDate_Annualization,
    (TO_DATE(`Expected Renewal Date`, 'yyyy-MM-dd')) AS `Expected Renewal Date`,
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    CAST(`Order: End Date (Calculated)` AS string) AS `Order: End Date (Calculated)`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    CAST(`Order: Start Date` AS string) AS `Order: Start Date`,
    Origin AS Origin,
    Product AS Product,
    `Product Code` AS `Product Code`,
    Quantity AS Quantity,
    CAST(Stage AS string) AS Stage,
    StartDate_Annualization AS StartDate_Annualization,
    TCV AS TCV
  
  FROM AlteryxSelect_2993 AS in0

),

Union_677 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_677')}}

),

Filter_696_reject AS (

  SELECT * 
  
  FROM Union_677 AS in0
  
  WHERE (
          (
            (
              NOT(
                ((UPPER(Product) = UPPER('CC Cloud')) AND (`Order: Activated Date` >= '2020-06-01'))
                AND (`Order: Activated Date` <= '2020-12-31'))
            )
            OR (
                 (
                   ((UPPER(Product) = UPPER('CC Cloud')) AND (`Order: Activated Date` >= '2020-06-01'))
                   AND (`Order: Activated Date` <= '2020-12-31')
                 ) IS NULL
               )
          )
          AND (
                (
                  NOT(
                    `Sum_Total Price (new)` = 0)
                ) OR (`Sum_Total Price (new)` IS NULL)
              )
        )

),

Join_884_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_884_inner')}}

),

Formula_708_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_708_0')}}

),

Join_700_left AS (

  SELECT in0.*
  
  FROM Join_884_inner AS in0
  ANTI JOIN Formula_708_0 AS in1
     ON (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Mas90 Customer Number`)

),

RecordID_833 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__RecordID_833')}}

),

Join_823_right AS (

  SELECT in0.*
  
  FROM RecordID_833 AS in0
  ANTI JOIN Summarize_842 AS in1
     ON (in1.RecordID = in0.RecordID)

),

Union_697 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_696_reject', 'Join_700_left', 'Join_823_right'], 
      [
        '[{"name": "Renewed_Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "String"}, {"name": "Order: Opportunity: Opportunity Name", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Renewal_Order: Activated Date", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Business Subtype", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Renewal_Order: Order", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Loser Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Order: Opportunity: Actual Closed Date", "dataType": "Date"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Item/Product", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Reduction Amount", "dataType": "Double"}, {"name": "Renewal_StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Renewal_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "Renewed_Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "String"}, {"name": "Order: Opportunity: Opportunity Name", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Renewal_Order: Activated Date", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Business Subtype", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Renewal_Order: Order", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Loser Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Order: Opportunity: Actual Closed Date", "dataType": "Date"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Item/Product", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Reduction Amount", "dataType": "Double"}, {"name": "Renewal_StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Updated Term", "dataType": "Double"}, {"name": "Renewal_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "Renewed_Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "String"}, {"name": "Order: Opportunity: Opportunity Name", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Renewal_Order: Activated Date", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Business Subtype", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Renewal_Order: Order", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Loser Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Order: Opportunity: Actual Closed Date", "dataType": "Date"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Item/Product", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Reduction Amount", "dataType": "Double"}, {"name": "Renewal_StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Renewal_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_1070 AS (

  SELECT 
    SUM(`PreReductions_Total Price`) AS `PreReductions_Total Price`,
    SUM(`Sum_Total Price (new)`) AS `Sum_Total Price (new)`,
    SUM(ACV) AS ACV,
    SUM(TCV) AS TCV,
    SUM(CAST(Sum_Quantity AS DECIMAL (19, 9))) AS Quantity,
    `Order: Account Name: Mas90 Customer Number` AS `Mas90 Customer Number`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Subscription Term` AS `Order: Subscription Term`,
    `Order: Order` AS `Order: Order`,
    `Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    StartDate_Annualization AS StartDate_Annualization,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Product Code` AS `Product Code`,
    `Order: Opportunity: Actual Closed Date` AS `Actual Closed Date`,
    Product AS Product,
    EndDate_Annualization AS EndDate_Annualization,
    `Order: Start Date` AS `Order: Start Date`
  
  FROM Union_697 AS in0
  
  GROUP BY 
    `Order: Account Name: Mas90 Customer Number`, 
    `Order: Sales Order Number`, 
    `Order: Subscription Term`, 
    `Order: Order`, 
    `Order: Opportunity: Renewed Contract: Order: Order`, 
    StartDate_Annualization, 
    `Order: End Date (Calculated)`, 
    `Order: Activated Date`, 
    `Product Code`, 
    `Order: Opportunity: Actual Closed Date`, 
    Product, 
    EndDate_Annualization, 
    `Order: Start Date`

),

Formula_62_0 AS (

  SELECT 
    CAST('Orders&OrdersProcessed' AS string) AS Origin,
    *
  
  FROM Summarize_1070 AS in0

),

Union_67_reformat_1 AS (

  SELECT 
    ACV AS ACV,
    `Actual Closed Date` AS `Actual Closed Date`,
    EndDate_Annualization AS EndDate_Annualization,
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    CAST(`Order: Activated Date` AS string) AS `Order: Activated Date`,
    CAST(`Order: End Date (Calculated)` AS string) AS `Order: End Date (Calculated)`,
    CAST(`Order: Opportunity: Renewed Contract: Order: Order` AS string) AS `Order: Opportunity: Renewed Contract: Order: Order`,
    CAST(`Order: Order` AS string) AS `Order: Order`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    CAST(`Order: Start Date` AS string) AS `Order: Start Date`,
    CAST(`Order: Subscription Term` AS string) AS `Order: Subscription Term`,
    Origin AS Origin,
    CAST(`PreReductions_Total Price` AS DOUBLE) AS `PreReductions_Total Price`,
    Product AS Product,
    `Product Code` AS `Product Code`,
    Quantity AS Quantity,
    StartDate_Annualization AS StartDate_Annualization,
    CAST(`Sum_Total Price (new)` AS DOUBLE) AS `Sum_Total Price (new)`,
    TCV AS TCV
  
  FROM Formula_62_0 AS in0

),

Union_67 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_67_reformat_1', 'Union_67_reformat_0'], 
      [
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "Date"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_1090 AS (

  SELECT * 
  
  FROM Union_67 AS in0
  
  WHERE (
          (
            NOT(
              UPPER(Stage) = UPPER('Closed Lost'))
          ) OR (Stage IS NULL)
        )

),

Summarize_726 AS (

  SELECT 
    SUM(`Sum_Total Price (new)`) AS `Sum_Total Price (new)`,
    SUM(ACV) AS ACV,
    SUM(TCV) AS TCV,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Subscription Term` AS `Order: Subscription Term`,
    StartDate_Annualization AS StartDate_Annualization,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    `Order: Activated Date` AS `Order: Activated Date`,
    Product AS Product,
    EndDate_Annualization AS EndDate_Annualization,
    `Order: Start Date` AS `Order: Start Date`
  
  FROM Filter_1090 AS in0
  
  GROUP BY 
    `Order: Sales Order Number`, 
    `Order: Subscription Term`, 
    StartDate_Annualization, 
    `Order: End Date (Calculated)`, 
    `Mas90 Customer Number`, 
    `Order: Activated Date`, 
    Product, 
    EndDate_Annualization, 
    `Order: Start Date`

),

MultiRowFormula_721_window AS (

  SELECT 
    *,
    lead(StartDate_Annualization, 1) OVER (PARTITION BY `Mas90 Customer Number`, Product ORDER BY `Mas90 Customer Number` ASC NULLS FIRST, Product ASC NULLS FIRST) AS StartDate_Annualization_lead1
  
  FROM Summarize_726 AS in0

),

MultiRowFormula_721_0 AS (

  SELECT 
    CAST(CAST(datediff(to_date(StartDate_Annualization_lead1), to_date(EndDate_Annualization)) AS INT) AS INT) AS GapDayCount,
    * EXCEPT (`StartDate_Annualization_lead1`)
  
  FROM MultiRowFormula_721_window AS in0

),

MultiRowFormula_710_window AS (

  SELECT 
    *,
    lead(EndDate_Annualization, 1) OVER (PARTITION BY `Mas90 Customer Number`, Product ORDER BY `Mas90 Customer Number` ASC NULLS FIRST, Product ASC NULLS FIRST) AS EndDate_Annualization_lead1,
    lead(StartDate_Annualization, 1) OVER (PARTITION BY `Mas90 Customer Number`, Product ORDER BY `Mas90 Customer Number` ASC NULLS FIRST, Product ASC NULLS FIRST) AS StartDate_Annualization_lead1,
    lead(ACV, 1) OVER (PARTITION BY `Mas90 Customer Number`, Product ORDER BY `Mas90 Customer Number` ASC NULLS FIRST, Product ASC NULLS FIRST) AS ACV_lead1
  
  FROM MultiRowFormula_721_0 AS in0

),

MultiRowFormula_710_0 AS (

  SELECT 
    CAST(CASE
      WHEN (
        (
          (
            (
              (year(StartDate_Annualization) < (year(EndDate_Annualization_lead1) - 1))
              AND (year(`Order: End Date (Calculated)`) = (year(EndDate_Annualization_lead1) - 1))
            )
            AND (
                  (
                    (NOT(YEAR (`Order: End Date (Calculated)`) = year(StartDate_Annualization_lead1)))
                    OR isnull(YEAR (`Order: End Date (Calculated)`))
                  )
                  OR isnull(YEAR (StartDate_Annualization_lead1))
                )
          )
          AND (GapDayCount > 60)
        )
        AND (ACV_lead1 > 0)
      )
        THEN 1
      ELSE 0
    END AS BOOLEAN) AS `Gap Flag`,
    * EXCEPT (`EndDate_Annualization_lead1`, `StartDate_Annualization_lead1`, `ACV_lead1`)
  
  FROM MultiRowFormula_710_window AS in0

),

Formula_948_0 AS (

  SELECT 
    (DATE_ADD(CAST(`Order: End Date (Calculated)` AS DATE), CAST(1 AS INTEGER))) AS StartDate_Annualization,
    * EXCEPT (`startdate_annualization`)
  
  FROM MultiRowFormula_710_0 AS in0

),

MultiRowFormula_947_window AS (

  SELECT 
    *,
    lead(`Order: Start Date`, 1) OVER (PARTITION BY `Mas90 Customer Number`, Product ORDER BY `Mas90 Customer Number` ASC NULLS FIRST, Product ASC NULLS FIRST) AS `Order: Start Date_lead1`
  
  FROM Formula_948_0 AS in0

),

MultiRowFormula_947_0 AS (

  SELECT 
    CASE
      WHEN (`Gap Flag` = 1)
        THEN date_add(`Order: Start Date_lead1`, CAST(-1 AS INT))
      ELSE EndDate_Annualization
    END AS EndDate_Annualization,
    * EXCEPT (`Order: Start Date_lead1`, `enddate_annualization`)
  
  FROM MultiRowFormula_947_window AS in0

),

Filter_949 AS (

  SELECT * 
  
  FROM MultiRowFormula_947_0 AS in0
  
  WHERE CAST(`Gap Flag` AS BOOLEAN)

),

Formula_1168_0 AS (

  SELECT 
    CAST(CASE
      WHEN ((`Order: Start Date` <= '2016-02-28') AND (`Order: End Date (Calculated)` >= '2016-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2020-02-28') AND (`Order: End Date (Calculated)` >= '2020-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2024-02-28') AND (`Order: End Date (Calculated)` >= '2024-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2028-02-28') AND (`Order: End Date (Calculated)` >= '2028-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      ELSE (CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT) + 1)
    END AS DOUBLE) AS TS_ContractDays,
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
  
  FROM Filter_949 AS in0

),

Formula_1168_1 AS (

  SELECT 
    CAST(((`Sum_Total Price (new)` / TS_ContractDays) * 365.25) AS DOUBLE) AS ACV,
    * EXCEPT (`acv`)
  
  FROM Formula_1168_0 AS in0

),

Formula_1168_2 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    CAST('Orders&OrdersProcessed' AS string) AS Origin,
    * EXCEPT (`tcv`)
  
  FROM Formula_1168_1 AS in0

),

AlteryxSelect_723 AS (

  SELECT * EXCEPT (`Order: Sales Order Number`, `Order: Subscription Term`, `Order: Activated Date`, `GapDayCount`, `Gap Flag`)
  
  FROM Formula_1168_2 AS in0

),

Union_845 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_67', 'AlteryxSelect_723'], 
      [
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_3120 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_845'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Formula_3125_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_3125_0')}}

),

Union_3121 AS (

  {{
    prophecy_basics.UnionByName(
      ['RecordID_3120', 'Formula_3125_0'], 
      [
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "Quantity", "dataType": "Double"}, {"name": "F9", "dataType": "Double"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "TCV", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_3122 AS (

  SELECT 
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Actual Closed Date` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Actual Closed Date` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Actual Closed Date` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Actual Closed Date` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Actual Closed Date` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS `Actual Closed Date`,
    * EXCEPT (`Actual Closed Date`)
  
  FROM Union_3121 AS in0

),

`Macro_3124_Engine Records` AS (

  SELECT 
    RecordID AS RecordID,
    ManualRecordID AS ManualRecordID,
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    Product AS Product,
    `Product Code` AS `Product Code`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    StartDate_Annualization AS StartDate_Annualization,
    EndDate_Annualization AS EndDate_Annualization,
    `Order: Start Date` AS `Order: Start Date`,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Order: Subscription Term` AS `Order: Subscription Term`,
    `Order: Order` AS `Order: Order`,
    `PreReductions_Total Price` AS `PreReductions_Total Price`,
    `Sum_Total Price (new)` AS `Sum_Total Price (new)`,
    ACV AS ACV,
    TCV AS TCV,
    Quantity AS Quantity,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Actual Closed Date` AS `Actual Closed Date`,
    Origin AS Origin,
    `Expected Renewal Date` AS `Expected Renewal Date`,
    `Created Date` AS `Created Date`,
    Stage AS Stage,
    * EXCEPT (`RecordID`, 
    `ManualRecordID`, 
    `Mas90 Customer Number`, 
    `Product`, 
    `Product Code`, 
    `Order: Sales Order Number`, 
    `Order: Opportunity: Renewed Contract: Order: Order`, 
    `StartDate_Annualization`, 
    `EndDate_Annualization`, 
    `Order: Start Date`, 
    `Order: End Date (Calculated)`, 
    `Order: Subscription Term`, 
    `Order: Order`, 
    `PreReductions_Total Price`, 
    `Sum_Total Price (new)`, 
    `ACV`, 
    `TCV`, 
    `Quantity`, 
    `Order: Activated Date`, 
    `Actual Closed Date`, 
    `Origin`, 
    `Expected Renewal Date`, 
    `Created Date`, 
    `Stage`)
  
  FROM AlteryxSelect_3122 AS in0

)

SELECT *

FROM Macro_3124_Engine AS Records
