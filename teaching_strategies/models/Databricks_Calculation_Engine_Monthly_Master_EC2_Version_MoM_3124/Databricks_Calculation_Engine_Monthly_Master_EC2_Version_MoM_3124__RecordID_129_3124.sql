{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_171_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_171_3124')}}

),

Filter_107_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_107_3124')}}

),

Join_170_3124_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Mas90 Customer Number`, `Product`)
  
  FROM Filter_107_3124 AS in0
  INNER JOIN Summarize_171_3124 AS in1
     ON ((in0.`Mas90 Customer Number` = in1.`Mas90 Customer Number`) AND (in0.Product = in1.Product))

),

Summarize_118_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_118_3124')}}

),

Summarize_116_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_116_3124')}}

),

Join_112_3124_right AS (

  SELECT in0.*
  
  FROM Summarize_118_3124 AS in0
  ANTI JOIN Summarize_116_3124 AS in1
     ON (
      ((in1.`Mas90 Customer Number` = in0.`Mas90 Customer Number`) AND (in1.Product = in0.Product))
      AND (in1.ARRMonth = in0.ARRMonth)
    )

),

Summarize_119_3124 AS (

  SELECT DISTINCT RecordID AS RecordID
  
  FROM Join_112_3124_right AS in0

),

Join_120_3124_inner AS (

  SELECT 
    in0.* EXCEPT (`RecordID`),
    in1.*
  
  FROM Join_170_3124_inner AS in0
  INNER JOIN Summarize_119_3124 AS in1
     ON (in0.RecordID = in1.RecordID)

),

Summarize_114_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_114_3124')}}

),

Join_81_3124_inner AS (

  SELECT 
    in0.RecordID AS RecordID,
    in0.ManualRecordID AS ManualRecordID,
    in0.`Mas90 Customer Number` AS `Mas90 Customer Number`,
    in0.Product AS Product,
    in0.`Product Code` AS `Product Code`,
    in0.`Order: Sales Order Number` AS `Order: Sales Order Number`,
    in0.`Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    in0.StartDate_Annualization AS StartDate_Annualization,
    in0.EndDate_Annualization AS EndDate_Annualization,
    in1.StartDate_Annualization AS Manual_StartDate_Annualization,
    in1.EndDate_Annualization AS Manual_EndDate_Annualization,
    in0.`Order: Start Date` AS `Order: Start Date`,
    in0.`Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    in0.`Order: Subscription Term` AS `Order: Subscription Term`,
    in0.`Order: Order` AS `Order: Order`,
    in0.`PreReductions_Total Price` AS `PreReductions_Total Price`,
    in0.`Sum_Total Price (new)` AS `Sum_Total Price (new)`,
    in0.ACV AS ACV,
    in0.TCV AS TCV,
    in0.Quantity AS Quantity,
    in0.`Order: Activated Date` AS `Order: Activated Date`,
    in0.`Actual Closed Date` AS `Actual Closed Date`,
    in0.Origin AS Origin,
    in0.`Expected Renewal Date` AS `Expected Renewal Date`,
    in0.`Created Date` AS `Created Date`,
    in0.Stage AS Stage,
    in0.MacroRecordID AS MacroRecordID,
    in0.* EXCEPT (`RecordID`, 
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
    `Stage`, 
    `MacroRecordID`),
    in1.* EXCEPT (`StartDate_Annualization`, `EndDate_Annualization`, `Mas90 Customer Number`, `Product`)
  
  FROM Join_120_3124_inner AS in0
  INNER JOIN Summarize_114_3124 AS in1
     ON ((in0.`Mas90 Customer Number` = in1.`Mas90 Customer Number`) AND (in0.Product = in1.Product))

),

Formula_83_3124_to_Formula_86_3124_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (StartDate_Annualization <= Manual_EndDate_Annualization)
          AND (EndDate_Annualization >= Manual_StartDate_Annualization)
        )
          THEN 1
        ELSE 0
      END
    ) AS BOOLEAN) AS `Date Overlap Flag`,
    *
  
  FROM Join_81_3124_inner AS in0

),

Formula_83_3124_to_Formula_86_3124_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          ((`Date Overlap Flag` = TRUE) AND (EndDate_Annualization > Manual_EndDate_Annualization))
          AND (StartDate_Annualization < Manual_StartDate_Annualization)
        )
          THEN 'Data period longer than manual'
        WHEN (
          (
            (Manual_StartDate_Annualization <= StartDate_Annualization)
            AND (StartDate_Annualization < Manual_EndDate_Annualization)
          )
          AND (EndDate_Annualization > Manual_EndDate_Annualization)
        )
          THEN 'Start date pushed'
        WHEN (
          (
            (Manual_StartDate_Annualization > StartDate_Annualization)
            AND (EndDate_Annualization <= Manual_EndDate_Annualization)
          )
          AND (EndDate_Annualization > Manual_StartDate_Annualization)
        )
          THEN 'End date cut'
        ELSE 'No overlap'
      END
    ) AS string) AS `Filtering Review`,
    (TO_DATE(StartDate_Annualization, 'yyyy-MM-dd')) AS Original_StartDate_Annualization,
    (TO_DATE(EndDate_Annualization, 'yyyy-MM-dd')) AS Original_EndDate_Annualization,
    *
  
  FROM Formula_83_3124_to_Formula_86_3124_0 AS in0

),

Formula_83_3124_to_Formula_86_3124_2 AS (

  SELECT 
    (
      TO_DATE(
        (
          CASE
            WHEN (UPPER(`Filtering Review`) = UPPER('End date cut'))
              THEN (DATE_ADD(Manual_StartDate_Annualization, CAST(-1 AS INTEGER)))
            ELSE EndDate_Annualization
          END
        ), 
        'yyyy-MM-dd')
    ) AS EndDate_Annualization,
    (
      TO_DATE(
        (
          CASE
            WHEN (UPPER(`Filtering Review`) = UPPER('Start date pushed'))
              THEN (DATE_ADD(Manual_EndDate_Annualization, CAST(1 AS INTEGER)))
            ELSE StartDate_Annualization
          END
        ), 
        'yyyy-MM-dd')
    ) AS StartDate_Annualization,
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
    * EXCEPT (`startdate_annualization`, `ts_contractdays`, `enddate_annualization`)
  
  FROM Formula_83_3124_to_Formula_86_3124_1 AS in0

),

Formula_83_3124_to_Formula_86_3124_3 AS (

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
    CAST(((ACV * TS_ContractDays) / 365.25) AS DOUBLE) AS `Original Amount`,
    * EXCEPT (`engine_contractdays`)
  
  FROM Formula_83_3124_to_Formula_86_3124_2 AS in0

),

Formula_83_3124_to_Formula_86_3124_4 AS (

  SELECT 
    CAST(((`Original Amount` / TS_ContractDays) * 365.25) AS DOUBLE) AS ACV,
    * EXCEPT (`acv`)
  
  FROM Formula_83_3124_to_Formula_86_3124_3 AS in0

),

Formula_83_3124_to_Formula_86_3124_5 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    * EXCEPT (`tcv`)
  
  FROM Formula_83_3124_to_Formula_86_3124_4 AS in0

),

Filter_84_3124_to_Filter_100_3124 AS (

  SELECT * 
  
  FROM Formula_83_3124_to_Formula_86_3124_5 AS in0
  
  WHERE (
          (
            (`Date Overlap Flag` = TRUE)
            AND (
                  (NOT(CAST(`Filtering Review` AS string) IN ('End date cut', 'Start date pushed')))
                  OR (`Filtering Review` IS NULL)
                )
          )
          AND (UPPER(`Filtering Review`) = UPPER('Data period longer than manual'))
        )

),

Formula_101_3124_0 AS (

  SELECT 
    (DATE_ADD(Manual_EndDate_Annualization, CAST(1 AS INTEGER))) AS `2nd Start Date`,
    (TO_DATE(EndDate_Annualization, 'yyyy-MM-dd')) AS `2nd End Date`,
    *
  
  FROM Filter_84_3124_to_Filter_100_3124 AS in0

),

Formula_99_3124_0 AS (

  SELECT 
    (TO_DATE(StartDate_Annualization, 'yyyy-MM-dd')) AS `1st Start Date`,
    (DATE_ADD(Manual_StartDate_Annualization, CAST(-1 AS INTEGER))) AS `1st End Date`,
    *
  
  FROM Filter_84_3124_to_Filter_100_3124 AS in0

),

Union_102_3124 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_99_3124_0', 'Formula_101_3124_0'], 
      [
        '[{"name": "1st Start Date", "dataType": "Date"}, {"name": "1st End Date", "dataType": "Date"}, {"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Original Amount", "dataType": "Double"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}]', 
        '[{"name": "2nd Start Date", "dataType": "Date"}, {"name": "2nd End Date", "dataType": "Date"}, {"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Original Amount", "dataType": "Double"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_104_3124_0 AS (

  SELECT 
    to_date(
      CASE
        WHEN (isnull(`1st Start Date`) OR (length(CAST(`1st Start Date` AS STRING)) = 0))
          THEN `2nd Start Date`
        ELSE `1st Start Date`
      END, 
      'yyyy-MM-dd') AS StartDate_Annualization,
    to_date(
      CASE
        WHEN (isnull(`1st End Date`) OR (length(CAST(`1st End Date` AS STRING)) = 0))
          THEN `2nd End Date`
        ELSE `1st End Date`
      END, 
      'yyyy-MM-dd') AS EndDate_Annualization,
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
    * EXCEPT (`startdate_annualization`, `ts_contractdays`, `enddate_annualization`)
  
  FROM Union_102_3124 AS in0

),

Formula_104_3124_1 AS (

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
    * EXCEPT (`engine_contractdays`)
  
  FROM Formula_104_3124_0 AS in0

),

AlteryxSelect_105_3124 AS (

  SELECT * EXCEPT (`1st Start Date`, `1st End Date`, `2nd Start Date`, `2nd End Date`)
  
  FROM Formula_104_3124_1 AS in0

),

Formula_103_3124_0 AS (

  SELECT 
    CAST(((ACV * TS_ContractDays) / 365.25) AS DOUBLE) AS `Original Amount`,
    * EXCEPT (`original amount`)
  
  FROM AlteryxSelect_105_3124 AS in0

),

Filter_84_3124_reject AS (

  SELECT * 
  
  FROM Formula_83_3124_to_Formula_86_3124_5 AS in0
  
  WHERE (
          (
            NOT(
              (`Date Overlap Flag` = TRUE)
              AND (
                    (NOT(CAST(`Filtering Review` AS string) IN ('End date cut', 'Start date pushed')))
                    OR (`Filtering Review` IS NULL)
                  ))
          )
          OR (
               (
                 (`Date Overlap Flag` = TRUE)
                 AND (
                       (NOT(CAST(`Filtering Review` AS string) IN ('End date cut', 'Start date pushed')))
                       OR (`Filtering Review` IS NULL)
                     )
               ) IS NULL
             )
        )

),

AlteryxSelect_85_3124 AS (

  SELECT * EXCEPT (`Original Amount`)
  
  FROM Filter_84_3124_reject AS in0

),

Summarize_88_3124 AS (

  SELECT 
    COUNT(DISTINCT `Filtering Review`) AS `CountDistinct_Filtering Review`,
    RecordID AS RecordID
  
  FROM AlteryxSelect_85_3124 AS in0
  
  GROUP BY RecordID

),

Filter_90_3124 AS (

  SELECT * 
  
  FROM AlteryxSelect_85_3124 AS in0
  
  WHERE (UPPER(`Filtering Review`) = UPPER('No overlap'))

),

Summarize_89_3124 AS (

  SELECT 
    COUNT(DISTINCT `Filtering Review`) AS `CountDistinct_Filtering Review`,
    RecordID AS RecordID
  
  FROM Filter_90_3124 AS in0
  
  GROUP BY RecordID

),

Join_91_3124_inner AS (

  SELECT 
    in0.`CountDistinct_Filtering Review` AS TotalDistinct,
    in1.`CountDistinct_Filtering Review` AS NoOverlap,
    in0.* EXCEPT (`CountDistinct_Filtering Review`),
    in1.* EXCEPT (`RecordID`, `CountDistinct_Filtering Review`)
  
  FROM Summarize_88_3124 AS in0
  INNER JOIN Summarize_89_3124 AS in1
     ON (in0.RecordID = in1.RecordID)

),

Filter_92_3124_reject AS (

  SELECT * 
  
  FROM Join_91_3124_inner AS in0
  
  WHERE (
          (
            NOT(
              (
                (
                  NOT(
                    TotalDistinct = NoOverlap)
                ) OR (TotalDistinct IS NULL)
              )
              OR (NoOverlap IS NULL))
          )
          OR (
               (
                 (
                   (
                     NOT(
                       TotalDistinct = NoOverlap)
                   ) OR (TotalDistinct IS NULL)
                 )
                 OR (NoOverlap IS NULL)
               ) IS NULL
             )
        )

),

Join_97_3124_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`RecordID`, `TotalDistinct`, `NoOverlap`)
  
  FROM AlteryxSelect_85_3124 AS in0
  INNER JOIN Filter_92_3124_reject AS in1
     ON (in0.RecordID = in1.RecordID)

),

Filter_92_3124 AS (

  SELECT * 
  
  FROM Join_91_3124_inner AS in0
  
  WHERE (
          (
            (
              NOT(
                TotalDistinct = NoOverlap)
            ) OR (TotalDistinct IS NULL)
          )
          OR (NoOverlap IS NULL)
        )

),

Join_97_3124_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_85_3124 AS in0
  ANTI JOIN Filter_92_3124_reject AS in1
     ON (in0.RecordID = in1.RecordID)

),

Join_93_3124_left AS (

  SELECT in0.*
  
  FROM Join_97_3124_left AS in0
  ANTI JOIN Filter_92_3124 AS in1
     ON (in0.RecordID = in1.RecordID)

),

Formula_103_3124_1 AS (

  SELECT 
    CAST(((`Original Amount` / TS_ContractDays) * 365.25) AS DOUBLE) AS ACV,
    * EXCEPT (`acv`)
  
  FROM Formula_103_3124_0 AS in0

),

Formula_103_3124_2 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    * EXCEPT (`tcv`)
  
  FROM Formula_103_3124_1 AS in0

),

Filter_107_3124_reject AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Filter_107_3124_reject')}}

),

Join_93_3124_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`RecordID`, `TotalDistinct`, `NoOverlap`)
  
  FROM Join_97_3124_left AS in0
  INNER JOIN Filter_92_3124 AS in1
     ON (in0.RecordID = in1.RecordID)

),

Filter_94_3124 AS (

  SELECT * 
  
  FROM Join_93_3124_inner AS in0
  
  WHERE (
          NOT(
            UPPER(`Filtering Review`) = UPPER('No overlap'))
        )

),

Union_82_3124 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Filter_94_3124', 
        'Formula_103_3124_2', 
        'Filter_107_3124_reject', 
        'Join_97_3124_inner', 
        'Join_93_3124_left'
      ], 
      [
        '[{"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}]', 
        '[{"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Original Amount", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}]', 
        '[{"name": "MacroRecordID", "dataType": "Integer"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "F9", "dataType": "Double"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "Stage", "dataType": "String"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}]', 
        '[{"name": "TCV", "dataType": "Double"}, {"name": "ACV", "dataType": "Double"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Filtering Review", "dataType": "String"}, {"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Date Overlap Flag", "dataType": "Boolean"}, {"name": "RecordID", "dataType": "Integer"}, {"name": "ManualRecordID", "dataType": "Integer"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Manual_StartDate_Annualization", "dataType": "Date"}, {"name": "Manual_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "Double"}, {"name": "Order: Order", "dataType": "String"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "Actual Closed Date", "dataType": "Date"}, {"name": "Origin", "dataType": "String"}, {"name": "Expected Renewal Date", "dataType": "Date"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Stage", "dataType": "String"}, {"name": "MacroRecordID", "dataType": "Integer"}, {"name": "F9", "dataType": "Double"}, {"name": "ContractTermDays", "dataType": "Double"}, {"name": "Customer Data ARR", "dataType": "Double"}, {"name": "Notes", "dataType": "String"}, {"name": "prophecy_recordId_564", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

MultiRowFormula_131_3124_window AS (

  SELECT 
    *,
    lag(`Filtering Review`, 1) OVER (PARTITION BY RecordID, `Mas90 Customer Number`, Product ORDER BY RecordID ASC NULLS FIRST, `Mas90 Customer Number` ASC NULLS FIRST, Product ASC NULLS FIRST) AS `Filtering Review_lag1`,
    lag(Manual_EndDate_Annualization, 1) OVER (PARTITION BY RecordID, `Mas90 Customer Number`, Product ORDER BY RecordID ASC NULLS FIRST, `Mas90 Customer Number` ASC NULLS FIRST, Product ASC NULLS FIRST) AS Manual_EndDate_Annualization_lag1,
    lag(Manual_StartDate_Annualization, 1) OVER (PARTITION BY RecordID, `Mas90 Customer Number`, Product ORDER BY RecordID ASC NULLS FIRST, `Mas90 Customer Number` ASC NULLS FIRST, Product ASC NULLS FIRST) AS Manual_StartDate_Annualization_lag1
  
  FROM Union_82_3124 AS in0

),

MultiRowFormula_131_3124_0 AS (

  SELECT 
    CAST(CASE
      WHEN (
        (
          (
            (
              (
                NOT(
                  `Filtering Review_lag1` = 'End date cut')
              ) OR isnull(`Filtering Review_lag1`)
            )
            AND (`Filtering Review` = 'End date cut')
          )
          AND (StartDate_Annualization <= Manual_EndDate_Annualization_lag1)
        )
        AND (EndDate_Annualization >= Manual_StartDate_Annualization_lag1)
      )
        THEN 1
      ELSE 0
    END AS BOOLEAN) AS `Duplication Safeguard Flag`,
    * EXCEPT (`Filtering Review_lag1`, `Manual_EndDate_Annualization_lag1`, `Manual_StartDate_Annualization_lag1`)
  
  FROM MultiRowFormula_131_3124_window AS in0

),

Filter_130_3124 AS (

  SELECT * 
  
  FROM MultiRowFormula_131_3124_0 AS in0
  
  WHERE (NOT CAST(`Duplication Safeguard Flag` AS BOOLEAN))

),

RecordID_129_3124 AS (

  {{
    prophecy_basics.RecordID(
      ['Filter_130_3124'], 
      'incremental_id', 
      'RecordID2', 
      'integer', 
      6, 
      1, 
      'groupLevel', 
      'first_column', 
      [], 
      [
        { 'expression': { 'expression': 'RecordID' }, 'sortType': 'asc' }, 
        { 'expression': { 'expression': 'prophecy_recordId_564' }, 'sortType': 'asc' }, 
        { 'expression': { 'expression': 'ManualRecordID' }, 'sortType': 'asc' }, 
        { 'expression': { 'expression': 'MacroRecordID' }, 'sortType': 'asc' }
      ]
    )
  }}

)

SELECT *

FROM RecordID_129_3124
