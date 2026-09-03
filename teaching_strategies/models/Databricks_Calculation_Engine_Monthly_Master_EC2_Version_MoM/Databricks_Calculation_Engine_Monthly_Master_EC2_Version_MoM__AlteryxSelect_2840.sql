{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_2718 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2718')}}

),

Filter_2828_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_2718 AS in0
  
  WHERE (NOT ((`Comparison Method` = 'Year to date')) OR isnull((`Comparison Method` = 'Year to date')))

),

RecordID_2836 AS (

  {{
    prophecy_basics.RecordID(
      ['Filter_2828_reject'], 
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

MultiRowFormula_2837_row_id_0 AS (

  SELECT 
    RecordID AS prophecy_row_id,
    *
  
  FROM RecordID_2836 AS in0

),

MultiRowFormula_2837_0 AS (

  SELECT 
    (LAG(`Customer Name`, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id NULLS FIRST)) AS `Customer Name_lag1`,
    (LAG(`Revenue Period`, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id NULLS FIRST)) AS `Revenue Period_lag1`,
    (LAG(RecordID, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id NULLS FIRST)) AS RecordID_lag1,
    *
  
  FROM MultiRowFormula_2837_row_id_0 AS in0

),

MultiRowFormula_2837_1 AS (

  SELECT 
    (
      CASE
        WHEN (
          (`Customer Name` = `Customer Name_lag1`)
          AND (`Revenue Period` = CAST(`Revenue Period_lag1` AS DATE))
        )
          THEN RecordID_lag1
        ELSE CAST((CAST(RecordID_lag1 AS INTEGER) + 1) AS string)
      END
    ) AS RecordID,
    * EXCEPT (`Customer Name_lag1`, `Revenue Period_lag1`, `RecordID_lag1`, `recordid`)
  
  FROM MultiRowFormula_2837_0 AS in0

),

MultiRowFormula_2837_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_2837_1 AS in0

),

AlteryxSelect_2839 AS (

  SELECT 
    CAST(RecordID AS string) AS RecordID,
    * EXCEPT (`RecordID`)
  
  FROM MultiRowFormula_2837_row_id_drop_0 AS in0

),

Formula_2838_0 AS (

  SELECT 
    CAST((CONCAT('1', (REVERSE((RPAD((REVERSE(RecordID)), 7, '0')))))) AS string) AS CustIDMonthKey,
    *
  
  FROM AlteryxSelect_2839 AS in0

),

AlteryxSelect_2840 AS (

  SELECT 
    CAST(RecordID AS INTEGER) AS RecordID,
    `Customer Name` AS `Customer Name`,
    Product AS Product,
    `Revenue Period` AS `Revenue Period`,
    Revenue AS Revenue,
    `Initial Revenue` AS `Initial Revenue`,
    `Raw Change Category` AS `Raw Change Category`,
    `Change Category` AS `Change Category`,
    `Cust Active Flag` AS `Cust Active Flag`,
    Cohort AS Cohort,
    `Cohort Tenure` AS `Cohort Tenure`,
    `Cust Level Cohort` AS `Cust Level Cohort`,
    `Cust Level Cohort Tenure` AS `Cust Level Cohort Tenure`,
    `Cohort Gross Retention` AS `Cohort Gross Retention`,
    `Cohort Gross Customer Retention` AS `Cohort Gross Customer Retention`,
    `Customer Level Flag` AS `Customer Level Flag`,
    `Customer Segment` AS `Customer Segment`,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    `Cust_Prod Active Flag` AS `Cust_Prod Active Flag`,
    Volume AS Volume,
    `Product Category` AS `Product Category`,
    Extra AS Extra,
    Extra2 AS Extra2,
    Extra3 AS Extra3,
    CustProdKeyStr AS CustProdKeyStr,
    `Recurring Flag` AS `Recurring Flag`,
    CustIDMonthKey AS CustIDMonthKey,
    * EXCEPT (`RecordID`, 
    `Customer Name`, 
    `Product`, 
    `Revenue Period`, 
    `Revenue`, 
    `Initial Revenue`, 
    `Raw Change Category`, 
    `Change Category`, 
    `Cust Active Flag`, 
    `Cohort`, 
    `Cohort Tenure`, 
    `Cust Level Cohort`, 
    `Cust Level Cohort Tenure`, 
    `Cohort Gross Retention`, 
    `Cohort Gross Customer Retention`, 
    `Customer Level Flag`, 
    `Customer Segment`, 
    `SubCustSeg1`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg5`, 
    `SubCustSeg6`, 
    `Cust_Prod Active Flag`, 
    `Volume`, 
    `Product Category`, 
    `Extra`, 
    `Extra2`, 
    `Extra3`, 
    `CustProdKeyStr`, 
    `Recurring Flag`, 
    `CustIDMonthKey`)
  
  FROM Formula_2838_0 AS in0

)

SELECT *

FROM AlteryxSelect_2840
