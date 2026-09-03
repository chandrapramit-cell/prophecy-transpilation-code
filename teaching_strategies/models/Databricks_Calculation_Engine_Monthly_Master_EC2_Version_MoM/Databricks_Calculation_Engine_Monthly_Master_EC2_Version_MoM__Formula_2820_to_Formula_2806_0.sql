{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Union_2799 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_2799')}}

),

AlteryxSelect_2805 AS (

  SELECT 
    `Change Category` AS `Change Category`,
    Cohort AS Cohort,
    `Cohort Active Flag` AS `Cohort Active Flag`,
    `Cohort Gross Customer Retention` AS `Cohort Gross Customer Retention`,
    `Cohort Gross Retention` AS `Cohort Gross Retention`,
    `Cohort Tenure` AS `Cohort Tenure`,
    `Customer Active Flag` AS `Customer Active Flag`,
    `Customer Level Cohort` AS `Customer Level Cohort`,
    `Customer Level Cohort Tenure` AS `Customer Level Cohort Tenure`,
    CustomerName AS CustomerName,
    First_NonZeroRevMonth AS First_NonZeroRevMonth,
    `Initial Revenue` AS `Initial Revenue`,
    Last_NonZeroRevMonth AS Last_NonZeroRevMonth,
    Revenue AS Revenue,
    RevMonth AS RevMonth,
    `Customer Segment` AS `Customer Segment`,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg2 AS SubCustSeg2,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    Volume AS Volume,
    * EXCEPT (`First_PosRevMonth`, 
    `Last_PosRevMonth`, 
    `Change Category`, 
    `Cohort`, 
    `Cohort Active Flag`, 
    `Cohort Gross Customer Retention`, 
    `Cohort Gross Retention`, 
    `Cohort Tenure`, 
    `Customer Active Flag`, 
    `Customer Level Cohort`, 
    `Customer Level Cohort Tenure`, 
    `CustomerName`, 
    `First_NonZeroRevMonth`, 
    `Initial Revenue`, 
    `Last_NonZeroRevMonth`, 
    `Revenue`, 
    `RevMonth`, 
    `Customer Segment`, 
    `SubCustSeg1`, 
    `SubCustSeg2`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg5`, 
    `SubCustSeg6`, 
    `Volume`)
  
  FROM Union_2799 AS in0

),

MultiFieldFormula_2821 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['AlteryxSelect_2805'], 
      "CASE WHEN (isnull(column_value) OR (length(CAST(column_value AS STRING)) = 0)) THEN 0 WHEN CAST(isnull(column_value) AS BOOLEAN) THEN 0 ELSE column_value END", 
      [
        'SubCustSeg5', 
        'Previous YetToRenew', 
        'Cohort Gross Customer Retention', 
        'Cohort', 
        'Volume', 
        'SubCustSeg1', 
        'Customer Active Flag', 
        'CustomerName', 
        'SubCustSeg6', 
        'Customer Segment', 
        'Initial Revenue', 
        'Cohort Gross Retention', 
        'Customer Level Cohort Tenure', 
        'RevMonth', 
        'Change Category', 
        'Last_NonZeroRevMonth', 
        'Revenue', 
        'First_NonZeroRevMonth', 
        'SubCustSeg3', 
        'SubCustSeg4', 
        'YetToRenew', 
        'Cohort Active Flag', 
        'Cohort Tenure', 
        'SubCustSeg2', 
        'Customer Level Cohort'
      ], 
      ['Revenue', 'Volume'], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Formula_2820_to_Formula_2806_0 AS (

  SELECT 
    CAST(' TOTAL CUSTOMER' AS string) AS Product,
    CAST('Y' AS string) AS `Customer Level Flag`,
    CAST(' TOTAL CUSTOMER' AS string) AS Prev_Product,
    CAST(0 AS INTEGER) AS `Cust_Prod Active Flag`,
    CAST(0 AS INTEGER) AS `Prev_Cust_Prod Active Flag`,
    CAST(' TOTAL CUSTOMER' AS string) AS `Product Category`,
    *
  
  FROM MultiFieldFormula_2821 AS in0

)

SELECT *

FROM Formula_2820_to_Formula_2806_0
