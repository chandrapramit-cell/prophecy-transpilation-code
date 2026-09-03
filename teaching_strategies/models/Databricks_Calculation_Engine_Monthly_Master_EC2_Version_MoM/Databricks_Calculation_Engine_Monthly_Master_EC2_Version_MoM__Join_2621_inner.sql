{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH TextInput_2620 AS (

  SELECT * 
  
  FROM {{ ref('seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_2620')}}

),

TextInput_2620_cast AS (

  SELECT 
    CAST(`Change Category` AS string) AS `Change Category`,
    CAST(`TS Change Category` AS string) AS `TS Change Category`
  
  FROM TextInput_2620 AS in0

),

AlteryxSelect_2834 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2834')}}

),

Filter_2850 AS (

  SELECT * 
  
  FROM AlteryxSelect_2834 AS in0
  
  WHERE (`Cust Active Flag` = CAST('1' AS INTEGER))

),

Summarize_2849 AS (

  SELECT 
    COUNT(DISTINCT `Customer Name`) AS `CountDistinct_Customer Name`,
    Cohort AS Cohort,
    `Cohort Tenure` AS `Cohort Tenure`
  
  FROM Filter_2850 AS in0
  
  GROUP BY 
    Cohort, `Cohort Tenure`

),

MultiRowFormula_2852_window AS (

  SELECT 
    *,
    lag(`CountDistinct_Customer Name`, 1) OVER (PARTITION BY Cohort ORDER BY Cohort ASC NULLS FIRST) AS `CountDistinct_Customer Name_lag1`
  
  FROM Summarize_2849 AS in0

),

MultiRowFormula_2852_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((`CountDistinct_Customer Name_lag1` = 0) OR (`CountDistinct_Customer Name` = 0))
          THEN 0
        WHEN (
          ((`CountDistinct_Customer Name` IS NULL) OR (`CountDistinct_Customer Name` IS NULL))
          OR ((LENGTH(`CountDistinct_Customer Name`)) = 0)
        )
          THEN NULL
        ELSE (`CountDistinct_Customer Name` / (`CountDistinct_Customer Name_lag1` - 0))
      END
    ) AS DECIMAL (19, 0)) AS Initial,
    * EXCEPT (`CountDistinct_Customer Name_lag1`)
  
  FROM MultiRowFormula_2852_window AS in0

),

MultiRowFormula_2853_window AS (

  SELECT 
    *,
    lag(`CountDistinct_Customer Name`, 1) OVER (PARTITION BY Cohort ORDER BY Cohort ASC NULLS FIRST) AS `CountDistinct_Customer Name_lag1`
  
  FROM MultiRowFormula_2852_0 AS in0

),

MultiRowFormula_2853_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((`CountDistinct_Customer Name_lag1` = 0) OR (`CountDistinct_Customer Name` = 0))
          THEN 0
        WHEN (
          ((`CountDistinct_Customer Name` IS NULL) OR (`CountDistinct_Customer Name` IS NULL))
          OR ((LENGTH(`CountDistinct_Customer Name`)) = 0)
        )
          THEN NULL
        ELSE (`CountDistinct_Customer Name` / (`CountDistinct_Customer Name_lag1` - 1))
      END
    ) AS DECIMAL (19, 0)) AS `One Period Prior`,
    * EXCEPT (`CountDistinct_Customer Name_lag1`)
  
  FROM MultiRowFormula_2853_window AS in0

),

Join_2854_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.Cohort = in1.Cohort) AND (in0.`Cohort Tenure` = in1.`Cohort Tenure`))
          THEN NULL
        ELSE NULL
      END
    ) AS Initial,
    (
      CASE
        WHEN ((in0.Cohort = in1.Cohort) AND (in0.`Cohort Tenure` = in1.`Cohort Tenure`))
          THEN NULL
        ELSE NULL
      END
    ) AS `One Period Prior`,
    in0.* EXCEPT (`Cohort`, `Cohort Tenure`),
    in1.* EXCEPT (`Initial`, `One Period Prior`)
  
  FROM AlteryxSelect_2834 AS in0
  LEFT JOIN MultiRowFormula_2853_0 AS in1
     ON ((in0.Cohort = in1.Cohort) AND (in0.`Cohort Tenure` = in1.`Cohort Tenure`))

),

AlteryxSelect_2859 AS (

  SELECT * EXCEPT (`RecordID`)
  
  FROM Join_2854_inner_UnionLeftOuter AS in0

),

AlteryxSelect_2862 AS (

  SELECT 
    `Customer Name` AS `Customer Name`,
    `Product Category` AS `Product Category`,
    Product AS Product,
    Extra AS Extra,
    Extra2 AS Extra2,
    Extra3 AS Extra3,
    `Customer Level Flag` AS `Customer Level Flag`,
    `Revenue Period` AS `Revenue Period`,
    Revenue AS Revenue,
    `Raw Change Category` AS `Raw Change Category`,
    `Change Category` AS `Change Category`,
    Volume AS Volume,
    `Initial Revenue` AS `Initial Revenue`,
    Cohort AS Cohort,
    `Cohort Tenure` AS `Cohort Tenure`,
    `Cohort Gross Retention` AS `Cohort Gross Retention`,
    Initial AS Initial,
    `One Period Prior` AS `One Period Prior`,
    `Cohort Gross Customer Retention` AS `Cohort Gross Customer Retention`,
    `Cust Active Flag` AS `Cust Active Flag`,
    CustIDMonthKey AS CustIDMonthKey,
    `Cust Level Cohort` AS `Cust Level Cohort`,
    `Cust Level Cohort Tenure` AS `Cust Level Cohort Tenure`,
    CustProdKeyStr AS CustProdKeyStr,
    `Customer Segment` AS `Customer Segment`,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    `Recurring Flag` AS `Recurring Flag`,
    `Cust_Prod Active Flag` AS `Cust_Prod Active Flag`,
    YetToRenew AS YetToRenew
  
  FROM AlteryxSelect_2859 AS in0

),

AlteryxSelect_2840 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2840')}}

),

Filter_2844 AS (

  SELECT * 
  
  FROM AlteryxSelect_2840 AS in0
  
  WHERE (`Cust Active Flag` = CAST('1' AS INTEGER))

),

Summarize_2843 AS (

  SELECT 
    COUNT(DISTINCT `Customer Name`) AS `CountDistinct_Customer Name`,
    Cohort AS Cohort,
    `Cohort Tenure` AS `Cohort Tenure`
  
  FROM Filter_2844 AS in0
  
  GROUP BY 
    Cohort, `Cohort Tenure`

),

MultiRowFormula_2846_window AS (

  SELECT 
    *,
    lag(`CountDistinct_Customer Name`, 1) OVER (PARTITION BY Cohort ORDER BY Cohort ASC NULLS FIRST) AS `CountDistinct_Customer Name_lag1`
  
  FROM Summarize_2843 AS in0

),

MultiRowFormula_2846_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((`CountDistinct_Customer Name_lag1` = 0) OR (`CountDistinct_Customer Name` = 0))
          THEN 0
        WHEN (
          ((`CountDistinct_Customer Name` IS NULL) OR (`CountDistinct_Customer Name` IS NULL))
          OR ((LENGTH(`CountDistinct_Customer Name`)) = 0)
        )
          THEN NULL
        ELSE (`CountDistinct_Customer Name` / (`CountDistinct_Customer Name_lag1` - 0))
      END
    ) AS DECIMAL (19, 0)) AS Initial,
    * EXCEPT (`CountDistinct_Customer Name_lag1`)
  
  FROM MultiRowFormula_2846_window AS in0

),

MultiRowFormula_2847_window AS (

  SELECT 
    *,
    lag(`CountDistinct_Customer Name`, 1) OVER (PARTITION BY Cohort ORDER BY Cohort ASC NULLS FIRST) AS `CountDistinct_Customer Name_lag1`
  
  FROM MultiRowFormula_2846_0 AS in0

),

MultiRowFormula_2847_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((`CountDistinct_Customer Name_lag1` = 0) OR (`CountDistinct_Customer Name` = 0))
          THEN 0
        WHEN (
          ((`CountDistinct_Customer Name` IS NULL) OR (`CountDistinct_Customer Name` IS NULL))
          OR ((LENGTH(`CountDistinct_Customer Name`)) = 0)
        )
          THEN NULL
        ELSE (`CountDistinct_Customer Name` / (`CountDistinct_Customer Name_lag1` - 1))
      END
    ) AS DECIMAL (19, 0)) AS `One Period Prior`,
    * EXCEPT (`CountDistinct_Customer Name_lag1`)
  
  FROM MultiRowFormula_2847_window AS in0

),

Join_2848_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.Cohort = in1.Cohort) AND (in0.`Cohort Tenure` = in1.`Cohort Tenure`))
          THEN NULL
        ELSE NULL
      END
    ) AS Initial,
    (
      CASE
        WHEN ((in0.Cohort = in1.Cohort) AND (in0.`Cohort Tenure` = in1.`Cohort Tenure`))
          THEN NULL
        ELSE NULL
      END
    ) AS `One Period Prior`,
    in0.* EXCEPT (`Cohort`, `Cohort Tenure`),
    in1.* EXCEPT (`Initial`, `One Period Prior`)
  
  FROM AlteryxSelect_2840 AS in0
  LEFT JOIN MultiRowFormula_2847_0 AS in1
     ON ((in0.Cohort = in1.Cohort) AND (in0.`Cohort Tenure` = in1.`Cohort Tenure`))

),

AlteryxSelect_2860 AS (

  SELECT * EXCEPT (`RecordID`)
  
  FROM Join_2848_inner_UnionLeftOuter AS in0

),

AlteryxSelect_2861 AS (

  SELECT 
    `Customer Name` AS `Customer Name`,
    `Product Category` AS `Product Category`,
    Product AS Product,
    Extra AS Extra,
    Extra2 AS Extra2,
    Extra3 AS Extra3,
    `Customer Level Flag` AS `Customer Level Flag`,
    `Revenue Period` AS `Revenue Period`,
    Revenue AS Revenue,
    `Raw Change Category` AS `Raw Change Category`,
    `Change Category` AS `Change Category`,
    Volume AS Volume,
    `Initial Revenue` AS `Initial Revenue`,
    Cohort AS Cohort,
    `Cohort Tenure` AS `Cohort Tenure`,
    `Cohort Gross Retention` AS `Cohort Gross Retention`,
    Initial AS Initial,
    `One Period Prior` AS `One Period Prior`,
    `Cohort Gross Customer Retention` AS `Cohort Gross Customer Retention`,
    `Cust Active Flag` AS `Cust Active Flag`,
    CustIDMonthKey AS CustIDMonthKey,
    `Cust Level Cohort` AS `Cust Level Cohort`,
    `Cust Level Cohort Tenure` AS `Cust Level Cohort Tenure`,
    CustProdKeyStr AS CustProdKeyStr,
    `Customer Segment` AS `Customer Segment`,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    `Recurring Flag` AS `Recurring Flag`,
    `Cust_Prod Active Flag` AS `Cust_Prod Active Flag`,
    YetToRenew AS YetToRenew
  
  FROM AlteryxSelect_2860 AS in0

),

Union_2619 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_2861', 'AlteryxSelect_2862'], 
      [
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "CustProdKeyStr", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Recurring Flag", "dataType": "String"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Initial", "dataType": "Decimal"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Raw Change Category", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust Level Cohort Tenure", "dataType": "Integer"}, {"name": "Customer Name", "dataType": "String"}, {"name": "Change Category", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "Extra2", "dataType": "String"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "CustIDMonthKey", "dataType": "String"}, {"name": "Extra", "dataType": "String"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Cust Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Revenue Period", "dataType": "Date"}, {"name": "Product Category", "dataType": "String"}, {"name": "Extra3", "dataType": "String"}, {"name": "One Period Prior", "dataType": "Decimal"}, {"name": "Product", "dataType": "String"}, {"name": "Cust Level Cohort", "dataType": "Date"}, {"name": "Cohort Tenure", "dataType": "Integer"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "CustProdKeyStr", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Recurring Flag", "dataType": "String"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Initial", "dataType": "Decimal"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Raw Change Category", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust Level Cohort Tenure", "dataType": "Integer"}, {"name": "Customer Name", "dataType": "String"}, {"name": "Change Category", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "Extra2", "dataType": "String"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "CustIDMonthKey", "dataType": "String"}, {"name": "Extra", "dataType": "String"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Cust Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Revenue Period", "dataType": "Date"}, {"name": "Product Category", "dataType": "String"}, {"name": "Extra3", "dataType": "String"}, {"name": "One Period Prior", "dataType": "Decimal"}, {"name": "Product", "dataType": "String"}, {"name": "Cust Level Cohort", "dataType": "Date"}, {"name": "Cohort Tenure", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_2621_inner AS (

  SELECT 
    in0.`Customer Name` AS `Customer Name`,
    in0.`Product Category` AS `Product Category`,
    in0.Product AS Product,
    in0.Extra AS Extra,
    in0.Extra2 AS Extra2,
    in0.Extra3 AS Extra3,
    in0.`Customer Level Flag` AS `Customer Level Flag`,
    in0.`Revenue Period` AS `Revenue Period`,
    in0.Revenue AS Revenue,
    in0.`Raw Change Category` AS `Raw Change Category`,
    in0.`Change Category` AS `Change Category`,
    in1.`TS Change Category` AS `TS Change Category`,
    in0.Volume AS Volume,
    in0.`Initial Revenue` AS `Initial Revenue`,
    in0.Cohort AS Cohort,
    in0.`Cohort Tenure` AS `Cohort Tenure`,
    in0.`Cohort Gross Retention` AS `Cohort Gross Retention`,
    in0.Initial AS Initial,
    in0.`One Period Prior` AS `One Period Prior`,
    in0.`Cohort Gross Customer Retention` AS `Cohort Gross Customer Retention`,
    in0.`Cust Active Flag` AS `Cust Active Flag`,
    in0.CustIDMonthKey AS CustIDMonthKey,
    in0.`Cust Level Cohort` AS `Cust Level Cohort`,
    in0.`Cust Level Cohort Tenure` AS `Cust Level Cohort Tenure`,
    in0.CustProdKeyStr AS CustProdKeyStr,
    in0.`Customer Segment` AS `Customer Segment`,
    in0.SubCustSeg1 AS SubCustSeg1,
    in0.SubCustSeg3 AS SubCustSeg3,
    in0.SubCustSeg4 AS SubCustSeg4,
    in0.SubCustSeg5 AS SubCustSeg5,
    in0.SubCustSeg6 AS SubCustSeg6,
    in0.`Recurring Flag` AS `Recurring Flag`,
    in0.`Cust_Prod Active Flag` AS `Cust_Prod Active Flag`,
    in0.YetToRenew AS YetToRenew,
    in0.* EXCEPT (`Customer Name`, 
    `Product Category`, 
    `Product`, 
    `Extra`, 
    `Extra2`, 
    `Extra3`, 
    `Customer Level Flag`, 
    `Revenue Period`, 
    `Revenue`, 
    `Raw Change Category`, 
    `Change Category`, 
    `Volume`, 
    `Initial Revenue`, 
    `Cohort`, 
    `Cohort Tenure`, 
    `Cohort Gross Retention`, 
    `Initial`, 
    `One Period Prior`, 
    `Cohort Gross Customer Retention`, 
    `Cust Active Flag`, 
    `CustIDMonthKey`, 
    `Cust Level Cohort`, 
    `Cust Level Cohort Tenure`, 
    `CustProdKeyStr`, 
    `Customer Segment`, 
    `SubCustSeg1`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg5`, 
    `SubCustSeg6`, 
    `Recurring Flag`, 
    `Cust_Prod Active Flag`, 
    `YetToRenew`),
    in1.* EXCEPT (`TS Change Category`, `Change Category`)
  
  FROM Union_2619 AS in0
  INNER JOIN TextInput_2620_cast AS in1
     ON (in0.`Change Category` = in1.`Change Category`)

)

SELECT *

FROM Join_2621_inner
