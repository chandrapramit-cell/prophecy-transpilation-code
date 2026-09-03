{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_1280 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1280')}}

),

Summarize_2866 AS (

  SELECT 
    SUM(Revenue) AS Revenue,
    SUM(YetToRenew) AS YetToRenew,
    MAX(Revenue) AS Max_Revenue,
    SUM(Volume) AS Volume,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg1 AS SubCustSeg1,
    CustomerName AS CustomerName,
    SubCustSeg6 AS SubCustSeg6,
    `Customer Segment` AS `Customer Segment`,
    RevMonth AS RevMonth,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    Product AS Product,
    SubCustSeg2 AS SubCustSeg2
  
  FROM Formula_1280 AS in0
  
  GROUP BY 
    SubCustSeg5, 
    SubCustSeg1, 
    CustomerName, 
    SubCustSeg6, 
    `Customer Segment`, 
    RevMonth, 
    SubCustSeg3, 
    SubCustSeg4, 
    Product, 
    SubCustSeg2

),

Filter_2868 AS (

  SELECT * 
  
  FROM Summarize_2866 AS in0
  
  WHERE (Revenue > 0)

),

Summarize_2886 AS (

  SELECT 
    MIN(RevMonth) AS First_PosRevMonth,
    MAX(RevMonth) AS Last_PosRevMonth,
    CustomerName AS CustomerName,
    Product AS Product
  
  FROM Filter_2868 AS in0
  
  GROUP BY 
    CustomerName, Product

),

Summarize_2887 AS (

  SELECT 
    *,
    MIN(First_PosRevMonth) OVER (PARTITION BY `Comparison Method`, CustomerName ORDER BY 1 ASC NULLS FIRST) AS Cust_First_PosRevMonth,
    MAX(Last_PosRevMonth) OVER (PARTITION BY `Comparison Method`, CustomerName ORDER BY 1 ASC NULLS FIRST) AS Cust_Last_PosRevMonth
  
  FROM Summarize_2886 AS in0

),

Join_2888_inner_formula AS (

  SELECT *
  
  FROM Summarize_2887 AS in0

),

Join_2874_inner AS (

  SELECT 
    in0.Revenue AS `Initial Revenue`,
    in0.* EXCEPT (`RevMonth`, `Revenue`, `Max_Revenue`, `Volume`),
    in1.* EXCEPT (`CustomerName`, 
    `Product`, 
    `First_PosRevMonth`, 
    `Last_PosRevMonth`, 
    `Cust_First_PosRevMonth`, 
    `Cust_Last_PosRevMonth`)
  
  FROM Summarize_2866 AS in0
  INNER JOIN Join_2888_inner_formula AS in1
     ON (
      ((in0.CustomerName = in1.CustomerName) AND (in0.Product = in1.Product))
      AND (in0.RevMonth = in1.First_PosRevMonth)
    )

),

Summarize_2885 AS (

  SELECT 
    SUM(Revenue) AS Cust_Level_Revenue,
    MAX(Revenue) AS Max_Cust_Level_Revenue,
    CustomerName AS CustomerName,
    RevMonth AS RevMonth
  
  FROM Summarize_2866 AS in0
  
  GROUP BY 
    CustomerName, RevMonth

),

Formula_2892_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Max_Cust_Level_Revenue > 0)
          THEN 1
        ELSE 0
      END
    ) AS INTEGER) AS `Customer Active Flag`,
    *
  
  FROM Summarize_2885 AS in0

),

Join_2884_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CustomerName`, `RevMonth`)
  
  FROM Summarize_2866 AS in0
  INNER JOIN Formula_2892_0 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.RevMonth = in1.RevMonth))

),

Formula_2893_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Revenue > 0)
          THEN 1
        ELSE 0
      END
    ) AS INTEGER) AS `Cust_Prod Active Flag`,
    *
  
  FROM Join_2884_inner AS in0

),

AlteryxSelect_2894 AS (

  SELECT * EXCEPT (`Max_Revenue`, `Max_Cust_Level_Revenue`)
  
  FROM Formula_2893_0 AS in0

),

Formula_2916 AS (

  SELECT *
  
  FROM AlteryxSelect_2894 AS in0

),

Join_2867_inner_UnionLeftOuter AS (

  SELECT 
    in1.Product AS Prev_Product,
    in1.YetToRenew AS `Previous YetToRenew`,
    in0.* EXCEPT (`SubCustSeg5`, 
    `Volume`, 
    `SubCustSeg1`, 
    `Customer Active Flag`, 
    `CustomerName`, 
    `SubCustSeg6`, 
    `Customer Segment`, 
    `Cust_Level_Revenue`, 
    `RevMonth`, 
    `Revenue`, 
    `Cust_Prod Active Flag`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg2`),
    in1.* EXCEPT (`Product`, `YetToRenew`)
  
  FROM Formula_2916 AS in0
  LEFT JOIN Formula_2916 AS in1
     ON (
      ((in0.CustomerName = in1.CustomerName) AND (in0.Product = in1.Product))
      AND (in0.`Previous Period` = in1.RevMonth)
    )

),

Join_2895_inner_UnionLeftOuter AS (

  SELECT 
    in0.* EXCEPT (`Cust_Level_Revenue`, `CustomerName`, `RevMonth`, `Customer Active Flag`),
    in1.*
  
  FROM Join_2867_inner_UnionLeftOuter AS in0
  LEFT JOIN Formula_2892_0 AS in1
     ON (in0.CustomerName = in1.CustomerName)

),

Filter_2899 AS (

  SELECT * 
  
  FROM Summarize_2866 AS in0
  
  WHERE (
          NOT(
            Revenue = 0)
        )

),

Summarize_2900 AS (

  SELECT 
    MIN(RevMonth) AS First_NonZeroRevMonth,
    MAX(RevMonth) AS Last_NonZeroRevMonth,
    CustomerName AS CustomerName,
    Product AS Product
  
  FROM Filter_2899 AS in0
  
  GROUP BY 
    CustomerName, Product

),

Join_2901_left_UnionFullOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CustomerName`, `Product`)
  
  FROM Join_2888_inner_formula AS in0
  FULL JOIN Summarize_2900 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.Product = in1.Product))

),

Join_2870_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CustomerName`, `Product`)
  
  FROM Join_2895_inner_UnionLeftOuter AS in0
  LEFT JOIN Join_2901_left_UnionFullOuter AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.Product = in1.Product))

),

Filter_2898 AS (

  SELECT * 
  
  FROM Join_2870_left_UnionLeftOuter AS in0
  
  WHERE ((RevMonth >= First_NonZeroRevMonth) AND (to_date(`Previous Period`) <= Last_NonZeroRevMonth))

),

Join_2875_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CustomerName`, 
    `Customer Segment`, 
    `SubCustSeg1`, 
    `SubCustSeg2`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg5`, 
    `SubCustSeg6`, 
    `Product`, 
    `YetToRenew`)
  
  FROM Filter_2898 AS in0
  LEFT JOIN Join_2874_inner AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.Product = in1.Product))

),

Formula_2872_to_Formula_2871_0 AS (

  SELECT 
    CAST((
      NOT(
        2 > 1)
    ) AS BOOLEAN) AS `check`,
    *
  
  FROM Join_2875_left_UnionLeftOuter AS in0

)

SELECT *

FROM Formula_2872_to_Formula_2871_0
