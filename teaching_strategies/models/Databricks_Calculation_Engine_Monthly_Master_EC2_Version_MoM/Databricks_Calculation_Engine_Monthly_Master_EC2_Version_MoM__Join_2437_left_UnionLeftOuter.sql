{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_267_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_267_inner')}}

),

Filter_1840_reject AS (

  SELECT * 
  
  FROM Join_267_inner AS in0
  
  WHERE (
          (NOT(`Renewed Contract: Order: Sales Order Number` IS NULL))
          OR ((`Renewed Contract: Order: Sales Order Number` IS NULL) IS NULL)
        )

),

CombinedOpportu_2436 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'CombinedOpportu_2436'
    )
  }}

),

Join_2437_left_UnionLeftOuter AS (

  SELECT 
    in0.`Account Name: Mas90 Customer Number` AS `Account Name: Mas90 Customer Number`,
    in0.`Primary Quote: End Date (Calculated)` AS `Primary Quote: End Date (Calculated)`,
    in0.`Primary Quote: Start Date` AS `Primary Quote: Start Date`,
    in0.Product AS Product,
    in0.`Opportunity Name` AS `Opportunity Name`,
    in0.Stage AS Stage,
    in0.`Renewed Contract: Contract Number` AS `Renewed Contract: Contract Number`,
    in0.Quantity AS Quantity,
    in0.`Actual Closed Date` AS `Actual Closed Date`,
    in0.`Expected Renewal Date` AS `Expected Renewal Date`,
    in0.`Created Date` AS `Created Date`,
    (
      CASE
        WHEN (
          (in0.`Opportunity Name` = in1.`Combined Opportunity Name`)
          AND (in0.`Product Code` = in1.ProductCode_NewOpp)
        )
          THEN in1.SalesOrderNumber_Original
        ELSE NULL
      END
    ) AS `Renewed Contract: Order: Sales Order Number`,
    (
      CASE
        WHEN (
          (in0.`Opportunity Name` = in1.`Combined Opportunity Name`)
          AND (in0.`Product Code` = in1.ProductCode_NewOpp)
        )
          THEN in1.ProductCode_Original
        ELSE NULL
      END
    ) AS `Product Code`,
    in0.* EXCEPT (`Account Name: Mas90 Customer Number`, 
    `Primary Quote: End Date (Calculated)`, 
    `Primary Quote: Start Date`, 
    `Product`, 
    `Opportunity Name`, 
    `Stage`, 
    `Renewed Contract: Contract Number`, 
    `Quantity`, 
    `Actual Closed Date`, 
    `Expected Renewal Date`, 
    `Created Date`, 
    `Product Code`, 
    `Renewed Contract: Order: Sales Order Number`),
    in1.* EXCEPT (`Combined Opportunity Name`, 
    `ProductCode_NewOpp`, 
    `SalesOrderNumber_NewOpp`, 
    `ProductCode_Original`, 
    `SalesOrderNumber_Original`)
  
  FROM Filter_1840_reject AS in0
  LEFT JOIN CombinedOpportu_2436 AS in1
     ON (
      (in0.`Opportunity Name` = in1.`Combined Opportunity Name`)
      AND (in0.`Product Code` = in1.ProductCode_NewOpp)
    )

)

SELECT *

FROM Join_2437_left_UnionLeftOuter
