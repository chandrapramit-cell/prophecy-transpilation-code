{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Union_677 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_677')}}

),

Summarize_2439 AS (

  SELECT 
    SUM(CAST(Sum_Quantity AS DECIMAL (19, 9))) AS Sum_Quantity,
    SUM(`PreReductions_Total Price`) AS `PreReductions_Total Price`,
    SUM(ACV) AS `Orders ACV`,
    `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Subscription Term` AS `Order: Subscription Term`,
    StartDate_Annualization AS Original_StartDate_Annualization,
    `Product Code` AS `Product Code`,
    Product AS Product,
    EndDate_Annualization AS Original_EndDate_Annualization
  
  FROM Union_677 AS in0
  
  GROUP BY 
    `Order: Account Name: Mas90 Customer Number`, 
    `Order: Sales Order Number`, 
    `Order: Subscription Term`, 
    StartDate_Annualization, 
    `Product Code`, 
    Product, 
    EndDate_Annualization

),

Formula_2441_0 AS (

  SELECT 
    CAST(((`Orders ACV` / `PreReductions_Total Price`) - 1) AS DOUBLE) AS `Value Increase %`,
    *
  
  FROM Summarize_2439 AS in0

),

Filter_2440 AS (

  SELECT * 
  
  FROM Formula_2441_0 AS in0
  
  WHERE ((CAST(`Order: Subscription Term` AS INTEGER) < 12) AND (`Value Increase %` > 1))

),

Formula_2598 AS (

  SELECT *
  
  FROM Filter_2440 AS in0

),

Filter_2580 AS (

  SELECT * 
  
  FROM Formula_2598 AS in0
  
  WHERE (Original_StartDate_Annualization >= to_date(`Start of Current Month`))

)

SELECT *

FROM Filter_2580
