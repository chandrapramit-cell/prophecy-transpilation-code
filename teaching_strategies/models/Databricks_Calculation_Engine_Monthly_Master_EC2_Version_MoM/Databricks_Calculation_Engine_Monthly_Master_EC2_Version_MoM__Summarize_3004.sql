{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH RecordID_3003 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__RecordID_3003')}}

),

Summarize_3004 AS (

  SELECT 
    SUM(CAST(Sum_Quantity AS DECIMAL (19, 9))) AS Sum_Quantity,
    SUM(ACV) AS `Orders ACV`,
    `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    StartDate_Annualization AS Original_StartDate_Annualization,
    `Product Code` AS `Product Code`,
    Product AS Product,
    EndDate_Annualization AS Original_EndDate_Annualization
  
  FROM RecordID_3003 AS in0
  
  GROUP BY 
    `Order: Account Name: Mas90 Customer Number`, 
    `Order: Sales Order Number`, 
    StartDate_Annualization, 
    `Product Code`, 
    Product, 
    EndDate_Annualization

)

SELECT *

FROM Summarize_3004
