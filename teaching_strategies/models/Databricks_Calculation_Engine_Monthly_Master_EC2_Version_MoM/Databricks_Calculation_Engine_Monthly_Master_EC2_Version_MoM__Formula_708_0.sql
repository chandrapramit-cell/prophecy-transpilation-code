{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_884_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_884_inner')}}

),

Summarize_926 AS (

  SELECT 
    SUM(`Sum_Total Price (new)`) AS `Sum_Total Price (new)`,
    MIN(StartDate_Annualization) AS StartDate_Annualization,
    MAX(EndDate_Annualization) AS EndDate_Annualization,
    SUM(CAST(Sum_Quantity AS DECIMAL (19, 9))) AS Quantity,
    `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    `Updated Term` AS `Updated Term`
  
  FROM Join_884_inner AS in0
  
  GROUP BY 
    `Order: Account Name: Mas90 Customer Number`, `Updated Term`

),

Formula_1167_0 AS (

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
  
  FROM Summarize_926 AS in0

),

Formula_1167_1 AS (

  SELECT 
    CAST(((`Sum_Total Price (new)` / Engine_ContractDays) * 365.25) AS DOUBLE) AS ACV,
    *
  
  FROM Formula_1167_0 AS in0

),

Formula_1167_2 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    *
  
  FROM Formula_1167_1 AS in0

),

AlteryxSelect_927 AS (

  SELECT 
    `Order: Account Name: Mas90 Customer Number` AS `Mas90 Customer Number`,
    Quantity AS Quantity,
    StartDate_Annualization AS StartDate_Annualization,
    EndDate_Annualization AS EndDate_Annualization,
    TCV AS TCV,
    `Updated Term` AS `Updated Term`,
    Engine_ContractDays AS Engine_ContractDays,
    * EXCEPT (`Sum_Total Price (new)`, 
    `ACV`, 
    `Quantity`, 
    `StartDate_Annualization`, 
    `EndDate_Annualization`, 
    `TCV`, 
    `Updated Term`, 
    `Engine_ContractDays`, 
    `Order: Account Name: Mas90 Customer Number`)
  
  FROM Formula_1167_2 AS in0

),

Formula_708_0 AS (

  SELECT 
    CAST('Orders&OrdersProcessed - Fall 2020 Early Renewals' AS string) AS Origin,
    CAST('CC Cloud' AS string) AS Product,
    *
  
  FROM AlteryxSelect_927 AS in0

)

SELECT *

FROM Formula_708_0
