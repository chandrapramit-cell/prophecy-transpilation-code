{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH DSN_Databricks__3331 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'DSN_Databricks__3331'
    )
  }}

),

AlteryxSelect_3332 AS (

  SELECT 
    Quantity AS Quantity,
    * EXCEPT (`Quantity`)
  
  FROM DSN_Databricks__3331 AS in0

),

Filter_868_to_Filter_109 AS (

  SELECT * 
  
  FROM AlteryxSelect_3332 AS in0
  
  WHERE (`Order: Activated Date` <= to_date({{ var('User__Current_Period') }}))

),

Formula_583_0 AS (

  SELECT 
    CAST(UPPER(CAST((REGEXP_REPLACE(`Product Code_Boomi`, '^[/]+|[/]+$', '')) AS string)) AS string) AS `Product Code_Boomi`,
    CAST((
      CASE
        WHEN (
          ((`Order: Subscription Term` = 0) OR (`Order: Subscription Term` IS NULL))
          OR ((LENGTH(CAST(`Order: Subscription Term` AS string))) = 0)
        )
          THEN (
            CAST((MONTHS_BETWEEN((TO_DATE(`Order: End Date (Calculated)`)), (TO_DATE(`Order: Start Date`)))) AS INTEGER)
            + 1
          )
        ELSE `Order: Subscription Term`
      END
    ) AS string) AS `Order: Subscription Term`,
    CAST(UPPER(`Order: Account Name: Mas90 Customer Number`) AS string) AS `Order: Account Name: Mas90 Customer Number`,
    CAST((
      CASE
        WHEN (
          (`Order: Opportunity: Renewed Contract: Order: Order` IS NULL)
          OR ((LENGTH(`Order: Opportunity: Renewed Contract: Order: Order`)) = 0)
        )
          THEN NULL
        ELSE `Order: Opportunity: Renewed Contract: Order: Order`
      END
    ) AS string) AS `Order: Opportunity: Renewed Contract: Order: Order`,
    * EXCEPT (`order: account name: mas90 customer number`, 
    `order: subscription term`, 
    `order: opportunity: renewed contract: order: order`, 
    `product code_boomi`)
  
  FROM Filter_868_to_Filter_109 AS in0

),

Filter_582 AS (

  SELECT * 
  
  FROM Formula_583_0 AS in0
  
  WHERE (`Order: End Date (Calculated)` >= to_date('2019-01-01'))

),

Summarize_586 AS (

  SELECT 
    SUM(`Total Price (new)`) AS `Sum_Total Price (new)`,
    SUM(Quantity) AS Sum_Quantity,
    `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    `Order: Opportunity: Opportunity Name` AS `Order: Opportunity: Opportunity Name`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Subscription Term` AS `Order: Subscription Term`,
    `Order: Business Subtype` AS `Order: Business Subtype`,
    `Order: Order` AS `Order: Order`,
    `Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Order: Opportunity: Actual Closed Date` AS `Order: Opportunity: Actual Closed Date`,
    `Product Code_Boomi` AS `Product Code`,
    `Order: Start Date` AS `Order: Start Date`
  
  FROM Filter_582 AS in0
  
  GROUP BY 
    `Order: Account Name: Mas90 Customer Number`, 
    `Order: Opportunity: Opportunity Name`, 
    `Order: Sales Order Number`, 
    `Order: Subscription Term`, 
    `Order: Business Subtype`, 
    `Order: Order`, 
    `Order: Opportunity: Renewed Contract: Order: Order`, 
    `Order: End Date (Calculated)`, 
    `Order: Activated Date`, 
    `Order: Opportunity: Actual Closed Date`, 
    `Product Code_Boomi`, 
    `Order: Start Date`

),

Filter_588 AS (

  SELECT * 
  
  FROM Summarize_586 AS in0
  
  WHERE (NOT(`Order: Account Name: Mas90 Customer Number` IS NULL))

)

SELECT *

FROM Filter_588
