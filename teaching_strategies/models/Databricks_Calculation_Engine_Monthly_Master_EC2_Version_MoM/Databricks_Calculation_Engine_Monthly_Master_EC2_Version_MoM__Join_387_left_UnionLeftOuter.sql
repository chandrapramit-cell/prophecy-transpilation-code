{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH DSN_Databricks__3336 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'DSN_Databricks__3336'
    )
  }}

),

AlteryxSelect_3337 AS (

  SELECT 
    MAS90_CUSTOMER_NUMBER AS `Account Name: Mas90 Customer Number`,
    CAST(OPPORTUNITY_NAME AS string) AS `Opportunity Name`,
    EXPECTED_RENEWAL_DATE AS `Expected Renewal Date`,
    STAGE AS Stage,
    CAST(BUSINESS_SUBTYPE AS string) AS `Business Subtype`,
    CAST(PRODUCT_CODE AS string) AS `Product Code`,
    QUANTITY AS Quantity,
    SALES_PRICE AS `Sales Price`,
    TOTAL_PRICE AS `Total Price`,
    PRIMARY_QUOTE_START_DATE AS `Primary Quote: Start Date`,
    PRIMARY_QUOTE_END_DATE AS `Primary Quote: End Date (Calculated)`,
    CAST(EXPECTED_RENEWAL_AMOUNT AS string) AS `Expected Renewal Amount`,
    RENEWED_CONTRACT_NUMBER AS `Renewed Contract: Contract Number`,
    RENEWED_CONTRACT_ORDER AS `Renewed Contract: Order: Order`,
    RENEWED_CONTRACT_SALES_ORDER_NUMBER AS `Renewed Contract: Order: Sales Order Number`,
    QUOTE_LINE_NET_TOTAL_AMOUNT AS `Quote Line: Renewed Subscription: Net Total Amount`,
    ACTUAL_CLOSED_DATE AS `Actual Closed Date`,
    CREATED_DATE AS `Created Date`,
    * EXCEPT (`opp_product_number`, 
    `Stage`, 
    `Quantity`, 
    `mas90_customer_number`, 
    `opportunity_name`, 
    `expected_renewal_date`, 
    `business_subtype`, 
    `product_code`, 
    `sales_price`, 
    `total_price`, 
    `primary_quote_start_date`, 
    `primary_quote_end_date`, 
    `expected_renewal_amount`, 
    `renewed_contract_number`, 
    `renewed_contract_order`, 
    `renewed_contract_sales_order_number`, 
    `quote_line_net_total_amount`, 
    `actual_closed_date`, 
    `created_date`)
  
  FROM DSN_Databricks__3336 AS in0

),

Filter_2539_to_Filter_97 AS (

  SELECT * 
  
  FROM AlteryxSelect_3337 AS in0
  
  WHERE (
          (
            (`Created Date` <= to_date({{ var('User__Current_Period') }}))
            AND NOT ((isnull(`Account Name: Mas90 Customer Number`) OR (length(`Account Name: Mas90 Customer Number`) = 0)))
          )
          AND NOT ((isnull(`Primary Quote: Start Date`) OR (length(CAST(`Primary Quote: Start Date` AS STRING)) = 0)))
        )

),

AlteryxSelect_260 AS (

  SELECT 
    CAST(Quantity AS DOUBLE) AS Quantity,
    CAST(`Total Price` AS DOUBLE) AS `Total Price`,
    * EXCEPT (`Quantity`, `Total Price`)
  
  FROM Filter_2539_to_Filter_97 AS in0

),

Formula_45_0 AS (

  SELECT 
    CAST(UPPER(CAST((REGEXP_REPLACE(`Product Code`, '^[/]+|[/]+$', '')) AS string)) AS string) AS `Product Code`,
    * EXCEPT (`product code`)
  
  FROM AlteryxSelect_260 AS in0

),

Summarize_1069 AS (

  SELECT 
    SUM(Quantity) AS Quantity,
    `Actual Closed Date` AS `Actual Closed Date`,
    `Primary Quote: End Date (Calculated)` AS `Primary Quote: End Date (Calculated)`,
    `Renewed Contract: Order: Sales Order Number` AS `Renewed Contract: Order: Sales Order Number`,
    `Created Date` AS `Created Date`,
    `Primary Quote: Start Date` AS `Primary Quote: Start Date`,
    `Account Name: Mas90 Customer Number` AS `Account Name: Mas90 Customer Number`,
    `Expected Renewal Date` AS `Expected Renewal Date`,
    `Renewed Contract: Contract Number` AS `Renewed Contract: Contract Number`,
    `Product Code` AS `Product Code`,
    `Opportunity Name` AS `Opportunity Name`,
    Stage AS Stage
  
  FROM Formula_45_0 AS in0
  
  GROUP BY 
    `Actual Closed Date`, 
    `Primary Quote: End Date (Calculated)`, 
    `Renewed Contract: Order: Sales Order Number`, 
    `Created Date`, 
    `Primary Quote: Start Date`, 
    `Account Name: Mas90 Customer Number`, 
    `Expected Renewal Date`, 
    `Renewed Contract: Contract Number`, 
    `Product Code`, 
    `Opportunity Name`, 
    Stage

),

Unique_123 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Unique_123')}}

),

Join_387_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Account Name: Mas90 Customer Number` = in1.`Loser Mas90 Customer Number`)
          THEN in1.`Winner Mas90 Customer Number`
        ELSE NULL
      END
    ) AS `Account Name: Mas90 Customer Number`,
    (
      CASE
        WHEN (in0.`Account Name: Mas90 Customer Number` = in1.`Loser Mas90 Customer Number`)
          THEN in0.`Account Name: Mas90 Customer Number`
        ELSE NULL
      END
    ) AS `Left_Account Name: Mas90 Customer Number`,
    in0.* EXCEPT (`Account Name: Mas90 Customer Number`),
    in1.* EXCEPT (`Loser Mas90 Customer Number`, `Winner Mas90 Customer Number`)
  
  FROM Summarize_1069 AS in0
  LEFT JOIN Unique_123 AS in1
     ON (in0.`Account Name: Mas90 Customer Number` = in1.`Loser Mas90 Customer Number`)

)

SELECT *

FROM Join_387_left_UnionLeftOuter
