{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH DSN_Databricks__3338 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'DSN_Databricks__3338'
    )
  }}

),

Filter_3347 AS (

  SELECT * 
  
  FROM DSN_Databricks__3338 AS in0
  
  WHERE (
          NOT(
            UPPER(TRANSACTION_TYPE) = UPPER('Credit memo'))
        )

),

AlteryxSelect_3339 AS (

  SELECT 
    TRANSACTION_LINES_NAME AS `Transaction Lines Name`,
    TRANSACTION_ORDER AS `Transaction: Order: Order`,
    PRODUCT_CODE AS `Product Code`,
    PRODUCT_NAME AS `Product Name`,
    QUANTITY AS Quantity,
    CAST(AMOUNT AS DOUBLE) AS Amount,
    CREATED_DATE AS `Created Date`,
    TRANSACTION_STATUS AS `Transaction: Status`,
    TRANSACTION_ACCOUNT_MAS90_CUSTOMER_NUMBER AS `Transaction: Account: Mas90 Customer Number`,
    ORDER_PRODUCT_PRODUCT_CODE_BOOMI AS `Order Product: Product Code_Boomi`,
    TRANSACTION_ORDER_SALES_ORDER_NUMBER AS `Transaction: Order: Sales Order Number`,
    TRANSACTION_TYPE AS `Transaction: Transaction Type`,
    TRANSACTION_NAME AS `Transaction: Transaction Name`,
    TRANSACTION_LINE_ID AS `Transaction Line ID`,
    START_DATE AS `Start Date`,
    END_DATE AS `End Date`,
    * EXCEPT (`related_account_casesafe_id`, 
    `Quantity`, 
    `Amount`, 
    `transaction_lines_name`, 
    `transaction_order`, 
    `product_code`, 
    `product_name`, 
    `created_date`, 
    `transaction_status`, 
    `transaction_account_mas90_customer_number`, 
    `order_product_product_code_boomi`, 
    `transaction_order_sales_order_number`, 
    `transaction_type`, 
    `transaction_name`, 
    `transaction_line_id`, 
    `start_date`, 
    `end_date`)
  
  FROM Filter_3347 AS in0

),

Formula_871_0 AS (

  SELECT 
    CAST(UPPER(CAST((REGEXP_REPLACE(`Product Code`, '^[/]+|[/]+$', '')) AS string)) AS string) AS `Product Code`,
    * EXCEPT (`product code`)
  
  FROM AlteryxSelect_3339 AS in0

),

Filter_873 AS (

  SELECT * 
  
  FROM Formula_871_0 AS in0
  
  WHERE (CAST(`Transaction: Status` AS string) IN ('Fully Applied', 'Partially Received', 'Refunded'))

),

Filter_874 AS (

  SELECT * 
  
  FROM Filter_873 AS in0
  
  WHERE (UPPER(`Transaction: Transaction Type`) = UPPER('Manual Adjustment'))

),

DbFileInput_243_2435 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'DbFileInput_243_2435'
    )
  }}

),

Formula_876_0 AS (

  SELECT 
    CAST((Amount * Quantity) AS string) AS Amount,
    * EXCEPT (`amount`)
  
  FROM Filter_874 AS in0

),

Union_875_reformat_1 AS (

  SELECT 
    CAST(Amount AS string) AS Amount,
    `Created Date` AS `Created Date`,
    `End Date` AS `End Date`,
    `Order Product: Product Code_Boomi` AS `Order Product: Product Code_Boomi`,
    `Product Code` AS `Product Code`,
    `Product Name` AS `Product Name`,
    Quantity AS Quantity,
    `Start Date` AS `Start Date`,
    `Transaction Line ID` AS `Transaction Line ID`,
    `Transaction Lines Name` AS `Transaction Lines Name`,
    `Transaction: Account: Mas90 Customer Number` AS `Transaction: Account: Mas90 Customer Number`,
    `Transaction: Order: Order` AS `Transaction: Order: Order`,
    `Transaction: Order: Sales Order Number` AS `Transaction: Order: Sales Order Number`,
    `Transaction: Status` AS `Transaction: Status`,
    `Transaction: Transaction Name` AS `Transaction: Transaction Name`,
    `Transaction: Transaction Type` AS `Transaction: Transaction Type`,
    asofdate AS asofdate
  
  FROM Formula_876_0 AS in0

),

Filter_874_reject AS (

  SELECT * 
  
  FROM Filter_873 AS in0
  
  WHERE (
          (
            NOT(
              UPPER(`Transaction: Transaction Type`) = UPPER('Manual Adjustment'))
          )
          OR ((UPPER(`Transaction: Transaction Type`) = UPPER('Manual Adjustment')) IS NULL)
        )

),

Union_875_reformat_0 AS (

  SELECT 
    CAST(Amount AS string) AS Amount,
    `Created Date` AS `Created Date`,
    `End Date` AS `End Date`,
    `Order Product: Product Code_Boomi` AS `Order Product: Product Code_Boomi`,
    `Product Code` AS `Product Code`,
    `Product Name` AS `Product Name`,
    Quantity AS Quantity,
    `Start Date` AS `Start Date`,
    `Transaction Line ID` AS `Transaction Line ID`,
    `Transaction Lines Name` AS `Transaction Lines Name`,
    `Transaction: Account: Mas90 Customer Number` AS `Transaction: Account: Mas90 Customer Number`,
    `Transaction: Order: Order` AS `Transaction: Order: Order`,
    `Transaction: Order: Sales Order Number` AS `Transaction: Order: Sales Order Number`,
    `Transaction: Status` AS `Transaction: Status`,
    `Transaction: Transaction Name` AS `Transaction: Transaction Name`,
    `Transaction: Transaction Type` AS `Transaction: Transaction Type`,
    asofdate AS asofdate
  
  FROM Filter_874_reject AS in0

),

Union_875 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_875_reformat_0', 'Union_875_reformat_1'], 
      [
        '[{"name": "Amount", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "End Date", "dataType": "Date"}, {"name": "Order Product: Product Code_Boomi", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Product Name", "dataType": "String"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Start Date", "dataType": "Date"}, {"name": "Transaction Line ID", "dataType": "String"}, {"name": "Transaction Lines Name", "dataType": "String"}, {"name": "Transaction: Account: Mas90 Customer Number", "dataType": "String"}, {"name": "Transaction: Order: Order", "dataType": "String"}, {"name": "Transaction: Order: Sales Order Number", "dataType": "String"}, {"name": "Transaction: Status", "dataType": "String"}, {"name": "Transaction: Transaction Name", "dataType": "String"}, {"name": "Transaction: Transaction Type", "dataType": "String"}, {"name": "asofdate", "dataType": "Date"}]', 
        '[{"name": "Amount", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "End Date", "dataType": "Date"}, {"name": "Order Product: Product Code_Boomi", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Product Name", "dataType": "String"}, {"name": "Quantity", "dataType": "Double"}, {"name": "Start Date", "dataType": "Date"}, {"name": "Transaction Line ID", "dataType": "String"}, {"name": "Transaction Lines Name", "dataType": "String"}, {"name": "Transaction: Account: Mas90 Customer Number", "dataType": "String"}, {"name": "Transaction: Order: Order", "dataType": "String"}, {"name": "Transaction: Order: Sales Order Number", "dataType": "String"}, {"name": "Transaction: Status", "dataType": "String"}, {"name": "Transaction: Transaction Name", "dataType": "String"}, {"name": "Transaction: Transaction Type", "dataType": "String"}, {"name": "asofdate", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_3088 AS (

  SELECT * 
  
  FROM Union_875 AS in0
  
  WHERE (`Created Date` <= to_date({{ var('User__Current_Period') }}))

),

Summarize_992 AS (

  SELECT 
    SUM(CAST(Amount AS DECIMAL (19, 9))) AS `Reduction Amount`,
    `Transaction: Account: Mas90 Customer Number` AS `Transaction: Account: Mas90 Customer Number`,
    `Transaction: Order: Sales Order Number` AS `Transaction: Order: Sales Order Number`,
    `Product Code` AS `Product Code`,
    `Created Date` AS `Created Date`
  
  FROM Filter_3088 AS in0
  
  GROUP BY 
    `Transaction: Account: Mas90 Customer Number`, 
    `Transaction: Order: Sales Order Number`, 
    `Product Code`, 
    `Created Date`

),

Union_1866 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_992', 'DbFileInput_243_2435'], 
      [
        '[{"name": "Reduction Amount", "dataType": "Decimal(29, 9)"}, {"name": "Transaction: Account: Mas90 Customer Number", "dataType": "String"}, {"name": "Transaction: Order: Sales Order Number", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}]', 
        '[{"name": "Transaction: Account: Mas90 Customer Number", "dataType": "String"}, {"name": "Transaction: Order: Order", "dataType": "String"}, {"name": "Created Date", "dataType": "Date"}, {"name": "Product Code", "dataType": "String"}, {"name": "Reduction Amount", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_595_left AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_595_left')}}

),

Summarize_608 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_608')}}

),

Join_594_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Join_595_left AS in0
  INNER JOIN Summarize_608 AS in1
     ON (in0.`Product Code` = in1.`Item/Product`)

),

Filter_683 AS (

  SELECT * 
  
  FROM Join_594_inner AS in0
  
  WHERE (
          (UPPER(Product) = UPPER('Credit'))
          AND (
                (
                  NOT(
                    `Sum_Total Price (new)` = 0)
                ) OR (`Sum_Total Price (new)` IS NULL)
              )
        )

),

Summarize_686 AS (

  SELECT 
    SUM(`Sum_Total Price (new)`) AS `Credit_Total Price (new)`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`
  
  FROM Filter_683 AS in0
  
  GROUP BY `Order: Sales Order Number`

),

Filter_683_reject AS (

  SELECT * 
  
  FROM Join_594_inner AS in0
  
  WHERE (
          (
            NOT(
              (UPPER(Product) = UPPER('Credit'))
              AND (
                    (
                      NOT(
                        `Sum_Total Price (new)` = 0)
                    ) OR (`Sum_Total Price (new)` IS NULL)
                  ))
          )
          OR (
               (
                 (UPPER(Product) = UPPER('Credit'))
                 AND (
                       (
                         NOT(
                           `Sum_Total Price (new)` = 0)
                       ) OR (`Sum_Total Price (new)` IS NULL)
                     )
               ) IS NULL
             )
        )

),

Join_687_inner AS (

  SELECT 
    in0.* EXCEPT (`Order: Sales Order Number`, `Credit_Total Price (new)`),
    in1.*
  
  FROM Summarize_686 AS in0
  INNER JOIN Filter_683_reject AS in1
     ON (in0.`Order: Sales Order Number` = in1.`Order: Sales Order Number`)

),

Summarize_689 AS (

  SELECT 
    *,
    SUM(`Sum_Total Price (new)`) OVER (PARTITION BY `Order: Sales Order Number` ORDER BY 1 ASC NULLS FIRST) AS `SalesOrder_Total Price (new)`
  
  FROM Join_687_inner AS in0

),

Join_692_inner_formula_0 AS (

  SELECT 
    `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    `Product Code` AS `Product Code`,
    `Order: Order` AS `Order: Order`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    `Order: Activated Date` AS `Order: Activated Date`,
    `Order: Start Date` AS `Order: Start Date`,
    `Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    `Order: Subscription Term` AS `Order: Subscription Term`,
    `Sum_Total Price (new)` AS `Sum_Total Price (new)`,
    `SalesOrder_Total Price (new)` AS `SalesOrder_Total Price (new)`,
    Sum_Quantity AS Sum_Quantity,
    `Item/Product` AS `Item/Product`,
    Product AS Product,
    `Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    * EXCEPT (`order: account name: mas90 customer number`, 
    `sum_quantity`, 
    `order: sales order number`, 
    `order: subscription term`, 
    `order: order`, 
    `order: opportunity: renewed contract: order: order`, 
    `salesorder_total price (new)`, 
    `order: end date (calculated)`, 
    `order: activated date`, 
    `product code`, 
    `sum_total price (new)`, 
    `item/product`, 
    `product`, 
    `order: start date`)
  
  FROM Summarize_689 AS in0

),

Join_695_inner_UnionRightOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Order: Sales Order Number` = in1.`Order: Sales Order Number`)
          THEN (
            CAST(in1.`Sum_Total Price (new)` AS DECIMAL (19, 9))
            / CAST(in1.`SalesOrder_Total Price (new)` AS DECIMAL (19, 9))
          )
        ELSE NULL
      END
    ) AS Weight,
    in1.`Order: Sales Order Number` AS `Order: Sales Order Number`,
    CASE
      WHEN (in0.`Order: Sales Order Number` = in1.`Order: Sales Order Number`)
        THEN (`Sum_Total Price (new)` + Credit)
      ELSE NULL
    END AS `Sum_Total Price (new)`,
    CASE
      WHEN (in0.`Order: Sales Order Number` = in1.`Order: Sales Order Number`)
        THEN (`Credit_Total Price (new)` * Weight)
      ELSE NULL
    END AS Credit,
    in0.* EXCEPT (`Order: Sales Order Number`),
    in1.* EXCEPT (`Order: Sales Order Number`, `Sum_Total Price (new)`)
  
  FROM Summarize_686 AS in0
  RIGHT JOIN Join_692_inner_formula_0 AS in1
     ON (in0.`Order: Sales Order Number` = in1.`Order: Sales Order Number`)

),

AlteryxSelect_940 AS (

  SELECT * EXCEPT (`SalesOrder_Total Price (new)`, `Credit_Total Price (new)`, `Weight`, `Credit`)
  
  FROM Join_695_inner_UnionRightOuter AS in0

),

Formula_944_to_Formula_942_0 AS (

  SELECT 
    (
      TO_DATE(
        (
          CASE
            WHEN CAST((`Order: Business Subtype` IN ('New', 'New-ATE')) AS BOOLEAN)
              THEN (DATE_TRUNC('month', (ARRAY_MIN((ARRAY(`Order: Opportunity: Actual Closed Date`, `Order: Start Date`))))))
            ELSE (DATE_TRUNC('month', `Order: Start Date`))
          END
        ), 
        'yyyy-MM-dd')
    ) AS StartDate_Annualization,
    (
      TO_DATE(
        (
          CASE
            WHEN ((SUBSTRING(`Order: Start Date`, 1, 7)) = (SUBSTRING(`Order: End Date (Calculated)`, 1, 7)))
              THEN CAST((TO_DATE((LAST_DAY(CAST(`Order: End Date (Calculated)` AS DATE))))) AS string)
            WHEN (
              ((TO_DATE((DATE_TRUNC('month', `Order: Start Date`)))) = (TO_DATE(`Order: Start Date`)))
              AND ((TO_DATE((LAST_DAY(CAST(`Order: End Date (Calculated)` AS DATE))))) = (TO_DATE(`Order: End Date (Calculated)`)))
            )
              THEN `Order: End Date (Calculated)`
            ELSE CAST((DATE_ADD((DATE_TRUNC('month', `Order: End Date (Calculated)`)), CAST(-1 AS INTEGER))) AS string)
          END
        ), 
        'yyyy-MM-dd')
    ) AS EndDate_Annualization,
    CAST(CASE
      WHEN ((`Order: Start Date` <= '2016-02-28') AND (`Order: End Date (Calculated)` >= '2016-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2020-02-28') AND (`Order: End Date (Calculated)` >= '2020-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2024-02-28') AND (`Order: End Date (Calculated)` >= '2024-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2028-02-28') AND (`Order: End Date (Calculated)` >= '2028-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      ELSE (CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT) + 1)
    END AS DOUBLE) AS TS_ContractDays,
    *
  
  FROM AlteryxSelect_940 AS in0

),

Formula_944_to_Formula_942_1 AS (

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
    CAST(((`Sum_Total Price (new)` / TS_ContractDays) * 365.25) AS DOUBLE) AS ACV,
    *
  
  FROM Formula_944_to_Formula_942_0 AS in0

),

Formula_944_to_Formula_942_2 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    *
  
  FROM Formula_944_to_Formula_942_1 AS in0

),

Summarize_888 AS (

  SELECT 
    DISTINCT `Order: Order` AS `Renewal_Order: Order`,
    `Order: Opportunity: Renewed Contract: Order: Order` AS `Renewed_Order: Opportunity: Renewed Contract: Order: Order`,
    StartDate_Annualization AS Renewal_StartDate_Annualization,
    `Order: Activated Date` AS `Renewal_Order: Activated Date`,
    EndDate_Annualization AS Renewal_EndDate_Annualization
  
  FROM Formula_944_to_Formula_942_2 AS in0

),

Join_889_right_UnionRightOuter AS (

  SELECT 
    in1.`Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    in1.`Product Code` AS `Product Code`,
    in1.`Order: Order` AS `Order: Order`,
    in1.`Order: Sales Order Number` AS `Order: Sales Order Number`,
    in1.`Order: Activated Date` AS `Order: Activated Date`,
    in1.`Order: Start Date` AS `Order: Start Date`,
    in1.`Order: End Date (Calculated)` AS `Order: End Date (Calculated)`,
    in1.`Order: Subscription Term` AS `Order: Subscription Term`,
    in1.`Sum_Total Price (new)` AS `Sum_Total Price (new)`,
    in1.Sum_Quantity AS Sum_Quantity,
    in1.`Order: Opportunity: Renewed Contract: Order: Order` AS `Order: Opportunity: Renewed Contract: Order: Order`,
    in1.`Item/Product` AS `Item/Product`,
    in1.Product AS Product,
    in1.StartDate_Annualization AS StartDate_Annualization,
    in1.EndDate_Annualization AS EndDate_Annualization,
    in1.ACV AS ACV,
    in1.TCV AS TCV,
    in0.Renewal_StartDate_Annualization AS Renewal_StartDate_Annualization,
    in0.Renewal_EndDate_Annualization AS Renewal_EndDate_Annualization,
    in0.`Renewal_Order: Activated Date` AS `Renewal_Order: Activated Date`,
    in0.`Renewal_Order: Order` AS `Renewal_Order: Order`,
    in0.`Renewed_Order: Opportunity: Renewed Contract: Order: Order` AS `Renewed_Order: Opportunity: Renewed Contract: Order: Order`,
    in1.`Order: Opportunity: Opportunity Name` AS `Order: Opportunity: Opportunity Name`,
    in1.TS_ContractDays AS TS_ContractDays,
    in1.Engine_ContractDays AS Engine_ContractDays,
    in0.* EXCEPT (`Renewal_StartDate_Annualization`, 
    `Renewal_EndDate_Annualization`, 
    `Renewal_Order: Activated Date`, 
    `Renewal_Order: Order`, 
    `Renewed_Order: Opportunity: Renewed Contract: Order: Order`),
    in1.* EXCEPT (`Order: Account Name: Mas90 Customer Number`, 
    `Product Code`, 
    `Order: Order`, 
    `Order: Sales Order Number`, 
    `Order: Activated Date`, 
    `Order: Start Date`, 
    `Order: End Date (Calculated)`, 
    `Order: Subscription Term`, 
    `Sum_Total Price (new)`, 
    `Sum_Quantity`, 
    `Order: Opportunity: Renewed Contract: Order: Order`, 
    `Item/Product`, 
    `Product`, 
    `StartDate_Annualization`, 
    `EndDate_Annualization`, 
    `ACV`, 
    `TCV`, 
    `Order: Opportunity: Opportunity Name`, 
    `TS_ContractDays`, 
    `Engine_ContractDays`)
  
  FROM Summarize_888 AS in0
  RIGHT JOIN Formula_944_to_Formula_942_2 AS in1
     ON (in0.`Renewal_Order: Order` = in1.`Order: Opportunity: Renewed Contract: Order: Order`)

),

Filter_982_reject AS (

  SELECT * 
  
  FROM Join_889_right_UnionRightOuter AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  (StartDate_Annualization <= Renewal_EndDate_Annualization)
                  AND (EndDate_Annualization <= Renewal_EndDate_Annualization))
              )
              OR (
                   (
                     (StartDate_Annualization <= Renewal_EndDate_Annualization)
                     AND (EndDate_Annualization <= Renewal_EndDate_Annualization)
                   ) IS NULL
                 )
            )
            AND (StartDate_Annualization <= Renewal_EndDate_Annualization)
          )
          AND (
                (
                  (
                    NOT(
                      UPPER(`Order: Activated Date`) = UPPER(`Renewal_Order: Activated Date`))
                  )
                  OR (`Order: Activated Date` IS NULL)
                )
                OR (`Renewal_Order: Activated Date` IS NULL)
              )
        )

),

Formula_896_to_Formula_1164_0 AS (

  SELECT 
    (DATE_ADD(Renewal_EndDate_Annualization, CAST(1 AS INTEGER))) AS StartDate_Annualization,
    (
      TO_DATE(
        (
          CASE
            WHEN (
              (
                ((SUBSTRING(`Order: Start Date`, 1, 7)) = (SUBSTRING(`Order: End Date (Calculated)`, 1, 7)))
                OR ((TO_DATE((DATE_TRUNC('month', `Order: Start Date`)))) = (TO_DATE(`Order: Start Date`)))
              )
              AND ((TO_DATE((LAST_DAY(CAST(`Order: End Date (Calculated)` AS DATE))))) = (TO_DATE(`Order: End Date (Calculated)`)))
            )
              THEN `Order: End Date (Calculated)`
            ELSE CAST((DATE_ADD((DATE_TRUNC('month', `Order: End Date (Calculated)`)), CAST(-1 AS INTEGER))) AS string)
          END
        ), 
        'yyyy-MM-dd')
    ) AS EndDate_Annualization,
    CAST(CASE
      WHEN ((`Order: Start Date` <= '2016-02-28') AND (`Order: End Date (Calculated)` >= '2016-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2020-02-28') AND (`Order: End Date (Calculated)` >= '2020-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2024-02-28') AND (`Order: End Date (Calculated)` >= '2024-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2028-02-28') AND (`Order: End Date (Calculated)` >= '2028-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      ELSE (CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT) + 1)
    END AS DOUBLE) AS TS_ContractDays,
    * EXCEPT (`startdate_annualization`, `ts_contractdays`, `enddate_annualization`)
  
  FROM Filter_982_reject AS in0

),

Formula_896_to_Formula_1164_1 AS (

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
    CAST(((`Sum_Total Price (new)` / TS_ContractDays) * 365.25) AS DOUBLE) AS ACV,
    * EXCEPT (`acv`, `engine_contractdays`)
  
  FROM Formula_896_to_Formula_1164_0 AS in0

),

Formula_896_to_Formula_1164_2 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    * EXCEPT (`tcv`)
  
  FROM Formula_896_to_Formula_1164_1 AS in0

),

Filter_982 AS (

  SELECT * 
  
  FROM Join_889_right_UnionRightOuter AS in0
  
  WHERE (
          (
            (
              (StartDate_Annualization <= Renewal_EndDate_Annualization)
              AND (EndDate_Annualization <= Renewal_EndDate_Annualization)
            )
            AND (StartDate_Annualization <= Renewal_EndDate_Annualization)
          )
          AND (
                (
                  (
                    NOT(
                      UPPER(`Order: Activated Date`) = UPPER(`Renewal_Order: Activated Date`))
                  )
                  OR (`Order: Activated Date` IS NULL)
                )
                OR (`Renewal_Order: Activated Date` IS NULL)
              )
        )

),

Filter_894_reject AS (

  SELECT * 
  
  FROM Join_889_right_UnionRightOuter AS in0
  
  WHERE (
          (
            NOT(
              (StartDate_Annualization <= Renewal_EndDate_Annualization)
              AND (
                    (
                      (
                        NOT(
                          UPPER(`Order: Activated Date`) = UPPER(`Renewal_Order: Activated Date`))
                      )
                      OR (`Order: Activated Date` IS NULL)
                    )
                    OR (`Renewal_Order: Activated Date` IS NULL)
                  ))
          )
          OR (
               (
                 (StartDate_Annualization <= Renewal_EndDate_Annualization)
                 AND (
                       (
                         (
                           NOT(
                             UPPER(`Order: Activated Date`) = UPPER(`Renewal_Order: Activated Date`))
                         )
                         OR (`Order: Activated Date` IS NULL)
                       )
                       OR (`Renewal_Order: Activated Date` IS NULL)
                     )
               ) IS NULL
             )
        )

),

Union_899 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_894_reject', 'Filter_982', 'Formula_896_to_Formula_1164_2'], 
      [
        '[{"name": "Renewed_Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "String"}, {"name": "Order: Opportunity: Opportunity Name", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Renewal_Order: Activated Date", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Business Subtype", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Renewal_Order: Order", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Order: Opportunity: Actual Closed Date", "dataType": "Date"}, {"name": "Item/Product", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Renewal_StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Renewal_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "Renewed_Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "String"}, {"name": "Order: Opportunity: Opportunity Name", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Renewal_Order: Activated Date", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Business Subtype", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Renewal_Order: Order", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Order: Opportunity: Actual Closed Date", "dataType": "Date"}, {"name": "Item/Product", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Renewal_StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Renewal_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "Renewed_Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "String"}, {"name": "Order: Opportunity: Opportunity Name", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Renewal_Order: Activated Date", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Business Subtype", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Renewal_Order: Order", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Order: Opportunity: Actual Closed Date", "dataType": "Date"}, {"name": "Item/Product", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Renewal_StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Renewal_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Unique_123 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Unique_123')}}

),

Join_125_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Loser Mas90 Customer Number`)
          THEN in1.`Winner Mas90 Customer Number`
        ELSE NULL
      END
    ) AS `Order: Account Name: Mas90 Customer Number`,
    in0.* EXCEPT (`Order: Account Name: Mas90 Customer Number`),
    in1.* EXCEPT (`Winner Mas90 Customer Number`)
  
  FROM Union_899 AS in0
  LEFT JOIN Unique_123 AS in1
     ON (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Loser Mas90 Customer Number`)

),

Join_675_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Transaction: Account: Mas90 Customer Number`, 
    `Transaction: Order: Sales Order Number`, 
    `Product Code`, 
    `Created Date`, 
    `Transaction: Order: Order`)
  
  FROM Join_125_left_UnionLeftOuter AS in0
  INNER JOIN Union_1866 AS in1
     ON (
      (in0.`Order: Sales Order Number` = in1.`Transaction: Order: Sales Order Number`)
      AND (in0.`Product Code` = in1.`Product Code`)
    )

),

Filter_664 AS (

  SELECT * 
  
  FROM Join_675_inner AS in0
  
  WHERE (
          (
            (
              CASE
                WHEN (
                  ((`Sum_Total Price (new)` / 0.01) < 0)
                  AND (((`Sum_Total Price (new)` / 0.01) - FLOOR((`Sum_Total Price (new)` / 0.01))) = 0.5)
                )
                  THEN CEIL((`Sum_Total Price (new)` / 0.01))
                ELSE ROUND((`Sum_Total Price (new)` / 0.01))
              END
            )
            * 0.01
          ) < (
            (
              CASE
                WHEN (
                  ((`Reduction Amount` / 0.01) < 0)
                  AND (((`Reduction Amount` / 0.01) - FLOOR((`Reduction Amount` / 0.01))) = 0.5)
                )
                  THEN CEIL((`Reduction Amount` / 0.01))
                ELSE ROUND((`Reduction Amount` / 0.01))
              END
            )
            * 0.01
          )
        )

),

Filter_664_reject AS (

  SELECT * 
  
  FROM Join_675_inner AS in0
  
  WHERE (
          (
            NOT(
              (
                (
                  CASE
                    WHEN (
                      ((`Sum_Total Price (new)` / 0.01) < 0)
                      AND (((`Sum_Total Price (new)` / 0.01) - FLOOR((`Sum_Total Price (new)` / 0.01))) = 0.5)
                    )
                      THEN CEIL((`Sum_Total Price (new)` / 0.01))
                    ELSE ROUND((`Sum_Total Price (new)` / 0.01))
                  END
                )
                * 0.01
              ) < (
                (
                  CASE
                    WHEN (
                      ((`Reduction Amount` / 0.01) < 0)
                      AND (((`Reduction Amount` / 0.01) - FLOOR((`Reduction Amount` / 0.01))) = 0.5)
                    )
                      THEN CEIL((`Reduction Amount` / 0.01))
                    ELSE ROUND((`Reduction Amount` / 0.01))
                  END
                )
                * 0.01
              ))
          )
          OR (
               (
                 (
                   (
                     CASE
                       WHEN (
                         ((`Sum_Total Price (new)` / 0.01) < 0)
                         AND (((`Sum_Total Price (new)` / 0.01) - FLOOR((`Sum_Total Price (new)` / 0.01))) = 0.5)
                       )
                         THEN CEIL((`Sum_Total Price (new)` / 0.01))
                       ELSE ROUND((`Sum_Total Price (new)` / 0.01))
                     END
                   )
                   * 0.01
                 ) < (
                   (
                     CASE
                       WHEN (
                         ((`Reduction Amount` / 0.01) < 0)
                         AND (((`Reduction Amount` / 0.01) - FLOOR((`Reduction Amount` / 0.01))) = 0.5)
                       )
                         THEN CEIL((`Reduction Amount` / 0.01))
                       ELSE ROUND((`Reduction Amount` / 0.01))
                     END
                   )
                   * 0.01
                 )
               ) IS NULL
             )
        )

),

Formula_663_to_Formula_1166_0 AS (

  SELECT 
    CAST(`Sum_Total Price (new)` AS DOUBLE) AS `PreReductions_Total Price`,
    CAST((`Sum_Total Price (new)` - `Reduction Amount`) AS DOUBLE) AS `Sum_Total Price (new)`,
    CAST(CASE
      WHEN ((`Order: Start Date` <= '2016-02-28') AND (`Order: End Date (Calculated)` >= '2016-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2020-02-28') AND (`Order: End Date (Calculated)` >= '2020-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2024-02-28') AND (`Order: End Date (Calculated)` >= '2024-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2028-02-28') AND (`Order: End Date (Calculated)` >= '2028-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      ELSE (CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT) + 1)
    END AS DOUBLE) AS TS_ContractDays,
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
    * EXCEPT (`ts_contractdays`, `sum_total price (new)`, `engine_contractdays`)
  
  FROM Filter_664_reject AS in0

),

Formula_663_to_Formula_1166_1 AS (

  SELECT 
    CAST(((`Sum_Total Price (new)` / TS_ContractDays) * 365.25) AS DOUBLE) AS ACV,
    * EXCEPT (`acv`)
  
  FROM Formula_663_to_Formula_1166_0 AS in0

),

Formula_663_to_Formula_1166_2 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    * EXCEPT (`tcv`)
  
  FROM Formula_663_to_Formula_1166_1 AS in0

),

Join_675_left AS (

  SELECT in0.*
  
  FROM Join_125_left_UnionLeftOuter AS in0
  ANTI JOIN Union_1866 AS in1
     ON (
      (in0.`Order: Sales Order Number` = in1.`Transaction: Order: Sales Order Number`)
      AND (in0.`Product Code` = in1.`Product Code`)
    )

),

Formula_681_0 AS (

  SELECT 
    CAST(`Sum_Total Price (new)` AS DOUBLE) AS `PreReductions_Total Price`,
    *
  
  FROM Join_675_left AS in0

),

Formula_666_to_Formula_1165_0 AS (

  SELECT 
    CAST(`Sum_Total Price (new)` AS DOUBLE) AS `PreReductions_Total Price`,
    CAST(`Sum_Total Price (new)` AS DOUBLE) AS `Sum_Total Price (new)`,
    CAST(CASE
      WHEN ((`Order: Start Date` <= '2016-02-28') AND (`Order: End Date (Calculated)` >= '2016-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2020-02-28') AND (`Order: End Date (Calculated)` >= '2020-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2024-02-28') AND (`Order: End Date (Calculated)` >= '2024-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      WHEN ((`Order: Start Date` <= '2028-02-28') AND (`Order: End Date (Calculated)` >= '2028-02-29'))
        THEN CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT)
      ELSE (CAST(datediff(to_date(`Order: End Date (Calculated)`), to_date(`Order: Start Date`)) AS INT) + 1)
    END AS DOUBLE) AS TS_ContractDays,
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
    * EXCEPT (`ts_contractdays`, `sum_total price (new)`, `engine_contractdays`)
  
  FROM Filter_664 AS in0

),

Formula_666_to_Formula_1165_1 AS (

  SELECT 
    CAST(((`Sum_Total Price (new)` / TS_ContractDays) * 365.25) AS DOUBLE) AS ACV,
    * EXCEPT (`acv`)
  
  FROM Formula_666_to_Formula_1165_0 AS in0

),

Formula_666_to_Formula_1165_2 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    * EXCEPT (`tcv`)
  
  FROM Formula_666_to_Formula_1165_1 AS in0

),

Union_677 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_681_0', 'Formula_666_to_Formula_1165_2', 'Formula_663_to_Formula_1166_2'], 
      [
        '[{"name": "Renewed_Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "String"}, {"name": "Order: Opportunity: Opportunity Name", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Renewal_Order: Activated Date", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Business Subtype", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Renewal_Order: Order", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Loser Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Order: Opportunity: Actual Closed Date", "dataType": "Date"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Item/Product", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Renewal_StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Renewal_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "Renewed_Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "String"}, {"name": "Order: Opportunity: Opportunity Name", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Renewal_Order: Activated Date", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Business Subtype", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Renewal_Order: Order", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Loser Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Order: Opportunity: Actual Closed Date", "dataType": "Date"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Item/Product", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Reduction Amount", "dataType": "Double"}, {"name": "Renewal_StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Renewal_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]', 
        '[{"name": "Renewed_Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "String"}, {"name": "Order: Opportunity: Opportunity Name", "dataType": "String"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Renewal_Order: Activated Date", "dataType": "String"}, {"name": "Order: Subscription Term", "dataType": "String"}, {"name": "Order: Business Subtype", "dataType": "String"}, {"name": "Order: Order", "dataType": "String"}, {"name": "Order: Opportunity: Renewed Contract: Order: Order", "dataType": "String"}, {"name": "Renewal_Order: Order", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "Order: End Date (Calculated)", "dataType": "String"}, {"name": "Order: Activated Date", "dataType": "String"}, {"name": "ACV", "dataType": "Double"}, {"name": "TCV", "dataType": "Double"}, {"name": "Product Code", "dataType": "String"}, {"name": "TS_ContractDays", "dataType": "Double"}, {"name": "Loser Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Total Price (new)", "dataType": "Double"}, {"name": "Order: Opportunity: Actual Closed Date", "dataType": "Date"}, {"name": "PreReductions_Total Price", "dataType": "Double"}, {"name": "Item/Product", "dataType": "String"}, {"name": "Engine_ContractDays", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "Reduction Amount", "dataType": "Double"}, {"name": "Renewal_StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}, {"name": "Renewal_EndDate_Annualization", "dataType": "Date"}, {"name": "Order: Start Date", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_677
