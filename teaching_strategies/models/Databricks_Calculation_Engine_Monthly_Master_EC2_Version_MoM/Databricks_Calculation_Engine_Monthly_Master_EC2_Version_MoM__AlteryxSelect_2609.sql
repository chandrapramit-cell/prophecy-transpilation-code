{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_2820_to_Formula_2806_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2820_to_Formula_2806_0')}}

),

Formula_2872_to_Formula_2871_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2872_to_Formula_2871_0')}}

),

Formula_2873_to_Formula_2876_0 AS (

  SELECT 
    (TO_DATE(First_PosRevMonth, 'yyyy-MM-dd')) AS Cohort,
    *
  
  FROM Formula_2872_to_Formula_2871_0 AS in0

),

Formula_2873_to_Formula_2876_1 AS (

  SELECT 
    CAST(CAST((MONTHS_BETWEEN((TO_DATE(RevMonth)), (TO_DATE(Cohort)))) AS INTEGER) AS INTEGER) AS `Cohort Tenure`,
    CAST(`Customer Active Flag` AS INTEGER) AS `Cohort Active Flag`,
    (TO_DATE(Cust_First_PosRevMonth, 'yyyy-MM-dd')) AS `Customer Level Cohort`,
    *
  
  FROM Formula_2873_to_Formula_2876_0 AS in0

),

Formula_2873_to_Formula_2876_2 AS (

  SELECT 
    CAST(CAST((MONTHS_BETWEEN((TO_DATE(RevMonth)), (TO_DATE(`Customer Level Cohort`)))) AS INTEGER) AS INTEGER) AS `Customer Level Cohort Tenure`,
    CAST((ARRAY_MIN((ARRAY(`Initial Revenue`, Revenue)))) AS DOUBLE) AS `Cohort Gross Retention`,
    CAST((
      CASE
        WHEN (`Cohort Active Flag` = 1)
          THEN `Initial Revenue`
        ELSE 0
      END
    ) AS DOUBLE) AS `Cohort Gross Customer Retention`,
    *
  
  FROM Formula_2873_to_Formula_2876_1 AS in0

),

Filter_2877_reject AS (

  SELECT * 
  
  FROM Formula_2873_to_Formula_2876_2 AS in0
  
  WHERE (
          NOT (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      NOT(
                                                        `Customer Segment` = `Prev_Customer Segment`)
                                                    ) OR isnull(`Customer Segment`)
                                                  )
                                                  OR isnull(`Prev_Customer Segment`)
                                                )
                                                OR (
                                                     NOT(
                                                       SubCustSeg1 = Prev_SubCustSeg1)
                                                   )
                                              )
                                              OR isnull(SubCustSeg1)
                                            )
                                            OR isnull(Prev_SubCustSeg1)
                                          )
                                          OR (
                                               NOT(
                                                 SubCustSeg2 = Prev_SubCustSeg2)
                                             )
                                        )
                                        OR isnull(SubCustSeg2)
                                      )
                                      OR isnull(Prev_SubCustSeg2)
                                    )
                                    OR (
                                         NOT(
                                           SubCustSeg3 = Prev_SubCustSeg3)
                                       )
                                  )
                                  OR isnull(SubCustSeg3)
                                )
                                OR isnull(Prev_SubCustSeg3)
                              )
                              OR (
                                   NOT(
                                     SubCustSeg4 = Prev_SubCustSeg4)
                                 )
                            )
                            OR isnull(SubCustSeg4)
                          )
                          OR isnull(Prev_SubCustSeg4)
                        )
                        OR (
                             NOT(
                               SubCustSeg5 = Prev_SubCustSeg5)
                           )
                      )
                      OR isnull(SubCustSeg5)
                    )
                    OR isnull(Prev_SubCustSeg5)
                  )
                  OR (
                       NOT(
                         SubCustSeg6 = Prev_SubCustSeg6)
                     )
                )
                OR isnull(SubCustSeg6)
              )
              OR isnull(Prev_SubCustSeg6)
            )
          )
          OR isnull(
               (
                 (
                   (
                     (
                       (
                         (
                           (
                             (
                               (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     (
                                                       (
                                                         NOT(
                                                           `Customer Segment` = `Prev_Customer Segment`)
                                                       ) OR isnull(`Customer Segment`)
                                                     )
                                                     OR isnull(`Prev_Customer Segment`)
                                                   )
                                                   OR (
                                                        NOT(
                                                          SubCustSeg1 = Prev_SubCustSeg1)
                                                      )
                                                 )
                                                 OR isnull(SubCustSeg1)
                                               )
                                               OR isnull(Prev_SubCustSeg1)
                                             )
                                             OR (
                                                  NOT(
                                                    SubCustSeg2 = Prev_SubCustSeg2)
                                                )
                                           )
                                           OR isnull(SubCustSeg2)
                                         )
                                         OR isnull(Prev_SubCustSeg2)
                                       )
                                       OR (
                                            NOT(
                                              SubCustSeg3 = Prev_SubCustSeg3)
                                          )
                                     )
                                     OR isnull(SubCustSeg3)
                                   )
                                   OR isnull(Prev_SubCustSeg3)
                                 )
                                 OR (
                                      NOT(
                                        SubCustSeg4 = Prev_SubCustSeg4)
                                    )
                               )
                               OR isnull(SubCustSeg4)
                             )
                             OR isnull(Prev_SubCustSeg4)
                           )
                           OR (
                                NOT(
                                  SubCustSeg5 = Prev_SubCustSeg5)
                              )
                         )
                         OR isnull(SubCustSeg5)
                       )
                       OR isnull(Prev_SubCustSeg5)
                     )
                     OR (
                          NOT(
                            SubCustSeg6 = Prev_SubCustSeg6)
                        )
                   )
                   OR isnull(SubCustSeg6)
                 )
                 OR isnull(Prev_SubCustSeg6)
               ))
        )

),

Filter_2877 AS (

  SELECT * 
  
  FROM Formula_2873_to_Formula_2876_2 AS in0
  
  WHERE (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  NOT(
                                                    `Customer Segment` = `Prev_Customer Segment`)
                                                ) OR isnull(`Customer Segment`)
                                              )
                                              OR isnull(`Prev_Customer Segment`)
                                            )
                                            OR (
                                                 NOT(
                                                   SubCustSeg1 = Prev_SubCustSeg1)
                                               )
                                          )
                                          OR isnull(SubCustSeg1)
                                        )
                                        OR isnull(Prev_SubCustSeg1)
                                      )
                                      OR (
                                           NOT(
                                             SubCustSeg2 = Prev_SubCustSeg2)
                                         )
                                    )
                                    OR isnull(SubCustSeg2)
                                  )
                                  OR isnull(Prev_SubCustSeg2)
                                )
                                OR (
                                     NOT(
                                       SubCustSeg3 = Prev_SubCustSeg3)
                                   )
                              )
                              OR isnull(SubCustSeg3)
                            )
                            OR isnull(Prev_SubCustSeg3)
                          )
                          OR (
                               NOT(
                                 SubCustSeg4 = Prev_SubCustSeg4)
                             )
                        )
                        OR isnull(SubCustSeg4)
                      )
                      OR isnull(Prev_SubCustSeg4)
                    )
                    OR (
                         NOT(
                           SubCustSeg5 = Prev_SubCustSeg5)
                       )
                  )
                  OR isnull(SubCustSeg5)
                )
                OR isnull(Prev_SubCustSeg5)
              )
              OR (
                   NOT(
                     SubCustSeg6 = Prev_SubCustSeg6)
                 )
            )
            OR isnull(SubCustSeg6)
          )
          OR isnull(Prev_SubCustSeg6)
        )

),

GenerateRows_2878 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Filter_2877'], 
      '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Cust_First_PosRevMonth", "dataType": "Date"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Customer Active Flag", "dataType": "Integer"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust_Level_Revenue", "dataType": "Double"}, {"name": "Customer Level Cohort Tenure", "dataType": "Integer"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Last_NonZeroRevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "First_NonZeroRevMonth", "dataType": "Date"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "check", "dataType": "Boolean"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "First_PosRevMonth", "dataType": "Date"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Cust_Last_PosRevMonth", "dataType": "Date"}, {"name": "Last_PosRevMonth", "dataType": "Date"}, {"name": "Product", "dataType": "String"}, {"name": "Cohort Active Flag", "dataType": "Integer"}, {"name": "Max_Cust_Level_Revenue", "dataType": "Double"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Customer Level Cohort", "dataType": "Date"}]', 
      '1', 
      '(RowCount <= 2)', 
      '(RowCount + 1)', 
      'RowCount', 
      '100', 
      'recursive'
    )
  }}

),

Formula_2879_to_Formula_2910_0 AS (

  SELECT 
    (TO_DATE(NULL, 'yyyy-MM-dd')) AS Cohort,
    CAST(NULL AS INTEGER) AS `Cohort Tenure`,
    CAST(NULL AS INTEGER) AS `Cohort Active Flag`,
    CAST(NULL AS DOUBLE) AS `Cohort Gross Retention`,
    CAST(NULL AS DOUBLE) AS `Cohort Gross Customer Retention`,
    CAST(0 AS DOUBLE) AS Revenue,
    CAST('Customer Segment Migration' AS string) AS `Change Category`,
    CAST(0 AS DOUBLE) AS Volume,
    * EXCEPT (`cohort gross customer retention`, 
    `cohort`, 
    `volume`, 
    `cohort gross retention`, 
    `revenue`, 
    `cohort active flag`, 
    `cohort tenure`)
  
  FROM GenerateRows_2878 AS in0

),

AlteryxSelect_2881 AS (

  SELECT * EXCEPT (`RowCount`)
  
  FROM Formula_2879_to_Formula_2910_0 AS in0

),

Union_2880 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_2877_reject', 'Filter_2877', 'AlteryxSelect_2881'], 
      [
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Cust_First_PosRevMonth", "dataType": "Date"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Customer Active Flag", "dataType": "Integer"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust_Level_Revenue", "dataType": "Double"}, {"name": "Customer Level Cohort Tenure", "dataType": "Integer"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Last_NonZeroRevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "First_NonZeroRevMonth", "dataType": "Date"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "check", "dataType": "Boolean"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "First_PosRevMonth", "dataType": "Date"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Cust_Last_PosRevMonth", "dataType": "Date"}, {"name": "Last_PosRevMonth", "dataType": "Date"}, {"name": "Product", "dataType": "String"}, {"name": "Cohort Active Flag", "dataType": "Integer"}, {"name": "Max_Cust_Level_Revenue", "dataType": "Double"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Customer Level Cohort", "dataType": "Date"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Cust_First_PosRevMonth", "dataType": "Date"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Customer Active Flag", "dataType": "Integer"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust_Level_Revenue", "dataType": "Double"}, {"name": "Customer Level Cohort Tenure", "dataType": "Integer"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Last_NonZeroRevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "First_NonZeroRevMonth", "dataType": "Date"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "check", "dataType": "Boolean"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "First_PosRevMonth", "dataType": "Date"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Cust_Last_PosRevMonth", "dataType": "Date"}, {"name": "Last_PosRevMonth", "dataType": "Date"}, {"name": "Product", "dataType": "String"}, {"name": "Cohort Active Flag", "dataType": "Integer"}, {"name": "Max_Cust_Level_Revenue", "dataType": "Double"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Customer Level Cohort", "dataType": "Date"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Cust_First_PosRevMonth", "dataType": "Date"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Customer Active Flag", "dataType": "Integer"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust_Level_Revenue", "dataType": "Double"}, {"name": "Customer Level Cohort Tenure", "dataType": "Integer"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Change Category", "dataType": "String"}, {"name": "Last_NonZeroRevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "First_NonZeroRevMonth", "dataType": "Date"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "check", "dataType": "Boolean"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "First_PosRevMonth", "dataType": "Date"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Cust_Last_PosRevMonth", "dataType": "Date"}, {"name": "Last_PosRevMonth", "dataType": "Date"}, {"name": "Product", "dataType": "String"}, {"name": "Cohort Active Flag", "dataType": "Integer"}, {"name": "Max_Cust_Level_Revenue", "dataType": "Double"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Customer Level Cohort", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_2890 AS (

  SELECT * EXCEPT (`Cust_Level_Revenue`, 
         `First_PosRevMonth`, 
         `Last_PosRevMonth`, 
         `Cust_First_PosRevMonth`, 
         `Cust_Last_PosRevMonth`)
  
  FROM Union_2880 AS in0

),

MultiFieldFormula_2912 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['AlteryxSelect_2890'], 
      "CASE WHEN CAST(isnull(column_value) AS BOOLEAN) THEN 0 WHEN (isnull(column_value) OR (length(CAST(column_value AS STRING)) = 0)) THEN 0 ELSE column_value END", 
      [
        'SubCustSeg5', 
        'Previous YetToRenew', 
        'Cohort Gross Customer Retention', 
        'Cohort', 
        'Volume', 
        'SubCustSeg1', 
        'Customer Active Flag', 
        'Prev_Product', 
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
        'Cust_Prod Active Flag', 
        'check', 
        'SubCustSeg3', 
        'SubCustSeg4', 
        'YetToRenew', 
        'Product', 
        'Cohort Active Flag', 
        'Max_Cust_Level_Revenue', 
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

Formula_2911_to_Formula_2891_0 AS (

  SELECT 
    CAST('N' AS string) AS `Customer Level Flag`,
    CAST('N/A' AS string) AS `Product Category`,
    *
  
  FROM MultiFieldFormula_2912 AS in0

),

AlteryxSelect_2915 AS (

  SELECT 
    CustomerName AS CustomerName,
    `Customer Segment` AS `Customer Segment`,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg2 AS SubCustSeg2,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    Product AS Product,
    RevMonth AS RevMonth,
    Revenue AS Revenue,
    YetToRenew AS YetToRenew,
    Volume AS Volume,
    `Cust_Prod Active Flag` AS `Customer Active Flag`,
    Prev_Product AS Prev_Product,
    `Previous YetToRenew` AS `Previous YetToRenew`,
    First_NonZeroRevMonth AS First_NonZeroRevMonth,
    Last_NonZeroRevMonth AS Last_NonZeroRevMonth,
    `Initial Revenue` AS `Initial Revenue`,
    `Change Category` AS `Change Category`,
    Cohort AS Cohort,
    `Cohort Tenure` AS `Cohort Tenure`,
    `Cohort Active Flag` AS `Cohort Active Flag`,
    `Customer Level Cohort` AS `Customer Level Cohort`,
    `Customer Level Cohort Tenure` AS `Customer Level Cohort Tenure`,
    `Cohort Gross Retention` AS `Cohort Gross Retention`,
    `Cohort Gross Customer Retention` AS `Cohort Gross Customer Retention`,
    `Customer Level Flag` AS `Customer Level Flag`,
    `Product Category` AS `Product Category`,
    * EXCEPT (`Customer Active Flag`, 
    `CustomerName`, 
    `Customer Segment`, 
    `SubCustSeg1`, 
    `SubCustSeg2`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg5`, 
    `SubCustSeg6`, 
    `Product`, 
    `RevMonth`, 
    `Revenue`, 
    `YetToRenew`, 
    `Volume`, 
    `Prev_Product`, 
    `Previous YetToRenew`, 
    `First_NonZeroRevMonth`, 
    `Last_NonZeroRevMonth`, 
    `Initial Revenue`, 
    `Change Category`, 
    `Cohort`, 
    `Cohort Tenure`, 
    `Cohort Active Flag`, 
    `Customer Level Cohort`, 
    `Customer Level Cohort Tenure`, 
    `Cohort Gross Retention`, 
    `Cohort Gross Customer Retention`, 
    `Customer Level Flag`, 
    `Product Category`, 
    `Cust_Prod Active Flag`)
  
  FROM Formula_2911_to_Formula_2891_0 AS in0

),

Union_2608 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_2820_to_Formula_2806_0', 'AlteryxSelect_2915'], 
      [
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Prev_Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Customer Active Flag", "dataType": "Integer"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Customer Level Cohort Tenure", "dataType": "Integer"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Change Category", "dataType": "String"}, {"name": "Last_NonZeroRevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "First_NonZeroRevMonth", "dataType": "Date"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Product Category", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Cohort Active Flag", "dataType": "Integer"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Customer Level Cohort", "dataType": "Date"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Customer Active Flag", "dataType": "Integer"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Customer Level Cohort Tenure", "dataType": "Integer"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Change Category", "dataType": "String"}, {"name": "Last_NonZeroRevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "First_NonZeroRevMonth", "dataType": "Date"}, {"name": "check", "dataType": "Boolean"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Product Category", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Cohort Active Flag", "dataType": "Integer"}, {"name": "Max_Cust_Level_Revenue", "dataType": "Double"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Customer Level Cohort", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_2618_0 AS (

  SELECT 
    CAST(`Change Category` AS string) AS `Raw Change Category`,
    *
  
  FROM Union_2608 AS in0

),

AlteryxSelect_2609 AS (

  SELECT 
    CustomerName AS `Customer Name`,
    Product AS Product,
    RevMonth AS `Revenue Period`,
    Revenue AS Revenue,
    `Initial Revenue` AS `Initial Revenue`,
    `Raw Change Category` AS `Raw Change Category`,
    `Change Category` AS `Change Category`,
    `Customer Active Flag` AS `Cust Active Flag`,
    Cohort AS Cohort,
    `Cohort Tenure` AS `Cohort Tenure`,
    `Customer Level Cohort` AS `Cust Level Cohort`,
    `Customer Level Cohort Tenure` AS `Cust Level Cohort Tenure`,
    `Cohort Gross Retention` AS `Cohort Gross Retention`,
    `Cohort Gross Customer Retention` AS `Cohort Gross Customer Retention`,
    `Customer Level Flag` AS `Customer Level Flag`,
    `Customer Segment` AS `Customer Segment`,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg2 AS SubCustSeg2,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    `Cust_Prod Active Flag` AS `Cust_Prod Active Flag`,
    `Prev_Cust_Prod Active Flag` AS `Prev_Cust_Prod Active Flag`,
    Prev_Product AS Prev_Product,
    Volume AS Volume,
    `Product Category` AS `Product Category`,
    YetToRenew AS YetToRenew,
    `Previous YetToRenew` AS `Previous YetToRenew`,
    check AS `check`,
    * EXCEPT (`Cohort Active Flag`, 
    `First_NonZeroRevMonth`, 
    `Last_NonZeroRevMonth`, 
    `Product`, 
    `Revenue`, 
    `Initial Revenue`, 
    `Raw Change Category`, 
    `Change Category`, 
    `Cohort`, 
    `Cohort Tenure`, 
    `Cohort Gross Retention`, 
    `Cohort Gross Customer Retention`, 
    `Customer Level Flag`, 
    `Customer Segment`, 
    `SubCustSeg1`, 
    `SubCustSeg2`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg5`, 
    `SubCustSeg6`, 
    `Cust_Prod Active Flag`, 
    `Prev_Cust_Prod Active Flag`, 
    `Prev_Product`, 
    `Volume`, 
    `Product Category`, 
    `YetToRenew`, 
    `Previous YetToRenew`, 
    `check`, 
    `CustomerName`, 
    `RevMonth`, 
    `Customer Active Flag`, 
    `Customer Level Cohort`, 
    `Customer Level Cohort Tenure`)
  
  FROM Formula_2618_0 AS in0

)

SELECT *

FROM AlteryxSelect_2609
