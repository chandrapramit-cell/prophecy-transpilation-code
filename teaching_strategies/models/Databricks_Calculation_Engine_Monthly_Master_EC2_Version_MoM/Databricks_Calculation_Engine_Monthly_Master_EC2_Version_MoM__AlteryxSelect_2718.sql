{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_3342 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3342')}}

),

AlteryxSelect_2703 AS (

  SELECT 
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    `Acquired ARR from ReadyRosie` AS `Acquired ARR from ReadyRosie`,
    `Acquired ARR from Quorum (QualityAssist)` AS `Acquired ARR from Quorum (QualityAssist)`
  
  FROM AlteryxSelect_3342 AS in0

),

Filter_2720_to_Filter_2731 AS (

  SELECT * 
  
  FROM AlteryxSelect_3342 AS in0
  
  WHERE (
          (
            (
              NOT(
                (LENGTH(`Mas90 Customer Number`)) = 0)
            )
            AND (NOT(`Date of First Activated Order` IS NULL))
          )
          AND (
                NOT(
                  (`Date of First Activated Order` IS NULL)
                  OR ((LENGTH(CAST(`Date of First Activated Order` AS string))) = 0))
              )
        )

),

AlteryxSelect_2723 AS (

  SELECT *
  
  FROM Filter_2720_to_Filter_2731 AS in0

),

Formula_2724_0 AS (

  SELECT 
    (TO_DATE((DATE_TRUNC('month', `Date of First Activated Order`)), 'yyyy-MM-dd')) AS `New Date`,
    *
  
  FROM AlteryxSelect_2723 AS in0

),

AlteryxSelect_2725 AS (

  SELECT 
    `New Date` AS `Date of First Activated Order`,
    * EXCEPT (`Date of First Activated Order`, `New Date`)
  
  FROM Formula_2724_0 AS in0

),

Summarize_2730 AS (

  SELECT 
    first(`Date of First Activated Order`) AS `First_Date of First Activated Order`,
    `Mas90 Customer Number` AS `Mas90 Customer Number`
  
  FROM AlteryxSelect_2725 AS in0
  
  GROUP BY `Mas90 Customer Number`

),

AlteryxSelect_2609 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_2609')}}

),

Filter_2704_to_Filter_2705 AS (

  SELECT * 
  
  FROM AlteryxSelect_2703 AS in0
  
  WHERE (
          (
            (
              NOT(
                (`Acquired ARR from ReadyRosie` IS NULL)
                OR ((LENGTH(CAST(`Acquired ARR from ReadyRosie` AS string))) = 0))
            )
            AND (NOT((`Mas90 Customer Number` IS NULL) OR ((LENGTH(`Mas90 Customer Number`)) = 0)))
          )
          AND (
                NOT(
                  `Acquired ARR from ReadyRosie` = 0)
              )
        )

),

Filter_2706 AS (

  SELECT * 
  
  FROM AlteryxSelect_2609 AS in0
  
  WHERE (
          (
            (UPPER(`Customer Level Flag`) = UPPER('N'))
            AND (CAST(`Change Category` AS string) IN ('New Customer', 'Customer Reactivation', 'Existing Customer, New Product', 'Product Reactivation'))
          )
          AND (UPPER(Product) = UPPER('ReadyRosie'))
        )

),

Filter_2707 AS (

  SELECT * 
  
  FROM Filter_2706 AS in0
  
  WHERE ((`Revenue Period` >= to_date('2019-06-01')) AND (`Revenue Period` <= to_date('2020-05-31')))

),

Join_2708_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Customer Name` = in1.`Mas90 Customer Number`)
          THEN 'Acquisition'
        ELSE NULL
      END
    ) AS `Change Category`,
    (
      CASE
        WHEN (in0.`Customer Name` = in1.`Mas90 Customer Number`)
          THEN 'Acquisition'
        ELSE NULL
      END
    ) AS `Raw Change Category`,
    in0.* EXCEPT (`Change Category`, `Raw Change Category`),
    in1.* EXCEPT (`Mas90 Customer Number`, `Acquired ARR from ReadyRosie`, `Acquired ARR from Quorum (QualityAssist)`)
  
  FROM Filter_2707 AS in0
  LEFT JOIN Filter_2704_to_Filter_2705 AS in1
     ON (in0.`Customer Name` = in1.`Mas90 Customer Number`)

),

Filter_2706_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_2609 AS in0
  
  WHERE (
          (
            NOT(
              (
                (UPPER(`Customer Level Flag`) = UPPER('N'))
                AND (CAST(`Change Category` AS string) IN ('New Customer', 'Customer Reactivation', 'Existing Customer, New Product', 'Product Reactivation'))
              )
              AND (UPPER(Product) = UPPER('ReadyRosie')))
          )
          OR (
               (
                 (
                   (UPPER(`Customer Level Flag`) = UPPER('N'))
                   AND (CAST(`Change Category` AS string) IN ('New Customer', 'Customer Reactivation', 'Existing Customer, New Product', 'Product Reactivation'))
                 )
                 AND (UPPER(Product) = UPPER('ReadyRosie'))
               ) IS NULL
             )
        )

),

Filter_2707_reject AS (

  SELECT * 
  
  FROM Filter_2706 AS in0
  
  WHERE (
          NOT (((`Revenue Period` >= to_date('2019-06-01')) AND (`Revenue Period` <= to_date('2020-05-31'))))
          OR isnull(((`Revenue Period` >= to_date('2019-06-01')) AND (`Revenue Period` <= to_date('2020-05-31'))))
        )

),

Union_2710 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_2706_reject', 'Filter_2707_reject', 'Join_2708_inner_UnionLeftOuter'], 
      [
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Prev_Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Raw Change Category", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust Level Cohort Tenure", "dataType": "Integer"}, {"name": "Customer Name", "dataType": "String"}, {"name": "Change Category", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "check", "dataType": "Boolean"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Cust Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Revenue Period", "dataType": "Date"}, {"name": "Product Category", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Cust Level Cohort", "dataType": "Date"}, {"name": "Max_Cust_Level_Revenue", "dataType": "Double"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Prev_Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Raw Change Category", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust Level Cohort Tenure", "dataType": "Integer"}, {"name": "Customer Name", "dataType": "String"}, {"name": "Change Category", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "check", "dataType": "Boolean"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Cust Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Revenue Period", "dataType": "Date"}, {"name": "Product Category", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Cust Level Cohort", "dataType": "Date"}, {"name": "Max_Cust_Level_Revenue", "dataType": "Double"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Prev_Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Raw Change Category", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust Level Cohort Tenure", "dataType": "Integer"}, {"name": "Customer Name", "dataType": "String"}, {"name": "Change Category", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "check", "dataType": "Boolean"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Cust Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Revenue Period", "dataType": "Date"}, {"name": "Product Category", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Cust Level Cohort", "dataType": "Date"}, {"name": "Max_Cust_Level_Revenue", "dataType": "Double"}, {"name": "Cohort Tenure", "dataType": "Integer"}, {"name": "SubCustSeg2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_2740_reject AS (

  SELECT * 
  
  FROM Union_2710 AS in0
  
  WHERE (
          (
            NOT(
              (UPPER(`Customer Level Flag`) = UPPER('Y'))
              AND (
                    (
                      NOT(
                        Revenue = 0)
                    ) OR (Revenue IS NULL)
                  ))
          )
          OR (
               (
                 (UPPER(`Customer Level Flag`) = UPPER('Y'))
                 AND (
                       (
                         NOT(
                           Revenue = 0)
                       ) OR (Revenue IS NULL)
                     )
               ) IS NULL
             )
        )

),

Union_2748_2 AS (

  SELECT 
    CAST(Revenue AS DOUBLE) AS prophecy_column_5,
    CAST(SubCustSeg3 AS string) AS prophecy_column_24,
    CAST(Prev_Product AS string) AS prophecy_column_37,
    CAST(SubCustSeg4 AS string) AS prophecy_column_25,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_14,
    CAST(`Customer Level Flag` AS string) AS prophecy_column_20,
    CAST(`Previous YetToRenew` AS DOUBLE) AS prophecy_column_46,
    CAST(`Prev_Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_29,
    CAST(`Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_28,
    CAST(Volume AS DOUBLE) AS prophecy_column_38,
    CAST(`Customer Segment` AS string) AS prophecy_column_21,
    CAST(`Initial Revenue` AS DOUBLE) AS prophecy_column_9,
    CAST(`Cust Active Flag` AS INTEGER) AS prophecy_column_13,
    CAST(`Customer Name` AS string) AS prophecy_column_2,
    CAST(YetToRenew AS DOUBLE) AS prophecy_column_45,
    CAST(`Cust Level Cohort Tenure` AS INTEGER) AS prophecy_column_17,
    CAST(SubCustSeg1 AS string) AS prophecy_column_22,
    CAST(`Product Category` AS string) AS prophecy_column_44,
    CAST(SubCustSeg6 AS string) AS prophecy_column_27,
    CAST(`Change Category` AS string) AS prophecy_column_12,
    CAST(Product AS string) AS prophecy_column_3,
    CAST(`Cohort Gross Retention` AS DOUBLE) AS prophecy_column_18,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_16,
    CAST(`Raw Change Category` AS string) AS prophecy_column_11,
    CAST(SubCustSeg5 AS string) AS prophecy_column_26,
    CAST(`Cohort Gross Customer Retention` AS DOUBLE) AS prophecy_column_19,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_4,
    CAST(`Cohort Tenure` AS INTEGER) AS prophecy_column_15,
    CAST(NULL AS string) AS prophecy_column_10,
    CAST(NULL AS DOUBLE) AS prophecy_column_42,
    CAST(NULL AS string) AS prophecy_column_1,
    CAST(NULL AS string) AS prophecy_column_6,
    CAST(NULL AS string) AS prophecy_column_33,
    CAST(NULL AS string) AS prophecy_column_41,
    CAST(NULL AS string) AS prophecy_column_32,
    CAST(NULL AS string) AS prophecy_column_34,
    CAST(NULL AS string) AS prophecy_column_7,
    CAST(NULL AS string) AS prophecy_column_39,
    CAST(NULL AS string) AS prophecy_column_35,
    CAST(NULL AS string) AS prophecy_column_31,
    CAST(NULL AS DOUBLE) AS prophecy_column_43,
    CAST(NULL AS string) AS prophecy_column_40,
    CAST(NULL AS string) AS prophecy_column_23,
    CAST(NULL AS string) AS prophecy_column_8,
    CAST(NULL AS string) AS prophecy_column_36,
    CAST(NULL AS string) AS prophecy_column_30
  
  FROM Filter_2740_reject AS in0

),

Filter_2740 AS (

  SELECT * 
  
  FROM Union_2710 AS in0
  
  WHERE (
          (UPPER(`Customer Level Flag`) = UPPER('Y'))
          AND (
                (
                  NOT(
                    Revenue = 0)
                ) OR (Revenue IS NULL)
              )
        )

),

Filter_2738 AS (

  SELECT * 
  
  FROM Union_2710 AS in0
  
  WHERE (
          ((UPPER(`Customer Level Flag`) = UPPER('N')) AND (UPPER(`Change Category`) = UPPER('Acquisition')))
          AND (UPPER(Product) = UPPER('ReadyRosie'))
        )

),

Join_2739_inner AS (

  SELECT 
    in1.`Customer Name` AS `Right_Customer Name`,
    in1.Product AS Right_Product,
    in1.`Revenue Period` AS `Right_Revenue Period`,
    in1.Revenue AS Right_Revenue,
    in1.`Initial Revenue` AS `Right_Initial Revenue`,
    in1.`Raw Change Category` AS `Right_Raw Change Category`,
    in1.`Change Category` AS `Right_Change Category`,
    in1.`Cust Active Flag` AS `Right_Cust Active Flag`,
    in1.Cohort AS Right_Cohort,
    in1.`Cohort Tenure` AS `Right_Cohort Tenure`,
    in1.`Cust Level Cohort` AS `Right_Cust Level Cohort`,
    in1.`Cust Level Cohort Tenure` AS `Right_Cust Level Cohort Tenure`,
    in1.`Cohort Gross Retention` AS `Right_Cohort Gross Retention`,
    in1.`Cohort Gross Customer Retention` AS `Right_Cohort Gross Customer Retention`,
    in1.`Customer Level Flag` AS `Right_Customer Level Flag`,
    in1.`Customer Segment` AS `Right_Customer Segment`,
    in1.SubCustSeg1 AS Right_SubCustSeg1,
    in1.SubCustSeg2 AS Right_SubCustSeg2,
    in1.SubCustSeg3 AS Right_SubCustSeg3,
    in1.SubCustSeg4 AS Right_SubCustSeg4,
    in1.SubCustSeg5 AS Right_SubCustSeg5,
    in1.SubCustSeg6 AS Right_SubCustSeg6,
    in1.`Cust_Prod Active Flag` AS `Right_Cust_Prod Active Flag`,
    in1.`Prev_Cust_Prod Active Flag` AS `Right_Prev_Cust_Prod Active Flag`,
    in1.Prev_Product AS Right_Prev_Product,
    in1.Volume AS Right_Volume,
    in1.`Product Category` AS `Right_Product Category`,
    in1.YetToRenew AS Right_YetToRenew,
    in1.`Previous YetToRenew` AS `Right_Previous YetToRenew`,
    in0.* EXCEPT (`check`, `Max_Cust_Level_Revenue`),
    in1.* EXCEPT (`Customer Name`, 
    `Product`, 
    `Revenue Period`, 
    `Revenue`, 
    `Initial Revenue`, 
    `Raw Change Category`, 
    `Change Category`, 
    `Cust Active Flag`, 
    `Cohort`, 
    `Cohort Tenure`, 
    `Cust Level Cohort`, 
    `Cust Level Cohort Tenure`, 
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
    `Previous YetToRenew`)
  
  FROM Filter_2738 AS in0
  INNER JOIN Filter_2740 AS in1
     ON (
      ((in0.`Customer Name` = in1.`Customer Name`) AND (in0.`Revenue Period` = in1.`Revenue Period`))
      AND (in0.`Comparison Method` = in1.`Comparison Method`)
    )

),

AlteryxSelect_2746 AS (

  SELECT 
    `Customer Name` AS `Customer Name`,
    Product AS Product,
    `Revenue Period` AS `Revenue Period`,
    Revenue AS Revenue,
    `Initial Revenue` AS `Initial Revenue`,
    `Raw Change Category` AS `Raw Change Category`,
    `Change Category` AS `Change Category`,
    `Cust Active Flag` AS `Cust Active Flag`,
    Cohort AS Cohort,
    `Cohort Tenure` AS `Cohort Tenure`,
    `Cust Level Cohort` AS `Cust Level Cohort`,
    `Cust Level Cohort Tenure` AS `Cust Level Cohort Tenure`,
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
    `Previous YetToRenew` AS `Previous YetToRenew`
  
  FROM Join_2739_inner AS in0

),

Formula_2747_0 AS (

  SELECT 
    CAST(' TOTAL CUSTOMER' AS string) AS Product,
    CAST('Y' AS string) AS `Customer Level Flag`,
    CAST(' TOTAL CUSTOMER' AS string) AS Prev_Product,
    CAST(0 AS INTEGER) AS `Cust_Prod Active Flag`,
    CAST(0 AS INTEGER) AS `Prev_Cust_Prod Active Flag`,
    CAST(' TOTAL CUSTOMER' AS string) AS `Product Category`,
    * EXCEPT (`customer level flag`, 
    `prev_cust_prod active flag`, 
    `prev_product`, 
    `cust_prod active flag`, 
    `product category`, 
    `product`)
  
  FROM AlteryxSelect_2746 AS in0

),

Union_2748_1 AS (

  SELECT 
    CAST(Revenue AS DOUBLE) AS prophecy_column_5,
    CAST(SubCustSeg3 AS string) AS prophecy_column_24,
    CAST(Prev_Product AS string) AS prophecy_column_37,
    CAST(SubCustSeg4 AS string) AS prophecy_column_25,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_14,
    CAST(`Customer Level Flag` AS string) AS prophecy_column_20,
    CAST(`Previous YetToRenew` AS DOUBLE) AS prophecy_column_46,
    CAST(`Prev_Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_29,
    CAST(`Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_28,
    CAST(Volume AS DOUBLE) AS prophecy_column_38,
    CAST(`Customer Segment` AS string) AS prophecy_column_21,
    CAST(`Initial Revenue` AS DOUBLE) AS prophecy_column_9,
    CAST(`Cust Active Flag` AS INTEGER) AS prophecy_column_13,
    CAST(`Customer Name` AS string) AS prophecy_column_2,
    CAST(YetToRenew AS DOUBLE) AS prophecy_column_45,
    CAST(`Cust Level Cohort Tenure` AS INTEGER) AS prophecy_column_17,
    CAST(SubCustSeg1 AS string) AS prophecy_column_22,
    CAST(`Product Category` AS string) AS prophecy_column_44,
    CAST(SubCustSeg6 AS string) AS prophecy_column_27,
    CAST(`Change Category` AS string) AS prophecy_column_12,
    CAST(Product AS string) AS prophecy_column_3,
    CAST(`Cohort Gross Retention` AS DOUBLE) AS prophecy_column_18,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_16,
    CAST(`Raw Change Category` AS string) AS prophecy_column_11,
    CAST(SubCustSeg5 AS string) AS prophecy_column_26,
    CAST(`Cohort Gross Customer Retention` AS DOUBLE) AS prophecy_column_19,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_4,
    CAST(`Cohort Tenure` AS INTEGER) AS prophecy_column_15,
    CAST(NULL AS string) AS prophecy_column_10,
    CAST(NULL AS DOUBLE) AS prophecy_column_42,
    CAST(NULL AS string) AS prophecy_column_1,
    CAST(NULL AS string) AS prophecy_column_6,
    CAST(NULL AS string) AS prophecy_column_33,
    CAST(NULL AS string) AS prophecy_column_41,
    CAST(NULL AS string) AS prophecy_column_32,
    CAST(NULL AS string) AS prophecy_column_34,
    CAST(NULL AS string) AS prophecy_column_7,
    CAST(NULL AS string) AS prophecy_column_39,
    CAST(NULL AS string) AS prophecy_column_35,
    CAST(NULL AS string) AS prophecy_column_31,
    CAST(NULL AS DOUBLE) AS prophecy_column_43,
    CAST(NULL AS string) AS prophecy_column_40,
    CAST(NULL AS string) AS prophecy_column_23,
    CAST(NULL AS string) AS prophecy_column_8,
    CAST(NULL AS string) AS prophecy_column_36,
    CAST(NULL AS string) AS prophecy_column_30
  
  FROM Formula_2747_0 AS in0

),

Formula_2744_0 AS (

  SELECT 
    CAST((Right_Revenue - Revenue) AS DOUBLE) AS Right_Revenue,
    * EXCEPT (`right_revenue`)
  
  FROM Join_2739_inner AS in0

),

AlteryxSelect_2742 AS (

  SELECT 
    `Right_Customer Name` AS `Right_Customer Name`,
    Right_Product AS Right_Product,
    `Right_Revenue Period` AS `Right_Revenue Period`,
    Right_Revenue AS Right_Revenue,
    `Right_Initial Revenue` AS `Right_Initial Revenue`,
    `Right_Raw Change Category` AS `Right_Raw Change Category`,
    `Right_Change Category` AS `Right_Change Category`,
    `Right_Cust Active Flag` AS `Right_Cust Active Flag`,
    Right_Cohort AS Right_Cohort,
    `Right_Cohort Tenure` AS `Right_Cohort Tenure`,
    `Right_Cust Level Cohort` AS `Right_Cust Level Cohort`,
    `Right_Cust Level Cohort Tenure` AS `Right_Cust Level Cohort Tenure`,
    `Right_Cohort Gross Retention` AS `Right_Cohort Gross Retention`,
    `Right_Cohort Gross Customer Retention` AS `Right_Cohort Gross Customer Retention`,
    `Right_Customer Level Flag` AS `Right_Customer Level Flag`,
    `Right_Customer Segment` AS `Right_Customer Segment`,
    Right_SubCustSeg1 AS Right_SubCustSeg1,
    Right_SubCustSeg2 AS Right_SubCustSeg2,
    Right_SubCustSeg3 AS Right_SubCustSeg3,
    Right_SubCustSeg4 AS Right_SubCustSeg4,
    Right_SubCustSeg5 AS Right_SubCustSeg5,
    Right_SubCustSeg6 AS Right_SubCustSeg6,
    `Right_Cust_Prod Active Flag` AS `Right_Cust_Prod Active Flag`,
    `Right_Prev_Cust_Prod Active Flag` AS `Right_Prev_Cust_Prod Active Flag`,
    Right_Prev_Product AS Right_Prev_Product,
    Right_Volume AS Right_Volume,
    `Right_Product Category` AS `Right_Product Category`,
    Right_YetToRenew AS Right_YetToRenew,
    `Right_Previous YetToRenew` AS `Right_Previous YetToRenew`
  
  FROM Formula_2744_0 AS in0

),

DynamicRename_2743 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['AlteryxSelect_2742'], 
      [
        'Right_Customer Segment', 
        'Right_Revenue', 
        'Right_Product', 
        'Right_Previous YetToRenew', 
        'Right_SubCustSeg2', 
        'Right_Cohort Gross Customer Retention', 
        'Right_Customer Level Flag', 
        'Right_Cohort', 
        'Right_Cohort Tenure', 
        'Right_Customer Name', 
        'Right_Cust Level Cohort', 
        'Right_Prev_Product', 
        'Right_Volume', 
        'Right_Initial Revenue', 
        'Right_Product Category', 
        'Right_SubCustSeg3', 
        'Right_Cohort Gross Retention', 
        'Right_SubCustSeg4', 
        'Right_Revenue Period', 
        'Right_YetToRenew', 
        'Right_Raw Change Category', 
        'Right_Cust Active Flag', 
        'Right_Cust_Prod Active Flag', 
        'Right_Change Category', 
        'Right_SubCustSeg6', 
        'Right_Cust Level Cohort Tenure', 
        'Right_SubCustSeg5', 
        'Right_Prev_Cust_Prod Active Flag', 
        'Right_SubCustSeg1'
      ], 
      'advancedRename', 
      [
        'Right_Customer Segment', 
        'Right_Revenue', 
        'Right_Product', 
        'Right_Previous YetToRenew', 
        'Right_SubCustSeg2', 
        'Right_Cohort Gross Customer Retention', 
        'Right_Customer Level Flag', 
        'Right_Cohort', 
        'Right_Cohort Tenure', 
        'Right_Customer Name', 
        'Right_Cust Level Cohort', 
        'Right_Prev_Product', 
        'Right_Volume', 
        'Right_Initial Revenue', 
        'Right_Product Category', 
        'Right_SubCustSeg3', 
        'Right_Cohort Gross Retention', 
        'Right_SubCustSeg4', 
        'Right_Revenue Period', 
        'Right_YetToRenew', 
        'Right_Raw Change Category', 
        'Right_Cust Active Flag', 
        'Right_Cust_Prod Active Flag', 
        'Right_Change Category', 
        'Right_SubCustSeg6', 
        'Right_Cust Level Cohort Tenure', 
        'Right_SubCustSeg5', 
        'Right_Prev_Cust_Prod Active Flag', 
        'Right_SubCustSeg1'
      ], 
      'Suffix', 
      '', 
      "
      CASE
        WHEN column_name LIKE 'Right_%' THEN SUBSTRING(column_name, LENGTH('Right_') + 1, LENGTH(column_name) - LENGTH('Right_'))
        ELSE column_name
      END
      "
    )
  }}

),

Formula_2745_0 AS (

  SELECT 
    CAST(CASE
      WHEN (`Revenue Change` < 0)
        THEN 'Downsell'
      WHEN (`Revenue Change` = 0)
        THEN 'Status Quo'
      ELSE 'Upsell'
    END AS STRING) AS `Change Category`,
    * EXCEPT (`change category`)
  
  FROM DynamicRename_2743 AS in0

),

Formula_2745_1 AS (

  SELECT 
    CAST(`Change Category` AS string) AS `Raw Change Category`,
    * EXCEPT (`raw change category`)
  
  FROM Formula_2745_0 AS in0

),

Union_2748_0 AS (

  SELECT 
    CAST(Revenue AS DOUBLE) AS prophecy_column_5,
    CAST(SubCustSeg3 AS string) AS prophecy_column_24,
    CAST(Prev_Product AS string) AS prophecy_column_37,
    CAST(SubCustSeg4 AS string) AS prophecy_column_25,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_14,
    CAST(`Customer Level Flag` AS string) AS prophecy_column_20,
    CAST(`Previous YetToRenew` AS DOUBLE) AS prophecy_column_46,
    CAST(`Prev_Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_29,
    CAST(`Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_28,
    CAST(Volume AS DOUBLE) AS prophecy_column_38,
    CAST(`Customer Segment` AS string) AS prophecy_column_21,
    CAST(`Initial Revenue` AS DOUBLE) AS prophecy_column_9,
    CAST(`Cust Active Flag` AS INTEGER) AS prophecy_column_13,
    CAST(`Customer Name` AS string) AS prophecy_column_2,
    CAST(YetToRenew AS DOUBLE) AS prophecy_column_45,
    CAST(`Cust Level Cohort Tenure` AS INTEGER) AS prophecy_column_17,
    CAST(SubCustSeg1 AS string) AS prophecy_column_22,
    CAST(`Product Category` AS string) AS prophecy_column_44,
    CAST(SubCustSeg6 AS string) AS prophecy_column_27,
    CAST(`Change Category` AS string) AS prophecy_column_12,
    CAST(Product AS string) AS prophecy_column_3,
    CAST(`Cohort Gross Retention` AS DOUBLE) AS prophecy_column_18,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_16,
    CAST(`Raw Change Category` AS string) AS prophecy_column_11,
    CAST(SubCustSeg5 AS string) AS prophecy_column_26,
    CAST(`Cohort Gross Customer Retention` AS DOUBLE) AS prophecy_column_19,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_4,
    CAST(`Cohort Tenure` AS INTEGER) AS prophecy_column_15,
    CAST(NULL AS string) AS prophecy_column_10,
    CAST(NULL AS DOUBLE) AS prophecy_column_42,
    CAST(NULL AS string) AS prophecy_column_1,
    CAST(NULL AS string) AS prophecy_column_6,
    CAST(NULL AS string) AS prophecy_column_33,
    CAST(NULL AS string) AS prophecy_column_41,
    CAST(NULL AS string) AS prophecy_column_32,
    CAST(NULL AS string) AS prophecy_column_34,
    CAST(NULL AS string) AS prophecy_column_7,
    CAST(NULL AS string) AS prophecy_column_39,
    CAST(NULL AS string) AS prophecy_column_35,
    CAST(NULL AS string) AS prophecy_column_31,
    CAST(NULL AS DOUBLE) AS prophecy_column_43,
    CAST(NULL AS string) AS prophecy_column_40,
    CAST(NULL AS string) AS prophecy_column_23,
    CAST(NULL AS string) AS prophecy_column_8,
    CAST(NULL AS string) AS prophecy_column_36,
    CAST(NULL AS string) AS prophecy_column_30
  
  FROM Formula_2745_1 AS in0

),

Join_2739_right AS (

  SELECT in0.*
  
  FROM Filter_2740 AS in0
  ANTI JOIN Filter_2738 AS in1
     ON (
      ((in1.`Customer Name` = in0.`Customer Name`) AND (in1.`Revenue Period` = in0.`Revenue Period`))
      AND (in1.`Comparison Method` = in0.`Comparison Method`)
    )

),

Union_2748_3 AS (

  SELECT 
    CAST(Revenue AS DOUBLE) AS prophecy_column_5,
    CAST(SubCustSeg3 AS string) AS prophecy_column_24,
    CAST(Prev_Product AS string) AS prophecy_column_37,
    CAST(SubCustSeg4 AS string) AS prophecy_column_25,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_14,
    CAST(`Customer Level Flag` AS string) AS prophecy_column_20,
    CAST(`Previous YetToRenew` AS DOUBLE) AS prophecy_column_46,
    CAST(`Prev_Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_29,
    CAST(`Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_28,
    CAST(Volume AS DOUBLE) AS prophecy_column_38,
    CAST(`Customer Segment` AS string) AS prophecy_column_21,
    CAST(`Initial Revenue` AS DOUBLE) AS prophecy_column_9,
    CAST(`Cust Active Flag` AS INTEGER) AS prophecy_column_13,
    CAST(`Customer Name` AS string) AS prophecy_column_2,
    CAST(YetToRenew AS DOUBLE) AS prophecy_column_45,
    CAST(`Cust Level Cohort Tenure` AS INTEGER) AS prophecy_column_17,
    CAST(SubCustSeg1 AS string) AS prophecy_column_22,
    CAST(`Product Category` AS string) AS prophecy_column_44,
    CAST(SubCustSeg6 AS string) AS prophecy_column_27,
    CAST(`Change Category` AS string) AS prophecy_column_12,
    CAST(Product AS string) AS prophecy_column_3,
    CAST(`Cohort Gross Retention` AS DOUBLE) AS prophecy_column_18,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_16,
    CAST(`Raw Change Category` AS string) AS prophecy_column_11,
    CAST(SubCustSeg5 AS string) AS prophecy_column_26,
    CAST(`Cohort Gross Customer Retention` AS DOUBLE) AS prophecy_column_19,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_4,
    CAST(`Cohort Tenure` AS INTEGER) AS prophecy_column_15,
    CAST(NULL AS string) AS prophecy_column_10,
    CAST(NULL AS DOUBLE) AS prophecy_column_42,
    CAST(NULL AS string) AS prophecy_column_1,
    CAST(NULL AS string) AS prophecy_column_6,
    CAST(NULL AS string) AS prophecy_column_33,
    CAST(NULL AS string) AS prophecy_column_41,
    CAST(NULL AS string) AS prophecy_column_32,
    CAST(NULL AS string) AS prophecy_column_34,
    CAST(NULL AS string) AS prophecy_column_7,
    CAST(NULL AS string) AS prophecy_column_39,
    CAST(NULL AS string) AS prophecy_column_35,
    CAST(NULL AS string) AS prophecy_column_31,
    CAST(NULL AS DOUBLE) AS prophecy_column_43,
    CAST(NULL AS string) AS prophecy_column_40,
    CAST(NULL AS string) AS prophecy_column_23,
    CAST(NULL AS string) AS prophecy_column_8,
    CAST(NULL AS string) AS prophecy_column_36,
    CAST(NULL AS string) AS prophecy_column_30
  
  FROM Join_2739_right AS in0

),

Union_2748 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_2748_3', 'Union_2748_2', 'Union_2748_0', 'Union_2748_1'], 
      [
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_37", "dataType": "String"}, {"name": "prophecy_column_44", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_29", "dataType": "Integer"}, {"name": "prophecy_column_18", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "Integer"}, {"name": "prophecy_column_38", "dataType": "Double"}, {"name": "prophecy_column_46", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Date"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "Date"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_45", "dataType": "Double"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "Integer"}, {"name": "prophecy_column_17", "dataType": "Integer"}, {"name": "prophecy_column_28", "dataType": "Integer"}, {"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Date"}]', 
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_37", "dataType": "String"}, {"name": "prophecy_column_44", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_29", "dataType": "Integer"}, {"name": "prophecy_column_18", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "Integer"}, {"name": "prophecy_column_38", "dataType": "Double"}, {"name": "prophecy_column_46", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Date"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "Date"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_45", "dataType": "Double"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "Integer"}, {"name": "prophecy_column_17", "dataType": "Integer"}, {"name": "prophecy_column_28", "dataType": "Integer"}, {"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Date"}]', 
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_37", "dataType": "String"}, {"name": "prophecy_column_44", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_29", "dataType": "Integer"}, {"name": "prophecy_column_18", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "Integer"}, {"name": "prophecy_column_38", "dataType": "Double"}, {"name": "prophecy_column_46", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Date"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "Date"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_45", "dataType": "Double"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "Integer"}, {"name": "prophecy_column_17", "dataType": "Integer"}, {"name": "prophecy_column_28", "dataType": "Integer"}, {"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Date"}]', 
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_37", "dataType": "String"}, {"name": "prophecy_column_44", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_29", "dataType": "Integer"}, {"name": "prophecy_column_18", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "Integer"}, {"name": "prophecy_column_38", "dataType": "Double"}, {"name": "prophecy_column_46", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Date"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "Date"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_45", "dataType": "Double"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "Integer"}, {"name": "prophecy_column_17", "dataType": "Integer"}, {"name": "prophecy_column_28", "dataType": "Integer"}, {"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_2748_postRename AS (

  SELECT 
    prophecy_column_26 AS SubCustSeg5,
    prophecy_column_46 AS `Previous YetToRenew`,
    prophecy_column_20 AS `Customer Level Flag`,
    prophecy_column_19 AS `Cohort Gross Customer Retention`,
    prophecy_column_14 AS Cohort,
    prophecy_column_38 AS Volume,
    prophecy_column_29 AS `Prev_Cust_Prod Active Flag`,
    prophecy_column_22 AS SubCustSeg1,
    prophecy_column_37 AS Prev_Product,
    prophecy_column_27 AS SubCustSeg6,
    prophecy_column_21 AS `Customer Segment`,
    prophecy_column_11 AS `Raw Change Category`,
    prophecy_column_9 AS `Initial Revenue`,
    prophecy_column_18 AS `Cohort Gross Retention`,
    prophecy_column_17 AS `Cust Level Cohort Tenure`,
    prophecy_column_2 AS `Customer Name`,
    prophecy_column_12 AS `Change Category`,
    prophecy_column_5 AS Revenue,
    prophecy_column_28 AS `Cust_Prod Active Flag`,
    prophecy_column_24 AS SubCustSeg3,
    prophecy_column_13 AS `Cust Active Flag`,
    prophecy_column_25 AS SubCustSeg4,
    prophecy_column_45 AS YetToRenew,
    prophecy_column_4 AS `Revenue Period`,
    prophecy_column_44 AS `Product Category`,
    prophecy_column_3 AS Product,
    prophecy_column_16 AS `Cust Level Cohort`,
    prophecy_column_15 AS `Cohort Tenure`,
    CAST(NULL AS string) AS Prev_SubCustSeg3,
    CAST(NULL AS string) AS `Volume Change`,
    CAST(NULL AS string) AS `Previous Period`,
    CAST(NULL AS string) AS `Rev Change Volume`,
    CAST(NULL AS string) AS `Revenue Change`,
    CAST(NULL AS string) AS Previous_Volume,
    CAST(NULL AS string) AS Prev_SubCustSeg4,
    CAST(NULL AS string) AS `Price Change`,
    CAST(NULL AS string) AS `Prev_Cust Active Flag`,
    CAST(NULL AS string) AS `Comparison Method`,
    CAST(NULL AS string) AS `Previous Revenue`,
    CAST(NULL AS string) AS Prev_SubCustSeg5,
    CAST(NULL AS string) AS `Rev Change Price`,
    CAST(NULL AS string) AS Prev_SubCustSeg1,
    CAST(NULL AS string) AS Prev_SubCustSeg6,
    CAST(NULL AS string) AS Prev_SubCustSeg2,
    CAST(NULL AS string) AS `Prev_Customer Segment`,
    CAST(NULL AS string) AS SubCustSeg2
  
  FROM Union_2748 AS in0

),

Filter_2713_reject AS (

  SELECT * 
  
  FROM Union_2748_postRename AS in0
  
  WHERE (
          (
            NOT(
              (
                (UPPER(`Customer Level Flag`) = UPPER('N'))
                AND (CAST(`Change Category` AS string) IN ('New Customer', 'Customer Reactivation', 'Existing Customer, New Product', 'Product Reactivation'))
              )
              AND (UPPER(Product) = UPPER('Quorum')))
          )
          OR (
               (
                 (
                   (UPPER(`Customer Level Flag`) = UPPER('N'))
                   AND (CAST(`Change Category` AS string) IN ('New Customer', 'Customer Reactivation', 'Existing Customer, New Product', 'Product Reactivation'))
                 )
                 AND (UPPER(Product) = UPPER('Quorum'))
               ) IS NULL
             )
        )

),

Filter_2713 AS (

  SELECT * 
  
  FROM Union_2748_postRename AS in0
  
  WHERE (
          (
            (UPPER(`Customer Level Flag`) = UPPER('N'))
            AND (CAST(`Change Category` AS string) IN ('New Customer', 'Customer Reactivation', 'Existing Customer, New Product', 'Product Reactivation'))
          )
          AND (UPPER(Product) = UPPER('Quorum'))
        )

),

Filter_2714_reject AS (

  SELECT * 
  
  FROM Filter_2713 AS in0
  
  WHERE (
          NOT (((`Revenue Period` >= to_date('2022-02-01')) AND (`Revenue Period` <= to_date('2023-01-31'))))
          OR isnull(((`Revenue Period` >= to_date('2022-02-01')) AND (`Revenue Period` <= to_date('2023-01-31'))))
        )

),

Filter_2711_to_Filter_2712 AS (

  SELECT * 
  
  FROM AlteryxSelect_2703 AS in0
  
  WHERE (
          (
            (
              NOT(
                (`Acquired ARR from Quorum (QualityAssist)` IS NULL)
                OR ((LENGTH(CAST(`Acquired ARR from Quorum (QualityAssist)` AS string))) = 0))
            )
            AND (NOT((`Mas90 Customer Number` IS NULL) OR ((LENGTH(`Mas90 Customer Number`)) = 0)))
          )
          AND (
                NOT(
                  `Acquired ARR from Quorum (QualityAssist)` = 0)
              )
        )

),

Filter_2714 AS (

  SELECT * 
  
  FROM Filter_2713 AS in0
  
  WHERE ((`Revenue Period` >= to_date('2022-02-01')) AND (`Revenue Period` <= to_date('2023-01-31')))

),

Join_2715_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Customer Name` = in1.`Mas90 Customer Number`)
          THEN 'Acquisition'
        ELSE NULL
      END
    ) AS `Change Category`,
    (
      CASE
        WHEN (in0.`Customer Name` = in1.`Mas90 Customer Number`)
          THEN 'Acquisition'
        ELSE NULL
      END
    ) AS `Raw Change Category`,
    in0.* EXCEPT (`Change Category`, `Raw Change Category`),
    in1.* EXCEPT (`Mas90 Customer Number`, `Acquired ARR from ReadyRosie`, `Acquired ARR from Quorum (QualityAssist)`)
  
  FROM Filter_2714 AS in0
  LEFT JOIN Filter_2711_to_Filter_2712 AS in1
     ON (in0.`Customer Name` = in1.`Mas90 Customer Number`)

),

Union_2717 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_2713_reject', 'Filter_2714_reject', 'Join_2715_inner_UnionLeftOuter'], 
      [
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Prev_Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Raw Change Category", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust Level Cohort Tenure", "dataType": "Integer"}, {"name": "Customer Name", "dataType": "String"}, {"name": "Change Category", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Cust Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Revenue Period", "dataType": "Date"}, {"name": "Product Category", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Cust Level Cohort", "dataType": "Date"}, {"name": "Cohort Tenure", "dataType": "Integer"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Prev_Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Raw Change Category", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust Level Cohort Tenure", "dataType": "Integer"}, {"name": "Customer Name", "dataType": "String"}, {"name": "Change Category", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Cust Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Revenue Period", "dataType": "Date"}, {"name": "Product Category", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Cust Level Cohort", "dataType": "Date"}, {"name": "Cohort Tenure", "dataType": "Integer"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Prev_Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Raw Change Category", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust Level Cohort Tenure", "dataType": "Integer"}, {"name": "Customer Name", "dataType": "String"}, {"name": "Change Category", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Cust Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Revenue Period", "dataType": "Date"}, {"name": "Product Category", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Cust Level Cohort", "dataType": "Date"}, {"name": "Cohort Tenure", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_2749 AS (

  SELECT * 
  
  FROM Union_2717 AS in0
  
  WHERE (
          ((UPPER(`Customer Level Flag`) = UPPER('N')) AND (UPPER(`Change Category`) = UPPER('Acquisition')))
          AND (UPPER(Product) = UPPER('Quorum'))
        )

),

Filter_2751 AS (

  SELECT * 
  
  FROM Union_2717 AS in0
  
  WHERE (
          (UPPER(`Customer Level Flag`) = UPPER('Y'))
          AND (
                (
                  NOT(
                    Revenue = 0)
                ) OR (Revenue IS NULL)
              )
        )

),

Join_2750_inner AS (

  SELECT 
    in1.`Customer Name` AS `Right_Customer Name`,
    in1.Product AS Right_Product,
    in1.`Revenue Period` AS `Right_Revenue Period`,
    in1.Revenue AS Right_Revenue,
    in1.`Initial Revenue` AS `Right_Initial Revenue`,
    in1.`Raw Change Category` AS `Right_Raw Change Category`,
    in1.`Change Category` AS `Right_Change Category`,
    in1.`Cust Active Flag` AS `Right_Cust Active Flag`,
    in1.Cohort AS Right_Cohort,
    in1.`Cohort Tenure` AS `Right_Cohort Tenure`,
    in1.`Cust Level Cohort` AS `Right_Cust Level Cohort`,
    in1.`Cust Level Cohort Tenure` AS `Right_Cust Level Cohort Tenure`,
    in1.`Cohort Gross Retention` AS `Right_Cohort Gross Retention`,
    in1.`Cohort Gross Customer Retention` AS `Right_Cohort Gross Customer Retention`,
    in1.`Customer Level Flag` AS `Right_Customer Level Flag`,
    in1.`Customer Segment` AS `Right_Customer Segment`,
    in1.SubCustSeg1 AS Right_SubCustSeg1,
    in1.SubCustSeg3 AS Right_SubCustSeg3,
    in1.SubCustSeg4 AS Right_SubCustSeg4,
    in1.SubCustSeg5 AS Right_SubCustSeg5,
    in1.SubCustSeg6 AS Right_SubCustSeg6,
    in1.`Cust_Prod Active Flag` AS `Right_Cust_Prod Active Flag`,
    in1.`Prev_Cust_Prod Active Flag` AS `Right_Prev_Cust_Prod Active Flag`,
    in1.Prev_Product AS Right_Prev_Product,
    in1.Volume AS Right_Volume,
    in1.`Product Category` AS `Right_Product Category`,
    in0.* EXCEPT (`Previous YetToRenew`, `YetToRenew`),
    in1.* EXCEPT (`Customer Name`, 
    `Product`, 
    `Revenue Period`, 
    `Revenue`, 
    `Initial Revenue`, 
    `Raw Change Category`, 
    `Change Category`, 
    `Cust Active Flag`, 
    `Cohort`, 
    `Cohort Tenure`, 
    `Cust Level Cohort`, 
    `Cust Level Cohort Tenure`, 
    `Cohort Gross Retention`, 
    `Cohort Gross Customer Retention`, 
    `Customer Level Flag`, 
    `Customer Segment`, 
    `SubCustSeg1`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg5`, 
    `SubCustSeg6`, 
    `Cust_Prod Active Flag`, 
    `Prev_Cust_Prod Active Flag`, 
    `Prev_Product`, 
    `Volume`, 
    `Product Category`)
  
  FROM Filter_2749 AS in0
  INNER JOIN Filter_2751 AS in1
     ON (
      ((in0.`Customer Name` = in1.`Customer Name`) AND (in0.`Revenue Period` = in1.`Revenue Period`))
      AND (in0.`Comparison Method` = in1.`Comparison Method`)
    )

),

AlteryxSelect_2757 AS (

  SELECT 
    `Customer Name` AS `Customer Name`,
    Product AS Product,
    `Revenue Period` AS `Revenue Period`,
    Revenue AS Revenue,
    `Initial Revenue` AS `Initial Revenue`,
    `Raw Change Category` AS `Raw Change Category`,
    `Change Category` AS `Change Category`,
    `Cust Active Flag` AS `Cust Active Flag`,
    Cohort AS Cohort,
    `Cohort Tenure` AS `Cohort Tenure`,
    `Cust Level Cohort` AS `Cust Level Cohort`,
    `Cust Level Cohort Tenure` AS `Cust Level Cohort Tenure`,
    `Cohort Gross Retention` AS `Cohort Gross Retention`,
    `Cohort Gross Customer Retention` AS `Cohort Gross Customer Retention`,
    `Customer Level Flag` AS `Customer Level Flag`,
    `Customer Segment` AS `Customer Segment`,
    SubCustSeg1 AS SubCustSeg1,
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
    `Previous YetToRenew` AS `Previous YetToRenew`
  
  FROM Join_2750_inner AS in0

),

Formula_2758_0 AS (

  SELECT 
    CAST(' TOTAL CUSTOMER' AS string) AS Product,
    CAST('Y' AS string) AS `Customer Level Flag`,
    CAST(' TOTAL CUSTOMER' AS string) AS Prev_Product,
    CAST(0 AS INTEGER) AS `Cust_Prod Active Flag`,
    CAST(0 AS INTEGER) AS `Prev_Cust_Prod Active Flag`,
    CAST(' TOTAL CUSTOMER' AS string) AS `Product Category`,
    * EXCEPT (`customer level flag`, 
    `prev_cust_prod active flag`, 
    `prev_product`, 
    `cust_prod active flag`, 
    `product category`, 
    `product`)
  
  FROM AlteryxSelect_2757 AS in0

),

Union_2759_1 AS (

  SELECT 
    CAST(Revenue AS DOUBLE) AS prophecy_column_5,
    CAST(SubCustSeg3 AS string) AS prophecy_column_24,
    CAST(Prev_Product AS string) AS prophecy_column_37,
    CAST(SubCustSeg4 AS string) AS prophecy_column_25,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_14,
    CAST(`Customer Level Flag` AS string) AS prophecy_column_20,
    CAST(`Previous YetToRenew` AS DOUBLE) AS prophecy_column_46,
    CAST(`Prev_Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_29,
    CAST(`Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_28,
    CAST(Volume AS DOUBLE) AS prophecy_column_38,
    CAST(`Customer Segment` AS string) AS prophecy_column_21,
    CAST(`Initial Revenue` AS DOUBLE) AS prophecy_column_9,
    CAST(`Cust Active Flag` AS INTEGER) AS prophecy_column_13,
    CAST(`Customer Name` AS string) AS prophecy_column_2,
    CAST(YetToRenew AS DOUBLE) AS prophecy_column_45,
    CAST(`Cust Level Cohort Tenure` AS INTEGER) AS prophecy_column_17,
    CAST(SubCustSeg1 AS string) AS prophecy_column_22,
    CAST(`Product Category` AS string) AS prophecy_column_44,
    CAST(SubCustSeg6 AS string) AS prophecy_column_27,
    CAST(`Change Category` AS string) AS prophecy_column_12,
    CAST(Product AS string) AS prophecy_column_3,
    CAST(`Cohort Gross Retention` AS DOUBLE) AS prophecy_column_18,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_16,
    CAST(`Raw Change Category` AS string) AS prophecy_column_11,
    CAST(SubCustSeg5 AS string) AS prophecy_column_26,
    CAST(`Cohort Gross Customer Retention` AS DOUBLE) AS prophecy_column_19,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_4,
    CAST(`Cohort Tenure` AS INTEGER) AS prophecy_column_15,
    CAST(NULL AS string) AS prophecy_column_10,
    CAST(NULL AS DOUBLE) AS prophecy_column_42,
    CAST(NULL AS string) AS prophecy_column_1,
    CAST(NULL AS string) AS prophecy_column_6,
    CAST(NULL AS string) AS prophecy_column_33,
    CAST(NULL AS string) AS prophecy_column_41,
    CAST(NULL AS string) AS prophecy_column_32,
    CAST(NULL AS string) AS prophecy_column_34,
    CAST(NULL AS string) AS prophecy_column_7,
    CAST(NULL AS string) AS prophecy_column_39,
    CAST(NULL AS string) AS prophecy_column_35,
    CAST(NULL AS string) AS prophecy_column_31,
    CAST(NULL AS DOUBLE) AS prophecy_column_43,
    CAST(NULL AS string) AS prophecy_column_40,
    CAST(NULL AS string) AS prophecy_column_23,
    CAST(NULL AS string) AS prophecy_column_8,
    CAST(NULL AS string) AS prophecy_column_36,
    CAST(NULL AS string) AS prophecy_column_30
  
  FROM Formula_2758_0 AS in0

),

Filter_2751_reject AS (

  SELECT * 
  
  FROM Union_2717 AS in0
  
  WHERE (
          (
            NOT(
              (UPPER(`Customer Level Flag`) = UPPER('Y'))
              AND (
                    (
                      NOT(
                        Revenue = 0)
                    ) OR (Revenue IS NULL)
                  ))
          )
          OR (
               (
                 (UPPER(`Customer Level Flag`) = UPPER('Y'))
                 AND (
                       (
                         NOT(
                           Revenue = 0)
                       ) OR (Revenue IS NULL)
                     )
               ) IS NULL
             )
        )

),

Union_2759_2 AS (

  SELECT 
    CAST(Revenue AS DOUBLE) AS prophecy_column_5,
    CAST(SubCustSeg3 AS string) AS prophecy_column_24,
    CAST(Prev_Product AS string) AS prophecy_column_37,
    CAST(SubCustSeg4 AS string) AS prophecy_column_25,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_14,
    CAST(`Customer Level Flag` AS string) AS prophecy_column_20,
    CAST(`Previous YetToRenew` AS DOUBLE) AS prophecy_column_46,
    CAST(`Prev_Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_29,
    CAST(`Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_28,
    CAST(Volume AS DOUBLE) AS prophecy_column_38,
    CAST(`Customer Segment` AS string) AS prophecy_column_21,
    CAST(`Initial Revenue` AS DOUBLE) AS prophecy_column_9,
    CAST(`Cust Active Flag` AS INTEGER) AS prophecy_column_13,
    CAST(`Customer Name` AS string) AS prophecy_column_2,
    CAST(YetToRenew AS DOUBLE) AS prophecy_column_45,
    CAST(`Cust Level Cohort Tenure` AS INTEGER) AS prophecy_column_17,
    CAST(SubCustSeg1 AS string) AS prophecy_column_22,
    CAST(`Product Category` AS string) AS prophecy_column_44,
    CAST(SubCustSeg6 AS string) AS prophecy_column_27,
    CAST(`Change Category` AS string) AS prophecy_column_12,
    CAST(Product AS string) AS prophecy_column_3,
    CAST(`Cohort Gross Retention` AS DOUBLE) AS prophecy_column_18,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_16,
    CAST(`Raw Change Category` AS string) AS prophecy_column_11,
    CAST(SubCustSeg5 AS string) AS prophecy_column_26,
    CAST(`Cohort Gross Customer Retention` AS DOUBLE) AS prophecy_column_19,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_4,
    CAST(`Cohort Tenure` AS INTEGER) AS prophecy_column_15,
    CAST(NULL AS string) AS prophecy_column_10,
    CAST(NULL AS DOUBLE) AS prophecy_column_42,
    CAST(NULL AS string) AS prophecy_column_1,
    CAST(NULL AS string) AS prophecy_column_6,
    CAST(NULL AS string) AS prophecy_column_33,
    CAST(NULL AS string) AS prophecy_column_41,
    CAST(NULL AS string) AS prophecy_column_32,
    CAST(NULL AS string) AS prophecy_column_34,
    CAST(NULL AS string) AS prophecy_column_7,
    CAST(NULL AS string) AS prophecy_column_39,
    CAST(NULL AS string) AS prophecy_column_35,
    CAST(NULL AS string) AS prophecy_column_31,
    CAST(NULL AS DOUBLE) AS prophecy_column_43,
    CAST(NULL AS string) AS prophecy_column_40,
    CAST(NULL AS string) AS prophecy_column_23,
    CAST(NULL AS string) AS prophecy_column_8,
    CAST(NULL AS string) AS prophecy_column_36,
    CAST(NULL AS string) AS prophecy_column_30
  
  FROM Filter_2751_reject AS in0

),

Join_2750_right AS (

  SELECT in0.*
  
  FROM Filter_2751 AS in0
  ANTI JOIN Filter_2749 AS in1
     ON (
      ((in1.`Customer Name` = in0.`Customer Name`) AND (in1.`Revenue Period` = in0.`Revenue Period`))
      AND (in1.`Comparison Method` = in0.`Comparison Method`)
    )

),

Union_2759_3 AS (

  SELECT 
    CAST(Revenue AS DOUBLE) AS prophecy_column_5,
    CAST(SubCustSeg3 AS string) AS prophecy_column_24,
    CAST(Prev_Product AS string) AS prophecy_column_37,
    CAST(SubCustSeg4 AS string) AS prophecy_column_25,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_14,
    CAST(`Customer Level Flag` AS string) AS prophecy_column_20,
    CAST(`Previous YetToRenew` AS DOUBLE) AS prophecy_column_46,
    CAST(`Prev_Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_29,
    CAST(`Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_28,
    CAST(Volume AS DOUBLE) AS prophecy_column_38,
    CAST(`Customer Segment` AS string) AS prophecy_column_21,
    CAST(`Initial Revenue` AS DOUBLE) AS prophecy_column_9,
    CAST(`Cust Active Flag` AS INTEGER) AS prophecy_column_13,
    CAST(`Customer Name` AS string) AS prophecy_column_2,
    CAST(YetToRenew AS DOUBLE) AS prophecy_column_45,
    CAST(`Cust Level Cohort Tenure` AS INTEGER) AS prophecy_column_17,
    CAST(SubCustSeg1 AS string) AS prophecy_column_22,
    CAST(`Product Category` AS string) AS prophecy_column_44,
    CAST(SubCustSeg6 AS string) AS prophecy_column_27,
    CAST(`Change Category` AS string) AS prophecy_column_12,
    CAST(Product AS string) AS prophecy_column_3,
    CAST(`Cohort Gross Retention` AS DOUBLE) AS prophecy_column_18,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_16,
    CAST(`Raw Change Category` AS string) AS prophecy_column_11,
    CAST(SubCustSeg5 AS string) AS prophecy_column_26,
    CAST(`Cohort Gross Customer Retention` AS DOUBLE) AS prophecy_column_19,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_4,
    CAST(`Cohort Tenure` AS INTEGER) AS prophecy_column_15,
    CAST(NULL AS string) AS prophecy_column_10,
    CAST(NULL AS DOUBLE) AS prophecy_column_42,
    CAST(NULL AS string) AS prophecy_column_1,
    CAST(NULL AS string) AS prophecy_column_6,
    CAST(NULL AS string) AS prophecy_column_33,
    CAST(NULL AS string) AS prophecy_column_41,
    CAST(NULL AS string) AS prophecy_column_32,
    CAST(NULL AS string) AS prophecy_column_34,
    CAST(NULL AS string) AS prophecy_column_7,
    CAST(NULL AS string) AS prophecy_column_39,
    CAST(NULL AS string) AS prophecy_column_35,
    CAST(NULL AS string) AS prophecy_column_31,
    CAST(NULL AS DOUBLE) AS prophecy_column_43,
    CAST(NULL AS string) AS prophecy_column_40,
    CAST(NULL AS string) AS prophecy_column_23,
    CAST(NULL AS string) AS prophecy_column_8,
    CAST(NULL AS string) AS prophecy_column_36,
    CAST(NULL AS string) AS prophecy_column_30
  
  FROM Join_2750_right AS in0

),

Formula_2755_0 AS (

  SELECT 
    CAST((Right_Revenue - Revenue) AS DOUBLE) AS Right_Revenue,
    * EXCEPT (`right_revenue`)
  
  FROM Join_2750_inner AS in0

),

AlteryxSelect_2753 AS (

  SELECT 
    `Right_Customer Name` AS `Right_Customer Name`,
    Right_Product AS Right_Product,
    `Right_Revenue Period` AS `Right_Revenue Period`,
    Right_Revenue AS Right_Revenue,
    `Right_Initial Revenue` AS `Right_Initial Revenue`,
    `Right_Raw Change Category` AS `Right_Raw Change Category`,
    `Right_Change Category` AS `Right_Change Category`,
    `Right_Cust Active Flag` AS `Right_Cust Active Flag`,
    Right_Cohort AS Right_Cohort,
    `Right_Cohort Tenure` AS `Right_Cohort Tenure`,
    `Right_Cust Level Cohort` AS `Right_Cust Level Cohort`,
    `Right_Cust Level Cohort Tenure` AS `Right_Cust Level Cohort Tenure`,
    `Right_Cohort Gross Retention` AS `Right_Cohort Gross Retention`,
    `Right_Cohort Gross Customer Retention` AS `Right_Cohort Gross Customer Retention`,
    `Right_Customer Level Flag` AS `Right_Customer Level Flag`,
    `Right_Customer Segment` AS `Right_Customer Segment`,
    Right_SubCustSeg1 AS Right_SubCustSeg1,
    Right_SubCustSeg3 AS Right_SubCustSeg3,
    Right_SubCustSeg4 AS Right_SubCustSeg4,
    Right_SubCustSeg5 AS Right_SubCustSeg5,
    Right_SubCustSeg6 AS Right_SubCustSeg6,
    `Right_Cust_Prod Active Flag` AS `Right_Cust_Prod Active Flag`,
    `Right_Prev_Cust_Prod Active Flag` AS `Right_Prev_Cust_Prod Active Flag`,
    Right_Prev_Product AS Right_Prev_Product,
    Right_Volume AS Right_Volume,
    `Right_Product Category` AS `Right_Product Category`
  
  FROM Formula_2755_0 AS in0

),

DynamicRename_2754 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['AlteryxSelect_2753'], 
      [
        'Right_Customer Segment', 
        'Right_Revenue', 
        'Right_Product', 
        'Right_Cohort Gross Customer Retention', 
        'Right_Customer Level Flag', 
        'Right_Cohort', 
        'Right_Cohort Tenure', 
        'Right_Customer Name', 
        'Right_Cust Level Cohort', 
        'Right_Prev_Product', 
        'Right_Volume', 
        'Right_Initial Revenue', 
        'Right_Product Category', 
        'Right_SubCustSeg3', 
        'Right_Cohort Gross Retention', 
        'Right_SubCustSeg4', 
        'Right_Revenue Period', 
        'Right_Raw Change Category', 
        'Right_Cust Active Flag', 
        'Right_Cust_Prod Active Flag', 
        'Right_Change Category', 
        'Right_SubCustSeg6', 
        'Right_Cust Level Cohort Tenure', 
        'Right_SubCustSeg5', 
        'Right_Prev_Cust_Prod Active Flag', 
        'Right_SubCustSeg1'
      ], 
      'advancedRename', 
      [
        'Right_Customer Segment', 
        'Right_Revenue', 
        'Right_Product', 
        'Right_Cohort Gross Customer Retention', 
        'Right_Customer Level Flag', 
        'Right_Cohort', 
        'Right_Cohort Tenure', 
        'Right_Customer Name', 
        'Right_Cust Level Cohort', 
        'Right_Prev_Product', 
        'Right_Volume', 
        'Right_Initial Revenue', 
        'Right_Product Category', 
        'Right_SubCustSeg3', 
        'Right_Cohort Gross Retention', 
        'Right_SubCustSeg4', 
        'Right_Revenue Period', 
        'Right_Raw Change Category', 
        'Right_Cust Active Flag', 
        'Right_Cust_Prod Active Flag', 
        'Right_Change Category', 
        'Right_SubCustSeg6', 
        'Right_Cust Level Cohort Tenure', 
        'Right_SubCustSeg5', 
        'Right_Prev_Cust_Prod Active Flag', 
        'Right_SubCustSeg1'
      ], 
      'Suffix', 
      '', 
      "
      CASE
        WHEN column_name LIKE 'Right_%' THEN SUBSTRING(column_name, LENGTH('Right_') + 1, LENGTH(column_name) - LENGTH('Right_'))
        ELSE column_name
      END
      "
    )
  }}

),

Formula_2756_0 AS (

  SELECT 
    CAST(CASE
      WHEN (`Revenue Change` < 0)
        THEN 'Downsell'
      WHEN (`Revenue Change` = 0)
        THEN 'Status Quo'
      ELSE 'Upsell'
    END AS STRING) AS `Change Category`,
    * EXCEPT (`change category`)
  
  FROM DynamicRename_2754 AS in0

),

Formula_2756_1 AS (

  SELECT 
    CAST(`Change Category` AS string) AS `Raw Change Category`,
    * EXCEPT (`raw change category`)
  
  FROM Formula_2756_0 AS in0

),

Union_2759_0 AS (

  SELECT 
    CAST(Revenue AS DOUBLE) AS prophecy_column_5,
    CAST(SubCustSeg3 AS string) AS prophecy_column_24,
    CAST(Prev_Product AS string) AS prophecy_column_37,
    CAST(SubCustSeg4 AS string) AS prophecy_column_25,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Cohort AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_14,
    CAST(`Customer Level Flag` AS string) AS prophecy_column_20,
    CAST(`Prev_Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_29,
    CAST(`Cust_Prod Active Flag` AS INTEGER) AS prophecy_column_28,
    CAST(Volume AS DOUBLE) AS prophecy_column_38,
    CAST(`Customer Segment` AS string) AS prophecy_column_21,
    CAST(`Initial Revenue` AS DOUBLE) AS prophecy_column_9,
    CAST(`Cust Active Flag` AS INTEGER) AS prophecy_column_13,
    CAST(`Customer Name` AS string) AS prophecy_column_2,
    CAST(`Cust Level Cohort Tenure` AS INTEGER) AS prophecy_column_17,
    CAST(SubCustSeg1 AS string) AS prophecy_column_22,
    CAST(`Product Category` AS string) AS prophecy_column_44,
    CAST(SubCustSeg6 AS string) AS prophecy_column_27,
    CAST(`Change Category` AS string) AS prophecy_column_12,
    CAST(Product AS string) AS prophecy_column_3,
    CAST(`Cohort Gross Retention` AS DOUBLE) AS prophecy_column_18,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Cust Level Cohort` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_16,
    CAST(`Raw Change Category` AS string) AS prophecy_column_11,
    CAST(SubCustSeg5 AS string) AS prophecy_column_26,
    CAST(`Cohort Gross Customer Retention` AS DOUBLE) AS prophecy_column_19,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(`Revenue Period` AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS prophecy_column_4,
    CAST(`Cohort Tenure` AS INTEGER) AS prophecy_column_15,
    CAST(NULL AS string) AS prophecy_column_10,
    CAST(NULL AS DOUBLE) AS prophecy_column_42,
    CAST(NULL AS string) AS prophecy_column_46,
    CAST(NULL AS string) AS prophecy_column_1,
    CAST(NULL AS string) AS prophecy_column_6,
    CAST(NULL AS string) AS prophecy_column_33,
    CAST(NULL AS string) AS prophecy_column_41,
    CAST(NULL AS string) AS prophecy_column_32,
    CAST(NULL AS string) AS prophecy_column_34,
    CAST(NULL AS DOUBLE) AS prophecy_column_45,
    CAST(NULL AS string) AS prophecy_column_7,
    CAST(NULL AS string) AS prophecy_column_39,
    CAST(NULL AS string) AS prophecy_column_35,
    CAST(NULL AS string) AS prophecy_column_31,
    CAST(NULL AS DOUBLE) AS prophecy_column_43,
    CAST(NULL AS string) AS prophecy_column_40,
    CAST(NULL AS string) AS prophecy_column_23,
    CAST(NULL AS string) AS prophecy_column_8,
    CAST(NULL AS string) AS prophecy_column_36,
    CAST(NULL AS string) AS prophecy_column_30
  
  FROM Formula_2756_1 AS in0

),

Union_2759 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_2759_3', 'Union_2759_2', 'Union_2759_0', 'Union_2759_1'], 
      [
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_37", "dataType": "String"}, {"name": "prophecy_column_44", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_29", "dataType": "Integer"}, {"name": "prophecy_column_18", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "Integer"}, {"name": "prophecy_column_38", "dataType": "Double"}, {"name": "prophecy_column_46", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Date"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "Date"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_45", "dataType": "Double"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "Integer"}, {"name": "prophecy_column_17", "dataType": "Integer"}, {"name": "prophecy_column_28", "dataType": "Integer"}, {"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Date"}]', 
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_37", "dataType": "String"}, {"name": "prophecy_column_44", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_29", "dataType": "Integer"}, {"name": "prophecy_column_18", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "Integer"}, {"name": "prophecy_column_38", "dataType": "Double"}, {"name": "prophecy_column_46", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Date"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "Date"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_45", "dataType": "Double"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "Integer"}, {"name": "prophecy_column_17", "dataType": "Integer"}, {"name": "prophecy_column_28", "dataType": "Integer"}, {"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Date"}]', 
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_37", "dataType": "String"}, {"name": "prophecy_column_44", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_29", "dataType": "Integer"}, {"name": "prophecy_column_18", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "Integer"}, {"name": "prophecy_column_38", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Date"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "Date"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "Integer"}, {"name": "prophecy_column_17", "dataType": "Integer"}, {"name": "prophecy_column_28", "dataType": "Integer"}, {"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Date"}]', 
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_37", "dataType": "String"}, {"name": "prophecy_column_44", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_29", "dataType": "Integer"}, {"name": "prophecy_column_18", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "Integer"}, {"name": "prophecy_column_38", "dataType": "Double"}, {"name": "prophecy_column_46", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Date"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "Date"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_45", "dataType": "Double"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "Integer"}, {"name": "prophecy_column_17", "dataType": "Integer"}, {"name": "prophecy_column_28", "dataType": "Integer"}, {"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Date"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_2759_postRename AS (

  SELECT 
    prophecy_column_26 AS SubCustSeg5,
    prophecy_column_46 AS `Previous YetToRenew`,
    prophecy_column_20 AS `Customer Level Flag`,
    prophecy_column_19 AS `Cohort Gross Customer Retention`,
    prophecy_column_14 AS Cohort,
    prophecy_column_38 AS Volume,
    prophecy_column_29 AS `Prev_Cust_Prod Active Flag`,
    prophecy_column_22 AS SubCustSeg1,
    prophecy_column_37 AS Prev_Product,
    prophecy_column_27 AS SubCustSeg6,
    prophecy_column_21 AS `Customer Segment`,
    prophecy_column_11 AS `Raw Change Category`,
    prophecy_column_9 AS `Initial Revenue`,
    prophecy_column_18 AS `Cohort Gross Retention`,
    prophecy_column_17 AS `Cust Level Cohort Tenure`,
    prophecy_column_2 AS `Customer Name`,
    prophecy_column_12 AS `Change Category`,
    prophecy_column_5 AS Revenue,
    prophecy_column_28 AS `Cust_Prod Active Flag`,
    prophecy_column_24 AS SubCustSeg3,
    prophecy_column_13 AS `Cust Active Flag`,
    prophecy_column_25 AS SubCustSeg4,
    prophecy_column_45 AS YetToRenew,
    prophecy_column_4 AS `Revenue Period`,
    prophecy_column_44 AS `Product Category`,
    prophecy_column_3 AS Product,
    prophecy_column_16 AS `Cust Level Cohort`,
    prophecy_column_15 AS `Cohort Tenure`,
    CAST(NULL AS string) AS Prev_SubCustSeg3,
    CAST(NULL AS string) AS `Volume Change`,
    CAST(NULL AS string) AS `Previous Period`,
    CAST(NULL AS string) AS `Rev Change Volume`,
    CAST(NULL AS string) AS `Revenue Change`,
    CAST(NULL AS string) AS Previous_Volume,
    CAST(NULL AS string) AS Prev_SubCustSeg4,
    CAST(NULL AS string) AS `Price Change`,
    CAST(NULL AS string) AS `Prev_Cust Active Flag`,
    CAST(NULL AS string) AS `Comparison Method`,
    CAST(NULL AS string) AS `Previous Revenue`,
    CAST(NULL AS string) AS Prev_SubCustSeg5,
    CAST(NULL AS string) AS `Rev Change Price`,
    CAST(NULL AS string) AS Prev_SubCustSeg1,
    CAST(NULL AS string) AS Prev_SubCustSeg6,
    CAST(NULL AS string) AS Prev_SubCustSeg2,
    CAST(NULL AS string) AS `Prev_Customer Segment`,
    CAST(NULL AS string) AS SubCustSeg2
  
  FROM Union_2759 AS in0

),

Filter_2726_reject AS (

  SELECT * 
  
  FROM Union_2759_postRename AS in0
  
  WHERE (
          (
            NOT(
              UPPER(`Change Category`) = UPPER('New Customer'))
          )
          OR ((UPPER(`Change Category`) = UPPER('New Customer')) IS NULL)
        )

),

Filter_2726 AS (

  SELECT * 
  
  FROM Union_2759_postRename AS in0
  
  WHERE (UPPER(`Change Category`) = UPPER('New Customer'))

),

Filter_2727 AS (

  SELECT * 
  
  FROM Filter_2726 AS in0
  
  WHERE (Revenue > 0)

),

Summarize_2728 AS (

  SELECT 
    *,
    first(`Revenue Period`) OVER (PARTITION BY `Customer Name` ORDER BY `Customer Name` ASC NULLS FIRST, `Revenue Period` ASC NULLS FIRST) AS `First_Revenue Period`,
    row_number() OVER (PARTITION BY `Customer Name` ORDER BY `Customer Name` ASC NULLS FIRST, `Revenue Period` ASC NULLS FIRST) AS row_number
  
  FROM Filter_2727 AS in0

),

`2728_filter` AS (

  SELECT * 
  
  FROM Summarize_2728 AS in0
  
  WHERE (row_number = 1)

),

Summarize_2728_drop_0 AS (

  SELECT * EXCEPT (`row_number`)
  
  FROM `2728_filter` AS in0

),

Join_2729_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Summarize_2728_drop_0 AS in0
  INNER JOIN Summarize_2730 AS in1
     ON (in0.`Customer Name` = in1.`Mas90 Customer Number`)

),

Formula_2732_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`First_Revenue Period` <= `First_Date of First Activated Order`)
          THEN 'New to company'
        WHEN (`First_Revenue Period` > `First_Date of First Activated Order`)
          THEN 'New to subscription'
        ELSE ''
      END
    ) AS string) AS `Change Category`,
    * EXCEPT (`change category`)
  
  FROM Join_2729_inner AS in0

),

Formula_2732_1 AS (

  SELECT 
    CAST(`Change Category` AS string) AS `Raw Change Category`,
    * EXCEPT (`raw change category`)
  
  FROM Formula_2732_0 AS in0

),

Join_2734_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.`Customer Name` = in1.`Customer Name`)
          THEN NULL
        ELSE 'New to company'
      END
    ) AS `Change Category`,
    (
      CASE
        WHEN (in0.`Customer Name` = in1.`Customer Name`)
          THEN NULL
        ELSE 'New to company'
      END
    ) AS `Raw Change Category`,
    in0.* EXCEPT (`Change Category`, 
    `Raw Change Category`, 
    `SubCustSeg5`, 
    `Previous YetToRenew`, 
    `Customer Level Flag`, 
    `Cohort Gross Customer Retention`, 
    `Cohort`, 
    `Volume`, 
    `Prev_Cust_Prod Active Flag`, 
    `SubCustSeg1`, 
    `Prev_Product`, 
    `SubCustSeg6`, 
    `Customer Segment`, 
    `Initial Revenue`, 
    `Cohort Gross Retention`, 
    `Cust Level Cohort Tenure`, 
    `Customer Name`, 
    `Revenue`, 
    `Cust_Prod Active Flag`, 
    `SubCustSeg3`, 
    `Cust Active Flag`, 
    `SubCustSeg4`, 
    `YetToRenew`, 
    `Revenue Period`, 
    `Product Category`, 
    `Product`, 
    `Cust Level Cohort`, 
    `Cohort Tenure`),
    in1.* EXCEPT (`Change Category`, `Raw Change Category`)
  
  FROM Filter_2726 AS in0
  LEFT JOIN Formula_2732_1 AS in1
     ON (in0.`Customer Name` = in1.`Customer Name`)

),

Union_2737 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_2726_reject', 'Join_2734_inner_UnionLeftOuter'], 
      [
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Prev_Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Raw Change Category", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust Level Cohort Tenure", "dataType": "Integer"}, {"name": "Customer Name", "dataType": "String"}, {"name": "Change Category", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Cust Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Revenue Period", "dataType": "Date"}, {"name": "Product Category", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Cust Level Cohort", "dataType": "Date"}, {"name": "Cohort Tenure", "dataType": "Integer"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Previous YetToRenew", "dataType": "Double"}, {"name": "Customer Level Flag", "dataType": "String"}, {"name": "Cohort Gross Customer Retention", "dataType": "Double"}, {"name": "First_Date of First Activated Order", "dataType": "Date"}, {"name": "Cohort", "dataType": "Date"}, {"name": "Volume", "dataType": "Double"}, {"name": "Prev_Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Prev_Product", "dataType": "String"}, {"name": "First_Revenue Period", "dataType": "Date"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "Raw Change Category", "dataType": "String"}, {"name": "Initial Revenue", "dataType": "Double"}, {"name": "Cohort Gross Retention", "dataType": "Double"}, {"name": "Cust Level Cohort Tenure", "dataType": "Integer"}, {"name": "Customer Name", "dataType": "String"}, {"name": "Change Category", "dataType": "String"}, {"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Revenue", "dataType": "Double"}, {"name": "Cust_Prod Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Cust Active Flag", "dataType": "Integer"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Revenue Period", "dataType": "Date"}, {"name": "Product Category", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "Cust Level Cohort", "dataType": "Date"}, {"name": "Cohort Tenure", "dataType": "Integer"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_2719_0 AS (

  SELECT 
    CAST((CONCAT(`Customer Name`, '|', `Product Category`, '|', Product)) AS string) AS CustProdKeyStr,
    CAST('' AS string) AS Extra,
    CAST('' AS string) AS Extra2,
    CAST('' AS string) AS Extra3,
    CAST('Recurring' AS string) AS `Recurring Flag`,
    *
  
  FROM Union_2737 AS in0

),

AlteryxSelect_2718 AS (

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
    `Cohort Gross Customer Retention` AS `Cohort Gross Customer Retention`,
    `Cust Active Flag` AS `Cust Active Flag`,
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
    YetToRenew AS YetToRenew,
    `Previous YetToRenew` AS `Previous YetToRenew`,
    * EXCEPT (`Prev_Cust_Prod Active Flag`, 
    `Prev_Product`, 
    `Customer Name`, 
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
    `Cohort Gross Customer Retention`, 
    `Cust Active Flag`, 
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
    `YetToRenew`, 
    `Previous YetToRenew`)
  
  FROM Formula_2719_0 AS in0

)

SELECT *

FROM AlteryxSelect_2718
