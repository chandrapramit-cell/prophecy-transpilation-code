{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_3128 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_3128')}}

),

Join_1012_right AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1012_right')}}

),

Join_2969_left AS (

  SELECT in0.*
  
  FROM Join_1012_right AS in0
  ANTI JOIN Summarize_3128 AS in1
     ON (in0.`Account Name: Mas90 Customer Number` = in1.`Mas90 Customer Number`)

),

Filter_1020 AS (

  SELECT * 
  
  FROM Join_2969_left AS in0
  
  WHERE (
          `Primary Quote: Start Date` <= to_date(
            concat(
              regexp_replace(regexp_replace(format_number(CAST(year(current_date()) AS DOUBLE), 0), ',', '__THS__'), '__THS__', ''), 
              '-12-31'))
        )

),

Join_2437_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2437_left_UnionLeftOuter')}}

),

Summarize_1027 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Summarize_1027')}}

),

Join_1012_left AS (

  SELECT in0.*
  
  FROM Summarize_1027 AS in0
  ANTI JOIN Join_2437_left_UnionLeftOuter AS in1
     ON (
      (
        (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Account Name: Mas90 Customer Number`)
        AND (in0.`Product Code` = in1.`Product Code`)
      )
      AND (in0.`Order: Sales Order Number` = in1.`Renewed Contract: Order: Sales Order Number`)
    )

),

Join_1012_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_1012_inner')}}

),

Filter_1129 AS (

  SELECT * 
  
  FROM Join_1012_inner AS in0
  
  WHERE coalesce(contains(lower(Stage), lower('Closed')), false)

),

Summarize_1128 AS (

  SELECT 
    COUNT(DISTINCT Stage) AS CountDistinct_Stage,
    `Product Code` AS `Product Code`,
    `Renewed Contract: Order: Sales Order Number` AS `Renewed Contract: Order: Sales Order Number`
  
  FROM Filter_1129 AS in0
  
  GROUP BY 
    `Product Code`, `Renewed Contract: Order: Sales Order Number`

),

Summarize_1127 AS (

  SELECT 
    COUNT(DISTINCT Stage) AS CountDistinct_Stage,
    `Product Code` AS `Product Code`,
    `Renewed Contract: Order: Sales Order Number` AS `Renewed Contract: Order: Sales Order Number`
  
  FROM Join_1012_inner AS in0
  
  GROUP BY 
    `Product Code`, `Renewed Contract: Order: Sales Order Number`

),

Join_1130_inner AS (

  SELECT 
    in0.CountDistinct_Stage AS TotalDistinct_Stage,
    in1.CountDistinct_Stage AS ClosedDistinct_Stage,
    in0.* EXCEPT (`CountDistinct_Stage`),
    in1.* EXCEPT (`Product Code`, `Renewed Contract: Order: Sales Order Number`, `CountDistinct_Stage`)
  
  FROM Summarize_1127 AS in0
  INNER JOIN Summarize_1128 AS in1
     ON (
      (in0.`Renewed Contract: Order: Sales Order Number` = in1.`Renewed Contract: Order: Sales Order Number`)
      AND (in0.`Product Code` = in1.`Product Code`)
    )

),

Filter_1126 AS (

  SELECT * 
  
  FROM Join_1130_inner AS in0
  
  WHERE (TotalDistinct_Stage = ClosedDistinct_Stage)

),

Join_1131_inner AS (

  SELECT 
    in0.* EXCEPT (`Product Code`, 
    `Renewed Contract: Order: Sales Order Number`, 
    `TotalDistinct_Stage`, 
    `ClosedDistinct_Stage`),
    in1.*
  
  FROM Filter_1126 AS in0
  INNER JOIN Summarize_1027 AS in1
     ON (
      (in0.`Product Code` = in1.`Product Code`)
      AND (in0.`Renewed Contract: Order: Sales Order Number` = in1.`Order: Sales Order Number`)
    )

),

Union_1132 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_1012_left', 'Join_1131_inner'], 
      [
        '[{"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "Double"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Orders ACV", "dataType": "Double"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Product", "dataType": "String"}]', 
        '[{"name": "Original_StartDate_Annualization", "dataType": "Date"}, {"name": "Order: Account Name: Mas90 Customer Number", "dataType": "String"}, {"name": "Sum_Quantity", "dataType": "Double"}, {"name": "Order: Sales Order Number", "dataType": "String"}, {"name": "Product Code", "dataType": "String"}, {"name": "Orders ACV", "dataType": "Double"}, {"name": "Original_EndDate_Annualization", "dataType": "Date"}, {"name": "Product", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_1030 AS (

  SELECT 
    *,
    MAX(Original_EndDate_Annualization) OVER (PARTITION BY `Order: Account Name: Mas90 Customer Number`, `Product Code` ORDER BY 1 ASC NULLS FIRST) AS Max_Original_EndDate_Annualization
  
  FROM Union_1132 AS in0

),

Summarize_1030_filter AS (

  SELECT * 
  
  FROM Summarize_1030 AS in0
  
  WHERE (Original_EndDate_Annualization = Max_Original_EndDate_Annualization)

),

Join_1031_inner_formula AS (

  SELECT *
  
  FROM Summarize_1030_filter AS in0

),

Summarize_2942 AS (

  SELECT 
    SUM(Sum_Quantity) AS Quantity,
    SUM(`Orders ACV`) AS `Orders ACV`,
    `Order: Account Name: Mas90 Customer Number` AS `Order: Account Name: Mas90 Customer Number`,
    `Product Code` AS `Product Code`,
    Original_EndDate_Annualization AS Original_EndDate_Annualization
  
  FROM Join_1031_inner_formula AS in0
  
  GROUP BY 
    `Order: Account Name: Mas90 Customer Number`, `Product Code`, Original_EndDate_Annualization

),

Join_1022_inner AS (

  SELECT 
    in1.`Product Code` AS `Right_Product Code`,
    in1.Quantity AS Right_Quantity,
    in0.* EXCEPT (`Order: Account Name: Mas90 Customer Number`, `Product Code`),
    in1.* EXCEPT (`Product Code`, `Quantity`)
  
  FROM Summarize_2942 AS in0
  INNER JOIN Filter_1020 AS in1
     ON (
      (in0.`Order: Account Name: Mas90 Customer Number` = in1.`Account Name: Mas90 Customer Number`)
      AND (in0.`Product Code` = in1.`Product Code`)
    )

),

Formula_1040_to_Formula_1170_0 AS (

  SELECT 
    (TO_DATE((DATE_TRUNC('month', `Primary Quote: Start Date`)), 'yyyy-MM-dd')) AS StartDate_Annualization,
    to_date(
      CASE
        WHEN (substring(CAST(`Primary Quote: Start Date` AS STRING), 1, 7) = substring(CAST(`Primary Quote: End Date (Calculated)` AS STRING), 1, 7))
          THEN to_date(last_day(`Primary Quote: End Date (Calculated)`))
        WHEN (
          (to_date(date_trunc('month', `Primary Quote: Start Date`)) = `Primary Quote: Start Date`)
          AND (to_date(last_day(`Primary Quote: End Date (Calculated)`)) = `Primary Quote: End Date (Calculated)`)
        )
          THEN `Primary Quote: End Date (Calculated)`
        ELSE date_add(date_trunc('month', `Primary Quote: End Date (Calculated)`), CAST(-1 AS INT))
      END, 
      'yyyy-MM-dd') AS EndDate_Annualization,
    *
  
  FROM Join_1022_inner AS in0

),

Formula_1040_to_Formula_1170_1 AS (

  SELECT 
    to_date(
      CASE
        WHEN (Original_EndDate_Annualization > to_date(date_trunc('month', `Primary Quote: Start Date`)))
          THEN date_add(Original_EndDate_Annualization, CAST(1 AS INT))
        ELSE StartDate_Annualization
      END, 
      'yyyy-MM-dd') AS StartDate_Annualization,
    * EXCEPT (`startdate_annualization`)
  
  FROM Formula_1040_to_Formula_1170_0 AS in0

),

Formula_1040_to_Formula_1170_2 AS (

  SELECT 
    to_date(
      CASE
        WHEN (Original_EndDate_Annualization > to_date(date_trunc('month', `Primary Quote: Start Date`)))
          THEN date_add(
            date_trunc(
              'month', 
              date_add(
                StartDate_Annualization, 
                CAST((
                  CAST(datediff(to_date(`Primary Quote: End Date (Calculated)`), to_date(`Primary Quote: Start Date`)) AS INT)
                  + 1
                ) AS INT))), 
            CAST(-1 AS INT))
        ELSE EndDate_Annualization
      END, 
      'yyyy-MM-dd') AS EndDate_Annualization,
    CAST(`Orders ACV` AS DOUBLE) AS ACV,
    * EXCEPT (`enddate_annualization`)
  
  FROM Formula_1040_to_Formula_1170_1 AS in0

),

Formula_1040_to_Formula_1170_3 AS (

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
  
  FROM Formula_1040_to_Formula_1170_2 AS in0

),

Formula_1040_to_Formula_1170_4 AS (

  SELECT 
    CAST(((ACV / 365.25) * Engine_ContractDays) AS DOUBLE) AS TCV,
    CAST('Renewals' AS string) AS Origin,
    *
  
  FROM Formula_1040_to_Formula_1170_3 AS in0

),

AlteryxSelect_1046 AS (

  SELECT 
    Quantity AS `Orders Quantity`,
    `Right_Product Code` AS `Product Code`,
    * EXCEPT (`Quantity`, `Right_Product Code`)
  
  FROM Formula_1040_to_Formula_1170_4 AS in0

)

SELECT *

FROM AlteryxSelect_1046
