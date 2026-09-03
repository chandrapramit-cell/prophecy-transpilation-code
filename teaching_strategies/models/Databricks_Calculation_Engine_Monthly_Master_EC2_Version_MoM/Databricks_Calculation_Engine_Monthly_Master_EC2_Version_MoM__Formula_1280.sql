{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH TextInput_1202 AS (

  SELECT * 
  
  FROM {{ ref('seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_1202')}}

),

TextInput_1202_cast AS (

  SELECT CAST(Placeholder AS string) AS Placeholder
  
  FROM TextInput_1202 AS in0

),

Formula_1192 AS (

  SELECT *
  
  FROM TextInput_1202_cast AS in0

),

GenerateRows_1189 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Formula_1192'], 
      '[{"name": "Placeholder", "dataType": "String"}]', 
      '1', 
      '(RowCount <= payload.`Number of Periods`)', 
      '(RowCount + 1)', 
      'RowCount', 
      '100', 
      'recursive'
    )
  }}

),

Formula_1191_0 AS (

  SELECT 
    (ADD_MONTHS('2017-12-01', RowCount)) AS RevMonth,
    CAST(0 AS DOUBLE) AS Revenue,
    CAST(0 AS DOUBLE) AS Volume,
    *
  
  FROM GenerateRows_1189 AS in0

),

AlteryxSelect_1193 AS (

  SELECT 
    CAST(Revenue AS DECIMAL (19, 6)) AS Revenue,
    * EXCEPT (`Placeholder`, `RowCount`, `Revenue`)
  
  FROM Formula_1191_0 AS in0

),

Formula_1183 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_1183')}}

),

Summarize_1188 AS (

  SELECT 
    DISTINCT SubCustSeg5 AS SubCustSeg5,
    SubCustSeg1 AS SubCustSeg1,
    CustomerName AS CustomerName,
    SubCustSeg6 AS SubCustSeg6,
    `Customer Segment` AS `Customer Segment`,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    Product AS Product,
    SubCustSeg2 AS SubCustSeg2
  
  FROM Formula_1183 AS in0

),

AppendFields_1190 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM AlteryxSelect_1193 AS in0
  INNER JOIN Summarize_1188 AS in1
     ON TRUE

),

Union_1195_reformat_0 AS (

  SELECT 
    `Customer Segment` AS `Customer Segment`,
    CustomerName AS CustomerName,
    Product AS Product,
    RevMonth AS RevMonth,
    CAST(Revenue AS DOUBLE) AS Revenue,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg2 AS SubCustSeg2,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    Volume AS Volume
  
  FROM AppendFields_1190 AS in0

),

Union_1195_reformat_1 AS (

  SELECT 
    `Customer Segment` AS `Customer Segment`,
    CustomerName AS CustomerName,
    Product AS Product,
    RevMonth AS RevMonth,
    CAST(Revenue AS DOUBLE) AS Revenue,
    SubCustSeg1 AS SubCustSeg1,
    SubCustSeg2 AS SubCustSeg2,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg6 AS SubCustSeg6,
    Volume AS Volume,
    CAST(YetToRenew AS DOUBLE) AS YetToRenew
  
  FROM Formula_1183 AS in0

),

Union_1195 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_1195_reformat_1', 'Union_1195_reformat_0'], 
      [
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "SubCustSeg2", "dataType": "String"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Decimal"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "SubCustSeg2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_1223 AS (

  SELECT 
    SUM(Revenue) AS Revenue,
    SUM(YetToRenew) AS YetToRenew,
    SUM(Volume) AS Volume,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg1 AS SubCustSeg1,
    CustomerName AS CustomerName,
    SubCustSeg6 AS SubCustSeg6,
    `Customer Segment` AS `Customer Segment`,
    RevMonth AS RevMonth,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    Product AS Product,
    SubCustSeg2 AS SubCustSeg2
  
  FROM Union_1195 AS in0
  
  GROUP BY 
    SubCustSeg5, 
    SubCustSeg1, 
    CustomerName, 
    SubCustSeg6, 
    `Customer Segment`, 
    RevMonth, 
    SubCustSeg3, 
    SubCustSeg4, 
    Product, 
    SubCustSeg2

),

Filter_1214 AS (

  SELECT * 
  
  FROM Summarize_1223 AS in0
  
  WHERE (
          NOT(
            Revenue = 0)
        )

),

Summarize_1213 AS (

  SELECT 
    MIN(RevMonth) AS Min_RevMonth,
    MAX(RevMonth) AS Max_RevMonth,
    CustomerName AS CustomerName
  
  FROM Filter_1214 AS in0
  
  GROUP BY CustomerName

),

Join_1215_inner AS (

  SELECT 
    in0.* EXCEPT (`CustomerName`),
    in1.*
  
  FROM Summarize_1213 AS in0
  INNER JOIN Summarize_1223 AS in1
     ON (in0.CustomerName = in1.CustomerName)

),

Filter_1216_to_Filter_1221 AS (

  SELECT * 
  
  FROM Join_1215_inner AS in0
  
  WHERE ((RevMonth >= Min_RevMonth) AND (RevMonth <= add_months(Max_RevMonth, 12)))

),

Summarize_1494 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    CustomerName AS CustomerName,
    SubCustSeg2 AS SubCustSeg2
  
  FROM Filter_1216_to_Filter_1221 AS in0
  
  GROUP BY 
    CustomerName, SubCustSeg2

),

Filter_1497 AS (

  SELECT * 
  
  FROM Summarize_1494 AS in0
  
  WHERE (
          NOT(
            UPPER(SubCustSeg2) = UPPER('Other'))
        )

),

Sample_1496 AS (

  {{
    prophecy_basics.Sample(
      ['Filter_1497'], 
      '[{"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg2", "dataType": "String"}, {"name": "Sum_Revenue", "dataType": "Double"}]', 
      'sampleGroup', 
      ['CustomerName'], 
      1002, 
      'firstN', 
      1, 
      []
    )
  }}

),

Summarize_1506 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    CustomerName AS CustomerName,
    SubCustSeg5 AS SubCustSeg5
  
  FROM Filter_1216_to_Filter_1221 AS in0
  
  GROUP BY 
    CustomerName, SubCustSeg5

),

Filter_1509 AS (

  SELECT * 
  
  FROM Summarize_1506 AS in0
  
  WHERE (
          NOT(
            UPPER(SubCustSeg5) = UPPER('Other'))
        )

),

Sample_1508 AS (

  {{
    prophecy_basics.Sample(
      ['Filter_1509'], 
      '[{"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg5", "dataType": "String"}, {"name": "Sum_Revenue", "dataType": "Double"}]', 
      'sampleGroup', 
      ['CustomerName'], 
      1002, 
      'firstN', 
      1, 
      []
    )
  }}

),

Summarize_1502 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    CustomerName AS CustomerName,
    SubCustSeg4 AS SubCustSeg4
  
  FROM Filter_1216_to_Filter_1221 AS in0
  
  GROUP BY 
    CustomerName, SubCustSeg4

),

Filter_1505 AS (

  SELECT * 
  
  FROM Summarize_1502 AS in0
  
  WHERE (
          NOT(
            UPPER(SubCustSeg4) = UPPER('Other'))
        )

),

Sample_1504 AS (

  {{
    prophecy_basics.Sample(
      ['Filter_1505'], 
      '[{"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "Sum_Revenue", "dataType": "Double"}]', 
      'sampleGroup', 
      ['CustomerName'], 
      1002, 
      'firstN', 
      1, 
      []
    )
  }}

),

Summarize_1486 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    CustomerName AS CustomerName,
    SubCustSeg1 AS SubCustSeg1
  
  FROM Filter_1216_to_Filter_1221 AS in0
  
  GROUP BY 
    CustomerName, SubCustSeg1

),

Filter_1489 AS (

  SELECT * 
  
  FROM Summarize_1486 AS in0
  
  WHERE (
          NOT(
            UPPER(SubCustSeg1) = UPPER('Other'))
        )

),

Sample_1488 AS (

  {{
    prophecy_basics.Sample(
      ['Filter_1489'], 
      '[{"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Sum_Revenue", "dataType": "Double"}]', 
      'sampleGroup', 
      ['CustomerName'], 
      1002, 
      'firstN', 
      1, 
      []
    )
  }}

),

Summarize_1498 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    CustomerName AS CustomerName,
    SubCustSeg3 AS SubCustSeg3
  
  FROM Filter_1216_to_Filter_1221 AS in0
  
  GROUP BY 
    CustomerName, SubCustSeg3

),

Filter_1501 AS (

  SELECT * 
  
  FROM Summarize_1498 AS in0
  
  WHERE (
          NOT(
            UPPER(SubCustSeg3) = UPPER('Other'))
        )

),

Sample_1500 AS (

  {{
    prophecy_basics.Sample(
      ['Filter_1501'], 
      '[{"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Sum_Revenue", "dataType": "Double"}]', 
      'sampleGroup', 
      ['CustomerName'], 
      1002, 
      'firstN', 
      1, 
      []
    )
  }}

),

Summarize_1512 AS (

  SELECT 
    SUM(Revenue) AS Sum_Revenue,
    CustomerName AS CustomerName,
    SubCustSeg6 AS SubCustSeg6
  
  FROM Filter_1216_to_Filter_1221 AS in0
  
  GROUP BY 
    CustomerName, SubCustSeg6

),

Filter_1515 AS (

  SELECT * 
  
  FROM Summarize_1512 AS in0
  
  WHERE (
          NOT(
            UPPER(SubCustSeg6) = UPPER('Other'))
        )

),

Sample_1514 AS (

  {{
    prophecy_basics.Sample(
      ['Filter_1515'], 
      '[{"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Sum_Revenue", "dataType": "Double"}]', 
      'sampleGroup', 
      ['CustomerName'], 
      1002, 
      'firstN', 
      1, 
      []
    )
  }}

),

Summarize_1493 AS (

  SELECT DISTINCT CustomerName AS CustomerName
  
  FROM Filter_1216_to_Filter_1221 AS in0

),

JoinMultiple_1510 AS (

  SELECT 
    in2.SubCustSeg2 AS SubCustSeg2,
    in5.SubCustSeg5 AS SubCustSeg5,
    in4.SubCustSeg4 AS SubCustSeg4,
    in0.CustomerName AS CustomerName,
    in3.SubCustSeg3 AS SubCustSeg3,
    in1.SubCustSeg1 AS SubCustSeg1,
    in6.SubCustSeg6 AS SubCustSeg6
  
  FROM Summarize_1493 AS in0
  FULL JOIN Sample_1488 AS in1
     ON (in0.CustomerName = in1.CustomerName)
  FULL JOIN Sample_1496 AS in2
     ON (coalesce(in0.CustomerName, in1.CustomerName) = in2.CustomerName)
  FULL JOIN Sample_1500 AS in3
     ON (coalesce(in0.CustomerName, in1.CustomerName, in2.CustomerName) = in3.CustomerName)
  FULL JOIN Sample_1504 AS in4
     ON (coalesce(in0.CustomerName, in1.CustomerName, in2.CustomerName, in3.CustomerName) = in4.CustomerName)
  FULL JOIN Sample_1508 AS in5
     ON (coalesce(in0.CustomerName, in1.CustomerName, in2.CustomerName, in3.CustomerName, in4.CustomerName) = in5.CustomerName)
  FULL JOIN Sample_1514 AS in6
     ON (coalesce(in0.CustomerName, in1.CustomerName, in2.CustomerName, in3.CustomerName, in4.CustomerName, in5.CustomerName) = in6.CustomerName)

),

MultiFieldFormula_1511 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['JoinMultiple_1510'], 
      "CASE WHEN CAST(isnull(column_value) AS BOOLEAN) THEN 'Other' WHEN (isnull(column_value) OR (length(column_value) = 0)) THEN 'Other' ELSE column_value END", 
      [
        'SubCustSeg5', 
        'SubCustSeg1', 
        'CustomerName', 
        'SubCustSeg6', 
        'SubCustSeg3', 
        'SubCustSeg4', 
        'SubCustSeg2'
      ], 
      ['SubCustSeg1', 'SubCustSeg2', 'SubCustSeg3', 'SubCustSeg4', 'SubCustSeg5', 'SubCustSeg6'], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Join_1490_inner AS (

  SELECT 
    in0.* EXCEPT (`SubCustSeg1`, `SubCustSeg2`, `SubCustSeg3`, `SubCustSeg4`, `SubCustSeg5`, `SubCustSeg6`),
    in1.* EXCEPT (`CustomerName`)
  
  FROM Filter_1216_to_Filter_1221 AS in0
  INNER JOIN MultiFieldFormula_1511 AS in1
     ON (in0.CustomerName = in1.CustomerName)

),

Join_1490_left AS (

  SELECT in0.*
  
  FROM Filter_1216_to_Filter_1221 AS in0
  ANTI JOIN MultiFieldFormula_1511 AS in1
     ON (in0.CustomerName = in1.CustomerName)

),

MultiFieldFormula_1491 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['Join_1490_left'], 
      "'Other'", 
      [
        'SubCustSeg5', 
        'Volume', 
        'SubCustSeg1', 
        'Min_RevMonth', 
        'CustomerName', 
        'SubCustSeg6', 
        'Customer Segment', 
        'RevMonth', 
        'Revenue', 
        'SubCustSeg3', 
        'Max_RevMonth', 
        'SubCustSeg4', 
        'YetToRenew', 
        'Product', 
        'SubCustSeg2'
      ], 
      [
        'SubCustSeg5', 
        'SubCustSeg1', 
        'CustomerName', 
        'SubCustSeg6', 
        'Customer Segment', 
        'SubCustSeg3', 
        'SubCustSeg4', 
        'Product', 
        'SubCustSeg2'
      ], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Union_1492 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_1490_inner', 'MultiFieldFormula_1491'], 
      [
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Min_RevMonth", "dataType": "Date"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Max_RevMonth", "dataType": "Date"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "SubCustSeg2", "dataType": "String"}]', 
        '[{"name": "SubCustSeg5", "dataType": "String"}, {"name": "Volume", "dataType": "Double"}, {"name": "SubCustSeg1", "dataType": "String"}, {"name": "Min_RevMonth", "dataType": "Date"}, {"name": "CustomerName", "dataType": "String"}, {"name": "SubCustSeg6", "dataType": "String"}, {"name": "Customer Segment", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Revenue", "dataType": "Double"}, {"name": "SubCustSeg3", "dataType": "String"}, {"name": "Max_RevMonth", "dataType": "Date"}, {"name": "SubCustSeg4", "dataType": "String"}, {"name": "YetToRenew", "dataType": "Double"}, {"name": "Product", "dataType": "String"}, {"name": "SubCustSeg2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_1516 AS (

  SELECT 
    SUM(Revenue) AS Revenue,
    CustomerName AS CustomerName,
    `Customer Segment` AS `Customer Segment`,
    RevMonth AS RevMonth
  
  FROM Union_1492 AS in0
  
  GROUP BY 
    CustomerName, `Customer Segment`, RevMonth

),

Summarize_1518 AS (

  SELECT 
    *,
    first(`Customer Segment`) OVER (PARTITION BY CustomerName, RevMonth ORDER BY CustomerName ASC NULLS FIRST, RevMonth ASC NULLS FIRST, Revenue DESC NULLS FIRST) AS `customer segment_tmp`,
    row_number() OVER (PARTITION BY CustomerName, RevMonth ORDER BY CustomerName ASC NULLS FIRST, RevMonth ASC NULLS FIRST, Revenue DESC NULLS FIRST) AS row_number
  
  FROM Summarize_1516 AS in0

),

Summarize_1518_rename AS (

  SELECT 
    `customer segment_tmp` AS `customer segment`,
    * EXCEPT (`customer segment`, `customer segment_tmp`)
  
  FROM Summarize_1518 AS in0

),

`1518_filter` AS (

  SELECT * 
  
  FROM Summarize_1518_rename AS in0
  
  WHERE (row_number = 1)

),

Summarize_1518_drop_0 AS (

  SELECT * EXCEPT (`row_number`)
  
  FROM `1518_filter` AS in0

),

Join_1519_inner AS (

  SELECT 
    in0.* EXCEPT (`Customer Segment`, `Revenue`),
    in1.* EXCEPT (`CustomerName`, `RevMonth`)
  
  FROM Union_1492 AS in0
  INNER JOIN Summarize_1518_drop_0 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.RevMonth = in1.RevMonth))

),

Filter_1527 AS (

  SELECT * 
  
  FROM Join_1519_inner AS in0
  
  WHERE (Revenue > 0)

),

Summarize_1528 AS (

  SELECT 
    *,
    first(RevMonth) OVER (PARTITION BY CustomerName ORDER BY CustomerName ASC NULLS FIRST, RevMonth ASC NULLS FIRST) AS revmonth_tmp,
    first(`Customer Segment`) OVER (PARTITION BY CustomerName ORDER BY CustomerName ASC NULLS FIRST, RevMonth ASC NULLS FIRST) AS `customer segment_tmp`,
    row_number() OVER (PARTITION BY CustomerName ORDER BY CustomerName ASC NULLS FIRST, RevMonth ASC NULLS FIRST) AS row_number
  
  FROM Filter_1527 AS in0

),

Summarize_1528_rename AS (

  SELECT 
    revmonth_tmp AS revmonth,
    `customer segment_tmp` AS `customer segment`,
    * EXCEPT (`revmonth`, `customer segment`, `revmonth_tmp`, `customer segment_tmp`)
  
  FROM Summarize_1528 AS in0

),

`1528_filter` AS (

  SELECT * 
  
  FROM Summarize_1528_rename AS in0
  
  WHERE (row_number = 1)

),

Summarize_1528_drop_0 AS (

  SELECT * EXCEPT (`row_number`)
  
  FROM `1528_filter` AS in0

),

Summarize_1522 AS (

  SELECT 
    SUM(Revenue) AS Revenue,
    CustomerName AS CustomerName,
    RevMonth AS RevMonth,
    `Customer Segment` AS `Customer Segment`
  
  FROM Join_1519_inner AS in0
  
  GROUP BY 
    CustomerName, RevMonth, `Customer Segment`

),

Join_1523_inner AS (

  SELECT 
    in1.RevMonth AS FirstPosRevMonth,
    in1.`Customer Segment` AS FirstPosCustSeg,
    in0.* EXCEPT (`Revenue`),
    in1.* EXCEPT (`CustomerName`, `RevMonth`, `Customer Segment`)
  
  FROM Summarize_1522 AS in0
  INNER JOIN Summarize_1528_drop_0 AS in1
     ON (in0.CustomerName = in1.CustomerName)

),

MultiRowFormula_1524_window AS (

  SELECT 
    *,
    lead(CustomerName, 1) OVER (PARTITION BY CustomerName ORDER BY CustomerName ASC NULLS FIRST) AS CustomerName_lead1,
    lead(`Customer Segment`, 1) OVER (PARTITION BY CustomerName ORDER BY CustomerName ASC NULLS FIRST) AS `Customer Segment_lead1`
  
  FROM Join_1523_inner AS in0

),

MultiRowFormula_1524_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((Revenue <= 0) AND (CAST(CustomerName AS INTEGER) = CustomerName_lead1))
          THEN CAST(`Customer Segment_lead1` AS string)
        ELSE `Customer Segment`
      END
    ) AS string) AS `New Cust Seg`,
    * EXCEPT (`CustomerName_lead1`, `Customer Segment_lead1`)
  
  FROM MultiRowFormula_1524_window AS in0

),

AlteryxSelect_1525 AS (

  SELECT * EXCEPT (`FirstPosRevMonth`, `FirstPosCustSeg`)
  
  FROM MultiRowFormula_1524_0 AS in0

),

Join_1526_inner_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.CustomerName = in1.CustomerName) AND (in0.RevMonth = in1.RevMonth))
          THEN NULL
        ELSE in0.`Customer Segment`
      END
    ) AS `New Cust Seg`,
    in0.* EXCEPT (`SubCustSeg5`, 
    `Volume`, 
    `SubCustSeg1`, 
    `Min_RevMonth`, 
    `CustomerName`, 
    `SubCustSeg6`, 
    `Customer Segment`, 
    `RevMonth`, 
    `Revenue`, 
    `SubCustSeg3`, 
    `Max_RevMonth`, 
    `SubCustSeg4`, 
    `YetToRenew`, 
    `Product`, 
    `SubCustSeg2`),
    in1.* EXCEPT (`New Cust Seg`)
  
  FROM Join_1519_inner AS in0
  LEFT JOIN AlteryxSelect_1525 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.RevMonth = in1.RevMonth))

),

AlteryxSelect_1521 AS (

  SELECT 
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Min_RevMonth AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Min_RevMonth AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Min_RevMonth AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Min_RevMonth AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Min_RevMonth AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS Min_RevMonth,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Max_RevMonth AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Max_RevMonth AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Max_RevMonth AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Max_RevMonth AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Max_RevMonth AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS Max_RevMonth,
    `New Cust Seg` AS `Customer Segment`,
    * EXCEPT (`Customer Segment`, `Min_RevMonth`, `Max_RevMonth`, `New Cust Seg`)
  
  FROM Join_1526_inner_UnionLeftOuter AS in0

),

Summarize_1194 AS (

  SELECT 
    SUM(Revenue) AS Revenue,
    SUM(YetToRenew) AS YetToRenew,
    SUM(Volume) AS Volume,
    SubCustSeg5 AS SubCustSeg5,
    SubCustSeg1 AS SubCustSeg1,
    CustomerName AS CustomerName,
    SubCustSeg6 AS SubCustSeg6,
    `Customer Segment` AS `Customer Segment`,
    RevMonth AS RevMonth,
    SubCustSeg3 AS SubCustSeg3,
    SubCustSeg4 AS SubCustSeg4,
    Product AS Product,
    SubCustSeg2 AS SubCustSeg2
  
  FROM AlteryxSelect_1521 AS in0
  
  GROUP BY 
    SubCustSeg5, 
    SubCustSeg1, 
    CustomerName, 
    SubCustSeg6, 
    `Customer Segment`, 
    RevMonth, 
    SubCustSeg3, 
    SubCustSeg4, 
    Product, 
    SubCustSeg2

),

AlteryxSelect_2522 AS (

  SELECT 
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(RevMonth AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(RevMonth AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(RevMonth AS string), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(RevMonth AS string), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(RevMonth AS string), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS RevMonth,
    * EXCEPT (`RevMonth`)
  
  FROM Summarize_1194 AS in0

),

Formula_1276 AS (

  SELECT *
  
  FROM AlteryxSelect_2522 AS in0

),

Join_1277_inner_UnionLeftOuter AS (

  SELECT 
    in1.Product AS Right_Product,
    in1.YetToRenew AS Right_YetToRenew,
    in0.* EXCEPT (`SubCustSeg5`, 
    `Volume`, 
    `SubCustSeg1`, 
    `CustomerName`, 
    `SubCustSeg6`, 
    `Customer Segment`, 
    `RevMonth`, 
    `Revenue`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg2`),
    in1.* EXCEPT (`Product`, `YetToRenew`)
  
  FROM Formula_1276 AS in0
  LEFT JOIN Formula_1276 AS in1
     ON ((in0.CustomerName = in1.CustomerName) AND (in0.Product = in1.Product))

),

Cleanse_1281 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_1277_inner_UnionLeftOuter'], 
      [
        { "name": "SubCustSeg5", "dataType": "String" }, 
        { "name": "Right_Product", "dataType": "String" }, 
        { "name": "Volume", "dataType": "Double" }, 
        { "name": "SubCustSeg1", "dataType": "String" }, 
        { "name": "CustomerName", "dataType": "String" }, 
        { "name": "SubCustSeg6", "dataType": "String" }, 
        { "name": "Customer Segment", "dataType": "String" }, 
        { "name": "RevMonth", "dataType": "Date" }, 
        { "name": "Revenue", "dataType": "Double" }, 
        { "name": "SubCustSeg3", "dataType": "String" }, 
        { "name": "SubCustSeg4", "dataType": "String" }, 
        { "name": "YetToRenew", "dataType": "Double" }, 
        { "name": "Right_YetToRenew", "dataType": "Double" }, 
        { "name": "Product", "dataType": "String" }, 
        { "name": "SubCustSeg2", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['Revenue', 'Volume'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Formula_1280 AS (

  SELECT *
  
  FROM Cleanse_1281 AS in0

)

SELECT *

FROM Formula_1280
