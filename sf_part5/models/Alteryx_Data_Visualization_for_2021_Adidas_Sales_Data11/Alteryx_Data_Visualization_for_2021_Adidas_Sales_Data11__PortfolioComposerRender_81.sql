{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AdidasUSSalesDa_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Alteryx_Data_Visualization_for_2021_Adidas_Sales_Data11', 'AdidasUSSalesDa_1') }}

),

AlteryxSelect_2 AS (

  SELECT * EXCLUDE ("RETAILER ID", "CITY", "PRICE PER UNIT", "UNITS SOLD", "OPERATING MARGIN")
  
  FROM AdidasUSSalesDa_1 AS in0

),

Formula_4_0 AS (

  SELECT 
    CAST((TO_CHAR("INVOICE DATE", 'MON YYYY')) AS STRING) AS "MONTH AMPERSAND YEAR OF INVOICE DATE",
    CAST((TO_CHAR("INVOICE DATE", 'YYYY')) AS STRING) AS "YEAR",
    *
  
  FROM AlteryxSelect_2 AS in0

),

AlteryxSelect_8 AS (

  SELECT 
    "MONTH AMPERSAND YEAR OF INVOICE DATE" AS "INVOICE DATE",
    * EXCLUDE ("INVOICE DATE", "SALES METHOD", "MONTH AMPERSAND YEAR OF INVOICE DATE")
  
  FROM Formula_4_0 AS in0

),

Summarize_53 AS (

  SELECT 
    SUM("TOTAL SALES") AS "TOTAL SALES",
    RETAILER AS RETAILER
  
  FROM AlteryxSelect_8 AS in0
  
  GROUP BY RETAILER

),

Sample_55 AS (

  {{
    prophecy_basics.Sample(
      ['Summarize_53'], 
      '[{"name": "TOTAL SALES", "dataType": "Float"}, {"name": "RETAILER", "dataType": "String"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      5
    )
  }}

),

Formula_56_0 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        (REGEXP_REPLACE((TO_CHAR(CAST("TOTAL SALES" AS FLOAT), 'FM999999999999999990')), ',', '__THS__')), 
        '__THS__', 
        ',')
    ) AS STRING) AS "TOTAL SALES FOR TOOLTIP",
    *
  
  FROM Sample_55 AS in0

),

Formula_56_1 AS (

  SELECT 
    CAST(concat(
      '$', 
      regexp_replace(
        regexp_replace(format_number(CAST("TOTAL SALES FOR TOOLTIP" AS DOUBLE), 0), ',', '__THS__'), 
        '__THS__', 
        ',')) AS STRING) AS "TOTAL SALES FOR TOOLTIP",
    * EXCLUDE ("TOTAL SALES FOR TOOLTIP")
  
  FROM Formula_56_0 AS in0

),

PlotlyCharting_58 AS (

  {{ prophecy_basics.ToDo('Component type: PlotlyCharting is not supported.') }}

),

PortfolioComposerText_69 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

JoinMultiple_75_in2 AS (

  {{
    prophecy_basics.RecordID(
      ['PortfolioComposerText_69'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_2', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Summarize_29 AS (

  SELECT 
    SUM("TOTAL SALES") AS "TOTAL SALES",
    STATE AS STATE
  
  FROM AlteryxSelect_8 AS in0
  
  GROUP BY STATE

),

Sample_34 AS (

  {{
    prophecy_basics.Sample(
      ['Summarize_29'], 
      '[{"name": "TOTAL SALES", "dataType": "Float"}, {"name": "STATE", "dataType": "String"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      5
    )
  }}

),

Summarize_9 AS (

  SELECT 
    SUM("TOTAL SALES") AS "SUM_TOTAL SALES",
    SUM("OPERATING PROFIT") AS "SUM_OPERATING PROFIT",
    RETAILER AS RETAILER,
    "REGION" AS "REGION",
    PRODUCT AS PRODUCT,
    "YEAR" AS "YEAR"
  
  FROM AlteryxSelect_8 AS in0
  
  GROUP BY 
    RETAILER, "REGION", PRODUCT, "YEAR"

),

Filter_10 AS (

  SELECT * 
  
  FROM Summarize_9 AS in0
  
  WHERE ("YEAR" = '2021')

),

AlteryxSelect_11 AS (

  SELECT * EXCLUDE ("REGION", "YEAR")
  
  FROM Filter_10 AS in0

),

CrossTab_13 AS (

  SELECT RETAILER AS Retailer
  
  FROM AlteryxSelect_11 AS in0
  PIVOT (
    SUM("SUM_TOTAL SALES")
    FOR Product
    IN (
      'Men_s_Apparel', 
      'Men_s_Street_Footwear', 
      'Women_s_Athletic_Footwear', 
      'Women_s_Apparel', 
      'Women_s_Street_Footwear', 
      'Men_s_Athletic_Footwear'
    )
  )

),

CrossTab_13_rename AS (

  SELECT 
    SUM_MEN_S_APPAREL AS MEN_S_APPAREL,
    SUM_MEN_S_STREET_FOOTWEAR AS MEN_S_STREET_FOOTWEAR,
    SUM_WOMEN_S_ATHLETIC_FOOTWEAR AS WOMEN_S_ATHLETIC_FOOTWEAR,
    SUM_WOMEN_S_APPAREL AS WOMEN_S_APPAREL,
    SUM_WOMEN_S_STREET_FOOTWEAR AS WOMEN_S_STREET_FOOTWEAR,
    SUM_MEN_S_ATHLETIC_FOOTWEAR AS MEN_S_ATHLETIC_FOOTWEAR,
    * EXCLUDE ("SUM_MEN_S_APPAREL", 
    "SUM_MEN_S_STREET_FOOTWEAR", 
    "SUM_WOMEN_S_ATHLETIC_FOOTWEAR", 
    "SUM_WOMEN_S_APPAREL", 
    "SUM_WOMEN_S_STREET_FOOTWEAR", 
    "SUM_MEN_S_ATHLETIC_FOOTWEAR")
  
  FROM CrossTab_13 AS in0

),

AlteryxSelect_14 AS (

  SELECT 
    MEN_S_APPAREL AS "MEN'S APPAREL",
    MEN_S_ATHLETIC_FOOTWEAR AS "MEN'S ATHLETIC FOOTWEAR",
    MEN_S_STREET_FOOTWEAR AS "MEN'S STREET FOOTWEAR",
    WOMEN_S_APPAREL AS "WOMEN'S APPAREL",
    WOMEN_S_ATHLETIC_FOOTWEAR AS "WOMEN'S ATHLETIC FOOTWEAR",
    WOMEN_S_STREET_FOOTWEAR AS "WOMEN'S STREET FOOTWEAR",
    * EXCLUDE ("MEN_S_APPAREL", 
    "MEN_S_ATHLETIC_FOOTWEAR", 
    "MEN_S_STREET_FOOTWEAR", 
    "WOMEN_S_APPAREL", 
    "WOMEN_S_ATHLETIC_FOOTWEAR", 
    "WOMEN_S_STREET_FOOTWEAR")
  
  FROM CrossTab_13_rename AS in0

),

Formula_18_0 AS (

  SELECT 
    CAST((
      (
        ((("MEN'S APPAREL" + "MEN'S ATHLETIC FOOTWEAR") + "MEN'S STREET FOOTWEAR") + "WOMEN'S APPAREL")
        + "WOMEN'S ATHLETIC FOOTWEAR"
      )
      + "WOMEN'S STREET FOOTWEAR"
    ) AS FLOAT) AS "TOTAL",
    *
  
  FROM AlteryxSelect_14 AS in0

),

MultiFieldFormula_24 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['Formula_18_0'], 
      "(REGEXP_REPLACE((REGEXP_REPLACE((TO_CHAR(CAST(CAST(column_value AS STRING) AS FLOAT), 'FM999999999999999990')), ',', '__THS__')), '__THS__', ','))", 
      [
        "MEN'S APPAREL", 
        "MEN'S STREET FOOTWEAR", 
        "WOMEN'S STREET FOOTWEAR", 
        "MEN'S ATHLETIC FOOTWEAR", 
        "WOMEN'S APPAREL", 
        'TOTAL', 
        "WOMEN'S ATHLETIC FOOTWEAR", 
        'RETAILER'
      ], 
      [
        "MEN'S APPAREL", 
        "MEN'S ATHLETIC FOOTWEAR", 
        "MEN'S STREET FOOTWEAR", 
        "WOMEN'S APPAREL", 
        "WOMEN'S ATHLETIC FOOTWEAR", 
        "WOMEN'S STREET FOOTWEAR", 
        'TOTAL'
      ], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

MultiFieldFormula_25 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['MultiFieldFormula_24'], 
      "(CONCAT('$', (REGEXP_REPLACE((REGEXP_REPLACE((TO_CHAR(CAST(column_value AS FLOAT), 'FM999999999999999990')), ',', '__THS__')), '__THS__', ','))))", 
      [
        "MEN'S APPAREL", 
        "MEN'S STREET FOOTWEAR", 
        "WOMEN'S STREET FOOTWEAR", 
        "MEN'S ATHLETIC FOOTWEAR", 
        "WOMEN'S APPAREL", 
        'TOTAL', 
        "WOMEN'S ATHLETIC FOOTWEAR", 
        'RETAILER'
      ], 
      [
        "MEN'S APPAREL", 
        "MEN'S ATHLETIC FOOTWEAR", 
        "MEN'S STREET FOOTWEAR", 
        "WOMEN'S APPAREL", 
        "WOMEN'S ATHLETIC FOOTWEAR", 
        "WOMEN'S STREET FOOTWEAR", 
        'TOTAL'
      ], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

PortfolioComposerTable_26 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

PortfolioComposerText_67 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

Formula_50_0 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        (REGEXP_REPLACE((TO_CHAR(CAST("TOTAL SALES" AS FLOAT), 'FM999999999999999990')), ',', '__THS__')), 
        '__THS__', 
        ',')
    ) AS STRING) AS "TOTAL SALES FOR TOOLTIP",
    *
  
  FROM Sample_34 AS in0

),

Formula_50_1 AS (

  SELECT 
    CAST(concat(
      '$', 
      regexp_replace(
        regexp_replace(format_number(CAST("TOTAL SALES FOR TOOLTIP" AS DOUBLE), 0), ',', '__THS__'), 
        '__THS__', 
        ',')) AS STRING) AS "TOTAL SALES FOR TOOLTIP",
    * EXCLUDE ("TOTAL SALES FOR TOOLTIP")
  
  FROM Formula_50_0 AS in0

),

PlotlyCharting_51 AS (

  {{ prophecy_basics.ToDo('Component type: PlotlyCharting is not supported.') }}

),

Summarize_60 AS (

  SELECT 
    SUM("SUM_TOTAL SALES") AS "TOTAL SALES",
    SUM("SUM_OPERATING PROFIT") AS "TOTAL OPERATING PROFIT",
    PRODUCT AS PRODUCT
  
  FROM Summarize_9 AS in0
  
  GROUP BY PRODUCT

),

Sample_62 AS (

  {{
    prophecy_basics.Sample(
      ['Summarize_60'], 
      '[{"name": "TOTAL SALES", "dataType": "Float"}, {"name": "TOTAL OPERATING PROFIT", "dataType": "Float"}, {"name": "PRODUCT", "dataType": "String"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      5
    )
  }}

),

PortfolioComposerText_68 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

JoinMultiple_75_in0 AS (

  {{
    prophecy_basics.RecordID(
      ['PortfolioComposerText_67'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_0', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Formula_63_0 AS (

  SELECT 
    CAST((
      REGEXP_REPLACE(
        (REGEXP_REPLACE((TO_CHAR(CAST("TOTAL SALES" AS FLOAT), 'FM999999999999999990')), ',', '__THS__')), 
        '__THS__', 
        ',')
    ) AS STRING) AS "TOTAL SALES FOR TOOLTIP",
    *
  
  FROM Sample_62 AS in0

),

Formula_63_1 AS (

  SELECT 
    CAST(concat(
      '$', 
      regexp_replace(
        regexp_replace(format_number(CAST("TOTAL SALES FOR TOOLTIP" AS DOUBLE), 0), ',', '__THS__'), 
        '__THS__', 
        ',')) AS STRING) AS "TOTAL SALES FOR TOOLTIP",
    * EXCLUDE ("TOTAL SALES FOR TOOLTIP")
  
  FROM Formula_63_0 AS in0

),

PortfolioComposerImage_71 AS (

  {{ prophecy_basics.ToDo('Component type: Image is not supported.') }}

),

PortfolioComposerText_72 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

PlotlyCharting_65 AS (

  {{ prophecy_basics.ToDo('Component type: PlotlyCharting is not supported.') }}

),

PortfolioComposerLayout_74 AS (

  {{ prophecy_basics.ToDo('Component type: Layout is not supported.') }}

),

JoinMultiple_75_in1 AS (

  {{
    prophecy_basics.RecordID(
      ['PortfolioComposerLayout_74'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_1', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

PortfolioComposerText_70 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

JoinMultiple_75_in3 AS (

  {{
    prophecy_basics.RecordID(
      ['PortfolioComposerText_70'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_3', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

JoinMultiple_75 AS (

  SELECT 
    in0."MEN'S STREET FOOTWEAR" AS "MEN'S STREET FOOTWEAR",
    in1.IMAGE AS IMAGE,
    in0."MEN'S APPAREL" AS "MEN'S APPAREL",
    in0."WOMEN'S STREET FOOTWEAR" AS "WOMEN'S STREET FOOTWEAR",
    in1."CHART TITLE" AS "CHART TITLE",
    in0.TEXT AS TEXT,
    in0."TOTAL" AS "TOTAL",
    in0.RETAILER AS RETAILER,
    in0."WOMEN'S APPAREL" AS "WOMEN'S APPAREL",
    in1.LAYOUT AS LAYOUT,
    in0."WOMEN'S ATHLETIC FOOTWEAR" AS "WOMEN'S ATHLETIC FOOTWEAR",
    in0."MEN'S ATHLETIC FOOTWEAR" AS "MEN'S ATHLETIC FOOTWEAR",
    in0."TABLE" AS "TABLE"
  
  FROM JoinMultiple_75_in0 AS in0
  FULL JOIN JoinMultiple_75_in1 AS in1
     ON (in0.RECORDPOSITIONFORJOIN_0 = in1.RECORDPOSITIONFORJOIN_1)
  FULL JOIN JoinMultiple_75_in2 AS in2
     ON (coalesce(in0.RECORDPOSITIONFORJOIN_0, in1.RECORDPOSITIONFORJOIN_1) = in2.RECORDPOSITIONFORJOIN_2)
  FULL JOIN JoinMultiple_75_in3 AS in3
     ON (coalesce(in0.RECORDPOSITIONFORJOIN_0, in1.RECORDPOSITIONFORJOIN_1, in2.RECORDPOSITIONFORJOIN_2) = in3.RECORDPOSITIONFORJOIN_3)

),

PortfolioComposerLayout_80 AS (

  {{ prophecy_basics.ToDo('Component type: Layout is not supported.') }}

),

PortfolioComposerRender_81 AS (

  {{ prophecy_basics.ToDo('Component type: Render is not supported.') }}

)

SELECT *

FROM PortfolioComposerRender_81
