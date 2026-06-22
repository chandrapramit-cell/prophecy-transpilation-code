{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_29 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Configuration_t_29_ref') }}

),

Sample_27 AS (

  {{
    prophecy_basics.Sample(
      ['Configuration_t_29'], 
      '[{"name": "Month", "dataType": "Date"}, {"name": "Weights", "dataType": "Double"}, {"name": "Number of working Days", "dataType": "Double"}, {"name": "F4", "dataType": "Double"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      1, 
      []
    )
  }}

),

AlteryxSelect_32 AS (

  SELECT `Number of working Days` AS `Number of working Days`
  
  FROM Sample_27 AS in0

),

AlteryxSelect_31 AS (

  SELECT 
    CAST(Month AS string) AS Month,
    Weights AS Weights
  
  FROM Configuration_t_29 AS in0

),

Formula_43_0 AS (

  SELECT 
    CAST(month(MONTH) AS STRING) AS Month,
    * EXCEPT (`month`)
  
  FROM AlteryxSelect_31 AS in0

),

NaN_Credit_DM_x_26 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'NaN_Credit_DM_x_26_ref') }}

),

TextToColumns_148 AS (

  {{
    prophecy_basics.TextToColumns(
      ['NaN_Credit_DM_x_26'], 
      'FileName', 
      "\[_\]", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'FileName', 
      'FileName', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_148_dropGem_0 AS (

  SELECT 
    FileName_1_FileName AS FileName1,
    FileName_2_FileName AS FileName2,
    * EXCEPT (`FileName_1_FileName`, `FileName_2_FileName`)
  
  FROM TextToColumns_148 AS in0

),

Formula_42_0 AS (

  SELECT 
    CAST(EXTRACT(MONTH FROM TRADEDATE) AS string) AS Month,
    CAST((ABS(USD_DERAMOUNTTOREPORT_MM) * 1000000) AS DOUBLE) AS `ABS U$ Notional`,
    CAST((
      CASE
        WHEN (FileName1 = 'NaN')
          THEN NULL
        ELSE FileName1
      END
    ) AS string) AS Region,
    *
  
  FROM TextToColumns_148_dropGem_0 AS in0

),

Cleanse_150 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_42_0'], 
      [
        { "name": "Month", "dataType": "String" }, 
        { "name": "ABS U$ Notional", "dataType": "Double" }, 
        { "name": "Region", "dataType": "String" }, 
        { "name": "FileName1", "dataType": "String" }, 
        { "name": "FileName2", "dataType": "String" }, 
        { "name": "DERAMOUNTTOREPORT_MM", "dataType": "Double" }, 
        { "name": "ACTUALDATE", "dataType": "Date" }, 
        { "name": "ISSUERCTPNAME", "dataType": "String" }, 
        { "name": "PORTFOLIO", "dataType": "String" }, 
        { "name": "DESKGROUPNAME", "dataType": "String" }, 
        { "name": "TRADEDATE", "dataType": "Date" }, 
        { "name": "FileName", "dataType": "String" }, 
        { "name": "BUSINESSCOLLECTIONNAME", "dataType": "String" }, 
        { "name": "INSTRUMENTCODE", "dataType": "String" }, 
        { "name": "PRODUCTCATEGORY", "dataType": "String" }, 
        { "name": "MISSTATUS", "dataType": "String" }, 
        { "name": "USD_DERAMOUNTTOREPORT_MM", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'ACTUALDATE', 
        'BUSINESSCOLLECTIONNAME', 
        'DERAMOUNTTOREPORT_MM', 
        'DESKGROUPNAME', 
        'INSTRUMENTCODE', 
        'ISSUERCTPNAME', 
        'MISSTATUS', 
        'PORTFOLIO', 
        'PRODUCTCATEGORY', 
        'TRADEDATE', 
        'USD_DERAMOUNTTOREPORT_MM', 
        'Month', 
        'ABS U$ Notional', 
        'Region'
      ], 
      false, 
      '', 
      false, 
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

Join_30_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Month`)
  
  FROM Cleanse_150 AS in0
  INNER JOIN Formula_43_0 AS in1
     ON (in0.Month = in1.Month)

),

AppendFields_33 AS (

  SELECT 
    in1.`ABS U$ Notional` AS `ABS U$ Notional`,
    in1.Weights AS Weights,
    in0.`Number of working Days` AS `Number of working Days`,
    in1.Region AS Region,
    in1.INSTRUMENTCODE AS INSTRUMENTCODE,
    in1.Month AS Month
  
  FROM AlteryxSelect_32 AS in0
  INNER JOIN Join_30_inner AS in1
     ON TRUE

),

Formula_50_0 AS (

  SELECT 
    CAST(((`ABS U$ Notional` * Weights) / `Number of working Days`) AS DOUBLE) AS Weighted_Sum,
    *
  
  FROM AppendFields_33 AS in0

)

SELECT *

FROM Formula_50_0
