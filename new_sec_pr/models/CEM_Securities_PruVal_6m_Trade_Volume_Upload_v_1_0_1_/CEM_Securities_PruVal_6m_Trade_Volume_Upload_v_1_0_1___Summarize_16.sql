{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_5 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Configuration_t_5_ref') }}

),

Sample_10 AS (

  {{
    prophecy_basics.Sample(
      ['Configuration_t_5'], 
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

AlteryxSelect_11 AS (

  SELECT `Number of working Days` AS `Number of working Days`
  
  FROM Sample_10 AS in0

),

AlteryxSelect_9 AS (

  SELECT 
    CAST(Month AS string) AS Month,
    Weights AS Weights
  
  FROM Configuration_t_5 AS in0

),

table_75_Output4_macro_op AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_75_Output4_macro_op') }}

),

TextToColumns_6 AS (

  {{
    prophecy_basics.TextToColumns(
      ['table_75_Output4_macro_op'], 
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

TextToColumns_6_dropGem_0 AS (

  SELECT 
    FileName_1_FileName AS FileName1,
    FileName_2_FileName AS FileName2,
    * EXCEPT (`FileName_1_FileName`, `FileName_2_FileName`)
  
  FROM TextToColumns_6 AS in0

),

TextToColumns_147 AS (

  {{
    prophecy_basics.TextToColumns(
      ['TextToColumns_6_dropGem_0'], 
      'FileName2', 
      "\[_\]", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'FileName2', 
      'FileName2', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_147_dropGem_0 AS (

  SELECT 
    FileName2_1_FileName2 AS FileName21,
    FileName2_2_FileName2 AS FileName22,
    * EXCEPT (`FileName2_1_FileName2`, `FileName2_2_FileName2`)
  
  FROM TextToColumns_147 AS in0

),

AlteryxSelect_7 AS (

  SELECT 
    CAST(ISIN AS string) AS ISIN,
    CAST(NetNotionalTradedUSD AS DOUBLE) AS NetNotionalTradedUSD,
    CAST(GrossNotionalTradedUSD AS DOUBLE) AS GrossNotionalTradedUSD,
    CAST(AverageNotionalTradedUSD AS DOUBLE) AS AverageNotionalTradedUSD,
    FileName1 AS Region,
    FileName22 AS Month
  
  FROM TextToColumns_147_dropGem_0 AS in0

),

Cleanse_151 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_7'], 
      [
        { "name": "ISIN", "dataType": "String" }, 
        { "name": "NetNotionalTradedUSD", "dataType": "Double" }, 
        { "name": "GrossNotionalTradedUSD", "dataType": "Double" }, 
        { "name": "AverageNotionalTradedUSD", "dataType": "Double" }, 
        { "name": "Region", "dataType": "String" }, 
        { "name": "Month", "dataType": "String" }
      ], 
      'keepOriginal', 
      [
        'ISIN', 
        'NetNotionalTradedUSD', 
        'GrossNotionalTradedUSD', 
        'AverageNotionalTradedUSD', 
        'Region', 
        'Month'
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

Join_8_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Month`)
  
  FROM Cleanse_151 AS in0
  INNER JOIN AlteryxSelect_9 AS in1
     ON (in0.Month = in1.Month)

),

AppendFields_12 AS (

  SELECT 
    in1.ISIN AS ISIN,
    in1.Weights AS Weights,
    in0.`Number of working Days` AS `Number of working Days`,
    in1.NetNotionalTradedUSD AS NetNotionalTradedUSD,
    in1.GrossNotionalTradedUSD AS GrossNotionalTradedUSD,
    in1.AverageNotionalTradedUSD AS AverageNotionalTradedUSD,
    in1.Region AS Region,
    in1.Month AS Month
  
  FROM AlteryxSelect_11 AS in0
  INNER JOIN Join_8_inner AS in1
     ON TRUE

),

Formula_13_0 AS (

  SELECT 
    CAST(((GrossNotionalTradedUSD / `Number of working Days`) * Weights) AS DOUBLE) AS `Weighted AverageNotionalTradedUSD`,
    CAST((
      CASE
        WHEN (Region = 'NaN')
          THEN NULL
        ELSE Region
      END
    ) AS string) AS Region,
    * EXCEPT (`region`)
  
  FROM AppendFields_12 AS in0

),

Summarize_16 AS (

  SELECT 
    SUM(`Weighted AverageNotionalTradedUSD`) AS `Sum_Weighted AverageNotionalTradedUSD`,
    Region AS Region,
    ISIN AS ISIN
  
  FROM Formula_13_0 AS in0
  
  GROUP BY 
    Region, ISIN

)

SELECT *

FROM Summarize_16
