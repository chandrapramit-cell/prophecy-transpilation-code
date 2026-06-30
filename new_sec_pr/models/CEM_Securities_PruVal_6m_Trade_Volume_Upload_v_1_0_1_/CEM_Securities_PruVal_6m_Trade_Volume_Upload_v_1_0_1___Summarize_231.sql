{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_58 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Configuration_t_58_ref') }}

),

Sample_56 AS (

  {{
    prophecy_basics.Sample(
      ['Configuration_t_58'], 
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

AlteryxSelect_61 AS (

  SELECT `Number of working Days` AS `Number of working Days`
  
  FROM Sample_56 AS in0

),

AlteryxSelect_60 AS (

  SELECT 
    CAST(Month AS string) AS Month,
    Weights AS Weights
  
  FROM Configuration_t_58 AS in0

),

Formula_69_0 AS (

  SELECT 
    CAST(month(MONTH) AS STRING) AS Month,
    * EXCEPT (`month`)
  
  FROM AlteryxSelect_60 AS in0

),

table_76_Output4_macro_op AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'table_76_Output4_macro_op') }}

),

TextToColumns_180 AS (

  {{
    prophecy_basics.TextToColumns(
      ['table_76_Output4_macro_op'], 
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

TextToColumns_180_dropGem_0 AS (

  SELECT 
    FileName_1_FileName AS FileName1,
    FileName_2_FileName AS FileName2,
    * EXCEPT (`FileName_1_FileName`, `FileName_2_FileName`)
  
  FROM TextToColumns_180 AS in0

),

Formula_182_0 AS (

  SELECT 
    CAST((REVERSE(FileName2)) AS string) AS FileName2,
    * EXCEPT (`filename2`)
  
  FROM TextToColumns_180_dropGem_0 AS in0

),

TextToColumns_181 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Formula_182_0'], 
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

TextToColumns_181_dropGem_0 AS (

  SELECT 
    FileName2_1_FileName2 AS FileName21,
    FileName2_2_FileName2 AS FileName22,
    * EXCEPT (`FileName2_1_FileName2`, `FileName2_2_FileName2`)
  
  FROM TextToColumns_181 AS in0

),

Formula_185_0 AS (

  SELECT 
    CAST((REVERSE(FileName21)) AS string) AS Month_1,
    *
  
  FROM TextToColumns_181_dropGem_0 AS in0

),

DateTime_183_0 AS (

  SELECT 
    (DATE_FORMAT((TO_TIMESTAMP(Month_1, 'yyyyMMdd')), 'yyyy-MM-dd')) AS variableDate,
    *
  
  FROM Formula_185_0 AS in0

),

AlteryxSelect_184 AS (

  SELECT 
    CAST(ISIN AS string) AS ISIN,
    Instrument AS Instrument,
    TradeDate AS TradeDate,
    CAST(`Notional USD` AS DOUBLE) AS `Notional USD`,
    FileName1 AS Region,
    CAST(variableDate AS string) AS variableDate
  
  FROM DateTime_183_0 AS in0

),

Formula_186_0 AS (

  SELECT 
    CAST(month(variableDate) AS STRING) AS Month,
    CAST(ABS(`Notional USD`) AS DOUBLE) AS ABS_Notional,
    *
  
  FROM AlteryxSelect_184 AS in0

),

Join_59_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Month`)
  
  FROM Formula_186_0 AS in0
  INNER JOIN Formula_69_0 AS in1
     ON (in0.Month = in1.Month)

),

AppendFields_62 AS (

  SELECT 
    in1.ISIN AS ISIN,
    in1.Weights AS Weights,
    in0.`Number of working Days` AS `Number of working Days`,
    in1.Region AS Region,
    in1.ABS_Notional AS ABS_Notional,
    in1.Instrument AS Instrument,
    in1.Month AS Month
  
  FROM AlteryxSelect_61 AS in0
  INNER JOIN Join_59_inner AS in1
     ON TRUE

),

Formula_73_0 AS (

  SELECT 
    CAST(((ABS_Notional * Weights) / `Number of working Days`) AS DECIMAL (19, 4)) AS Weighted_Sum,
    *
  
  FROM AppendFields_62 AS in0

),

Filter_232 AS (

  SELECT * 
  
  FROM Formula_73_0 AS in0
  
  WHERE (NOT(ISIN IS NULL))

),

Summarize_231 AS (

  SELECT 
    SUM(Weighted_Sum) AS Weighted_Sum,
    ISIN AS ISIN,
    Instrument AS Instrument
  
  FROM Filter_232 AS in0
  
  GROUP BY 
    ISIN, Instrument

)

SELECT *

FROM Summarize_231
