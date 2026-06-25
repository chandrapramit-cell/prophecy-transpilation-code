{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_231 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1___Summarize_231')}}

),

Configuration_t_57 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_', 'Configuration_t_57') }}

),

Join_63_inner AS (

  SELECT 
    in0.ISIN AS ISIN,
    in0.Weighted_Sum AS Weighted_Sum,
    in1.CUSIP AS CUSIP,
    in1.`INSTRUMENT CODE` AS `INSTRUMENT CODE`,
    in1.`PRODUCT TYPE` AS `PRODUCT TYPE`,
    in0.Instrument AS Instrument
  
  FROM Summarize_231 AS in0
  INNER JOIN Configuration_t_57 AS in1
     ON ((in0.Instrument = in1.`INSTRUMENT CODE`) AND (in0.ISIN = in1.ISIN))

),

Summarize_70 AS (

  SELECT 
    SUM(Weighted_Sum) AS `Daily volume`,
    ISIN AS ISIN,
    CUSIP AS CUSIP,
    `PRODUCT TYPE` AS `PRODUCT TYPE`
  
  FROM Join_63_inner AS in0
  
  GROUP BY 
    ISIN, CUSIP, `PRODUCT TYPE`

),

Filter_72 AS (

  SELECT * 
  
  FROM Summarize_70 AS in0
  
  WHERE (NOT(ISIN IS NULL))

),

Formula_64_0 AS (

  SELECT 
    CAST('Daily_trade_Volume_Credit_EM' AS string) AS Name,
    CAST('Upload' AS string) AS ACTION,
    *
  
  FROM Filter_72 AS in0

),

AlteryxSelect_65 AS (

  SELECT 
    ACTION AS ACTION,
    ISIN AS ISIN,
    CUSIP AS CUSIP,
    `Daily volume` AS `Daily volume`,
    `PRODUCT TYPE` AS Product_type,
    Name AS Name,
    * EXCEPT (`ACTION`, `ISIN`, `CUSIP`, `Daily volume`, `Name`, `PRODUCT TYPE`)
  
  FROM Formula_64_0 AS in0

)

SELECT *

FROM AlteryxSelect_65
