{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Summarize_16 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1___Summarize_16')}}

),

Configuration_t_14 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_', 'Configuration_t_14') }}

),

Join_15_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`ISIN`)
  
  FROM Summarize_16 AS in0
  INNER JOIN Configuration_t_14 AS in1
     ON (in0.ISIN = in1.ISIN)

),

Formula_20_0 AS (

  SELECT 
    CAST('Upload' AS string) AS ACTION,
    CAST((CONCAT('Daily_trade_Volume_Rates', Region)) AS string) AS Name,
    *
  
  FROM Join_15_inner AS in0

),

AlteryxSelect_21 AS (

  SELECT 
    ACTION AS ACTION,
    ISIN AS ISIN,
    `Sum_Weighted AverageNotionalTradedUSD` AS Sum_Daily_Volume,
    Mapping AS Product_Type,
    Name AS Name,
    * EXCEPT (`ACTION`, `ISIN`, `Name`, `Sum_Weighted AverageNotionalTradedUSD`, `Mapping`)
  
  FROM Formula_20_0 AS in0

)

SELECT *

FROM AlteryxSelect_21
