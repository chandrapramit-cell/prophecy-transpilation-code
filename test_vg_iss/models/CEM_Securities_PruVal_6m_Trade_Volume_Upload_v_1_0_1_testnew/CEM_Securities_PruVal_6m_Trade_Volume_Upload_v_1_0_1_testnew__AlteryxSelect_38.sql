{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_28 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew', 
      'Configuration_t_28'
    )
  }}

),

Formula_50_0 AS (

  SELECT *
  
  FROM {{ ref('CEM_Securities_PruVal_6m_Trade_Volume_Upload_v_1_0_1_testnew__Formula_50_0')}}

),

Join_35_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Formula_50_0 AS in0
  INNER JOIN Configuration_t_28 AS in1
     ON (in0.INSTRUMENTCODE = in1.`INSTRUMENT CODE`)

),

Summarize_44 AS (

  SELECT 
    SUM(Weighted_Sum) AS `Daily volume`,
    Region AS Region,
    ISIN AS ISIN,
    CUSIP AS CUSIP,
    `PRODUCT TYPE` AS `PRODUCT TYPE`
  
  FROM Join_35_inner AS in0
  
  GROUP BY 
    Region, ISIN, CUSIP, `PRODUCT TYPE`

),

Filter_48 AS (

  SELECT * 
  
  FROM Summarize_44 AS in0
  
  WHERE (NOT(ISIN IS NULL))

),

Formula_37_0 AS (

  SELECT 
    CAST((CONCAT('Daily_trade_Volume_Credit_DM', ' ', Region)) AS string) AS Name,
    CAST('Upload' AS string) AS ACTION,
    *
  
  FROM Filter_48 AS in0

),

AlteryxSelect_38 AS (

  SELECT 
    ACTION AS ACTION,
    ISIN AS ISIN,
    CUSIP AS CUSIP,
    `Daily volume` AS `Daily volume`,
    `PRODUCT TYPE` AS Product_type,
    Name AS Name,
    * EXCEPT (`ACTION`, `ISIN`, `CUSIP`, `Daily volume`, `Name`, `PRODUCT TYPE`)
  
  FROM Formula_37_0 AS in0

)

SELECT *

FROM AlteryxSelect_38
