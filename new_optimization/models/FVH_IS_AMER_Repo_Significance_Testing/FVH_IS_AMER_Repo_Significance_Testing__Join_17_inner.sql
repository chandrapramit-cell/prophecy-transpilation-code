{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_8_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing__Formula_8_0')}}

),

DynamicInput_207 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing', 'DynamicInput_207') }}

),

Join_17_inner AS (

  SELECT 
    in1.Curve AS Right_Curve,
    in0.* EXCEPT (`COLLATERAL_CONTRACT_ID`, 
    `Tenor`, 
    `Region`, 
    `Risk Type`, 
    `Portfolio`, 
    `SPN`, 
    `SOURCE_TRADE_ID`, 
    `SOURCE_TRADE`, 
    `COLLATERAL_TYPE`, 
    `Curve Family`, 
    `CURVE_DESCRIPTOR`, 
    `ISIN`, 
    `Amount Total`, 
    `CURVE_TYPE`, 
    `Source System`, 
    `IS_INTERNAL`, 
    `Sub Type`),
    in1.* EXCEPT (`Curve`)
  
  FROM DynamicInput_207 AS in0
  INNER JOIN Formula_8_0 AS in1
     ON (in0.Curve = in1.Curve)

)

SELECT *

FROM Join_17_inner
