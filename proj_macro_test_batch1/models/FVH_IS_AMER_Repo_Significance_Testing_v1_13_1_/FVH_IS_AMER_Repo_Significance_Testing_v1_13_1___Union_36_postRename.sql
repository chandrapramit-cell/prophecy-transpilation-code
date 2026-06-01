{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_31_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_31_0')}}

),

AlteryxSelect_45 AS (

  SELECT * EXCEPT (`SOURCE_TRADE`, `SOURCE_TRADE_ID`)
  
  FROM Formula_31_0 AS in0

),

AlteryxSelect_42 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___AlteryxSelect_42')}}

),

Join_34_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`F1`)
  
  FROM AlteryxSelect_45 AS in0
  INNER JOIN AlteryxSelect_42 AS in1
     ON (in0.ISIN = in1.F1)

),

Join_34_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_45 AS in0
  ANTI JOIN AlteryxSelect_42 AS in1
     ON (in0.ISIN = in1.F1)

),

Union_36_1 AS (

  SELECT 
    CAST(COLLATERAL_CONTRACT_ID AS string) AS prophecy_column_5,
    CAST(CURVE_TYPE AS string) AS prophecy_column_10,
    CAST(COLLATERAL_TYPE AS string) AS prophecy_column_14,
    CAST(`Source System` AS string) AS prophecy_column_1,
    CAST(SPN AS INTEGER) AS prophecy_column_6,
    CAST(CURVE_DESCRIPTOR AS string) AS prophecy_column_9,
    CAST(Portfolio AS string) AS prophecy_column_13,
    CAST(`Risk Type` AS string) AS prophecy_column_2,
    CAST(TradeID AS string) AS prophecy_column_17,
    CAST(`Curve Family` AS string) AS prophecy_column_12,
    CAST(IS_INTERNAL AS string) AS prophecy_column_7,
    CAST(Tenor AS string) AS prophecy_column_3,
    CAST(Curve AS string) AS prophecy_column_16,
    CAST(`Sub Type` AS string) AS prophecy_column_11,
    CAST(ISIN AS string) AS prophecy_column_8,
    CAST(`Amount Total` AS DOUBLE) AS prophecy_column_4,
    CAST(Region AS string) AS prophecy_column_15
  
  FROM Join_34_left AS in0

),

Formula_35_0 AS (

  SELECT 
    CAST(ISIN AS string) AS Curve,
    * EXCEPT (`curve`)
  
  FROM Join_34_inner AS in0

),

Union_36_0 AS (

  SELECT 
    CAST(COLLATERAL_CONTRACT_ID AS string) AS prophecy_column_5,
    CAST(CURVE_TYPE AS string) AS prophecy_column_10,
    CAST(COLLATERAL_TYPE AS string) AS prophecy_column_14,
    CAST(Feb AS DOUBLE) AS prophecy_column_20,
    CAST(`Source System` AS string) AS prophecy_column_1,
    CAST(SPN AS INTEGER) AS prophecy_column_6,
    CAST(CURVE_DESCRIPTOR AS string) AS prophecy_column_9,
    CAST(Portfolio AS string) AS prophecy_column_13,
    CAST(`Risk Type` AS string) AS prophecy_column_2,
    CAST(TradeID AS string) AS prophecy_column_17,
    CAST(`Curve Family` AS string) AS prophecy_column_12,
    CAST(IS_INTERNAL AS string) AS prophecy_column_7,
    CAST(Tenor AS string) AS prophecy_column_3,
    CAST(F3 AS DOUBLE) AS prophecy_column_18,
    CAST(Curve AS string) AS prophecy_column_16,
    CAST(`Sub Type` AS string) AS prophecy_column_11,
    CAST(ISIN AS string) AS prophecy_column_8,
    CAST(F4 AS DOUBLE) AS prophecy_column_19,
    CAST(`Amount Total` AS DOUBLE) AS prophecy_column_4,
    CAST(Region AS string) AS prophecy_column_15
  
  FROM Formula_35_0 AS in0

),

Union_36 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_36_1', 'Union_36_0'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Integer"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "Double"}, {"name": "prophecy_column_15", "dataType": "String"}]', 
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Integer"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "Double"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Double"}, {"name": "prophecy_column_15", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_36_postRename AS (

  SELECT 
    prophecy_column_5 AS COLLATERAL_CONTRACT_ID,
    prophecy_column_19 AS F4,
    prophecy_column_3 AS Tenor,
    prophecy_column_15 AS Region,
    prophecy_column_2 AS `Risk Type`,
    prophecy_column_13 AS Portfolio,
    prophecy_column_16 AS Curve,
    prophecy_column_6 AS SPN,
    prophecy_column_20 AS Feb,
    prophecy_column_18 AS F3,
    prophecy_column_14 AS COLLATERAL_TYPE,
    prophecy_column_12 AS `Curve Family`,
    prophecy_column_9 AS CURVE_DESCRIPTOR,
    prophecy_column_8 AS ISIN,
    prophecy_column_4 AS `Amount Total`,
    prophecy_column_10 AS CURVE_TYPE,
    prophecy_column_1 AS `Source System`,
    prophecy_column_7 AS IS_INTERNAL,
    prophecy_column_17 AS TradeID,
    prophecy_column_11 AS `Sub Type`
  
  FROM Union_36 AS in0

)

SELECT *

FROM Union_36_postRename
