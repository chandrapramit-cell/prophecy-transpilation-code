{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH DynamicInput_221 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('FVH_IS_AMER_Repo_Significance_Testing_v1_13', 'DynamicInput_221') }}

),

AlteryxSelect_169 AS (

  SELECT 
    CAST(SPN AS INTEGER) AS SPN,
    * EXCEPT (`SPN`)
  
  FROM DynamicInput_221 AS in0

),

Formula_57_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Formula_57_0')}}

),

Formula_56_0 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13__Formula_56_0')}}

),

Join_58_inner AS (

  SELECT 
    in1.Charge AS Std,
    in0.*,
    in1.* EXCEPT (`CP_TYPE`, `COUNTERPARTY`, `VA_CURVE`, `TENOR`, `Charge`, `Concat`)
  
  FROM Formula_57_0 AS in0
  INNER JOIN Formula_56_0 AS in1
     ON (in0.Concat = in1.Concat)

),

Join_167_inner AS (

  SELECT 
    in0.`VA Map` AS `VA Map`,
    in1.`UCN Credit Rating Obligor` AS `UCN Credit Rating Obligor`,
    in0.`Amount Total` AS `Amount Total`,
    in0.CURVE_TYPE AS CURVE_TYPE,
    in0.COLLATERAL_CONTRACT_ID AS COLLATERAL_CONTRACT_ID,
    in0.Portfolio AS Portfolio,
    in0.Curve AS Curve,
    in0.COLLATERAL_TYPE AS COLLATERAL_TYPE,
    in0.SPN AS SPN,
    in0.Concat AS Concat,
    in0.Tenor AS Tenor,
    in0.Std AS Std,
    in0.`VA Curve` AS `VA Curve`,
    in0.IS_INTERNAL AS IS_INTERNAL,
    in0.TradeID AS TradeID,
    in0.`Curve Family` AS `Curve Family`,
    in1.SPN AS Right_SPN,
    in0.Region AS Region,
    in0.`Sub Type` AS `Sub Type`,
    in0.`Risk Type` AS `Risk Type`,
    in0.`Source System` AS `Source System`,
    in0.CURVE_DESCRIPTOR AS CURVE_DESCRIPTOR
  
  FROM Join_58_inner AS in0
  INNER JOIN AlteryxSelect_169 AS in1
     ON (in0.SPN = in1.SPN)

),

Union_171_1 AS (

  SELECT 
    CAST(COLLATERAL_CONTRACT_ID AS string) AS prophecy_column_5,
    CAST(`Sub Type` AS string) AS prophecy_column_10,
    CAST(Region AS string) AS prophecy_column_14,
    CAST(Std AS DOUBLE) AS prophecy_column_20,
    CAST(`Source System` AS string) AS prophecy_column_1,
    CAST(SPN AS INTEGER) AS prophecy_column_6,
    CAST(`UCN Credit Rating Obligor` AS string) AS prophecy_column_21,
    CAST(CURVE_TYPE AS string) AS prophecy_column_9,
    CAST(COLLATERAL_TYPE AS string) AS prophecy_column_13,
    CAST(`Risk Type` AS string) AS prophecy_column_2,
    CAST(`VA Curve` AS string) AS prophecy_column_17,
    CAST(Right_SPN AS INTEGER) AS prophecy_column_22,
    CAST(Portfolio AS string) AS prophecy_column_12,
    CAST(IS_INTERNAL AS string) AS prophecy_column_7,
    CAST(Tenor AS string) AS prophecy_column_3,
    CAST(`VA Map` AS string) AS prophecy_column_18,
    CAST(TradeID AS string) AS prophecy_column_16,
    CAST(`Curve Family` AS string) AS prophecy_column_11,
    CAST(CURVE_DESCRIPTOR AS string) AS prophecy_column_8,
    CAST(Concat AS string) AS prophecy_column_19,
    CAST(`Amount Total` AS DOUBLE) AS prophecy_column_4,
    CAST(Curve AS string) AS prophecy_column_15
  
  FROM Join_167_inner AS in0

),

Join_167_left AS (

  SELECT in0.*
  
  FROM Join_58_inner AS in0
  ANTI JOIN AlteryxSelect_169 AS in1
     ON (in0.SPN = in1.SPN)

),

Union_171_0 AS (

  SELECT 
    CAST(COLLATERAL_CONTRACT_ID AS string) AS prophecy_column_5,
    CAST(`Sub Type` AS string) AS prophecy_column_10,
    CAST(F4 AS DOUBLE) AS prophecy_column_24,
    CAST(Feb AS DOUBLE) AS prophecy_column_25,
    CAST(Region AS string) AS prophecy_column_14,
    CAST(Std AS DOUBLE) AS prophecy_column_20,
    CAST(`Source System` AS string) AS prophecy_column_1,
    CAST(SPN AS INTEGER) AS prophecy_column_6,
    CAST(CURVE_TYPE AS string) AS prophecy_column_9,
    CAST(COLLATERAL_TYPE AS string) AS prophecy_column_13,
    CAST(`Risk Type` AS string) AS prophecy_column_2,
    CAST(`VA Curve` AS string) AS prophecy_column_17,
    CAST(F6 AS DOUBLE) AS prophecy_column_27,
    CAST(Portfolio AS string) AS prophecy_column_12,
    CAST(IS_INTERNAL AS string) AS prophecy_column_7,
    CAST(Tenor AS string) AS prophecy_column_3,
    CAST(`VA Map` AS string) AS prophecy_column_18,
    CAST(TradeID AS string) AS prophecy_column_16,
    CAST(`Curve Family` AS string) AS prophecy_column_11,
    CAST(Right_F3 AS DOUBLE) AS prophecy_column_26,
    CAST(F3 AS DOUBLE) AS prophecy_column_23,
    CAST(CURVE_DESCRIPTOR AS string) AS prophecy_column_8,
    CAST(Concat AS string) AS prophecy_column_19,
    CAST(`Amount Total` AS DOUBLE) AS prophecy_column_4,
    CAST(Curve AS string) AS prophecy_column_15
  
  FROM Join_167_left AS in0

),

Union_171 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_171_1', 'Union_171_0'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Integer"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "Integer"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_19", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "Double"}, {"name": "prophecy_column_15", "dataType": "String"}]', 
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "Double"}, {"name": "prophecy_column_25", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Integer"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_27", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "Double"}, {"name": "prophecy_column_23", "dataType": "Double"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_19", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "Double"}, {"name": "prophecy_column_15", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_171_postRename AS (

  SELECT 
    prophecy_column_17 AS `VA Curve`,
    prophecy_column_20 AS Std,
    prophecy_column_5 AS COLLATERAL_CONTRACT_ID,
    prophecy_column_18 AS `VA Map`,
    prophecy_column_24 AS F4,
    prophecy_column_19 AS Concat,
    prophecy_column_3 AS Tenor,
    prophecy_column_14 AS Region,
    prophecy_column_2 AS `Risk Type`,
    prophecy_column_12 AS Portfolio,
    prophecy_column_15 AS Curve,
    prophecy_column_6 AS SPN,
    prophecy_column_25 AS Feb,
    prophecy_column_23 AS F3,
    prophecy_column_13 AS COLLATERAL_TYPE,
    prophecy_column_11 AS `Curve Family`,
    prophecy_column_8 AS CURVE_DESCRIPTOR,
    prophecy_column_27 AS F6,
    prophecy_column_4 AS `Amount Total`,
    prophecy_column_9 AS CURVE_TYPE,
    prophecy_column_1 AS `Source System`,
    prophecy_column_7 AS IS_INTERNAL,
    prophecy_column_22 AS Right_SPN,
    prophecy_column_16 AS TradeID,
    prophecy_column_10 AS `Sub Type`,
    prophecy_column_21 AS `UCN Credit Rating Obligor`,
    prophecy_column_26 AS Right_F3
  
  FROM Union_171 AS in0

),

Formula_173_0 AS (

  SELECT 
    CAST((`UCN Credit Rating Obligor` IS NULL) AS DOUBLE) AS Flag,
    *
  
  FROM Union_171_postRename AS in0

),

Formula_173_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Flag = -1)
          THEN '0'
        ELSE `UCN Credit Rating Obligor`
      END
    ) AS string) AS `UCN Credit Rating Obligor`,
    * EXCEPT (`ucn credit rating obligor`)
  
  FROM Formula_173_0 AS in0

),

TextInput_166 AS (

  SELECT * 
  
  FROM {{ ref('seed_166')}}

),

TextInput_166_cast AS (

  SELECT 
    CAST(UCN AS string) AS UCN,
    CAST(CP_TYPE AS string) AS CP_TYPE
  
  FROM TextInput_166 AS in0

),

Join_168_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`UCN`)
  
  FROM Formula_173_1 AS in0
  INNER JOIN TextInput_166_cast AS in1
     ON (in0.`UCN Credit Rating Obligor` = in1.UCN)

)

SELECT *

FROM Join_168_inner
