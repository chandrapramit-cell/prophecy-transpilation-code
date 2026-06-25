{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_204_1 AS (

  SELECT *
  
  FROM {{ ref('FVH_IS_AMER_Repo_Significance_Testing_v1_13_1___Formula_204_1')}}

),

DynamicInput_205 AS (

  {{ prophecy_basics.ToDo('Only Excel files are supported in Dynamic Input gem of prophecy.') }}

),

Formula_132_0 AS (

  SELECT 
    CAST('AMER' AS string) AS Region,
    *
  
  FROM DynamicInput_205 AS in0

),

Union_131_0 AS (

  SELECT 
    CAST(`Risk Type` AS string) AS prophecy_column_5,
    CAST(`Risk Ccy` AS string) AS prophecy_column_10,
    CAST(COLLATERAL_TYPE AS string) AS prophecy_column_24,
    CAST(Region AS string) AS prophecy_column_37,
    CAST(COLLATERAL_CONTRACT_ID AS string) AS prophecy_column_25,
    CAST(`Curve Family` AS string) AS prophecy_column_14,
    CAST(Bucket AS string) AS prophecy_column_20,
    CAST(ISIN AS string) AS prophecy_column_29,
    CAST(Desk AS string) AS prophecy_column_1,
    CAST(`Report Name` AS string) AS prophecy_column_6,
    CAST(IS_INTERNAL AS string) AS prophecy_column_28,
    CAST(Position AS string) AS prophecy_column_21,
    CAST(SOURCE_TRADE_ID AS string) AS prophecy_column_33,
    CAST(Ccy AS string) AS prophecy_column_9,
    CAST(`Sub Type` AS string) AS prophecy_column_13,
    CAST(`Book Name` AS string) AS prophecy_column_2,
    CAST(COLLATERAL_CURVE AS string) AS prophecy_column_32,
    CAST(CURVE_DESCRIPTOR AS string) AS prophecy_column_34,
    CAST(Expiry AS string) AS prophecy_column_17,
    CAST(ID_OU AS string) AS prophecy_column_22,
    CAST(SPN AS string) AS prophecy_column_27,
    CAST(variableType AS string) AS prophecy_column_12,
    CAST(`Product Type` AS string) AS prophecy_column_7,
    CAST(`Book Environment` AS string) AS prophecy_column_3,
    CAST(CURVE_TYPE AS string) AS prophecy_column_35,
    CAST(Maturity AS string) AS prophecy_column_18,
    CAST(Tenor AS string) AS prophecy_column_16,
    CAST(DT_MATURITY AS string) AS prophecy_column_31,
    CAST(LE AS string) AS prophecy_column_11,
    CAST(COUNTERPARTY AS string) AS prophecy_column_26,
    CAST(SOURCE_TRADE AS string) AS prophecy_column_23,
    CAST(`Pcp Product Type` AS string) AS prophecy_column_8,
    CAST(Portfolio AS string) AS prophecy_column_36,
    CAST(CUSIP_ISIN AS string) AS prophecy_column_30,
    CAST(`Amount Total` AS string) AS prophecy_column_19,
    CAST(`Source System` AS string) AS prophecy_column_4,
    CAST(`Curve Part` AS string) AS prophecy_column_15
  
  FROM Formula_132_0 AS in0

),

DynamicInput_206 AS (

  {{ prophecy_basics.ToDo('Only Excel files are supported in Dynamic Input gem of prophecy.') }}

),

Formula_133_0 AS (

  SELECT 
    CAST('GEM' AS string) AS Region,
    *
  
  FROM DynamicInput_206 AS in0

),

Union_131_1 AS (

  SELECT 
    CAST(`Risk Type` AS string) AS prophecy_column_5,
    CAST(`Risk Ccy` AS string) AS prophecy_column_10,
    CAST(COLLATERAL_TYPE AS string) AS prophecy_column_24,
    CAST(Region AS string) AS prophecy_column_37,
    CAST(COLLATERAL_CONTRACT_ID AS string) AS prophecy_column_25,
    CAST(`Curve Family` AS string) AS prophecy_column_14,
    CAST(Bucket AS string) AS prophecy_column_20,
    CAST(ISIN AS string) AS prophecy_column_29,
    CAST(Desk AS string) AS prophecy_column_1,
    CAST(`Report Name` AS string) AS prophecy_column_6,
    CAST(IS_INTERNAL AS string) AS prophecy_column_28,
    CAST(Position AS string) AS prophecy_column_21,
    CAST(SOURCE_TRADE_ID AS string) AS prophecy_column_33,
    CAST(Ccy AS string) AS prophecy_column_9,
    CAST(`Sub Type` AS string) AS prophecy_column_13,
    CAST(`Book Name` AS string) AS prophecy_column_2,
    CAST(COLLATERAL_CURVE AS string) AS prophecy_column_32,
    CAST(CURVE_DESCRIPTOR AS string) AS prophecy_column_34,
    CAST(Expiry AS string) AS prophecy_column_17,
    CAST(ID_OU AS string) AS prophecy_column_22,
    CAST(SPN AS string) AS prophecy_column_27,
    CAST(variableType AS string) AS prophecy_column_12,
    CAST(`Product Type` AS string) AS prophecy_column_7,
    CAST(`Book Environment` AS string) AS prophecy_column_3,
    CAST(CURVE_TYPE AS string) AS prophecy_column_35,
    CAST(Maturity AS string) AS prophecy_column_18,
    CAST(Tenor AS string) AS prophecy_column_16,
    CAST(DT_MATURITY AS string) AS prophecy_column_31,
    CAST(LE AS string) AS prophecy_column_11,
    CAST(COUNTERPARTY AS string) AS prophecy_column_26,
    CAST(SOURCE_TRADE AS string) AS prophecy_column_23,
    CAST(`Pcp Product Type` AS string) AS prophecy_column_8,
    CAST(Portfolio AS string) AS prophecy_column_36,
    CAST(CUSIP_ISIN AS string) AS prophecy_column_30,
    CAST(`Amount Total` AS string) AS prophecy_column_19,
    CAST(`Source System` AS string) AS prophecy_column_4,
    CAST(`Curve Part` AS string) AS prophecy_column_15
  
  FROM Formula_133_0 AS in0

),

Union_131 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_131_0', 'Union_131_1'], 
      [
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_37", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_29", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_28", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_33", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_32", "dataType": "String"}, {"name": "prophecy_column_34", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_35", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_31", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_23", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_36", "dataType": "String"}, {"name": "prophecy_column_30", "dataType": "String"}, {"name": "prophecy_column_19", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}]', 
        '[{"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_24", "dataType": "String"}, {"name": "prophecy_column_37", "dataType": "String"}, {"name": "prophecy_column_25", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}, {"name": "prophecy_column_20", "dataType": "String"}, {"name": "prophecy_column_29", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_28", "dataType": "String"}, {"name": "prophecy_column_21", "dataType": "String"}, {"name": "prophecy_column_33", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_32", "dataType": "String"}, {"name": "prophecy_column_34", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_22", "dataType": "String"}, {"name": "prophecy_column_27", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_35", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_31", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_26", "dataType": "String"}, {"name": "prophecy_column_23", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_36", "dataType": "String"}, {"name": "prophecy_column_30", "dataType": "String"}, {"name": "prophecy_column_19", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_131_postRename AS (

  SELECT 
    prophecy_column_15 AS `Curve Part`,
    prophecy_column_20 AS Bucket,
    prophecy_column_26 AS COUNTERPARTY,
    prophecy_column_25 AS COLLATERAL_CONTRACT_ID,
    prophecy_column_1 AS Desk,
    prophecy_column_32 AS COLLATERAL_CURVE,
    prophecy_column_21 AS Position,
    prophecy_column_16 AS Tenor,
    prophecy_column_7 AS `Product Type`,
    prophecy_column_37 AS Region,
    prophecy_column_5 AS `Risk Type`,
    prophecy_column_36 AS Portfolio,
    prophecy_column_27 AS SPN,
    prophecy_column_8 AS `Pcp Product Type`,
    prophecy_column_10 AS `Risk Ccy`,
    prophecy_column_33 AS SOURCE_TRADE_ID,
    prophecy_column_23 AS SOURCE_TRADE,
    prophecy_column_3 AS `Book Environment`,
    prophecy_column_30 AS CUSIP_ISIN,
    prophecy_column_24 AS COLLATERAL_TYPE,
    prophecy_column_14 AS `Curve Family`,
    prophecy_column_11 AS LE,
    prophecy_column_34 AS CURVE_DESCRIPTOR,
    prophecy_column_29 AS ISIN,
    prophecy_column_17 AS Expiry,
    prophecy_column_18 AS Maturity,
    prophecy_column_19 AS `Amount Total`,
    prophecy_column_22 AS ID_OU,
    prophecy_column_35 AS CURVE_TYPE,
    prophecy_column_4 AS `Source System`,
    prophecy_column_31 AS DT_MATURITY,
    prophecy_column_28 AS IS_INTERNAL,
    prophecy_column_6 AS `Report Name`,
    prophecy_column_9 AS Ccy,
    prophecy_column_13 AS `Sub Type`,
    prophecy_column_2 AS `Book Name`,
    prophecy_column_12 AS variableType
  
  FROM Union_131 AS in0

),

Cleanse_135 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Union_131_postRename'], 
      [
        { "name": "Curve Part", "dataType": "String" }, 
        { "name": "Bucket", "dataType": "String" }, 
        { "name": "COUNTERPARTY", "dataType": "String" }, 
        { "name": "COLLATERAL_CONTRACT_ID", "dataType": "String" }, 
        { "name": "Desk", "dataType": "String" }, 
        { "name": "COLLATERAL_CURVE", "dataType": "String" }, 
        { "name": "Position", "dataType": "String" }, 
        { "name": "Tenor", "dataType": "String" }, 
        { "name": "Product Type", "dataType": "String" }, 
        { "name": "Region", "dataType": "String" }, 
        { "name": "Risk Type", "dataType": "String" }, 
        { "name": "Portfolio", "dataType": "String" }, 
        { "name": "SPN", "dataType": "String" }, 
        { "name": "Pcp Product Type", "dataType": "String" }, 
        { "name": "Risk Ccy", "dataType": "String" }, 
        { "name": "SOURCE_TRADE_ID", "dataType": "String" }, 
        { "name": "SOURCE_TRADE", "dataType": "String" }, 
        { "name": "Book Environment", "dataType": "String" }, 
        { "name": "CUSIP_ISIN", "dataType": "String" }, 
        { "name": "COLLATERAL_TYPE", "dataType": "String" }, 
        { "name": "Curve Family", "dataType": "String" }, 
        { "name": "LE", "dataType": "String" }, 
        { "name": "CURVE_DESCRIPTOR", "dataType": "String" }, 
        { "name": "ISIN", "dataType": "String" }, 
        { "name": "Expiry", "dataType": "String" }, 
        { "name": "Maturity", "dataType": "String" }, 
        { "name": "Amount Total", "dataType": "String" }, 
        { "name": "ID_OU", "dataType": "String" }, 
        { "name": "CURVE_TYPE", "dataType": "String" }, 
        { "name": "Source System", "dataType": "String" }, 
        { "name": "DT_MATURITY", "dataType": "String" }, 
        { "name": "IS_INTERNAL", "dataType": "String" }, 
        { "name": "Report Name", "dataType": "String" }, 
        { "name": "Ccy", "dataType": "String" }, 
        { "name": "Sub Type", "dataType": "String" }, 
        { "name": "Book Name", "dataType": "String" }, 
        { "name": "variableType", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['ISIN'], 
      true, 
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

AlteryxSelect_2 AS (

  SELECT 
    `Source System` AS `Source System`,
    `Risk Type` AS `Risk Type`,
    Tenor AS Tenor,
    CAST(`Amount Total` AS DOUBLE) AS `Amount Total`,
    SOURCE_TRADE AS SOURCE_TRADE,
    COLLATERAL_CONTRACT_ID AS COLLATERAL_CONTRACT_ID,
    CAST(SPN AS INTEGER) AS SPN,
    IS_INTERNAL AS IS_INTERNAL,
    ISIN AS ISIN,
    SOURCE_TRADE_ID AS SOURCE_TRADE_ID,
    CURVE_DESCRIPTOR AS CURVE_DESCRIPTOR,
    CURVE_TYPE AS CURVE_TYPE,
    `Sub Type` AS `Sub Type`,
    `Curve Family` AS `Curve Family`,
    Portfolio AS Portfolio,
    COLLATERAL_TYPE AS COLLATERAL_TYPE,
    Region AS Region,
    * EXCEPT (`Desk`, 
    `Book Name`, 
    `Book Environment`, 
    `Report Name`, 
    `Product Type`, 
    `Pcp Product Type`, 
    `Ccy`, 
    `Risk Ccy`, 
    `LE`, 
    `variableType`, 
    `Curve Part`, 
    `Expiry`, 
    `Maturity`, 
    `Bucket`, 
    `Position`, 
    `ID_OU`, 
    `COUNTERPARTY`, 
    `CUSIP_ISIN`, 
    `DT_MATURITY`, 
    `COLLATERAL_CURVE`, 
    `Source System`, 
    `Risk Type`, 
    `Tenor`, 
    `Amount Total`, 
    `SOURCE_TRADE`, 
    `COLLATERAL_CONTRACT_ID`, 
    `SPN`, 
    `IS_INTERNAL`, 
    `ISIN`, 
    `SOURCE_TRADE_ID`, 
    `CURVE_DESCRIPTOR`, 
    `CURVE_TYPE`, 
    `Sub Type`, 
    `Curve Family`, 
    `Portfolio`, 
    `COLLATERAL_TYPE`, 
    `Region`)
  
  FROM Cleanse_135 AS in0

),

Filter_4 AS (

  SELECT * 
  
  FROM AlteryxSelect_2 AS in0
  
  WHERE (
          (
            NOT(
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (`Source System` IN ('AthenaFX', 'Orion', 'Athena-TRS', 'Athena-Fut', 'Athena-Sec'))
                                    OR (CURVE_TYPE = 'Base')
                                  )
                                  OR (CURVE_TYPE = 'Basis')
                                )
                                OR (CURVE_TYPE = 'Funding')
                              )
                              OR (CURVE_TYPE = 'IndexBasis')
                            )
                            OR (CURVE_TYPE = 'TenorBasis')
                          )
                          OR (`Risk Type` = 'Tenor Basis')
                        )
                        OR (`Risk Type` = 'Credit Delta')
                      )
                      OR (`Risk Type` = 'FX Delta')
                    )
                    OR (`Risk Type` = 'CSA')
                  )
                  OR (`Risk Type` = 'Ccy Basis')
                )
                OR (`Risk Type` = 'Libor-ois')
              )
              OR (`Risk Type` = 'IR Vega'))
          )
          OR (
               (
                 (
                   (
                     (
                       (
                         (
                           (
                             (
                               (
                                 (
                                   (
                                     (
                                       (`Source System` IN ('AthenaFX', 'Orion', 'Athena-TRS', 'Athena-Fut', 'Athena-Sec'))
                                       OR (CURVE_TYPE = 'Base')
                                     )
                                     OR (CURVE_TYPE = 'Basis')
                                   )
                                   OR (CURVE_TYPE = 'Funding')
                                 )
                                 OR (CURVE_TYPE = 'IndexBasis')
                               )
                               OR (CURVE_TYPE = 'TenorBasis')
                             )
                             OR (`Risk Type` = 'Tenor Basis')
                           )
                           OR (`Risk Type` = 'Credit Delta')
                         )
                         OR (`Risk Type` = 'FX Delta')
                       )
                       OR (`Risk Type` = 'CSA')
                     )
                     OR (`Risk Type` = 'Ccy Basis')
                   )
                   OR (`Risk Type` = 'Libor-ois')
                 )
                 OR (`Risk Type` = 'IR Vega')
               ) IS NULL
             )
        )

),

Formula_8_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Region = 'GEM')
          THEN CURVE_DESCRIPTOR
        WHEN (`Source System` = 'Kapital')
          THEN `Curve Family`
        ELSE CURVE_DESCRIPTOR
      END
    ) AS string) AS Curve,
    *
  
  FROM Filter_4 AS in0

)

SELECT *

FROM Formula_8_0
