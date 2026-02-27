{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH DynamicInput_40 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_', 'DynamicInput_40') }}

),

Sample_73 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Sample_73')}}

),

RegEx_42 AS (

  {{
    prophecy_basics.Regex(
      ['Sample_73'], 
      [{ 'columnName': 'regex_col1', 'dataType': 'string', 'rgxExpression': '(.*$)' }], 
      '[{"name": "FullPath", "dataType": "String"}, {"name": "Directory", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ShortFileName", "dataType": "String"}, {"name": "CreationTime", "dataType": "Bigint"}, {"name": "LastAccessTime", "dataType": "Bigint"}, {"name": "LastWriteTime", "dataType": "Bigint"}, {"name": "Size", "dataType": "Bigint"}, {"name": "AttributeArchive", "dataType": "Boolean"}, {"name": "AttributeCompressed", "dataType": "Boolean"}, {"name": "AttributeEncrypted", "dataType": "Boolean"}, {"name": "AttributeHidden", "dataType": "Boolean"}, {"name": "AttributeReadOnly", "dataType": "Boolean"}, {"name": "AttributeSystem", "dataType": "Boolean"}, {"name": "AttributeTemporary", "dataType": "Boolean"}, {"name": "CC_Check", "dataType": "String"}]', 
      'FileName', 
      '\.(?!.*\_)(.*$)', 
      'replace', 
      true, 
      false, 
      '', 
      true, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      '', 
      false
    )
  }}

),

RegEx_42_rename_0 AS (

  SELECT 
    FileName_replaced AS FileName,
    * EXCEPT (`FileName_replaced`, `filename`)
  
  FROM RegEx_42 AS in0

),

Join_41_inner AS (

  SELECT 
    in1.LastWriteTime AS FileLastWriteTime,
    in0.*,
    in1.* EXCEPT (`FullPath`, 
    `Directory`, 
    `FileName`, 
    `ShortFileName`, 
    `CreationTime`, 
    `LastAccessTime`, 
    `LastWriteTime`, 
    `Size`, 
    `AttributeArchive`, 
    `AttributeCompressed`, 
    `AttributeEncrypted`, 
    `AttributeHidden`, 
    `AttributeReadonly`, 
    `AttributeSystem`, 
    `AttributeTemporary`)
  
  FROM DynamicInput_40 AS in0
  INNER JOIN RegEx_42_rename_0 AS in1
     ON (in0.FileName = in1.FileName)

),

Filter_22 AS (

  SELECT * 
  
  FROM Join_41_inner AS in0
  
  WHERE not((length(CAST(`Fiscal Year` AS STRING)) = 0))

),

RegEx_28 AS (

  {{
    prophecy_basics.Regex(
      ['Filter_22'], 
      [
        { 'columnName': 'PERIOD', 'dataType': 'String', 'rgxExpression': '^\\d+-(\\d+)-.+EXP_(\\w{1}\\d{1})_' }, 
        { 'columnName': 'LEDGER', 'dataType': 'String', 'rgxExpression': '^\\d+-(\\d+)-.+EXP_(\\w{1}\\d{1})_' }
      ], 
      '[{"name": "FileName", "dataType": "String"}, {"name": "CC_Check", "dataType": "String"}, {"name": "FileLastWriteTime", "dataType": "Timestamp"}]', 
      'FileName', 
      '^\d+-(\d+)-.+EXP_(\w{1}\d{1})_', 
      'parse', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      '', 
      false
    )
  }}

),

Formula_27 AS (

  SELECT *
  
  FROM RegEx_28 AS in0

),

MultiFieldFormula_24 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['Formula_27'], 
      "regexp_replace(column_value, ',', '')", 
      ['FileLastWriteTime', 'LEDGER', 'PERIOD', 'FileName', 'CC_Check'], 
      ['LEDGER', 'PERIOD', 'FileName', 'CC_Check'], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Formula_23_0 AS (

  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS variableTIMESTAMP,
    CAST((GETENVIRONMENTVARIABLE('USERNAME')) AS string) AS USER,
    CAST('SAP' AS string) AS SOURCE,
    *
  
  FROM MultiFieldFormula_24 AS in0

),

DynamicRename_26_before AS (

  SELECT 
    CAST(NULL AS string) AS Send__BusArea,
    CAST(NULL AS string) AS Bus__transaction,
    CAST(NULL AS string) AS `Ref__ procedure`,
    CAST(NULL AS string) AS `Distr__ Ch__`,
    CAST(NULL AS string) AS `Core Dim__ 2`,
    CAST(NULL AS string) AS `Nat__ of Business`,
    CAST(NULL AS string) AS `Acc__slashMat__ Year`,
    CAST(NULL AS string) AS `Core Dim__ 1`,
    CAST(NULL AS string) AS `Uw__ Year`,
    CAST(NULL AS string) AS `Amt Transactn curr__`,
    CAST(NULL AS string) AS `Amt Cum__ TCurr`,
    CAST(NULL AS string) AS `Amt Co__Cd__ currency`,
    CAST(NULL AS string) AS `Amt Cum__lc__cur`,
    CAST(NULL AS string) AS `Amt Glob__comp__curr__`,
    *
  
  FROM Formula_23_0 AS in0

),

DynamicRename_26 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_26_before'], 
      [
        'Send__BusArea', 
        'Core Dim__ 2', 
        'FileLastWriteTime', 
        'Distr__ Ch__', 
        'SOURCE', 
        'LEDGER', 
        'Amt Co__Cd__ currency', 
        'Acc__slashMat__ Year', 
        'Core Dim__ 1', 
        'Amt Transactn curr__', 
        'USER', 
        'PERIOD', 
        'FileName', 
        'Bus__transaction', 
        'CC_Check', 
        'Uw__ Year', 
        'variableTIMESTAMP', 
        'Amt Cum__lc__cur', 
        'Amt Cum__ TCurr', 
        'Amt Glob__comp__curr__', 
        'Ref__ procedure', 
        'Nat__ of Business'
      ], 
      'advancedRename', 
      [
        'Send__BusArea', 
        'Core Dim__ 2', 
        'variableTIMESTAMP', 
        'FileLastWriteTime', 
        'Distr__ Ch__', 
        'SOURCE', 
        'LEDGER', 
        'Amt Co__Cd__ currency', 
        'Core Dim__ 1', 
        'Amt Transactn curr__', 
        'USER', 
        'PERIOD', 
        'Acc__slashMat__ Year', 
        'FileName', 
        'Bus__transaction', 
        'CC_Check', 
        'Uw__ Year', 
        'Amt Cum__lc__cur', 
        'Amt Cum__ TCurr', 
        'Amt Glob__comp__curr__', 
        'Ref__ procedure', 
        'Nat__ of Business'
      ], 
      'Suffix', 
      '', 
      "upper(regexp_replace(column_name, '(?i)[^\\w\\d]', '_'))"
    )
  }}

),

DynamicRename_26_after AS (

  SELECT 
    SEND__BUSAREA AS Send__BusArea,
    BUS__TRANSACTION AS Bus__transaction,
    CAST(NULL AS string) AS Ref___procedure,
    CAST(NULL AS string) AS Distr___Ch__,
    CAST(NULL AS string) AS Core_Dim___2,
    CAST(NULL AS string) AS Nat___of_Business,
    CAST(NULL AS string) AS Acc__Mat___Year,
    CAST(NULL AS string) AS Core_Dim___1,
    CAST(NULL AS string) AS Uw___Year,
    CAST(NULL AS string) AS Amt_Transactn_curr__,
    CAST(NULL AS string) AS Amt_Cum___TCurr,
    CAST(NULL AS string) AS Amt_Co__Cd___currency,
    CAST(NULL AS string) AS Amt_Cum__lc__cur,
    CAST(NULL AS string) AS Amt_Glob__comp__curr__,
    * EXCEPT (`Send__BusArea`, `Bus__transaction`)
  
  FROM DynamicRename_26 AS in0

),

AlteryxSelect_25 AS (

  SELECT 
    variableTIMESTAMP AS variableTIMESTAMP,
    USER AS USER,
    SOURCE AS SOURCE,
    FILENAME AS FILENAME,
    PERIOD AS PERIOD,
    LEDGER AS LEDGER,
    FILELASTWRITETIME AS FILELASTWRITETIME,
    CC_CHECK AS CC_CHECK,
    CAST(NULL AS string) AS REPORTING_DATE,
    CAST(NULL AS string) AS FISCAL_YEAR,
    CAST(NULL AS string) AS ACCOUNT_NUMBER,
    CAST(NULL AS string) AS COMPANY_CODE,
    CAST(NULL AS string) AS FUNCTIONAL_AREA,
    CAST(NULL AS string) AS SEGMENT,
    CAST(NULL AS string) AS NAT__OF_BUSINESS,
    CAST(NULL AS string) AS ACC__MAT__YEAR,
    CAST(NULL AS string) AS TRADING_PARTNER,
    CAST(NULL AS string) AS COUNTRY,
    CAST(NULL AS string) AS ORIGINAL_CEDENT,
    CAST(NULL AS string) AS PROFIT_CENTER,
    CAST(NULL AS string) AS BUSINESS_AREA,
    CAST(NULL AS string) AS CURRENCY_2,
    CAST(NULL AS string) AS CURRENCY_3,
    CAST(NULL AS string) AS RECORD_TYPE,
    CAST(NULL AS string) AS variableVERSION,
    CAST(NULL AS string) AS CO_AREA,
    CAST(NULL AS string) AS COST_CENTER,
    CAST(NULL AS string) AS PARTNER_SEGMENT,
    CAST(NULL AS string) AS PARTNER_PC,
    CAST(NULL AS string) AS SENDER_COST_CTR,
    CAST(NULL AS string) AS SEND_BUSAREA,
    CAST(NULL AS string) AS PARTNER_FAREA,
    CAST(NULL AS string) AS BUS_TRANSACTION,
    CAST(NULL AS string) AS TRANSACTN_TYPE,
    CAST(NULL AS string) AS CURRENCY,
    CAST(NULL AS string) AS BASE_UNIT,
    CAST(NULL AS string) AS REF__PROCEDURE,
    CAST(NULL AS string) AS LOGICAL_SYSTEM,
    CAST(NULL AS string) AS COST_ELEMENT,
    CAST(NULL AS string) AS DISTR__CH_,
    CAST(NULL AS string) AS CORE_DIM__2,
    CAST(NULL AS string) AS LOB,
    CAST(NULL AS string) AS CORE_DIM__1,
    CAST(NULL AS string) AS UW__YEAR,
    CAST(NULL AS string) AS COUNTER_PARTY,
    CAST(NULL AS string) AS LOCACAPPR,
    CAST(NULL AS string) AS DEBIT_CREDIT,
    CAST(NULL AS string) AS AMT_TRANSACTN_CURR_,
    CAST(NULL AS string) AS AMT_CUM__TCURR,
    CAST(NULL AS string) AS AMT_CO_CD__CURRENCY,
    CAST(NULL AS string) AS AMT_CUM_LC_CUR,
    CAST(NULL AS string) AS AMT_GLOB_COMP_CURR_,
    CAST(NULL AS string) AS AMT_CUMGLCOCUR,
    CAST(NULL AS string) AS QUANTITY,
    CAST(NULL AS string) AS CUMULATIVE_QUANTITY,
    CAST(NULL AS string) AS ACCOUNT,
    CAST(NULL AS string) AS SCHEMES_AND_PROGRAMS,
    CAST(NULL AS string) AS MAJOR_LOB,
    CAST(NULL AS string) AS GROUP_LOB,
    CAST(NULL AS string) AS SUMMARY_SEGMENT,
    CAST(NULL AS string) AS SUB_SEGMENT,
    CAST(NULL AS string) AS SCHEMES,
    CAST(NULL AS string) AS STATE,
    CAST(NULL AS string) AS SEG,
    CAST(NULL AS string) AS variableDATE,
    CAST(NULL AS string) AS `VALUE`,
    CAST(NULL AS string) AS RATE,
    CAST(NULL AS string) AS COMPANY_CODE2,
    * EXCEPT (`variableTIMESTAMP`, `USER`, `SOURCE`, `FILENAME`, `PERIOD`, `LEDGER`, `FILELASTWRITETIME`, `CC_CHECK`)
  
  FROM DynamicRename_26_after AS in0

)

SELECT *

FROM AlteryxSelect_25
