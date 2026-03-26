{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Sample_73 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Sample_73')}}

),

RegEx_42 AS (

  {{
    prophecy_basics.Regex(
      ['Sample_73'], 
      [], 
      '[{"name": "ATTRIBUTETEMPORARY", "dataType": "Boolean"}, {"name": "ATTRIBUTEENCRYPTED", "dataType": "Boolean"}, {"name": "ATTRIBUTESYSTEM", "dataType": "Boolean"}, {"name": "FILENAME", "dataType": "String"}, {"name": "ATTRIBUTECOMPRESSED", "dataType": "Boolean"}, {"name": "CC_CHECK", "dataType": "String"}, {"name": "SIZE", "dataType": "Integer"}, {"name": "ATTRIBUTEARCHIVE", "dataType": "Boolean"}, {"name": "LASTACCESSTIME", "dataType": "Timestamp"}, {"name": "DIRECTORY", "dataType": "String"}, {"name": "ATTRIBUTEREADONLY", "dataType": "Boolean"}, {"name": "CREATIONTIME", "dataType": "Timestamp"}, {"name": "FULLPATH", "dataType": "String"}, {"name": "LASTWRITETIME", "dataType": "Timestamp"}, {"name": "SHORTFILENAME", "dataType": "String"}, {"name": "ATTRIBUTEHIDDEN", "dataType": "Boolean"}]', 
      'FILENAME', 
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
    FILENAME_REPLACED AS FILENAME,
    * EXCLUDE ("FILENAME_REPLACED", "FILENAME")
  
  FROM RegEx_42 AS in0

),

DynamicInput_40 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_', 'DynamicInput_40') }}

),

Join_41_inner AS (

  SELECT 
    in1.LASTWRITETIME AS FILELASTWRITETIME,
    in0.*,
    in1.* EXCLUDE ("FULLPATH", 
    "DIRECTORY", 
    "FILENAME", 
    "SHORTFILENAME", 
    "CREATIONTIME", 
    "LASTACCESSTIME", 
    "LASTWRITETIME", 
    "SIZE", 
    "ATTRIBUTEARCHIVE", 
    "ATTRIBUTECOMPRESSED", 
    "ATTRIBUTEENCRYPTED", 
    "ATTRIBUTEHIDDEN", 
    "ATTRIBUTEREADONLY", 
    "ATTRIBUTESYSTEM", 
    "ATTRIBUTETEMPORARY")
  
  FROM DynamicInput_40 AS in0
  INNER JOIN RegEx_42_rename_0 AS in1
     ON (in0.FILENAME = in1.FILENAME)

),

Filter_22 AS (

  SELECT * 
  
  FROM Join_41_inner AS in0
  
  WHERE not(
          length(CAST("FISCAL YEAR" AS string)) = 0)

),

RegEx_28 AS (

  {{
    prophecy_basics.Regex(
      ['Filter_22'], 
      [
        { 'columnName': 'PERIOD', 'dataType': 'String', 'rgxExpression': '^\\d+-(\\d+)-.+EXP_(\\w{1}\\d{1})_' }, 
        { 'columnName': 'LEDGER', 'dataType': 'String', 'rgxExpression': '^\\d+-(\\d+)-.+EXP_(\\w{1}\\d{1})_' }
      ], 
      '[{"name": "FILENAME", "dataType": "String"}, {"name": "CC_CHECK", "dataType": "String"}, {"name": "FILELASTWRITETIME", "dataType": "Timestamp"}]', 
      'FILENAME', 
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
      "(REGEXP_REPLACE(COLUMN_VALUE, ',', ''))", 
      ['FILENAME', 'LEDGER', 'CC_CHECK', 'PERIOD', 'FILELASTWRITETIME'], 
      ['LEDGER', 'PERIOD', 'FILENAME', 'CC_CHECK'], 
      false, 
      'Suffix', 
      ''
    )
  }}

),

Formula_23_0 AS (

  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS VARIABLETIMESTAMP,
    CAST((GETENVIRONMENTVARIABLE('USERNAME')) AS VARstring) AS USER,
    'SAP' AS SOURCE,
    *
  
  FROM MultiFieldFormula_24 AS in0

),

DynamicRename_26_before AS (

  SELECT 
    CAST(NULL AS VARstring) AS SEND__BUSAREA,
    CAST(NULL AS VARstring) AS BUS__TRANSACTION,
    CAST(NULL AS VARstring) AS "REF__ PROCEDURE",
    CAST(NULL AS VARstring) AS "DISTR__ CH__",
    CAST(NULL AS VARstring) AS "CORE DIM__ 2",
    CAST(NULL AS VARstring) AS "NAT__ OF BUSINESS",
    CAST(NULL AS VARstring) AS "ACC__SLASHMAT__ YEAR",
    CAST(NULL AS VARstring) AS "CORE DIM__ 1",
    CAST(NULL AS VARstring) AS "UW__ YEAR",
    CAST(NULL AS VARstring) AS "AMT TRANSACTN CURR__",
    CAST(NULL AS VARstring) AS "AMT CUM__ TCURR",
    CAST(NULL AS VARstring) AS "AMT CO__CD__ CURRENCY",
    CAST(NULL AS VARstring) AS "AMT CUM__LC__CUR",
    CAST(NULL AS VARstring) AS "AMT GLOB__COMP__CURR__",
    *
  
  FROM Formula_23_0 AS in0

),

DynamicRename_26 AS (

  {{
    prophecy_basics.MultiColumnRename(
      ['DynamicRename_26_before'], 
      [
        'SEND__BUSAREA', 
        'CORE DIM__ 2', 
        'FILELASTWRITETIME', 
        'DISTR__ CH__', 
        'SOURCE', 
        'LEDGER', 
        'AMT CO__CD__ CURRENCY', 
        'ACC__SLASHMAT__ YEAR', 
        'CORE DIM__ 1', 
        'AMT TRANSACTN CURR__', 
        'USER', 
        'PERIOD', 
        'FILENAME', 
        'BUS__TRANSACTION', 
        'CC_CHECK', 
        'UW__ YEAR', 
        'VARIABLETIMESTAMP', 
        'AMT CUM__LC__CUR', 
        'AMT CUM__ TCURR', 
        'AMT GLOB__COMP__CURR__', 
        'REF__ PROCEDURE', 
        'NAT__ OF BUSINESS'
      ], 
      'advancedRename', 
      [
        'UW__ YEAR', 
        'AMT CO__CD__ CURRENCY', 
        'CORE DIM__ 2', 
        'FILENAME', 
        'SOURCE', 
        'LEDGER', 
        'AMT CUM__LC__CUR', 
        'CC_CHECK', 
        'AMT TRANSACTN CURR__', 
        'AMT GLOB__COMP__CURR__', 
        'USER', 
        'VARIABLETIMESTAMP', 
        'SEND__BUSAREA', 
        'ACC__SLASHMAT__ YEAR', 
        'PERIOD', 
        'CORE DIM__ 1', 
        'DISTR__ CH__', 
        'REF__ PROCEDURE', 
        'NAT__ OF BUSINESS', 
        'AMT CUM__ TCURR', 
        'FILELASTWRITETIME', 
        'BUS__TRANSACTION'
      ], 
      'Suffix', 
      '', 
      "upper(regexp_replace(column_name, '(?i)[^\\w\\d]', '_'))"
    )
  }}

),

DynamicRename_26_after AS (

  SELECT 
    SEND__BUSAREA AS SEND__BUSAREA,
    BUS__TRANSACTION AS BUS__TRANSACTION,
    CAST(NULL AS VARstring) AS REF___PROCEDURE,
    CAST(NULL AS VARstring) AS DISTR___CH__,
    CAST(NULL AS VARstring) AS CORE_DIM___2,
    CAST(NULL AS VARstring) AS NAT___OF_BUSINESS,
    CAST(NULL AS VARstring) AS ACC__MAT___YEAR,
    CAST(NULL AS VARstring) AS CORE_DIM___1,
    CAST(NULL AS VARstring) AS UW___YEAR,
    CAST(NULL AS VARstring) AS AMT_TRANSACTN_CURR__,
    CAST(NULL AS VARstring) AS AMT_CUM___TCURR,
    CAST(NULL AS VARstring) AS AMT_CO__CD___CURRENCY,
    CAST(NULL AS VARstring) AS AMT_CUM__LC__CUR,
    CAST(NULL AS VARstring) AS AMT_GLOB__COMP__CURR__,
    * EXCLUDE ("SEND__BUSAREA", "BUS__TRANSACTION")
  
  FROM DynamicRename_26 AS in0

),

AlteryxSelect_25 AS (

  SELECT 
    VARIABLETIMESTAMP AS VARIABLETIMESTAMP,
    USER AS USER,
    SOURCE AS SOURCE,
    FILENAME AS FILENAME,
    PERIOD AS PERIOD,
    LEDGER AS LEDGER,
    FILELASTWRITETIME AS FILELASTWRITETIME,
    CC_CHECK AS CC_CHECK,
    CAST(NULL AS VARstring) AS REPORTING_DATE,
    CAST(NULL AS VARstring) AS FISCAL_YEAR,
    CAST(NULL AS VARstring) AS ACCOUNT_NUMBER,
    CAST(NULL AS VARstring) AS COMPANY_CODE,
    CAST(NULL AS VARstring) AS FUNCTIONAL_AREA,
    CAST(NULL AS VARstring) AS SEGMENT,
    CAST(NULL AS VARstring) AS NAT__OF_BUSINESS,
    CAST(NULL AS VARstring) AS ACC__MAT__YEAR,
    CAST(NULL AS VARstring) AS TRADING_PARTNER,
    CAST(NULL AS VARstring) AS COUNTRY,
    CAST(NULL AS VARstring) AS ORIGINAL_CEDENT,
    CAST(NULL AS VARstring) AS PROFIT_CENTER,
    CAST(NULL AS VARstring) AS BUSINESS_AREA,
    CAST(NULL AS VARstring) AS CURRENCY_2,
    CAST(NULL AS VARstring) AS CURRENCY_3,
    CAST(NULL AS VARstring) AS RECORD_TYPE,
    CAST(NULL AS VARstring) AS VARIABLEVERSION,
    CAST(NULL AS VARstring) AS CO_AREA,
    CAST(NULL AS VARstring) AS COST_CENTER,
    CAST(NULL AS VARstring) AS PARTNER_SEGMENT,
    CAST(NULL AS VARstring) AS PARTNER_PC,
    CAST(NULL AS VARstring) AS SENDER_COST_CTR,
    CAST(NULL AS VARstring) AS SEND_BUSAREA,
    CAST(NULL AS VARstring) AS PARTNER_FAREA,
    CAST(NULL AS VARstring) AS BUS_TRANSACTION,
    CAST(NULL AS VARstring) AS TRANSACTN_TYPE,
    CAST(NULL AS VARstring) AS CURRENCY,
    CAST(NULL AS VARstring) AS BASE_UNIT,
    CAST(NULL AS VARstring) AS REF__PROCEDURE,
    CAST(NULL AS VARstring) AS LOGICAL_SYSTEM,
    CAST(NULL AS VARstring) AS COST_ELEMENT,
    CAST(NULL AS VARstring) AS DISTR__CH_,
    CAST(NULL AS VARstring) AS CORE_DIM__2,
    CAST(NULL AS VARstring) AS LOB,
    CAST(NULL AS VARstring) AS CORE_DIM__1,
    CAST(NULL AS VARstring) AS UW__YEAR,
    CAST(NULL AS VARstring) AS COUNTER_PARTY,
    CAST(NULL AS VARstring) AS LOCACAPPR,
    CAST(NULL AS VARstring) AS DEBIT_CREDIT,
    CAST(NULL AS VARstring) AS AMT_TRANSACTN_CURR_,
    CAST(NULL AS VARstring) AS AMT_CUM__TCURR,
    CAST(NULL AS VARstring) AS AMT_CO_CD__CURRENCY,
    CAST(NULL AS VARstring) AS AMT_CUM_LC_CUR,
    CAST(NULL AS VARstring) AS AMT_GLOB_COMP_CURR_,
    CAST(NULL AS VARstring) AS AMT_CUMGLCOCUR,
    CAST(NULL AS VARstring) AS QUANTITY,
    CAST(NULL AS VARstring) AS CUMULATIVE_QUANTITY,
    CAST(NULL AS VARstring) AS ACCOUNT,
    CAST(NULL AS VARstring) AS SCHEMES_AND_PROGRAMS,
    CAST(NULL AS VARstring) AS MAJOR_LOB,
    CAST(NULL AS VARstring) AS GROUP_LOB,
    CAST(NULL AS VARstring) AS SUMMARY_SEGMENT,
    CAST(NULL AS VARstring) AS SUB_SEGMENT,
    CAST(NULL AS VARstring) AS SCHEMES,
    CAST(NULL AS VARstring) AS STATE,
    CAST(NULL AS VARstring) AS SEG,
    CAST(NULL AS VARstring) AS VARIABLEDATE,
    CAST(NULL AS VARstring) AS "VALUE",
    CAST(NULL AS VARstring) AS RATE,
    CAST(NULL AS VARstring) AS COMPANY_CODE2,
    * EXCLUDE ("VARIABLETIMESTAMP", "USER", "SOURCE", "FILENAME", "PERIOD", "LEDGER", "FILELASTWRITETIME", "CC_CHECK")
  
  FROM DynamicRename_26_after AS in0

)

SELECT *

FROM AlteryxSelect_25
