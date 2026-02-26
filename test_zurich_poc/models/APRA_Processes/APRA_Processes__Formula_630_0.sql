{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Directory_604 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'Directory_604') }}

),

Directory_604_reformat AS (

  SELECT 
    path AS FullPath,
    parent_directory AS Directory,
    name AS FileName,
    name AS ShortFileName,
    creation_time AS CreationTime,
    modification_time AS LastAccessTime,
    modification_time AS LastWriteTime,
    size_in_bytes AS Size,
    CAST(NULL AS BOOLEAN) AS AttributeArchive,
    CAST(NULL AS BOOLEAN) AS AttributeCompressed,
    CAST(NULL AS BOOLEAN) AS AttributeEncrypted,
    CAST(NULL AS BOOLEAN) AS AttributeHidden,
    CAST(NULL AS BOOLEAN) AS AttributeReadOnly,
    CAST(NULL AS BOOLEAN) AS AttributeSystem,
    CAST(NULL AS BOOLEAN) AS AttributeTemporary
  
  FROM Directory_604 AS in0

),

RegEx_605 AS (

  {{
    prophecy_basics.Regex(
      ['Directory_604_reformat'], 
      [{ 'columnName': 'regex_col1', 'dataType': 'string', 'rgxExpression': '(\\d*)' }], 
      '[{"name": "FullPath", "dataType": "String"}, {"name": "Directory", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ShortFileName", "dataType": "String"}, {"name": "CreationTime", "dataType": "Bigint"}, {"name": "LastAccessTime", "dataType": "Bigint"}, {"name": "LastWriteTime", "dataType": "Bigint"}, {"name": "Size", "dataType": "Bigint"}, {"name": "AttributeArchive", "dataType": "Boolean"}, {"name": "AttributeCompressed", "dataType": "Boolean"}, {"name": "AttributeEncrypted", "dataType": "Boolean"}, {"name": "AttributeHidden", "dataType": "Boolean"}, {"name": "AttributeReadOnly", "dataType": "Boolean"}, {"name": "AttributeSystem", "dataType": "Boolean"}, {"name": "AttributeTemporary", "dataType": "Boolean"}]', 
      'FileName', 
      '(?:S2TP|S2NZTP)_(\d*).*.TXT', 
      'match', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      'FullPath_Matched', 
      false
    )
  }}

),

RegEx_605_typeCastGem AS (

  SELECT 
    CAST(FullPath_Matched AS BOOLEAN) AS FullPath_Matched,
    * EXCEPT (`FullPath_Matched`)
  
  FROM RegEx_605 AS in0

),

Filter_606 AS (

  SELECT * 
  
  FROM RegEx_605_typeCastGem AS in0
  
  WHERE CAST(FullPath_Matched AS BOOLEAN)

),

RegEx_607 AS (

  {{
    prophecy_basics.Regex(
      ['Filter_606'], 
      [{ 'columnName': 'Month_End_Date', 'dataType': 'String', 'rgxExpression': '(\\d*)' }], 
      '[{"name": "FullPath_Matched", "dataType": "Boolean"}, {"name": "FullPath", "dataType": "String"}, {"name": "Directory", "dataType": "String"}, {"name": "FileName", "dataType": "String"}, {"name": "ShortFileName", "dataType": "String"}, {"name": "CreationTime", "dataType": "Bigint"}, {"name": "LastAccessTime", "dataType": "Bigint"}, {"name": "LastWriteTime", "dataType": "Bigint"}, {"name": "Size", "dataType": "Bigint"}, {"name": "AttributeArchive", "dataType": "Boolean"}, {"name": "AttributeCompressed", "dataType": "Boolean"}, {"name": "AttributeEncrypted", "dataType": "Boolean"}, {"name": "AttributeHidden", "dataType": "Boolean"}, {"name": "AttributeReadOnly", "dataType": "Boolean"}, {"name": "AttributeSystem", "dataType": "Boolean"}, {"name": "AttributeTemporary", "dataType": "Boolean"}]', 
      'FileName', 
      '(?:S2TP|S2NZTP)_(\d*).*.TXT', 
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

Formula_608_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((DAY((TO_TIMESTAMP((REGEXP_REPLACE(Month_End_Date, '\\.\\d+', '')), 'ddMMyyyy')))) <= 3)
          THEN (
            TO_TIMESTAMP(
              (
                TO_TIMESTAMP(
                  (REGEXP_REPLACE((CONCAT('01', (SUBSTRING(Month_End_Date, (((LENGTH(Month_End_Date)) - 6) + 1), 6)))), '\\.\\d+', '')), 
                  'ddMMyyyy')
              ))
          )
        WHEN ((DAY((TO_TIMESTAMP((REGEXP_REPLACE(Month_End_Date, '\\.\\d+', '')), 'ddMMyyyy')))) >= 27)
          THEN (
            TO_TIMESTAMP(
              (
                TO_TIMESTAMP(
                  (
                    REGEXP_REPLACE(
                      (
                        CONCAT(
                          '01', 
                          (
                            DATE_FORMAT(
                              (ADD_MONTHS((TO_TIMESTAMP((REGEXP_REPLACE(Month_End_Date, '\\.\\d+', '')), 'ddMMyyyy')), 1)), 
                              'MMyyyy')
                          ))
                      ), 
                      '\\.\\d+', 
                      '')
                  ), 
                  'ddMMyyyy')
              ))
          )
        ELSE (TO_TIMESTAMP((REGEXP_REPLACE(Month_End_Date, '\\.\\d+', '')), 'ddMMyyyy'))
      END
    ) AS VARCHAR (1000)) AS Month_End_Date,
    * EXCEPT (`month_end_date`)
  
  FROM RegEx_607 AS in0

),

AlteryxSelect_609 AS (

  SELECT 
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(Month_End_Date AS STRING), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Month_End_Date AS STRING), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE)
        WHEN ((TRY_TO_TIMESTAMP(CAST(Month_End_Date AS STRING), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN CAST((TRY_TO_TIMESTAMP(CAST(Month_End_Date AS STRING), 'yyyy-MM-dd HH:mm:ss')) AS DATE)
        ELSE CAST((TRY_TO_TIMESTAMP(CAST(Month_End_Date AS STRING), 'yyyy-MM-dd')) AS DATE)
      END
    ) AS Month_End_Date,
    * EXCEPT (`Month_End_Date`)
  
  FROM Formula_608_0 AS in0

),

Sample_611 AS (

  {{ prophecy_basics.Sample('AlteryxSelect_609', [], 1002, 'firstN', 2) }}

),

MultiRowFormula_612_row_id_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_row_id,
    *
  
  FROM Sample_611 AS in0

),

MultiRowFormula_612_0 AS (

  SELECT 
    (LAG(Month_End_Date, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id NULLS FIRST)) AS Month_End_Date_lag1,
    (LEAD(Month_End_Date, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id NULLS FIRST)) AS Month_End_Date_lead1,
    *
  
  FROM MultiRowFormula_612_row_id_0 AS in0

),

MultiRowFormula_612_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Month_End_Date_lag1 = CAST(NULL AS STRING))
          THEN (CAST(Month_End_Date_lead1 AS DATE) = Month_End_Date)
        ELSE (CAST(Month_End_Date_lag1 AS DATE) = Month_End_Date)
      END
    ) AS BOOLEAN) AS S2_Date_Check,
    * EXCEPT (`Month_End_Date_lag1`, `Month_End_Date_lead1`)
  
  FROM MultiRowFormula_612_0 AS in0

),

MultiRowFormula_612_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_612_1 AS in0

),

Formula_630_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (
            coalesce(
              CAST((DATE_FORMAT((ADD_MONTHS(Month_End_Date, -1)), 'yyyyMM')) AS DOUBLE), 
              CAST((REGEXP_EXTRACT((DATE_FORMAT((ADD_MONTHS(Month_End_Date, -1)), 'yyyyMM')), '^[0-9]+', 0)) AS INTEGER), 
              0)
          ) = (
            coalesce(
              CAST((DATE_FORMAT((DATE_ADD(CURRENT_TIMESTAMP, CAST(-27 AS INTEGER))), 'yyyyMM')) AS DOUBLE), 
              CAST((REGEXP_EXTRACT((DATE_FORMAT((DATE_ADD(CURRENT_TIMESTAMP, CAST(-27 AS INTEGER))), 'yyyyMM')), '^[0-9]+', 0)) AS INTEGER), 
              0)
          )
        )
          THEN FullPath
        ELSE '\\ms.zurich.com.au\\DFS\\AlteryX\\Prod\\Finance\\Reinsurance Processes\\MHC and APRA Reporting\\04_Inputs\\S2TP DUMMY.TXT'
      END
    ) AS VARCHAR (1000)) AS FullPath,
    * EXCEPT (`fullpath`)
  
  FROM MultiRowFormula_612_row_id_drop_0 AS in0

)

SELECT *

FROM Formula_630_0
