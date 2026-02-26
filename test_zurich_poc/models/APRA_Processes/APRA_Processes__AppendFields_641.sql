{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Unique_489 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Unique_489')}}

),

Unique_495 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Unique_495')}}

),

Union_574 AS (

  {{
    prophecy_basics.UnionByName(
      ['Unique_489', 'Unique_495'], 
      [
        '[{"name": "v_Reinsurer", "dataType": "String"}, {"name": "Filename", "dataType": "String"}]', 
        '[{"name": "v_TZAC", "dataType": "String"}, {"name": "Filename", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_446_0 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_446_0')}}

),

Unique_634 AS (

  SELECT * 
  
  FROM Formula_446_0 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY Period, Source ORDER BY Period, Source) = 1

),

TextInput_635 AS (

  SELECT * 
  
  FROM {{ ref('seed_635')}}

),

TextInput_635_cast AS (

  SELECT CAST(Source AS STRING) AS Source
  
  FROM TextInput_635 AS in0

),

Join_637_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.Source = in1.Source)
          THEN in1.Source
        ELSE NULL
      END
    ) AS Right_Source,
    in0.*,
    in1.* EXCEPT (`Source`)
  
  FROM TextInput_635_cast AS in0
  LEFT JOIN Unique_634 AS in1
     ON (in0.Source = in1.Source)

),

Emailrecipients_518 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'Emailrecipients_518') }}

),

Filter_520 AS (

  SELECT * 
  
  FROM Emailrecipients_518 AS in0
  
  WHERE (ToslashCc = 'To')

),

Summarize_523 AS (

  SELECT (CONCAT_WS(';', (COLLECT_LIST(`Email address`)))) AS `Email address`
  
  FROM Filter_520 AS in0

),

Formula_636_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((Right_Source IS NULL) AS BOOLEAN)
          THEN 'No'
        ELSE 'Yes'
      END
    ) AS STRING) AS `Process CompletedquesMark`,
    CAST(CASE
      WHEN CAST(isnull(Period) AS BOOLEAN)
        THEN coalesce(
          CAST(date_format(date_add(current_timestamp(), CAST(-27 AS INT)), 'yyyyMM') AS DOUBLE), 
          CAST(regexp_extract(date_format(date_add(current_timestamp(), CAST(-27 AS INT)), 'yyyyMM'), '^[0-9]+', 0) AS INT), 
          0)
      ELSE Period
    END AS STRING) AS Period,
    * EXCEPT (`period`)
  
  FROM Join_637_left_UnionLeftOuter AS in0

),

PortfolioComposerTable_633 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

Summarize_640 AS (

  SELECT MAX(variableTimestamp) AS variableTimestamp
  
  FROM Formula_636_0 AS in0

),

AppendFields_639 AS (

  SELECT 
    in0.* EXCEPT (`variableTimestamp`),
    in1.*
  
  FROM PortfolioComposerTable_633 AS in0
  INNER JOIN Summarize_640 AS in1
     ON TRUE

),

PortfolioComposerText_632 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

AppendFields_519 AS (

  SELECT 
    in0.`Email address` AS To_Email_Address,
    in0.* EXCEPT (`Email address`),
    in1.*
  
  FROM Summarize_523 AS in0
  INNER JOIN PortfolioComposerText_632 AS in1
     ON TRUE

),

Mappings_xlsx_Q_578 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'Mappings_xlsx_Q_578') }}

),

Formula_532_0 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_532_0')}}

),

Sample_536 AS (

  {{ prophecy_basics.Sample('Formula_532_0', [], 1002, 'firstN', 1) }}

),

CountRecords_577 AS (

  SELECT COUNT('1') AS `Count`
  
  FROM Union_574 AS in0

),

Formula_593_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Count = 0)
          THEN 'No APRA Unmapped.txt'
        ELSE 'APRA Unmapped.xlsx'
      END
    ) AS STRING) AS Unmapped_Filename,
    *
  
  FROM CountRecords_577 AS in0

),

AppendFields_602 AS (

  SELECT 
    in1.Filename AS Filename,
    in0.Unmapped_Filename AS Unmapped_Filename
  
  FROM Formula_593_0 AS in0
  INNER JOIN Sample_536 AS in1
     ON TRUE

),

Filter_521 AS (

  SELECT * 
  
  FROM Emailrecipients_518 AS in0
  
  WHERE (ToslashCc = 'Cc')

),

Summarize_524 AS (

  SELECT (CONCAT_WS(';', (COLLECT_LIST(`Email address`)))) AS `Email address`
  
  FROM Filter_521 AS in0

),

AppendFields_522 AS (

  SELECT 
    in0.`Email address` AS Cc_Email_Address,
    in0.* EXCEPT (`Email address`),
    in1.*
  
  FROM Summarize_524 AS in0
  INNER JOIN AppendFields_519 AS in1
     ON TRUE

),

AppendFields_531 AS (

  SELECT 
    in1.variableTimestamp AS variableTimestamp,
    in0.Unmapped_Filename AS Unmapped_Filename,
    in1.Text AS Text,
    in1.Cc_Email_Address AS Cc_Email_Address,
    in1.To_Email_Address AS To_Email_Address,
    in0.Filename AS Filename,
    in1.Table AS Table
  
  FROM AppendFields_602 AS in0
  INNER JOIN AppendFields_522 AS in1
     ON TRUE

),

Sample_642 AS (

  {{ prophecy_basics.Sample('Mappings_xlsx_Q_578', [], 1002, 'firstN', 1) }}

),

RegEx_643 AS (

  {{
    prophecy_basics.Regex(
      ['Sample_642'], 
      [{ 'columnName': 'MappingFile', 'dataType': 'String', 'rgxExpression': '(\\\\\\\\[^|]+?\\.xlsx)' }], 
      '[{"name": "Sheet Names", "dataType": "String"}, {"name": "FileName", "dataType": "String"}]', 
      'FileName', 
      '(\\\\[^|]+?\.xlsx)', 
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

AppendFields_641 AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Sheet Names`, `FileName`)
  
  FROM AppendFields_531 AS in0
  INNER JOIN RegEx_643 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_641
