{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "QA_TEST_RANDOM"
  })
}}

WITH TextInput_108 AS (

  SELECT * 
  
  FROM {{ ref('seed_alteryx_all_test_stage2_sql_108')}}

),

TextInput_108_cast AS (

  SELECT 
    CUSTOMERID AS CUSTOMERID,
    CAST(CITY AS STRING) AS CITY,
    VISITS AS VISITS,
    CAST(SPEND AS FLOAT) AS SPEND,
    CAST(LATITUDE AS FLOAT) AS LATITUDE
  
  FROM TextInput_108 AS in0

),

Formula_105_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (((SPEND / VISITS) < 0) AND (((SPEND / VISITS) - FLOOR(((SPEND / VISITS) / 1))) = 0.5))
          THEN CEIL(((SPEND / VISITS) / 1))
        ELSE ROUND((SPEND / VISITS))
      END
    ) AS FLOAT) AS AVERAGESPENDPERVISIT,
    *
  
  FROM TextInput_108_cast AS in0

),

TextInput_260 AS (

  SELECT * 
  
  FROM {{ ref('seed_alteryx_all_test_stage2_sql_260')}}

),

TextInput_260_cast AS (

  SELECT 
    (TRY_TO_TIMESTAMP(CAST(JOINDATE AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS JOINDATE,
    (TRY_TO_TIMESTAMP(CAST(FIRSTPURCHASEDATE AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS FIRSTPURCHASEDATE
  
  FROM TextInput_260 AS in0

),

JoinMultiple_78_in1 AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_260_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_1', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

TextInput_75 AS (

  SELECT * 
  
  FROM {{ ref('seed_alteryx_all_test_stage2_sql_75')}}

),

TextInput_75_cast AS (

  SELECT 
    CUSTOMERID AS CUSTOMERID,
    CAST(FIRSTNAME AS STRING) AS FIRSTNAME,
    CAST(LASTNAME AS STRING) AS LASTNAME,
    CAST(GENDER AS STRING) AS GENDER,
    CAST("REGION" AS STRING) AS "REGION",
    CAST(SCORE AS STRING) AS SCORE
  
  FROM TextInput_75 AS in0

),

JoinMultiple_78_in0 AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_75_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN_0', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

JoinMultiple_78 AS (

  SELECT 
    in0.CUSTOMERID AS CUSTOMERID,
    in0.FIRSTNAME AS FIRSTNAME,
    in0.LASTNAME AS LASTNAME,
    in0.GENDER AS GENDER,
    in0."REGION" AS "REGION",
    in0.SCORE AS SCORE,
    in1.JOINDATE AS JOINDATE,
    in1.FIRSTPURCHASEDATE AS FIRSTPURCHASEDATE
  
  FROM JoinMultiple_78_in0 AS in0
  FULL JOIN JoinMultiple_78_in1 AS in1
     ON (in0.RECORDPOSITIONFORJOIN_0 = in1.RECORDPOSITIONFORJOIN_1)

),

Filter_67 AS (

  SELECT * 
  
  FROM JoinMultiple_78 AS in0
  
  WHERE (NOT(LASTNAME IS NULL))

),

Formula_102_0 AS (

  SELECT 
    CAST(INITCAP(CITY) AS STRING) AS CITY_TITLECASE,
    *
  
  FROM TextInput_108_cast AS in0

),

TextInput_142 AS (

  SELECT * 
  
  FROM {{ ref('seed_alteryx_all_test_stage2_sql_142')}}

),

TextInput_13 AS (

  SELECT * 
  
  FROM {{ ref('seed_alteryx_all_test_stage2_sql_13')}}

),

TextInput_13_cast AS (

  SELECT 
    CAST(ADDRESS AS STRING) AS ADDRESS,
    CAST(CITY AS STRING) AS CITY,
    CAST(STATE AS STRING) AS STATE,
    ZIP AS ZIP,
    CAST(COMPANY AS STRING) AS COMPANY,
    "AVERAGE SALE" AS "AVERAGE SALE"
  
  FROM TextInput_13 AS in0

),

SelectRecords_3_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_13_cast'], 
      'incremental_id', 
      'ROW_NUMBER', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

TextInput_123 AS (

  SELECT * 
  
  FROM {{ ref('seed_alteryx_all_test_stage2_sql_123')}}

),

TextInput_123_cast AS (

  SELECT 
    CUSTOMERID AS CUSTOMERID,
    CAST("NAME" AS STRING) AS "NAME",
    CAST(ADDRESS AS STRING) AS ADDRESS
  
  FROM TextInput_123 AS in0

),

RegEx_117 AS (

  {{
    prophecy_basics.Regex(
      ['TextInput_123_cast'], 
      [
        { 'columnName': 'FIRST NAME', 'dataType': 'String', 'rgxExpression': '([a-z]+)' }, 
        { 'columnName': 'LAST NAME', 'dataType': 'String', 'rgxExpression': '([a-z]+)' }
      ], 
      '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "ADDRESS", "dataType": "String"}]', 
      'NAME', 
      '([a-z]+)\s([a-z]+)', 
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
      false, 
      '_REPLACED'
    )
  }}

),

RegEx_109 AS (

  {{
    prophecy_basics.Regex(
      ['TextInput_123_cast'], 
      [
        { 'columnName': 'regex_col1', 'dataType': 'string', 'rgxExpression': '(.*)' }, 
        { 'columnName': 'regex_col2', 'dataType': 'string', 'rgxExpression': '(.*)' }, 
        { 'columnName': 'regex_col3', 'dataType': 'string', 'rgxExpression': '(.*)' }, 
        { 'columnName': 'regex_col4', 'dataType': 'int', 'rgxExpression': '(\\d{5})' }, 
        { 'columnName': 'regex_col5', 'dataType': 'int', 'rgxExpression': '(-\\d{4})' }
      ], 
      '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "ADDRESS", "dataType": "String"}]', 
      'ADDRESS', 
      '(.*),(.*),(.*)\s(\d{5})(-\d{4})?', 
      'replace', 
      true, 
      false, 
      '$4: $2, $3', 
      true, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      '', 
      false, 
      '_REPLACED'
    )
  }}

),

RegEx_109_rename_0 AS (

  SELECT 
    ADDRESS_REPLACED AS ADDRESS,
    * EXCLUDE ("ADDRESS_REPLACED", "ADDRESS")
  
  FROM RegEx_109 AS in0

),

RegEx_116 AS (

  {{
    prophecy_basics.Regex(
      ['TextInput_123_cast'], 
      [], 
      '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "ADDRESS", "dataType": "String"}]', 
      'ADDRESS', 
      '.*-\d{4}', 
      'match', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      'HAS_ZIP4', 
      false, 
      '_REPLACED'
    )
  }}

),

RegEx_116_typeCastGem AS (

  SELECT 
    HAS_ZIP4 AS HAS_ZIP4,
    * EXCLUDE ("HAS_ZIP4")
  
  FROM RegEx_116 AS in0

),

RegEx_114 AS (

  {{
    prophecy_basics.Regex(
      ['TextInput_123_cast'], 
      [{ 'columnName': 'ZIP4', 'dataType': 'String', 'rgxExpression': '(\\d{4})' }], 
      '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "ADDRESS", "dataType": "String"}]', 
      'ADDRESS', 
      '-(\d{4})', 
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
      false, 
      '_REPLACED'
    )
  }}

),

Union_221 AS (

  {{
    prophecy_basics.UnionByName(
      ['RegEx_114', 'RegEx_117', 'RegEx_109_rename_0', 'RegEx_116_typeCastGem'], 
      [
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "ADDRESS", "dataType": "String"}, {"name": "ZIP4", "dataType": "String"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "ADDRESS", "dataType": "String"}, {"name": "FIRST NAME", "dataType": "String"}, {"name": "LAST NAME", "dataType": "String"}]', 
        '[{"name": "ADDRESS", "dataType": "String"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}]', 
        '[{"name": "HAS_ZIP4", "dataType": "Number"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "ADDRESS", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_222 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_221'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_223 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_222'], 
      ['RECORDID'], 
      ['CUSTOMERID', 'NAME', 'ADDRESS', 'HAS_ZIP4', 'FIRST NAME', 'LAST NAME', 'ZIP4'], 
      'NAME', 
      'VALUE', 
      ['RECORDID', 'CUSTOMERID', 'NAME', 'ADDRESS', 'ZIP4', 'FIRST NAME', 'LAST NAME', 'HAS_ZIP4'], 
      true
    )
  }}

),

Filter_66 AS (

  SELECT * 
  
  FROM JoinMultiple_78 AS in0
  
  WHERE (JOINDATE >= FIRSTPURCHASEDATE)

),

Formula_104_0 AS (

  SELECT 
    CAST('Alteryx Sample Data' AS STRING) AS RECORDSOURCE,
    (
      CASE
        WHEN (LATITUDE > 39.7)
          THEN 'North'
        ELSE 'South'
      END
    ) AS "REGION",
    CAST(INITCAP(CITY) AS STRING) AS CITY,
    * EXCLUDE ("CITY")
  
  FROM TextInput_108_cast AS in0

),

Formula_101_0 AS (

  SELECT 
    (
      CASE
        WHEN (LATITUDE > 39.7)
          THEN 'North'
        ELSE 'South'
      END
    ) AS "REGION",
    *
  
  FROM TextInput_108_cast AS in0

),

Formula_106_0 AS (

  SELECT 
    CAST((SPEND / VISITS) AS FLOAT) AS AVERAGESPENDPERVISIT,
    *
  
  FROM TextInput_108_cast AS in0

),

Formula_106_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((AVERAGESPENDPERVISIT < 0) AND ((AVERAGESPENDPERVISIT - FLOOR((AVERAGESPENDPERVISIT / 1))) = 0.5))
          THEN CEIL((AVERAGESPENDPERVISIT / 1))
        ELSE ROUND(AVERAGESPENDPERVISIT)
      END
    ) AS FLOAT) AS AVERAGESPENDPERVISIT,
    * EXCLUDE ("AVERAGESPENDPERVISIT")
  
  FROM Formula_106_0 AS in0

),

Formula_103_0 AS (

  SELECT 
    CAST(INITCAP(CITY) AS STRING) AS CITY,
    * EXCLUDE ("CITY")
  
  FROM TextInput_108_cast AS in0

),

Formula_100_0 AS (

  SELECT 
    CAST('Alteryx Sample Data' AS STRING) AS RECORDSOURCE,
    *
  
  FROM TextInput_108_cast AS in0

),

Union_216 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Formula_101_0', 
        'Formula_104_0', 
        'Formula_106_1', 
        'Formula_102_0', 
        'Formula_103_0', 
        'Formula_100_0', 
        'Formula_105_0'
      ], 
      [
        '[{"name": "REGION", "dataType": "String"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "CITY", "dataType": "String"}, {"name": "VISITS", "dataType": "Number"}, {"name": "SPEND", "dataType": "Float"}, {"name": "LATITUDE", "dataType": "Float"}]', 
        '[{"name": "RECORDSOURCE", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "CITY", "dataType": "String"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "VISITS", "dataType": "Number"}, {"name": "SPEND", "dataType": "Float"}, {"name": "LATITUDE", "dataType": "Float"}]', 
        '[{"name": "AVERAGESPENDPERVISIT", "dataType": "Float"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "CITY", "dataType": "String"}, {"name": "VISITS", "dataType": "Number"}, {"name": "SPEND", "dataType": "Float"}, {"name": "LATITUDE", "dataType": "Float"}]', 
        '[{"name": "CITY_TITLECASE", "dataType": "String"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "CITY", "dataType": "String"}, {"name": "VISITS", "dataType": "Number"}, {"name": "SPEND", "dataType": "Float"}, {"name": "LATITUDE", "dataType": "Float"}]', 
        '[{"name": "CITY", "dataType": "String"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "VISITS", "dataType": "Number"}, {"name": "SPEND", "dataType": "Float"}, {"name": "LATITUDE", "dataType": "Float"}]', 
        '[{"name": "RECORDSOURCE", "dataType": "String"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "CITY", "dataType": "String"}, {"name": "VISITS", "dataType": "Number"}, {"name": "SPEND", "dataType": "Float"}, {"name": "LATITUDE", "dataType": "Float"}]', 
        '[{"name": "AVERAGESPENDPERVISIT", "dataType": "Float"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "CITY", "dataType": "String"}, {"name": "VISITS", "dataType": "Number"}, {"name": "SPEND", "dataType": "Float"}, {"name": "LATITUDE", "dataType": "Float"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

SelectRecords_8_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_13_cast'], 
      'incremental_id', 
      'ROW_NUMBER', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

SelectRecords_8 AS (

  SELECT * 
  
  FROM SelectRecords_8_rowNumber AS in0
  
  WHERE (ROW_NUMBER >= 20)

),

SelectRecords_3 AS (

  SELECT * 
  
  FROM SelectRecords_3_rowNumber AS in0
  
  WHERE (
          ((((ROW_NUMBER <= 87) OR (ROW_NUMBER >= 109)) AND (ROW_NUMBER <= 113)) OR (ROW_NUMBER = 10800))
          OR (ROW_NUMBER >= 20000)
        )

),

SelectRecords_3_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_3 AS in0

),

SelectRecords_8_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_8 AS in0

),

SelectRecords_7_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_13_cast'], 
      'incremental_id', 
      'ROW_NUMBER', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

SelectRecords_7 AS (

  SELECT * 
  
  FROM SelectRecords_7_rowNumber AS in0
  
  WHERE (ROW_NUMBER = 10800)

),

SelectRecords_7_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_7 AS in0

),

SelectRecords_5_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_13_cast'], 
      'incremental_id', 
      'ROW_NUMBER', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

SelectRecords_5 AS (

  SELECT * 
  
  FROM SelectRecords_5_rowNumber AS in0
  
  WHERE ((ROW_NUMBER >= 10) AND (ROW_NUMBER <= 13))

),

SelectRecords_5_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_5 AS in0

),

Union_163 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'SelectRecords_3_cleanup_0', 
        'SelectRecords_5_cleanup_0', 
        'SelectRecords_7_cleanup_0', 
        'SelectRecords_8_cleanup_0'
      ], 
      [
        '[{"name": "ADDRESS", "dataType": "String"}, {"name": "CITY", "dataType": "String"}, {"name": "STATE", "dataType": "String"}, {"name": "ZIP", "dataType": "Number"}, {"name": "COMPANY", "dataType": "String"}, {"name": "AVERAGE SALE", "dataType": "Number"}]', 
        '[{"name": "ADDRESS", "dataType": "String"}, {"name": "CITY", "dataType": "String"}, {"name": "STATE", "dataType": "String"}, {"name": "ZIP", "dataType": "Number"}, {"name": "COMPANY", "dataType": "String"}, {"name": "AVERAGE SALE", "dataType": "Number"}]', 
        '[{"name": "ADDRESS", "dataType": "String"}, {"name": "CITY", "dataType": "String"}, {"name": "STATE", "dataType": "String"}, {"name": "ZIP", "dataType": "Number"}, {"name": "COMPANY", "dataType": "String"}, {"name": "AVERAGE SALE", "dataType": "Number"}]', 
        '[{"name": "ADDRESS", "dataType": "String"}, {"name": "CITY", "dataType": "String"}, {"name": "STATE", "dataType": "String"}, {"name": "ZIP", "dataType": "Number"}, {"name": "COMPANY", "dataType": "String"}, {"name": "AVERAGE SALE", "dataType": "Number"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_164 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_163'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Filter_77_reject AS (

  SELECT * 
  
  FROM TextInput_75_cast AS in0
  
  WHERE (
          (
            NOT(NOT(
              (LENGTH(LASTNAME)) = 0))
          )
          OR (
               (
                 NOT(
                   (LENGTH(LASTNAME)) = 0)
               ) IS NULL
             )
        )

),

Filter_66_reject AS (

  SELECT * 
  
  FROM JoinMultiple_78 AS in0
  
  WHERE (
          (
            NOT(
              JOINDATE >= FIRSTPURCHASEDATE)
          ) OR ((JOINDATE >= FIRSTPURCHASEDATE) IS NULL)
        )

),

Filter_64_reject AS (

  SELECT * 
  
  FROM JoinMultiple_78 AS in0
  
  WHERE (
          (
            NOT(
              JOINDATE <= '2024-10-19')
          ) OR ((JOINDATE <= '2024-10-19') IS NULL)
        )

),

Filter_68_reject AS (

  SELECT * 
  
  FROM JoinMultiple_78 AS in0
  
  WHERE (
          (
            NOT(
              CUSTOMERID > 30)
          ) OR ((CUSTOMERID > 30) IS NULL)
        )

),

Filter_76_reject AS (

  SELECT * 
  
  FROM TextInput_75_cast AS in0
  
  WHERE (
          (
            NOT(
              (POSITION('as' IN LASTNAME)) > 0)
          ) OR (((POSITION('as' IN LASTNAME)) > 0) IS NULL)
        )

),

Filter_67_reject AS (

  SELECT * 
  
  FROM JoinMultiple_78 AS in0
  
  WHERE ((NOT(NOT(LASTNAME IS NULL))) OR ((NOT(LASTNAME IS NULL)) IS NULL))

),

Filter_65_reject AS (

  SELECT * 
  
  FROM JoinMultiple_78 AS in0
  
  WHERE (
          (NOT(("REGION" = 'South') OR ((LENGTH(CAST((REGEXP_SUBSTR(UPPER("REGION"), '.*WEST')) AS STRING))) > 0)))
          OR ((("REGION" = 'South') OR ((LENGTH(CAST((REGEXP_SUBSTR(UPPER("REGION"), '.*WEST')) AS STRING))) > 0)) IS NULL)
        )

),

Union_211 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Filter_76_reject', 
        'Filter_77_reject', 
        'Filter_66_reject', 
        'Filter_64_reject', 
        'Filter_68_reject', 
        'Filter_65_reject', 
        'Filter_67_reject'
      ], 
      [
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_212 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_211'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_213 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_212'], 
      ['RECORDID'], 
      ['CUSTOMERID', 'FIRSTNAME', 'LASTNAME', 'GENDER', 'REGION', 'SCORE', 'JOINDATE', 'FIRSTPURCHASEDATE'], 
      'NAME', 
      'VALUE', 
      [
        'RECORDID', 
        'CUSTOMERID', 
        'FIRSTNAME', 
        'LASTNAME', 
        'GENDER', 
        'REGION', 
        'SCORE', 
        'JOINDATE', 
        'FIRSTPURCHASEDATE'
      ], 
      true
    )
  }}

),

Formula_215_0 AS (

  SELECT 
    CAST('filter2' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_213 AS in0

),

Filter_77 AS (

  SELECT * 
  
  FROM TextInput_75_cast AS in0
  
  WHERE (
          NOT(
            (LENGTH(LASTNAME)) = 0)
        )

),

Filter_68 AS (

  SELECT * 
  
  FROM JoinMultiple_78 AS in0
  
  WHERE (CUSTOMERID > 30)

),

Filter_76 AS (

  SELECT * 
  
  FROM TextInput_75_cast AS in0
  
  WHERE ((POSITION('as' IN LASTNAME)) > 0)

),

Filter_65 AS (

  SELECT * 
  
  FROM JoinMultiple_78 AS in0
  
  WHERE (("REGION" = 'South') OR ((LENGTH(CAST((REGEXP_SUBSTR(UPPER("REGION"), '.*WEST')) AS STRING))) > 0))

),

Filter_64 AS (

  SELECT * 
  
  FROM JoinMultiple_78 AS in0
  
  WHERE (JOINDATE <= '2024-10-19')

),

Union_206 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_64', 'Filter_65', 'Filter_68', 'Filter_67', 'Filter_66', 'Filter_77', 'Filter_76'], 
      [
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "REGION", "dataType": "String"}, {"name": "SCORE", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_207 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_206'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_208 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_207'], 
      ['RECORDID'], 
      ['CUSTOMERID', 'FIRSTNAME', 'LASTNAME', 'GENDER', 'REGION', 'SCORE', 'JOINDATE', 'FIRSTPURCHASEDATE'], 
      'NAME', 
      'VALUE', 
      [
        'RECORDID', 
        'CUSTOMERID', 
        'FIRSTNAME', 
        'LASTNAME', 
        'GENDER', 
        'REGION', 
        'SCORE', 
        'JOINDATE', 
        'FIRSTPURCHASEDATE'
      ], 
      true
    )
  }}

),

Formula_210_0 AS (

  SELECT 
    CAST('filter1' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_208 AS in0

),

TextInput_142_cast AS (

  SELECT 
    CAST(FIRSTNAME AS STRING) AS FIRSTNAME,
    CAST(LASTNAME AS STRING) AS LASTNAME,
    (TRY_TO_TIMESTAMP(CAST(FIRSTPURCHASEDATE AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS FIRSTPURCHASEDATE
  
  FROM TextInput_142 AS in0

),

Join_132_right_rightRecordPosition AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_142_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_165 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_164'], 
      ['RECORDID'], 
      ['ADDRESS', 'CITY', 'STATE', 'ZIP', 'COMPANY', 'AVERAGE SALE'], 
      'NAME', 
      'VALUE', 
      ['RECORDID', 'ADDRESS', 'CITY', 'STATE', 'ZIP', 'COMPANY', 'AVERAGE SALE'], 
      true
    )
  }}

),

Formula_167_0 AS (

  SELECT 
    CAST('select records' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_165 AS in0

),

TextInput_141 AS (

  SELECT * 
  
  FROM {{ ref('seed_alteryx_all_test_stage2_sql_141')}}

),

TextInput_141_cast AS (

  SELECT 
    CUSTOMERID AS CUSTOMERID,
    CAST(FIRSTNAME AS STRING) AS FIRSTNAME,
    CAST(LASTNAME AS STRING) AS LASTNAME,
    CAST(GENDER AS STRING) AS GENDER,
    (TRY_TO_TIMESTAMP(CAST(JOINDATE AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS JOINDATE
  
  FROM TextInput_141 AS in0

),

Join_132_right_leftRecordPosition AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_141_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Join_132_right AS (

  SELECT in0.* EXCLUDE ("RECORDPOSITIONFORJOIN")
  
  FROM Join_132_right_rightRecordPosition AS in0
  LEFT JOIN Join_132_right_leftRecordPosition AS in1
     ON (in0.RECORDPOSITIONFORJOIN = in1.RECORDPOSITIONFORJOIN)

),

TextInput_126 AS (

  SELECT * 
  
  FROM {{ ref('seed_alteryx_all_test_stage2_sql_126')}}

),

TextInput_126_cast AS (

  SELECT 
    CUSTOMERID AS CUSTOMERID,
    (TRY_TO_TIMESTAMP(CAST(FIRSTPURCHASEDATE AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')) AS FIRSTPURCHASEDATE
  
  FROM TextInput_126 AS in0

),

Join_127_right AS (

  SELECT in0.*
  
  FROM TextInput_126_cast AS in0
  LEFT JOIN TextInput_141_cast AS in1
     ON (in1.CUSTOMERID = in0.CUSTOMERID)

),

Join_129_right AS (

  SELECT in0.*
  
  FROM TextInput_142_cast AS in0
  LEFT JOIN TextInput_141_cast AS in1
     ON ((in1.FIRSTNAME = in0.FIRSTNAME) AND (in1.LASTNAME = in0.LASTNAME))

),

Union_236 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_127_right', 'Join_129_right', 'Join_132_right'], 
      [
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_127_left AS (

  SELECT in0.*
  
  FROM TextInput_141_cast AS in0
  LEFT JOIN TextInput_126_cast AS in1
     ON (in0.CUSTOMERID = in1.CUSTOMERID)

),

Join_132_inner_rightRecordPosition AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_142_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Join_132_inner_leftRecordPosition AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_141_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Join_132_inner AS (

  SELECT 
    in1.FIRSTNAME AS RIGHT_FIRSTNAME,
    in1.LASTNAME AS RIGHT_LASTNAME,
    in0.* EXCLUDE ("RECORDPOSITIONFORJOIN"),
    in1.* EXCLUDE ("FIRSTNAME", "LASTNAME", "RECORDPOSITIONFORJOIN")
  
  FROM Join_132_inner_leftRecordPosition AS in0
  INNER JOIN Join_132_inner_rightRecordPosition AS in1
     ON (in0.RECORDPOSITIONFORJOIN = in1.RECORDPOSITIONFORJOIN)

),

Join_127_inner AS (

  SELECT 
    in1.CUSTOMERID AS RIGHT_CUSTOMERID,
    in0.*,
    in1.* EXCLUDE ("CUSTOMERID")
  
  FROM TextInput_141_cast AS in0
  INNER JOIN TextInput_126_cast AS in1
     ON (in0.CUSTOMERID = in1.CUSTOMERID)

),

Join_129_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("FIRSTNAME", "LASTNAME")
  
  FROM TextInput_141_cast AS in0
  INNER JOIN TextInput_142_cast AS in1
     ON ((in0.FIRSTNAME = in1.FIRSTNAME) AND (in0.LASTNAME = in1.LASTNAME))

),

TextInput_133 AS (

  SELECT * 
  
  FROM {{ ref('seed_alteryx_all_test_stage2_sql_133')}}

),

TextInput_133_cast AS (

  SELECT 
    ID AS ID,
    CAST(DATASTREAM AS STRING) AS DATASTREAM,
    CAST(NOTE AS STRING) AS NOTE
  
  FROM TextInput_133 AS in0

),

Union_226 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_127_inner', 'Join_129_inner', 'Join_132_inner', 'TextInput_133_cast'], 
      [
        '[{"name": "RIGHT_CUSTOMERID", "dataType": "Number"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "RIGHT_FIRSTNAME", "dataType": "String"}, {"name": "RIGHT_LASTNAME", "dataType": "String"}, {"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}, {"name": "FIRSTPURCHASEDATE", "dataType": "Timestamp"}]', 
        '[{"name": "ID", "dataType": "Number"}, {"name": "DATASTREAM", "dataType": "String"}, {"name": "NOTE", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_227 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_226'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

RecordID_217 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_216'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Join_132_left_leftRecordPosition AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_141_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Join_132_left_rightRecordPosition AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_142_cast'], 
      'incremental_id', 
      'RECORDPOSITIONFORJOIN', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Join_132_left AS (

  SELECT in0.* EXCLUDE ("RECORDPOSITIONFORJOIN")
  
  FROM Join_132_left_leftRecordPosition AS in0
  LEFT JOIN Join_132_left_rightRecordPosition AS in1
     ON (in0.RECORDPOSITIONFORJOIN = in1.RECORDPOSITIONFORJOIN)

),

Join_129_left AS (

  SELECT in0.*
  
  FROM TextInput_141_cast AS in0
  LEFT JOIN TextInput_142_cast AS in1
     ON ((in0.FIRSTNAME = in1.FIRSTNAME) AND (in0.LASTNAME = in1.LASTNAME))

),

Union_231 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_127_left', 'Join_129_left', 'Join_132_left'], 
      [
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}]', 
        '[{"name": "CUSTOMERID", "dataType": "Number"}, {"name": "FIRSTNAME", "dataType": "String"}, {"name": "LASTNAME", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "JOINDATE", "dataType": "Timestamp"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

RecordID_232 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_231'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_233 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_232'], 
      ['RECORDID'], 
      ['CUSTOMERID', 'FIRSTNAME', 'LASTNAME', 'GENDER', 'JOINDATE'], 
      'NAME', 
      'VALUE', 
      ['RECORDID', 'CUSTOMERID', 'FIRSTNAME', 'LASTNAME', 'GENDER', 'JOINDATE'], 
      true
    )
  }}

),

RecordID_237 AS (

  {{
    prophecy_basics.RecordID(
      ['Union_236'], 
      'incremental_id', 
      'RECORDID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Transpose_228 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_227'], 
      ['RECORDID'], 
      [
        'CUSTOMERID', 
        'FIRSTNAME', 
        'LASTNAME', 
        'GENDER', 
        'JOINDATE', 
        'RIGHT_CUSTOMERID', 
        'FIRSTPURCHASEDATE', 
        'RIGHT_FIRSTNAME', 
        'RIGHT_LASTNAME', 
        'ID', 
        'DATASTREAM', 
        'NOTE'
      ], 
      'NAME', 
      'VALUE', 
      [
        'RECORDID', 
        'RIGHT_CUSTOMERID', 
        'CUSTOMERID', 
        'FIRSTNAME', 
        'LASTNAME', 
        'GENDER', 
        'JOINDATE', 
        'FIRSTPURCHASEDATE', 
        'RIGHT_FIRSTNAME', 
        'RIGHT_LASTNAME', 
        'ID', 
        'DATASTREAM', 
        'NOTE'
      ], 
      true
    )
  }}

),

Formula_225_0 AS (

  SELECT 
    CAST('regex' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_223 AS in0

),

Transpose_218 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_217'], 
      ['RECORDID'], 
      [
        'CUSTOMERID', 
        'CITY', 
        'VISITS', 
        'SPEND', 
        'LATITUDE', 
        'RECORDSOURCE', 
        'REGION', 
        'CITY_TITLECASE', 
        'AVERAGESPENDPERVISIT'
      ], 
      'NAME', 
      'VALUE', 
      [
        'RECORDID', 
        'REGION', 
        'CUSTOMERID', 
        'CITY', 
        'VISITS', 
        'SPEND', 
        'LATITUDE', 
        'RECORDSOURCE', 
        'AVERAGESPENDPERVISIT', 
        'CITY_TITLECASE'
      ], 
      true
    )
  }}

),

Formula_220_0 AS (

  SELECT 
    CAST('formula' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_218 AS in0

),

Formula_235_0 AS (

  SELECT 
    CAST('join-l' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_233 AS in0

),

Formula_230_0 AS (

  SELECT 
    CAST('join-j' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_228 AS in0

),

Transpose_238 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_237'], 
      ['RECORDID'], 
      ['CUSTOMERID', 'FIRSTPURCHASEDATE', 'FIRSTNAME', 'LASTNAME'], 
      'NAME', 
      'VALUE', 
      ['RECORDID', 'CUSTOMERID', 'FIRSTPURCHASEDATE', 'FIRSTNAME', 'LASTNAME'], 
      true
    )
  }}

),

Formula_240_0 AS (

  SELECT 
    CAST('join-r' AS STRING) AS TOOLNAME,
    *
  
  FROM Transpose_238 AS in0

),

Union_243 AS (

  {{
    prophecy_basics.UnionByName(
      [
        'Formula_240_0', 
        'Formula_230_0', 
        'Formula_225_0', 
        'Formula_210_0', 
        'Formula_235_0', 
        'Formula_215_0', 
        'Formula_167_0', 
        'Formula_220_0'
      ], 
      [
        '[{"name": "TOOLNAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}]', 
        '[{"name": "TOOLNAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}]', 
        '[{"name": "TOOLNAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}]', 
        '[{"name": "TOOLNAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}]', 
        '[{"name": "TOOLNAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}]', 
        '[{"name": "TOOLNAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}]', 
        '[{"name": "TOOLNAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}]', 
        '[{"name": "TOOLNAME", "dataType": "String"}, {"name": "RECORDID", "dataType": "Number"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_246 AS (

  SELECT 
    "NAME" AS "NAME",
    "VALUE" AS "VALUE",
    TOOLNAME AS TOOLNAME
  
  FROM Union_243 AS in0

),

RegEx_245 AS (

  {{
    prophecy_basics.Regex(
      ['AlteryxSelect_246'], 
      [], 
      '[{"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "TOOLNAME", "dataType": "String"}]', 
      'VALUE', 
      ',', 
      'replace', 
      true, 
      false, 
      '.', 
      true, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      '', 
      false, 
      '_REPLACED'
    )
  }}

),

RegEx_245_rename_0 AS (

  SELECT 
    VALUE_REPLACED AS "VALUE",
    * EXCLUDE ("VALUE_REPLACED", "VALUE")
  
  FROM RegEx_245 AS in0

)

SELECT *

FROM RegEx_245_rename_0
