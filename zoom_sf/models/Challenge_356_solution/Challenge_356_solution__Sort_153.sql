{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH words_txt_130 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Challenge_356_solution', 'words_txt_130') }}

),

Formula_147_0 AS (

  SELECT 
    CAST(FIELD_1 AS STRING) AS WORD,
    (LENGTH(FIELD_1)) AS LENGTH,
    *
  
  FROM words_txt_130 AS in0

),

RegEx_142 AS (

  {{
    prophecy_basics.Regex(
      ['Formula_147_0'], 
      [{ 'columnName': 'regex_col1', 'dataType': 'string', 'rgxExpression': '(.)' }], 
      '[{"name": "WORD", "dataType": "String"}, {"name": "LENGTH", "dataType": "Number"}, {"name": "FIELD_1", "dataType": "String"}]', 
      'FIELD_1', 
      '(.)', 
      'tokenize', 
      true, 
      false, 
      '', 
      false, 
      'splitRows', 
      1, 
      'dropExtraWithoutWarning', 
      'GENERATED', 
      '', 
      false
    )
  }}

),

RegEx_142_dropGem_0 AS (

  SELECT 
    GENERATED AS FIELD_1,
    * EXCLUDE ("GENERATED", "TOKEN_SEQUENCE", "FIELD_1")
  
  FROM RegEx_142 AS in0

),

TextInput_131 AS (

  SELECT * 
  
  FROM {{ ref('seed_131')}}

),

TextInput_131_cast AS (

  SELECT 
    CAST(FIELD1 AS STRING) AS FIELD1,
    CAST(FIELD2 AS STRING) AS FIELD2,
    CAST(FIELD3 AS STRING) AS FIELD3
  
  FROM TextInput_131 AS in0

),

RecordID_158 AS (

  {{
    prophecy_basics.RecordID(
      ['TextInput_131_cast'], 
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

Transpose_136 AS (

  {{
    prophecy_basics.Transpose(
      ['RecordID_158'], 
      ['RECORDID'], 
      ['FIELD1', 'FIELD2', 'FIELD3'], 
      'NAME', 
      '"VALUE"', 
      ['RECORDID', 'FIELD1', 'FIELD2', 'FIELD3'], 
      true
    )
  }}

),

Filter_138 AS (

  SELECT * 
  
  FROM Transpose_136 AS in0
  
  WHERE (NOT(VALUE IS NULL))

),

Formula_139_0 AS (

  SELECT 
    CAST(LOWER(VALUE) AS STRING) AS "VALUE",
    (
      CASE
        WHEN (RECORDID = 2)
          THEN 1
        ELSE 0
      END
    ) AS "CONTAINS MIDDLE LETTERQUESMARK",
    * EXCLUDE ("VALUE")
  
  FROM Filter_138 AS in0

),

Join_143_inner AS (

  SELECT 
    in1.WORD AS WORD,
    in0.VALUE AS LETTER,
    in1.LENGTH AS LENGTH,
    in0."CONTAINS MIDDLE LETTERQUESMARK" AS "CONTAINS MIDDLE LETTERQUESMARK",
    in0.* EXCLUDE ("VALUE", "CONTAINS MIDDLE LETTERQUESMARK", "NAME", "RECORDID"),
    in1.* EXCLUDE ("WORD", "LENGTH", "FIELD_1")
  
  FROM Formula_139_0 AS in0
  INNER JOIN RegEx_142_dropGem_0 AS in1
     ON (in0.VALUE = in1.FIELD_1)

),

Summarize_148 AS (

  SELECT 
    MAX("CONTAINS MIDDLE LETTERQUESMARK") AS "CONTAINS MIDDLE LETTERQUESMARK",
    COUNT(
      (
        CASE
          WHEN ((LETTER IS NULL) OR (CAST(LETTER AS STRING) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS "COUNT",
    WORD AS WORD,
    MAX(LENGTH) AS LENGTH
  
  FROM Join_143_inner AS in0
  
  GROUP BY WORD

),

Filter_151_to_Filter_150 AS (

  SELECT * 
  
  FROM Summarize_148 AS in0
  
  WHERE (((CAST(LENGTH AS DOUBLE) = COUNT) AND "CONTAINS MIDDLE LETTERQUESMARK") AND (LENGTH >= 4))

),

AlteryxSelect_152 AS (

  SELECT 
    WORD AS WORD,
    LENGTH AS LENGTH
  
  FROM Filter_151_to_Filter_150 AS in0

),

Sort_153 AS (

  SELECT * 
  
  FROM AlteryxSelect_152 AS in0
  
  ORDER BY LENGTH DESC

)

SELECT *

FROM Sort_153
