{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH GenerateRows_49 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('challenge_154_solution_DL', 'GenerateRows_49') }}

),

Formula_50_0 AS (

  SELECT 
    CAST(REVERSE((bqutil.fn.to_binary(CAST(Scenario AS INT64)))) AS STRING) AS Bin,
    (Scenario + 1) AS Scenario,
    * EXCEPT (`scenario`)
  
  FROM GenerateRows_49 AS in0

),

RegEx_51 AS (

  {{
    prophecy_basics.Regex(
      ['Formula_50_0'], 
      [{ 'columnName': 'regex_col1', 'dataType': 'string', 'rgxExpression': '(.)' }], 
      '[{"name": "Bin", "dataType": "String"}, {"name": "Scenario", "dataType": "Numeric"}, {"name": "Count", "dataType": "Numeric"}]', 
      'Bin', 
      '(.)', 
      'tokenize', 
      true, 
      false, 
      '', 
      false, 
      'splitRows', 
      1, 
      'dropExtraWithoutWarning', 
      'generated', 
      '', 
      false
    )
  }}

),

RecordID_52 AS (

  SELECT *
  
  FROM {{ ref('challenge_154_solution_DL__RecordID_52')}}

),

RegEx_51_dropGem_0 AS (

  SELECT 
    generated AS Bin,
    * EXCEPT (`generated`, `token_position`, `token_sequence`, `bin`)
  
  FROM RegEx_51 AS in0

),

MultiRowFormula_54_row_id_0 AS (

  SELECT 
    (ROW_NUMBER() OVER ()) AS prophecy_row_id,
    *
  
  FROM RegEx_51_dropGem_0 AS in0

),

MultiRowFormula_54 AS (

  {{ prophecy_basics.ToDo('Multi Row Formula tool for this case is not supported by transpiler in SQL') }}

),

MultiRowFormula_54_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_54 AS in0

),

Formula_53_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Bin = '0')
          THEN 'Off'
        ELSE 'On '
      END
    ) AS STRING) AS Bin,
    * EXCEPT (`bin`)
  
  FROM MultiRowFormula_54_row_id_drop_0 AS in0

),

Join_55_inner AS (

  SELECT 
    in0.COUNT AS `Count`,
    in0.Scenario AS Scenario,
    in1.Options AS `Option 1`,
    in0.Bin AS `Option 2`,
    in0.RecordID AS RecordID
  
  FROM Formula_53_0 AS in0
  INNER JOIN RecordID_52 AS in1
     ON (in0.RecordID = in1.RecordID)

),

Sort_56 AS (

  SELECT * 
  
  FROM Join_55_inner AS in0
  
  ORDER BY CAST(Scenario AS STRING) ASC, CAST(RecordID AS STRING) ASC

),

AlteryxSelect_57 AS (

  SELECT * EXCEPT (`Count`, `RecordID`)
  
  FROM Sort_56 AS in0

)

SELECT *

FROM AlteryxSelect_57
