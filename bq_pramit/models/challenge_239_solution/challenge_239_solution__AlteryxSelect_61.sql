{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH RecordID_52 AS (

  SELECT *
  
  FROM {{ ref('challenge_239_solution__RecordID_52')}}

),

Summarize_56 AS (

  SELECT *
  
  FROM {{ ref('challenge_239_solution__Summarize_56')}}

),

Join_57_left AS (

  SELECT in0.*
  
  FROM RecordID_52 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM Summarize_56 AS in1
    
    WHERE in1.RecordID IS NOT NULL
  ) AS in1_keys
     ON (in0.RecordID = in1_keys.RecordID)
  
  WHERE (in1_keys.RecordID IS NULL)

),

Transpose_55 AS (

  {{
    prophecy_basics.Transpose(
      ['Join_57_left'], 
      [], 
      ['RecordID', 's', 'e', 'n', 'd', 'm', 'o', 'r', 'y'], 
      'Name', 
      'Value', 
      ['e', 's', 'n', 'y', 'm', 'r', 'o', 'd', 'RecordID'], 
      true
    )
  }}

),

AlteryxSelect_62 AS (

  SELECT 
    CAST(VALUE AS STRING) AS `Value`,
    * EXCEPT (`Value`)
  
  FROM Transpose_55 AS in0

),

FindReplace_60_allRules AS (

  SELECT ARRAY_AGG(struct(Name AS Name, Value AS Value)) AS _rules
  
  FROM AlteryxSelect_62 AS in0

),

TextInput_41 AS (

  SELECT * 
  
  FROM {{ ref('seed_41')}}

),

TextInput_41_cast AS (

  SELECT CAST(Problem AS STRING) AS Problem
  
  FROM TextInput_41 AS in0

),

FindReplace_60_join AS (

  SELECT 
    in0.Problem AS Problem,
    in1.`_rules` AS _rules
  
  FROM TextInput_41_cast AS in0
  FULL JOIN FindReplace_60_allRules AS in1
     ON TRUE

),

FindReplace_60_0 AS (

  SELECT 
    aggregate(
      _rules, 
      `Problem`, 
      (acc, rule) -> regexp_replace(acc, rule['Name'], rule['Value'])) AS Problem,
    * EXCEPT (`Problem`, `problem`)
  
  FROM FindReplace_60_join AS in0

),

FindReplace_60_reorg_0 AS (

  SELECT * EXCEPT (`_rules`)
  
  FROM FindReplace_60_0 AS in0

),

AlteryxSelect_61 AS (

  SELECT Problem AS solution
  
  FROM FindReplace_60_reorg_0 AS in0

)

SELECT *

FROM AlteryxSelect_61
