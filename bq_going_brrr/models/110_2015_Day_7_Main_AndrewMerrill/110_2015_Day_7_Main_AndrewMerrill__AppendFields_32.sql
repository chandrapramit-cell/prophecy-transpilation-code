{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_1 AS (

  SELECT * 
  
  FROM {{ ref('seed_1')}}

),

TextInput_1_cast AS (

  SELECT CAST(Logic AS STRING) AS Logic
  
  FROM TextInput_1 AS in0

),

Detour_34_out1 AS (

  SELECT * 
  
  FROM TextInput_1_cast AS in0
  
  WHERE TRUE

),

Formula_36_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Logic, '(?i)(.*) -> (\\w+)', '$1')) AS STRING) AS Input,
    *
  
  FROM Detour_34_out1 AS in0

),

Detour_34_out0 AS (

  SELECT * 
  
  FROM TextInput_1_cast AS in0
  
  WHERE FALSE

),

Formula_2_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(Logic, '(?i)(.*) -> (\\w+)', '$1')) AS STRING) AS Input,
    *
  
  FROM Detour_34_out0 AS in0

),

Formula_2_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 3)
          THEN (SPLIT(INPUT, '\\s+'))[(SAFE_OFFSET((CAST(1 AS INT64) - 1)))]
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 2)
          THEN '65535'
        ELSE '0'
      END
    ) AS string) AS `Input 1`,
    CAST((
      CASE
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 3)
          THEN (SPLIT(INPUT, '\\s+'))[(SAFE_OFFSET((CAST(2 AS INT64) - 1)))]
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 2)
          THEN 'NOT'
        ELSE 'OR'
      END
    ) AS string) AS Operation,
    CAST((
      CASE
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 3)
          THEN (SPLIT(INPUT, '\\s+'))[(SAFE_OFFSET((CAST(3 AS INT64) - 1)))]
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 2)
          THEN (SPLIT(INPUT, '\\s+'))[(SAFE_OFFSET((CAST(2 AS INT64) - 1)))]
        ELSE INPUT
      END
    ) AS string) AS `Input 2`,
    CAST((REGEXP_REPLACE(Logic, '(?i)(.*) -> (\\w+)', '$2')) AS STRING) AS Output,
    *
  
  FROM Formula_2_0 AS in0

),

Formula_36_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 3)
          THEN (SPLIT(INPUT, '\\s+'))[(SAFE_OFFSET((CAST(1 AS INT64) - 1)))]
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 2)
          THEN '65535'
        ELSE '0'
      END
    ) AS string) AS `Input 1`,
    CAST((
      CASE
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 3)
          THEN (SPLIT(INPUT, '\\s+'))[(SAFE_OFFSET((CAST(2 AS INT64) - 1)))]
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 2)
          THEN 'NOT'
        ELSE 'OR'
      END
    ) AS string) AS Operation,
    CAST((
      CASE
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 3)
          THEN (SPLIT(INPUT, '\\s+'))[(SAFE_OFFSET((CAST(3 AS INT64) - 1)))]
        WHEN ((ARRAY_LENGTH((SPLIT(INPUT, '\\s+')))) = 2)
          THEN (SPLIT(INPUT, '\\s+'))[(SAFE_OFFSET((CAST(2 AS INT64) - 1)))]
        ELSE INPUT
      END
    ) AS string) AS `Input 2`,
    CAST((REGEXP_REPLACE(Logic, '(?i)(.*) -> (\\w+)', '$2')) AS STRING) AS Output,
    *
  
  FROM Formula_36_0 AS in0

),

Formula_36_2 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Output = 'b')
          THEN '16076'
        ELSE `Input 2`
      END
    ) AS STRING) AS `Input 2`,
    * EXCEPT (`input 2`)
  
  FROM Formula_36_1 AS in0

),

DetourEnd_35 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_36_2', 'Formula_2_1'], 
      [
        '[{"name": "Input 2", "dataType": "String"}, {"name": "Input 1", "dataType": "String"}, {"name": "Operation", "dataType": "String"}, {"name": "Output", "dataType": "String"}, {"name": "Input", "dataType": "String"}, {"name": "Logic", "dataType": "String"}]', 
        '[{"name": "Input 1", "dataType": "String"}, {"name": "Operation", "dataType": "String"}, {"name": "Input 2", "dataType": "String"}, {"name": "Output", "dataType": "String"}, {"name": "Input", "dataType": "String"}, {"name": "Logic", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Unique_3 AS (

  SELECT * 
  
  FROM DetourEnd_35 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY Output ORDER BY Output) = 1

),

AlteryxSelect_19 AS (

  SELECT * EXCEPT (`Logic`, `Input`)
  
  FROM Unique_3 AS in0

),

Formula_20_0 AS (

  SELECT 
    CAST((CONCAT('-', Output, '-x')) AS STRING) AS Logic,
    *
  
  FROM AlteryxSelect_19 AS in0

),

Summarize_22 AS (

  SELECT (ARRAY_TO_STRING((ARRAY_AGG(Logic)), ',')) AS Logic
  
  FROM Formula_20_0 AS in0

),

CountRecords_21 AS (

  SELECT COUNT(*) AS `Count`
  
  FROM Formula_20_0 AS in0

),

AppendFields_23 AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM CountRecords_21 AS in0
  INNER JOIN Summarize_22 AS in1
     ON TRUE

),

AppendFields_32 AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Logic`)
  
  FROM AppendFields_23 AS in0
  INNER JOIN Formula_20_0 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_32
