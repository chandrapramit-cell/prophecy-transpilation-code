{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_8_cast AS (

  SELECT *
  
  FROM {{ ref('DRP_VQF_Concat__TextInput_8_cast')}}

),

Cleanse_22 AS (

  {{
    prophecy_basics.DataCleansing(
      ['TextInput_8_cast'], 
      [
        { "name": "C1071007", "dataType": "String" }, 
        { "name": "c10071032", "dataType": "String" }, 
        { "name": "old_field", "dataType": "String" }
      ], 
      'keepOriginal', 
      [], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
      false, 
      false, 
      true, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Formula_23_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(C1071007, 'ddMmmyyyy', '')) AS string) AS C1071007,
    CAST((REGEXP_REPLACE(c10071032, 'ddMmmyyyy', '')) AS string) AS c10071032,
    CAST((REGEXP_REPLACE(old_field, 'ddMmmyyyy', '')) AS string) AS old_field,
    * EXCEPT (`c1071007`, `c10071032`, `old_field`)
  
  FROM Cleanse_22 AS in0

),

Formula_23_1 AS (

  SELECT 
    CAST((REGEXP_REPLACE(C1071007, '', ' ')) AS string) AS C1071007,
    CAST((REGEXP_REPLACE(c10071032, '', ' ')) AS string) AS c10071032,
    CAST((REGEXP_REPLACE(old_field, '', ' ')) AS string) AS old_field,
    * EXCEPT (`c1071007`, `c10071032`, `old_field`)
  
  FROM Formula_23_0 AS in0

),

Cleanse_24 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_23_1'], 
      [
        { "name": "C1071007", "dataType": "String" }, 
        { "name": "c10071032", "dataType": "String" }, 
        { "name": "old_field", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['C1071007', 'c10071032', 'old_field'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      true, 
      false, 
      false, 
      true, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

)

SELECT *

FROM Cleanse_24
