{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH aka_test_oracle_428 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('wf_standard_core_recruitment_review_thaded', 'aka_test_oracle_428') }}

),

AlteryxSelect_422 AS (

  SELECT study_id AS study_id
  
  FROM aka_test_oracle_428 AS in0

),

AlteryxSelect_423 AS (

  SELECT protocol_id AS study_id
  
  FROM aka_test_oracle_428 AS in0

),

AlteryxSelect_424 AS (

  SELECT study_number_pfe AS study_id
  
  FROM aka_test_oracle_428 AS in0

),

Union_425 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_422', 'AlteryxSelect_423', 'AlteryxSelect_424'], 
      [
        '[{"name": "study_id", "dataType": "String"}]', 
        '[{"name": "study_id", "dataType": "String"}]', 
        '[{"name": "study_id", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Unique_426 AS (

  SELECT * 
  
  FROM Union_425 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY study_id ORDER BY study_id) = 1

),

AlteryxSelect_427 AS (

  SELECT study_id AS study_id
  
  FROM Unique_426 AS in0

),

RecordID_434 AS (

  SELECT 
    *,
    row_number() OVER (ORDER BY 1) AS `RecordID`
  
  FROM AlteryxSelect_427

),

Formula_435_to_Formula_433_0 AS (

  SELECT 
    CAST(CEIL((RecordID / 1000)) AS INTEGER) AS variablegroup,
    CAST(concat('\'', study_id, '\'') AS STRING) AS study_id,
    * EXCEPT (`study_id`)
  
  FROM RecordID_434 AS in0

),

Summarize_432 AS (

  SELECT 
    concat_ws(',', collect_list(study_id)) AS study_list,
    variablegroup AS variablegroup
  
  FROM Formula_435_to_Formula_433_0 AS in0
  
  GROUP BY variablegroup

)

SELECT *

FROM Summarize_432
