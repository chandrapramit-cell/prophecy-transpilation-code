{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_58 AS (

  SELECT * 
  
  FROM {{ ref('seed_58')}}

),

TextInput_58_cast AS (

  SELECT 
    CAST("LANGUAGE" AS STRING) AS "LANGUAGE",
    CAST(VARIABLETYPE AS STRING) AS VARIABLETYPE,
    CAST("HOLIDAY MESSAGES" AS STRING) AS "HOLIDAY MESSAGES"
  
  FROM TextInput_58 AS in0

),

Filter_2_to_Filter_73 AS (

  SELECT * 
  
  FROM TextInput_58_cast AS in0
  
  WHERE (("LANGUAGE" = 'English') AND (VARIABLETYPE = 'Corporate'))

),

Formula_32_0 AS (

  SELECT 
    CAST((uniform(0, 1, random())) AS STRING) AS RANDOM,
    *
  
  FROM Filter_2_to_Filter_73 AS in0

),

PortfolioComposerImage_40 AS (

  {{ prophecy_basics.ToDo('Component type: Image is not supported.') }}

),

SelectRecords_50_rowNumber AS (

  {{
    prophecy_basics.RecordID(
      ['Formula_32_0'], 
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

SelectRecords_50 AS (

  SELECT * 
  
  FROM SelectRecords_50_rowNumber AS in0
  
  WHERE (ROW_NUMBER = 1)

),

SelectRecords_50_cleanup_0 AS (

  SELECT * EXCLUDE ("ROW_NUMBER")
  
  FROM SelectRecords_50 AS in0

),

PortfolioComposerText_25 AS (

  {{ prophecy_basics.ToDo('Component type: Report Text is not supported.') }}

),

PortfolioComposerTable_46 AS (

  {{ prophecy_basics.ToDo('Component type: Portfolio Composer Table is not supported.') }}

),

JoinMultiple_21_in0 AS (

  {{
    prophecy_basics.RecordID(
      ['PortfolioComposerTable_46'], 
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

JoinMultiple_21_in1 AS (

  {{
    prophecy_basics.RecordID(
      ['PortfolioComposerImage_40'], 
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

JoinMultiple_21 AS (

  SELECT 
    in0.RANDOM AS RANDOM,
    in0."HOLIDAY MESSAGES" AS "HOLIDAY MESSAGES",
    in1.IMAGE AS IMAGE,
    in0.TEXT AS TEXT,
    in0."LANGUAGE" AS "LANGUAGE",
    in0.VARIABLETYPE AS VARIABLETYPE,
    in0."TABLE" AS "TABLE"
  
  FROM JoinMultiple_21_in0 AS in0
  FULL JOIN JoinMultiple_21_in1 AS in1
     ON (in0.RECORDPOSITIONFORJOIN_0 = in1.RECORDPOSITIONFORJOIN_1)

),

Overlay_43 AS (

  {{ prophecy_basics.ToDo('Component type: PortfolioPluginsGui.ComposerOverlay.Overlay is not supported.') }}

),

PortfolioComposerRender_49 AS (

  {{ prophecy_basics.ToDo('Component type: Render is not supported.') }}

)

SELECT *

FROM PortfolioComposerRender_49
