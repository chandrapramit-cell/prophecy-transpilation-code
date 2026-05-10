{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH PortfolioComposerImage_40 AS (

  {{ prophecy_basics.ToDo('Component type: Image is not supported.') }}

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

Type_yxdb_6 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('challenge_350_solution', 'Type_yxdb_6') }}

),

Filter_2 AS (

  SELECT * 
  
  FROM Type_yxdb_6 AS in0
  
  WHERE ("HOLIDAY MESSAGES" = 'At this special time of year, we want to take a moment to thank you for your dedication to our company. Happy Holidays!')

),

Formula_32_0 AS (

  SELECT 
    {{ var('VARIABLE32_FORMULAFIELDS_FORMULAFIELDFIELDHEADER_EXPRESSION') }} AS HEADER,
    1 AS PERSONALMESSAGE,
    {{ var('VARIABLE32_FORMULAFIELDS_FORMULAFIELDFIELDFOOTER_EXPRESSION') }} AS FOOTER,
    *
  
  FROM Filter_2 AS in0

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

JoinMultiple_21 AS (

  SELECT 
    in0.PERSONALMESSAGE AS PERSONALMESSAGE,
    in0."HOLIDAY MESSAGES" AS "HOLIDAY MESSAGES",
    in1.IMAGE AS IMAGE,
    in0.TEXT AS TEXT,
    in0."LANGUAGE" AS "LANGUAGE",
    in0.HEADER AS HEADER,
    in0."TYPE" AS "TYPE",
    in0.FOOTER AS FOOTER,
    in0."TABLE" AS "TABLE"
  
  FROM JoinMultiple_21_in0 AS in0
  FULL JOIN JoinMultiple_21_in1 AS in1
     ON (in0.RECORDPOSITIONFORJOIN_0 = in1.RECORDPOSITIONFORJOIN_1)

),

Overlay_43 AS (

  {{ prophecy_basics.ToDo('Component type: PortfolioPluginsGui.ComposerOverlay.Overlay is not supported.') }}

),

Detour_61_out0 AS (

  SELECT * 
  
  FROM Overlay_43 AS in0
  
  WHERE FALSE

),

PortfolioComposerRender_49 AS (

  {{ prophecy_basics.ToDo('Component type: Render is not supported.') }}

)

SELECT *

FROM PortfolioComposerRender_49
