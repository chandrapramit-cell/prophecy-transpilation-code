{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_29_0 AS (

  SELECT *
  
  FROM {{ ref('13_Chicago_FI_Fact_Tables__Formula_29_0')}}

),

Filter_38 AS (

  {#VisualGroup: Container29#}
  SELECT * 
  
  FROM Formula_29_0 AS in0
  
  WHERE (NOT(VIOLATIONS IS NULL))

),

AlteryxSelect_40 AS (

  {#VisualGroup: Container29#}
  SELECT 
    "INSPECTION ID" AS "INSPECTION ID",
    VIOLATIONS AS VIOLATIONS,
    DI_CREATEDATE AS DI_CREATEDATE,
    DI_WORKFLOWFILENAME AS DI_WORKFLOWFILENAME,
    CAST(NULL AS STRING) AS DI_WORKFLOWDIRECTORY
  
  FROM Filter_38 AS in0

),

TextToColumns_41 AS (

  {#VisualGroup: Container29#}
  {{
    prophecy_basics.TextToColumns(
      ['AlteryxSelect_40'], 
      'VIOLATIONS', 
      "\|", 
      'splitRows', 
      1, 
      'leaveExtraCharLastCol', 
      'VIOLATIONS', 
      'VIOLATIONS', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_41_dropGem_0 AS (

  {#VisualGroup: Container29#}
  SELECT 
    GENERATEDCOLUMNNAME AS VIOLATIONS,
    * EXCLUDE ("GENERATEDCOLUMNNAME", "VIOLATIONS")
  
  FROM TextToColumns_41 AS in0

),

RegEx_39 AS (

  {#VisualGroup: Container29#}
  {{
    prophecy_basics.Regex(
      ['TextToColumns_41_dropGem_0'], 
      [], 
      '[{"name": "VIOLATIONS", "dataType": "String"}, {"name": "INSPECTION ID", "dataType": "String"}, {"name": "DI_CREATEDATE", "dataType": "Timestamp"}, {"name": "DI_WORKFLOWFILENAME", "dataType": "String"}, {"name": "DI_WORKFLOWDIRECTORY", "dataType": "String"}]', 
      'VIOLATIONS', 
      '- Comments:', 
      'replace', 
      true, 
      false, 
      '@ COMMENT(S):', 
      true, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      '', 
      false, 
      '_replaced2'
    )
  }}

),

RegEx_39_rename_0 AS (

  {#VisualGroup: Container29#}
  SELECT 
    VIOLATIONS_REPLACED AS VIOLATIONS,
    * EXCLUDE ("VIOLATIONS_REPLACED", "VIOLATIONS")
  
  FROM RegEx_39 AS in0

),

TextToColumns_42 AS (

  {#VisualGroup: Container29#}
  {{
    prophecy_basics.TextToColumns(
      ['RegEx_39_rename_0'], 
      'VIOLATIONS', 
      "@", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'VIOLATIONS', 
      'VIOLATIONS', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_42_dropGem_0 AS (

  {#VisualGroup: Container29#}
  SELECT 
    VIOLATIONS_1_VIOLATIONS AS VOILATIONS1,
    VIOLATIONS_2_VIOLATIONS AS VOILATIONS2,
    *
  
  FROM TextToColumns_42 AS in0

),

Formula_48_0 AS (

  {#VisualGroup: Container29#}
  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS DI_CREATEDATE,
    * EXCLUDE ("DI_CREATEDATE")
  
  FROM TextToColumns_42_dropGem_0 AS in0

),

AlteryxSelect_49 AS (

  {#VisualGroup: Container29#}
  SELECT 
    "INSPECTION ID" AS "INSPECTION ID",
    VIOLATIONS AS VIOLATIONS,
    DI_CREATEDATE AS DI_CREATEDATE,
    DI_WORKFLOWDIRECTORY AS DI_WORKFLOWDIRECTORY,
    DI_WORKFLOWFILENAME AS DI_WORKFLOWFILENAME,
    VOILATIONS1 AS VOILATIONS1,
    VOILATIONS2 AS VIOLATIONCOMMENT
  
  FROM Formula_48_0 AS in0

),

TextToColumns_37 AS (

  {#VisualGroup: Container29#}
  {{
    prophecy_basics.TextToColumns(
      ['AlteryxSelect_49'], 
      'VOILATIONS1', 
      "\.", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'VOILATIONS1', 
      'VOILATIONS1', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_37_dropGem_0 AS (

  {#VisualGroup: Container29#}
  SELECT 
    VOILATIONS1_1_VOILATIONS1 AS VOILATIONSCODE1,
    VOILATIONS1_2_VOILATIONS1 AS VOILATIONSCODE2,
    *
  
  FROM TextToColumns_37 AS in0

),

Cleanse_43 AS (

  {#VisualGroup: Container29#}
  {{
    prophecy_basics.DataCleansing(
      ['TextToColumns_37_dropGem_0'], 
      [
        { "name": "DI_WORKFLOWFILENAME", "dataType": "String" }, 
        { "name": "INSPECTION ID", "dataType": "Integer" }, 
        { "name": "DI_WORKFLOWDIRECTORY", "dataType": "String" }, 
        { "name": "VIOLATIONS", "dataType": "String" }, 
        { "name": "VOILATIONSCODE1", "dataType": "String" }, 
        { "name": "VIOLATIONCOMMENT", "dataType": "String" }, 
        { "name": "DI_CREATEDATE", "dataType": "Date" }, 
        { "name": "VOILATIONS1_1_VOILATIONS1", "dataType": "String" }, 
        { "name": "VOILATIONS1_2_VOILATIONS1", "dataType": "String" }, 
        { "name": "VOILATIONS1", "dataType": "String" }, 
        { "name": "VOILATIONSCODE2", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['VOILATIONSCODE1', 'VOILATIONSCODE2'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

AlteryxSelect_50 AS (

  {#VisualGroup: Container29#}
  SELECT 
    CAST(VOILATIONSCODE1 AS INTEGER) AS VIOLATIONCODE,
    VOILATIONSCODE2 AS VIOLATIONDESCRIPTION,
    * EXCLUDE ("VOILATIONSCODE1", "VOILATIONSCODE2")
  
  FROM Cleanse_43 AS in0

),

DSN_Chicago_SSM_44 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('13_Chicago_FI_Fact_Tables', 'DSN_Chicago_SSM_44') }}

),

AlteryxSelect_47 AS (

  {#VisualGroup: Container29#}
  SELECT 
    VIOLATIONSK AS VIOLATIONSK,
    VIOLATIONCODE AS VIOLATIONCODE,
    VIOLATIONDESCRIPTION AS VIOLATIONDESCRIPTION
  
  FROM DSN_Chicago_SSM_44 AS in0

),

Join_45_inner AS (

  {#VisualGroup: Container29#}
  SELECT 
    in1.VIOLATIONSK AS VIOLATIONSK,
    in1.VIOLATIONDESCRIPTION AS VIOLATIONDESCRIPTION,
    in0.DI_CREATEDATE AS DI_CREATEDATE,
    in0.VIOLATIONCOMMENT AS VIOLATIONCOMMENT,
    in1.VIOLATIONCODE AS VIOLATIONCODE,
    in0."INSPECTION ID" AS "INSPECTION ID",
    in0.DI_WORKFLOWDIRECTORY AS DI_WORKFLOWDIRECTORY
  
  FROM AlteryxSelect_50 AS in0
  INNER JOIN AlteryxSelect_47 AS in1
     ON ((in0.VIOLATIONCODE = in1.VIOLATIONCODE) AND (in0.VIOLATIONDESCRIPTION = in1.VIOLATIONDESCRIPTION))

),

AlteryxSelect_46 AS (

  {#VisualGroup: Container29#}
  SELECT 
    "INSPECTION ID" AS "INSPECTION ID",
    DI_CREATEDATE AS DI_CREATEDATE,
    DI_WORKFLOWDIRECTORY AS DI_WORKFLOWDIRECTORY,
    VIOLATIONCOMMENT AS VIOLATIONCOMMENT,
    VIOLATIONSK AS VIOLATIONSK,
    VIOLATIONCODE AS VIOLATIONCODE,
    VIOLATIONDESCRIPTION AS VIOLATIONDESCRIPTION
  
  FROM Join_45_inner AS in0

),

Unique_51 AS (

  {#VisualGroup: Container29#}
  SELECT * 
  
  FROM AlteryxSelect_46 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY "INSPECTION ID", VIOLATIONSK ORDER BY "INSPECTION ID", VIOLATIONSK) = 1

),

Formula_58_0 AS (

  {#VisualGroup: Container29#}
  SELECT 
    (TO_TIMESTAMP(CURRENT_TIMESTAMP)) AS DI_CREATEDATE,
    CAST({{ var('WORKFLOW_NAME') }} AS STRING) AS DI_WORKFLOWFILENAME,
    * EXCLUDE ("DI_CREATEDATE")
  
  FROM Unique_51 AS in0

)

SELECT *

FROM Formula_58_0
