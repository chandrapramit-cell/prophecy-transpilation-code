{{
  config({    
    "materialized": "table",
    "alias": "aka_GPD_UDDL_Wr_212",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Formula_207_0 AS (

  SELECT *
  
  FROM {{ ref('1_CRIS_Get_Source_Data__Formula_207_0')}}

),

AlteryxSelect_208 AS (

  SELECT 
    CATEGORY AS CATEGORY,
    PARENT AS PARENT,
    ELEMENT AS ELEMENT,
    ELEVEL AS ELEVEL,
    ELEVEL_DEF AS ELEVEL_DEF,
    DS AS DS
  
  FROM Formula_207_0 AS in0

),

AlteryxSelect_227 AS (

  SELECT 
    CAST(PARENT AS string) AS Parent,
    ELEMENT AS Element,
    CAST(ELEVEL AS string) AS ELevel,
    ELEVEL_DEF AS Elevel_Def,
    * EXCEPT (`Parent`, `Element`, `ELevel`, `Elevel_Def`)
  
  FROM AlteryxSelect_208 AS in0

),

FindReplace_233_allRules AS (

  SELECT collect_list(struct(ELevel AS ELevel, Element AS Element, Elevel_Def AS Elevel_Def, Parent AS Parent, DS AS DS)) AS _rules
  
  FROM AlteryxSelect_227 AS in0

),

FindReplace_229_allRules AS (

  SELECT collect_list(struct(Elevel_Def AS Elevel_Def, Parent AS Parent, DS AS DS, Element AS Element)) AS _rules
  
  FROM AlteryxSelect_227 AS in0

),

AlteryxSelect_209 AS (

  SELECT ELEMENT AS element
  
  FROM Formula_207_0 AS in0

),

Unique_211 AS (

  SELECT * 
  
  FROM AlteryxSelect_209 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY element ORDER BY element) = 1

),

Formula_210_0 AS (

  SELECT 
    CAST(1 AS INTEGER) AS target_level,
    *
  
  FROM Unique_211 AS in0

),

FindReplace_229_join AS (

  SELECT 
    in0.element AS element,
    in0.target_level AS target_level,
    in1._rules AS _rules
  
  FROM Formula_210_0 AS in0
  FULL JOIN FindReplace_229_allRules AS in1
     ON TRUE

),

FindReplace_229_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`Element`, concat('(?<=^|\\s)', concat('(?i)', rule['Element']), '(?=\\s|$)'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_229_join AS in0

),

FindReplace_229_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.Elevel_Def')) AS Elevel_Def,
    (GET_JSON_OBJECT(_extracted_rule, '$.Parent')) AS Parent,
    (GET_JSON_OBJECT(_extracted_rule, '$.DS')) AS DS,
    * EXCEPT (`_rules`, `_extracted_rule`)
  
  FROM FindReplace_229_0 AS in0

),

Formula_254_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((Parent IS NULL) AS BOOLEAN)
          THEN NULL
        ELSE (CONCAT((SUBSTRING(Elevel_Def, 1, 1)), '-', UPPER(DS), ' (', element, ')'))
      END
    ) AS string) AS COST_CENTER_DESC,
    *
  
  FROM FindReplace_229_reorg_0 AS in0

),

AlteryxSelect_255 AS (

  SELECT * EXCEPT (`DS`)
  
  FROM Formula_254_0 AS in0

),

FindReplace_230_allRules AS (

  SELECT collect_list(struct(ELEVEL AS ELEVEL, ELEVEL_DEF AS ELEVEL_DEF, PARENT AS PARENT, ELEMENT AS ELEMENT, DS AS DS)) AS _rules
  
  FROM AlteryxSelect_227 AS in0

),

FindReplace_230_join AS (

  SELECT 
    in0.Parent AS Parent,
    in0.COST_CENTER_DESC AS COST_CENTER_DESC,
    in0.target_level AS target_level,
    in0.element AS element,
    in1._rules AS _rules,
    in0.Elevel_Def AS Elevel_Def
  
  FROM AlteryxSelect_255 AS in0
  FULL JOIN FindReplace_230_allRules AS in1
     ON TRUE

),

FindReplace_230_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`PARENT`, concat('(?<=^|\\s)', concat('(?i)', rule['ELEMENT']), '(?=\\s|$)'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_230_join AS in0

),

FindReplace_230_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.PARENT')) AS PARENT,
    (GET_JSON_OBJECT(_extracted_rule, '$.ELEVEL')) AS ELEVEL,
    (GET_JSON_OBJECT(_extracted_rule, '$.ELEVEL_DEF')) AS ELEVEL_DEF,
    (GET_JSON_OBJECT(_extracted_rule, '$.DS')) AS DS,
    * EXCEPT (`_rules`, `_extracted_rule`, `parent`, `elevel_def`)
  
  FROM FindReplace_230_0 AS in0

),

Filter_231_reject AS (

  SELECT * 
  
  FROM FindReplace_230_reorg_0 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  target_level = CAST(ELEVEL AS INTEGER))
              ) OR (target_level IS NULL)
            )
            OR (ELEVEL IS NULL)
          )
          OR ((target_level = CAST(ELEVEL AS INTEGER)) IS NULL)
        )

),

FindReplace_246_allRules AS (

  SELECT collect_list(struct(ELevel AS ELevel, Element AS Element, Elevel_Def AS Elevel_Def, Parent AS Parent, DS AS DS)) AS _rules
  
  FROM AlteryxSelect_227 AS in0

),

AlteryxSelect_232 AS (

  {#Selects and renames elements while excluding specific fields to streamline a filter result for analysis.#}
  SELECT 
    element AS c_Element,
    PARENT AS Element,
    Elevel_Def AS Elevel_Def,
    Parent AS Parent,
    * EXCEPT (`Elevel_Def`, `ELevel`, `DS`, `Element`, `Parent`)
  
  FROM Filter_231_reject AS in0

),

FindReplace_233_join AS (

  SELECT 
    in0.Parent AS Parent,
    in0.COST_CENTER_DESC AS COST_CENTER_DESC,
    in0.target_level AS target_level,
    in0.c_Element AS c_Element,
    in0.Element AS Element,
    in1._rules AS _rules,
    in0.Elevel_Def AS Elevel_Def
  
  FROM AlteryxSelect_232 AS in0
  FULL JOIN FindReplace_233_allRules AS in1
     ON TRUE

),

FindReplace_233_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`Parent`, concat('(?<=^|\\s)', concat('(?i)', rule['Element']), '(?=\\s|$)'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_233_join AS in0

),

FindReplace_233_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.ELevel')) AS ELevel,
    (GET_JSON_OBJECT(_extracted_rule, '$.Elevel_Def')) AS Elevel_Def,
    (GET_JSON_OBJECT(_extracted_rule, '$.Parent')) AS Parent,
    (GET_JSON_OBJECT(_extracted_rule, '$.DS')) AS DS,
    * EXCEPT (`_rules`, `_extracted_rule`, `parent`, `elevel_def`)
  
  FROM FindReplace_233_0 AS in0

),

Filter_234_reject AS (

  SELECT * 
  
  FROM FindReplace_233_reorg_0 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  target_level = CAST(ELevel AS INTEGER))
              ) OR (target_level IS NULL)
            )
            OR (ELevel IS NULL)
          )
          OR ((target_level = CAST(ELevel AS INTEGER)) IS NULL)
        )

),

AlteryxSelect_235 AS (

  SELECT 
    Parent AS Element,
    Elevel_Def2 AS Elevel_Def,
    Parent2 AS Parent,
    * EXCEPT (`Element`, `Elevel_Def`, `ELevel`, `DS`, `Parent`)
  
  FROM Filter_234_reject AS in0

),

FindReplace_236_allRules AS (

  SELECT collect_list(struct(ELevel AS ELevel, Element AS Element, Elevel_Def AS Elevel_Def, Parent AS Parent, DS AS DS)) AS _rules
  
  FROM AlteryxSelect_227 AS in0

),

FindReplace_236_join AS (

  SELECT 
    in0.Parent AS Parent,
    in0.COST_CENTER_DESC AS COST_CENTER_DESC,
    in0.target_level AS target_level,
    in0.c_Element AS c_Element,
    in0.Element AS Element,
    in1._rules AS _rules,
    in0.Elevel_Def AS Elevel_Def
  
  FROM AlteryxSelect_235 AS in0
  FULL JOIN FindReplace_236_allRules AS in1
     ON TRUE

),

FindReplace_236_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`Parent`, concat('(?<=^|\\s)', concat('(?i)', rule['Element']), '(?=\\s|$)'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_236_join AS in0

),

FindReplace_236_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.ELevel')) AS ELevel,
    (GET_JSON_OBJECT(_extracted_rule, '$.Elevel_Def')) AS Elevel_Def,
    (GET_JSON_OBJECT(_extracted_rule, '$.Parent')) AS Parent,
    (GET_JSON_OBJECT(_extracted_rule, '$.DS')) AS DS,
    * EXCEPT (`_rules`, `_extracted_rule`, `parent`, `elevel_def`)
  
  FROM FindReplace_236_0 AS in0

),

Filter_237_reject AS (

  SELECT * 
  
  FROM FindReplace_236_reorg_0 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  target_level = CAST(ELevel AS INTEGER))
              ) OR (target_level IS NULL)
            )
            OR (ELevel IS NULL)
          )
          OR ((target_level = CAST(ELevel AS INTEGER)) IS NULL)
        )

),

AlteryxSelect_242 AS (

  SELECT 
    Parent AS Element,
    Elevel_Def2 AS Elevel_Def,
    Parent2 AS Parent,
    * EXCEPT (`Element`, `Elevel_Def`, `ELevel`, `DS`, `Parent`)
  
  FROM Filter_237_reject AS in0

),

FindReplace_243_allRules AS (

  SELECT collect_list(struct(ELevel AS ELevel, Element AS Element, Elevel_Def AS Elevel_Def, Parent AS Parent, DS AS DS)) AS _rules
  
  FROM AlteryxSelect_227 AS in0

),

FindReplace_243_join AS (

  SELECT 
    in0.Parent AS Parent,
    in0.COST_CENTER_DESC AS COST_CENTER_DESC,
    in0.target_level AS target_level,
    in0.c_Element AS c_Element,
    in0.Element AS Element,
    in1._rules AS _rules,
    in0.Elevel_Def AS Elevel_Def
  
  FROM AlteryxSelect_242 AS in0
  FULL JOIN FindReplace_243_allRules AS in1
     ON TRUE

),

FindReplace_243_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`Parent`, concat('(?<=^|\\s)', concat('(?i)', rule['Element']), '(?=\\s|$)'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_243_join AS in0

),

FindReplace_243_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.ELevel')) AS ELevel,
    (GET_JSON_OBJECT(_extracted_rule, '$.Elevel_Def')) AS Elevel_Def,
    (GET_JSON_OBJECT(_extracted_rule, '$.Parent')) AS Parent,
    (GET_JSON_OBJECT(_extracted_rule, '$.DS')) AS DS,
    * EXCEPT (`_rules`, `_extracted_rule`, `parent`, `elevel_def`)
  
  FROM FindReplace_243_0 AS in0

),

Filter_244_reject AS (

  SELECT * 
  
  FROM FindReplace_243_reorg_0 AS in0
  
  WHERE (
          (
            (
              (
                NOT(
                  target_level = CAST(ELevel AS INTEGER))
              ) OR (target_level IS NULL)
            )
            OR (ELevel IS NULL)
          )
          OR ((target_level = CAST(ELevel AS INTEGER)) IS NULL)
        )

),

AlteryxSelect_245 AS (

  SELECT 
    Parent AS Element,
    Elevel_Def2 AS Elevel_Def,
    Parent2 AS Parent,
    * EXCEPT (`Element`, `Elevel_Def`, `ELevel`, `DS`, `Parent`)
  
  FROM Filter_244_reject AS in0

),

FindReplace_246_join AS (

  SELECT 
    in0.Parent AS Parent,
    in0.COST_CENTER_DESC AS COST_CENTER_DESC,
    in0.target_level AS target_level,
    in0.c_Element AS c_Element,
    in0.Element AS Element,
    in1._rules AS _rules,
    in0.Elevel_Def AS Elevel_Def
  
  FROM AlteryxSelect_245 AS in0
  FULL JOIN FindReplace_246_allRules AS in1
     ON TRUE

),

FindReplace_246_0 AS (

  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract(`Parent`, concat('(?<=^|\\s)', concat('(?i)', rule['Element']), '(?=\\s|$)'), 0)) > 0), 
          1)), 
      '{}') AS _extracted_rule,
    *
  
  FROM FindReplace_246_join AS in0

),

FindReplace_246_reorg_0 AS (

  SELECT 
    (GET_JSON_OBJECT(_extracted_rule, '$.ELevel')) AS ELevel,
    (GET_JSON_OBJECT(_extracted_rule, '$.Elevel_Def')) AS Elevel_Def,
    (GET_JSON_OBJECT(_extracted_rule, '$.Parent')) AS Parent,
    (GET_JSON_OBJECT(_extracted_rule, '$.DS')) AS DS,
    * EXCEPT (`_rules`, `_extracted_rule`, `parent`, `elevel_def`)
  
  FROM FindReplace_246_0 AS in0

),

Filter_231 AS (

  SELECT * 
  
  FROM FindReplace_230_reorg_0 AS in0
  
  WHERE (target_level = CAST(ELEVEL AS INTEGER))

),

Union_250 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_231', 'Filter_231_reject'], 
      [
        '[{"name": "PARENT", "dataType": "String"}, {"name": "ELEVEL", "dataType": "String"}, {"name": "ELEVEL_DEF", "dataType": "String"}, {"name": "DS", "dataType": "String"}, {"name": "COST_CENTER_DESC", "dataType": "String"}, {"name": "target_level", "dataType": "Integer"}, {"name": "element", "dataType": "String"}]', 
        '[{"name": "PARENT", "dataType": "String"}, {"name": "ELEVEL", "dataType": "String"}, {"name": "ELEVEL_DEF", "dataType": "String"}, {"name": "DS", "dataType": "String"}, {"name": "COST_CENTER_DESC", "dataType": "String"}, {"name": "target_level", "dataType": "Integer"}, {"name": "element", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_244 AS (

  SELECT * 
  
  FROM FindReplace_243_reorg_0 AS in0
  
  WHERE (target_level = CAST(ELevel AS INTEGER))

),

Union_253 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_244_reject', 'Filter_244'], 
      [
        '[{"name": "c_Element", "dataType": "String"}, {"name": "Element", "dataType": "String"}, {"name": "target_level", "dataType": "Integer"}, {"name": "Parent", "dataType": "String"}, {"name": "COST_CENTER_DESC", "dataType": "String"}, {"name": "Elevel_Def", "dataType": "String"}, {"name": "ELevel", "dataType": "String"}, {"name": "DS", "dataType": "String"}]', 
        '[{"name": "c_Element", "dataType": "String"}, {"name": "Element", "dataType": "String"}, {"name": "target_level", "dataType": "Integer"}, {"name": "Parent", "dataType": "String"}, {"name": "COST_CENTER_DESC", "dataType": "String"}, {"name": "Elevel_Def", "dataType": "String"}, {"name": "ELevel", "dataType": "String"}, {"name": "DS", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_259 AS (

  SELECT *
  
  FROM Union_253 AS in0

),

AlteryxSelect_248 AS (

  SELECT 
    Parent AS ZONE_ID,
    * EXCEPT (`Target_Level`, `Element`, `Elevel_Def`, `ELevel`, `DS`, `Parent`)
  
  FROM Formula_259 AS in0

),

Filter_247 AS (

  SELECT * 
  
  FROM FindReplace_246_reorg_0 AS in0
  
  WHERE (target_level = CAST(ELevel AS INTEGER))

),

Formula_260 AS (

  SELECT *
  
  FROM Filter_247 AS in0

),

AlteryxSelect_249 AS (

  SELECT 
    Parent AS DIVISION_ID,
    * EXCEPT (`Target_Level`, `Element`, `Elevel_Def`, `ELevel`, `DS`, `Parent`)
  
  FROM Formula_260 AS in0

),

Formula_256 AS (

  SELECT *
  
  FROM Union_250 AS in0

),

AlteryxSelect_238 AS (

  SELECT 
    element AS c_Element,
    PARENT AS ORG_ID,
    * EXCEPT (`Target_Level`, `Elevel_Def`, `ELevel`, `DS`, `Element`, `Parent`)
  
  FROM Formula_256 AS in0

),

Filter_237 AS (

  SELECT * 
  
  FROM FindReplace_236_reorg_0 AS in0
  
  WHERE (target_level = CAST(ELevel AS INTEGER))

),

Union_252 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_237_reject', 'Filter_237'], 
      [
        '[{"name": "c_Element", "dataType": "String"}, {"name": "Element", "dataType": "String"}, {"name": "target_level", "dataType": "Integer"}, {"name": "Parent", "dataType": "String"}, {"name": "COST_CENTER_DESC", "dataType": "String"}, {"name": "Elevel_Def", "dataType": "String"}, {"name": "ELevel", "dataType": "String"}, {"name": "DS", "dataType": "String"}]', 
        '[{"name": "c_Element", "dataType": "String"}, {"name": "Element", "dataType": "String"}, {"name": "target_level", "dataType": "Integer"}, {"name": "Parent", "dataType": "String"}, {"name": "COST_CENTER_DESC", "dataType": "String"}, {"name": "Elevel_Def", "dataType": "String"}, {"name": "ELevel", "dataType": "String"}, {"name": "DS", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_258 AS (

  SELECT *
  
  FROM Union_252 AS in0

),

AlteryxSelect_240 AS (

  SELECT 
    Parent AS LINE_ID,
    * EXCEPT (`Target_Level`, `Element`, `Elevel_Def`, `ELevel`, `DS`, `Parent`)
  
  FROM Formula_258 AS in0

),

Filter_234 AS (

  SELECT * 
  
  FROM FindReplace_233_reorg_0 AS in0
  
  WHERE (target_level = CAST(ELevel AS INTEGER))

),

Union_251 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_234_reject', 'Filter_234'], 
      [
        '[{"name": "ELevel", "dataType": "String"}, {"name": "Elevel_Def", "dataType": "String"}, {"name": "Parent", "dataType": "String"}, {"name": "DS", "dataType": "String"}, {"name": "COST_CENTER_DESC", "dataType": "String"}, {"name": "target_level", "dataType": "Integer"}, {"name": "c_Element", "dataType": "String"}, {"name": "Element", "dataType": "String"}]', 
        '[{"name": "ELevel", "dataType": "String"}, {"name": "Elevel_Def", "dataType": "String"}, {"name": "Parent", "dataType": "String"}, {"name": "DS", "dataType": "String"}, {"name": "COST_CENTER_DESC", "dataType": "String"}, {"name": "target_level", "dataType": "Integer"}, {"name": "c_Element", "dataType": "String"}, {"name": "Element", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_257 AS (

  SELECT *
  
  FROM Union_251 AS in0

),

AlteryxSelect_239 AS (

  SELECT 
    Parent AS FUNCTION_ID,
    * EXCEPT (`Target_Level`, `Element`, `Elevel_Def`, `ELevel`, `DS`, `Parent`)
  
  FROM Formula_257 AS in0

),

JoinMultiple_241 AS (

  SELECT 
    in0.COST_CENTER_DESC AS COST_CENTER_DESC,
    in0.c_Element AS c_Element,
    in1.FUNCTION_ID AS FUNCTION_ID,
    in4.DIVISION_ID AS DIVISION_ID,
    in3.ZONE_ID AS ZONE_ID,
    in2.LINE_ID AS LINE_ID,
    in0.ORG_ID AS ORG_ID
  
  FROM AlteryxSelect_238 AS in0
  FULL JOIN AlteryxSelect_239 AS in1
     ON (in0.c_Element = in1.c_Element)
  FULL JOIN AlteryxSelect_240 AS in2
     ON (coalesce(in0.c_Element, in1.c_Element) = in2.c_Element)
  FULL JOIN AlteryxSelect_248 AS in3
     ON (coalesce(in0.c_Element, in1.c_Element, in2.c_Element) = in3.c_Element)
  FULL JOIN AlteryxSelect_249 AS in4
     ON (coalesce(in0.c_Element, in1.c_Element, in2.c_Element, in3.c_Element) = in4.c_Element)

),

AlteryxSelect_261 AS (

  SELECT 
    c_Element AS c_Element,
    COST_CENTER_DESC AS COST_CENTER_DESC,
    ORG_ID AS ORG_ID,
    ORGANIZATION AS ORGANIZATION,
    FUNCTION_ID AS FUNCTION_ID,
    variableFUNCTION AS variableFUNCTION,
    LINE_ID AS LINE_ID,
    LINE AS LINE,
    ZONE_ID AS ZONE_ID,
    ZONE AS ZONE,
    DIVISION_ID AS DIVISION_ID,
    DIVISION AS DIVISION,
    * EXCEPT (`c_Element`, `COST_CENTER_DESC`, `ORG_ID`, `FUNCTION_ID`, `LINE_ID`, `ZONE_ID`, `DIVISION_ID`)
  
  FROM JoinMultiple_241 AS in0

),

AlteryxSelect_213 AS (

  SELECT 
    c_Element AS cost_center,
    COST_CENTER_DESC AS COST_CENTER_DESC,
    CAST(DIVISION_ID AS string) AS DIVISION_ID,
    DIVISION AS DIVISION,
    CAST(ZONE_ID AS string) AS ZONE_ID,
    ZONE AS ZONE,
    CAST(LINE_ID AS string) AS LINE_ID,
    LINE AS LINE,
    CAST(FUNCTION_ID AS string) AS FUNCTION_ID,
    variableFUNCTION AS variableFUNCTION,
    CAST(ORG_ID AS string) AS ORG_ID,
    ORGANIZATION AS ORGANIZATION,
    * EXCEPT (`COST_CENTER_DESC`, 
    `DIVISION_ID`, 
    `DIVISION`, 
    `ZONE_ID`, 
    `ZONE`, 
    `LINE_ID`, 
    `LINE`, 
    `FUNCTION_ID`, 
    `variableFUNCTION`, 
    `ORG_ID`, 
    `ORGANIZATION`, 
    `c_Element`)
  
  FROM AlteryxSelect_261 AS in0

),

Formula_214_0 AS (

  SELECT 
    (TO_TIMESTAMP((TO_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'UTC')))) AS loaded_date,
    *
  
  FROM AlteryxSelect_213 AS in0

)

SELECT *

FROM Formula_214_0
