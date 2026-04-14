{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Attributecodes__225 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Option_mapping_SKU', 'Attributecodes__225') }}

),

FindReplace_228_allRules AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT array_agg(struct("ATTRIBUTE CODE" AS "ATTRIBUTE CODE", ATTRIBUTE AS ATTRIBUTE)) AS _RULES
  
  FROM Attributecodes__225 AS in0

),

FindReplace_180_allRules AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT array_agg(struct("ATTRIBUTE CODE" AS "ATTRIBUTE CODE", ATTRIBUTE AS ATTRIBUTE)) AS _RULES
  
  FROM Attributecodes__225 AS in0

),

FindReplace_182_allRules AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT array_agg(struct("ATTRIBUTE CODE" AS "ATTRIBUTE CODE", ATTRIBUTE AS ATTRIBUTE)) AS _RULES
  
  FROM Attributecodes__225 AS in0

),

FindReplace_227_allRules AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT array_agg(struct("ATTRIBUTE CODE" AS "ATTRIBUTE CODE", ATTRIBUTE AS ATTRIBUTE)) AS _RULES
  
  FROM Attributecodes__225 AS in0

),

FindReplace_179_allRules AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT array_agg(struct("ATTRIBUTE CODE" AS "ATTRIBUTE CODE", ATTRIBUTE AS ATTRIBUTE)) AS _RULES
  
  FROM Attributecodes__225 AS in0

),

FindReplace_224_allRules AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT array_agg(struct("ATTRIBUTE CODE" AS "ATTRIBUTE CODE", ATTRIBUTE AS ATTRIBUTE)) AS _RULES
  
  FROM Attributecodes__225 AS in0

),

DynamicRename_255 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Option_mapping_SKU', 'DynamicRename_255') }}

),

DynamicSelect_256 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    "LISTING PRICE" AS "LISTING PRICE",
    "AGE GROUP" AS "AGE GROUP",
    "DETAILED PRODUCT GROUP" AS "DETAILED PRODUCT GROUP",
    "COLOR GROUP" AS "COLOR GROUP",
    UPPER_MATERIAL AS UPPER_MATERIAL,
    "19" AS "19",
    "23" AS "23",
    "SIZE RANGE" AS "SIZE RANGE",
    "PRICE GROUP" AS "PRICE GROUP",
    "SPECIALIZED FUNCTION" AS "SPECIALIZED FUNCTION",
    "HEEL HEIGHT" AS "HEEL HEIGHT",
    "15" AS "15",
    COLLECTION AS COLLECTION,
    "LIFESTYLE GROUP" AS "LIFESTYLE GROUP",
    "DESIGN INSPIRATION" AS "DESIGN INSPIRATION",
    "22" AS "22",
    "PRODUCT GROUP" AS "PRODUCT GROUP",
    "MG GROUP" AS "MG GROUP",
    "MCH EXPLANATION" AS "MCH EXPLANATION",
    SKU AS SKU,
    SIZE AS SIZE,
    F45 AS F45,
    "COPYRIGHT GROUP" AS "COPYRIGHT GROUP",
    "16" AS "16",
    "21" AS "21",
    "43" AS "43",
    "MOLD CODE" AS "MOLD CODE",
    "PRODUCT LINE" AS "PRODUCT LINE",
    "STYLE COLOR" AS "STYLE COLOR",
    "LAUNCH SEASON" AS "LAUNCH SEASON",
    "SIZE GROUP" AS "SIZE GROUP",
    "42" AS "42",
    "ACTIVITY GROUP" AS "ACTIVITY GROUP",
    "20" AS "20",
    "MC EXPLANATION" AS "MC EXPLANATION",
    F47 AS F47,
    "SIMILAR GROUP" AS "SIMILAR GROUP",
    "18" AS "18",
    "MCH GROUP" AS "MCH GROUP",
    UNIT AS UNIT,
    BRAND AS BRAND,
    SOLE_TYPE AS SOLE_TYPE,
    "NAME EXPLANATION" AS "NAME EXPLANATION",
    "41" AS "41",
    STYLE AS STYLE,
    GENDER AS GENDER,
    COLOR AS COLOR,
    "SHOE STYLE" AS "SHOE STYLE",
    "SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE" AS "SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE",
    "DEDICATED FUNCTIONS" AS "DEDICATED FUNCTIONS",
    "IMAGE COPYRIGHT" AS "IMAGE COPYRIGHT"
  
  FROM DynamicRename_255 AS in0

),

Formula_176_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST((CONCAT(GENDER, "SHOE STYLE")) AS STRING) AS GTCL,
    1 AS "COLOR GROUP",
    1 AS "SPECIALIZED FUNCTION",
    1 AS "AGE GROUP",
    1 AS UPPER_MATERIAL,
    1 AS "HEEL HEIGHT",
    1 AS "LIFESTYLE GROUP",
    1 AS "SIMILAR GROUP",
    * EXCLUDE ("AGE GROUP", 
    "COLOR GROUP", 
    "UPPER_MATERIAL", 
    "SPECIALIZED FUNCTION", 
    "HEEL HEIGHT", 
    "LIFESTYLE GROUP", 
    "SIMILAR GROUP")
  
  FROM DynamicSelect_256 AS in0

),

Unique_178 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM Formula_176_0 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY SKU ORDER BY SKU) = 1

),

FindReplace_224_join AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    in0.F47 AS F47,
    in0."LISTING PRICE" AS "LISTING PRICE",
    in0."AGE GROUP" AS "AGE GROUP",
    in0."DEDICATED FUNCTIONS" AS "DEDICATED FUNCTIONS",
    in0.SKU AS SKU,
    in0."DETAILED PRODUCT GROUP" AS "DETAILED PRODUCT GROUP",
    in0."19" AS "19",
    in0."23" AS "23",
    in0.COLLECTION AS COLLECTION,
    in0.SIZE AS SIZE,
    in0."NAME EXPLANATION" AS "NAME EXPLANATION",
    in0."15" AS "15",
    in0."PRODUCT GROUP" AS "PRODUCT GROUP",
    in0."SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE" AS "SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE",
    in0."SPECIALIZED FUNCTION" AS "SPECIALIZED FUNCTION",
    in0.STYLE AS STYLE,
    in0."LIFESTYLE GROUP" AS "LIFESTYLE GROUP",
    in0."22" AS "22",
    in0."SIZE RANGE" AS "SIZE RANGE",
    in0."16" AS "16",
    in0."HEEL HEIGHT" AS "HEEL HEIGHT",
    in0.SOLE_TYPE AS SOLE_TYPE,
    in0.GTCL AS GTCL,
    in0."MCH GROUP" AS "MCH GROUP",
    in0.COLOR AS COLOR,
    in0."DESIGN INSPIRATION" AS "DESIGN INSPIRATION",
    in0."21" AS "21",
    in0.BRAND AS BRAND,
    in0."43" AS "43",
    in0."PRICE GROUP" AS "PRICE GROUP",
    in0."IMAGE COPYRIGHT" AS "IMAGE COPYRIGHT",
    in0."SHOE STYLE" AS "SHOE STYLE",
    in0."PRODUCT LINE" AS "PRODUCT LINE",
    in0."COPYRIGHT GROUP" AS "COPYRIGHT GROUP",
    in0."42" AS "42",
    in0."MC EXPLANATION" AS "MC EXPLANATION",
    in0."20" AS "20",
    in0.UPPER_MATERIAL AS UPPER_MATERIAL,
    in1._RULES AS _RULES,
    in0."SIMILAR GROUP" AS "SIMILAR GROUP",
    in0."MOLD CODE" AS "MOLD CODE",
    in0.F45 AS F45,
    in0."STYLE COLOR" AS "STYLE COLOR",
    in0.UNIT AS UNIT,
    in0."LAUNCH SEASON" AS "LAUNCH SEASON",
    in0."18" AS "18",
    in0."COLOR GROUP" AS "COLOR GROUP",
    in0."41" AS "41",
    in0."SIZE GROUP" AS "SIZE GROUP",
    in0."MG GROUP" AS "MG GROUP",
    in0."MCH EXPLANATION" AS "MCH EXPLANATION",
    in0."ACTIVITY GROUP" AS "ACTIVITY GROUP",
    in0.GENDER AS GENDER
  
  FROM Unique_178 AS in0
  FULL JOIN FindReplace_224_allRules AS in1
     ON TRUE

),

FindReplace_224_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract("PRICE GROUP", rule['ATTRIBUTE'], 0)) > 0), 1)), 
      '{}') AS _EXTRACTED_RULE,
    *
  
  FROM FindReplace_224_join AS in0

),

FindReplace_224_reorg_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (GET_JSON_OBJECT(_EXTRACTED_RULE, '$.Attribute code')) AS "ATTRIBUTE CODE",
    * EXCLUDE ("_RULES", "_EXTRACTED_RULE")
  
  FROM FindReplace_224_0 AS in0

),

Formula_214_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST('If IsNull([Attribute code]) then +"P**" else [Attribute code] endif' AS STRING) AS "ATTRIBUTE CODE",
    * EXCLUDE ("ATTRIBUTE CODE")
  
  FROM FindReplace_224_reorg_0 AS in0

),

FindReplace_179_join AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    in0.F47 AS F47,
    in0."LISTING PRICE" AS "LISTING PRICE",
    in0."AGE GROUP" AS "AGE GROUP",
    in0."DEDICATED FUNCTIONS" AS "DEDICATED FUNCTIONS",
    in0.SKU AS SKU,
    in0."DETAILED PRODUCT GROUP" AS "DETAILED PRODUCT GROUP",
    in0."19" AS "19",
    in0."23" AS "23",
    in0.COLLECTION AS COLLECTION,
    in0.SIZE AS SIZE,
    in0."NAME EXPLANATION" AS "NAME EXPLANATION",
    in0."ATTRIBUTE CODE" AS "ATTRIBUTE CODE",
    in0."15" AS "15",
    in0."PRODUCT GROUP" AS "PRODUCT GROUP",
    in0."SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE" AS "SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE",
    in0."SPECIALIZED FUNCTION" AS "SPECIALIZED FUNCTION",
    in0.STYLE AS STYLE,
    in0."LIFESTYLE GROUP" AS "LIFESTYLE GROUP",
    in0."22" AS "22",
    in0."SIZE RANGE" AS "SIZE RANGE",
    in0."16" AS "16",
    in0."HEEL HEIGHT" AS "HEEL HEIGHT",
    in0.SOLE_TYPE AS SOLE_TYPE,
    in0.GTCL AS GTCL,
    in0."MCH GROUP" AS "MCH GROUP",
    in0.COLOR AS COLOR,
    in0."DESIGN INSPIRATION" AS "DESIGN INSPIRATION",
    in0."21" AS "21",
    in0.BRAND AS BRAND,
    in0."43" AS "43",
    in0."PRICE GROUP" AS "PRICE GROUP",
    in0."IMAGE COPYRIGHT" AS "IMAGE COPYRIGHT",
    in0."SHOE STYLE" AS "SHOE STYLE",
    in0."PRODUCT LINE" AS "PRODUCT LINE",
    in0."COPYRIGHT GROUP" AS "COPYRIGHT GROUP",
    in0."42" AS "42",
    in0."MC EXPLANATION" AS "MC EXPLANATION",
    in0."20" AS "20",
    in0.UPPER_MATERIAL AS UPPER_MATERIAL,
    in1._RULES AS _RULES,
    in0."SIMILAR GROUP" AS "SIMILAR GROUP",
    in0."MOLD CODE" AS "MOLD CODE",
    in0.F45 AS F45,
    in0."STYLE COLOR" AS "STYLE COLOR",
    in0.UNIT AS UNIT,
    in0."LAUNCH SEASON" AS "LAUNCH SEASON",
    in0."18" AS "18",
    in0."COLOR GROUP" AS "COLOR GROUP",
    in0."41" AS "41",
    in0."SIZE GROUP" AS "SIZE GROUP",
    in0."MG GROUP" AS "MG GROUP",
    in0."MCH EXPLANATION" AS "MCH EXPLANATION",
    in0."ACTIVITY GROUP" AS "ACTIVITY GROUP",
    in0.GENDER AS GENDER
  
  FROM Formula_214_0 AS in0
  FULL JOIN FindReplace_179_allRules AS in1
     ON TRUE

),

FindReplace_179_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract("COLOR GROUP", rule['ATTRIBUTE'], 0)) > 0), 1)), 
      '{}') AS _EXTRACTED_RULE,
    *
  
  FROM FindReplace_179_join AS in0

),

FindReplace_179_reorg_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (GET_JSON_OBJECT(_EXTRACTED_RULE, '$.Attribute code')) AS "ATTRIBUTE CODE2",
    * EXCLUDE ("_RULES", "_EXTRACTED_RULE")
  
  FROM FindReplace_179_0 AS in0

),

Formula_215_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST('If IsNull([Attribute code2]) then +"C**" else [Attribute code2] endif' AS STRING) AS "ATTRIBUTE CODE2",
    *
  
  FROM FindReplace_179_reorg_0 AS in0

),

FindReplace_227_join AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    in0.F47 AS F47,
    in0."LISTING PRICE" AS "LISTING PRICE",
    in0."AGE GROUP" AS "AGE GROUP",
    in0."DEDICATED FUNCTIONS" AS "DEDICATED FUNCTIONS",
    in0.SKU AS SKU,
    in0."DETAILED PRODUCT GROUP" AS "DETAILED PRODUCT GROUP",
    in0."19" AS "19",
    in0."23" AS "23",
    in0.COLLECTION AS COLLECTION,
    in0.SIZE AS SIZE,
    in0."NAME EXPLANATION" AS "NAME EXPLANATION",
    in0."ATTRIBUTE CODE" AS "ATTRIBUTE CODE",
    in0."15" AS "15",
    in0."PRODUCT GROUP" AS "PRODUCT GROUP",
    in0."SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE" AS "SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE",
    in0."SPECIALIZED FUNCTION" AS "SPECIALIZED FUNCTION",
    in0.STYLE AS STYLE,
    in0."LIFESTYLE GROUP" AS "LIFESTYLE GROUP",
    in0."22" AS "22",
    in0."SIZE RANGE" AS "SIZE RANGE",
    in0."16" AS "16",
    in0."HEEL HEIGHT" AS "HEEL HEIGHT",
    in0.SOLE_TYPE AS SOLE_TYPE,
    in0.GTCL AS GTCL,
    in0."MCH GROUP" AS "MCH GROUP",
    in0.COLOR AS COLOR,
    in0."DESIGN INSPIRATION" AS "DESIGN INSPIRATION",
    in0."21" AS "21",
    in0.BRAND AS BRAND,
    in0."43" AS "43",
    in0."PRICE GROUP" AS "PRICE GROUP",
    in0."IMAGE COPYRIGHT" AS "IMAGE COPYRIGHT",
    in0."SHOE STYLE" AS "SHOE STYLE",
    in0."PRODUCT LINE" AS "PRODUCT LINE",
    in0."COPYRIGHT GROUP" AS "COPYRIGHT GROUP",
    in0."42" AS "42",
    in0."ATTRIBUTE CODE2" AS "ATTRIBUTE CODE2",
    in0."MC EXPLANATION" AS "MC EXPLANATION",
    in0."20" AS "20",
    in0.UPPER_MATERIAL AS UPPER_MATERIAL,
    in1._RULES AS _RULES,
    in0."SIMILAR GROUP" AS "SIMILAR GROUP",
    in0."MOLD CODE" AS "MOLD CODE",
    in0.F45 AS F45,
    in0."STYLE COLOR" AS "STYLE COLOR",
    in0.UNIT AS UNIT,
    in0."LAUNCH SEASON" AS "LAUNCH SEASON",
    in0."18" AS "18",
    in0."COLOR GROUP" AS "COLOR GROUP",
    in0."41" AS "41",
    in0."SIZE GROUP" AS "SIZE GROUP",
    in0."MG GROUP" AS "MG GROUP",
    in0."MCH EXPLANATION" AS "MCH EXPLANATION",
    in0."ACTIVITY GROUP" AS "ACTIVITY GROUP",
    in0.GENDER AS GENDER
  
  FROM Formula_215_0 AS in0
  FULL JOIN FindReplace_227_allRules AS in1
     ON TRUE

),

FindReplace_227_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract("AGE GROUP", rule['ATTRIBUTE'], 0)) > 0), 1)), 
      '{}') AS _EXTRACTED_RULE,
    *
  
  FROM FindReplace_227_join AS in0

),

FindReplace_227_reorg_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (GET_JSON_OBJECT(_EXTRACTED_RULE, '$.Attribute code')) AS "ATTRIBUTE CODE2",
    * EXCLUDE ("_RULES", "_EXTRACTED_RULE", "ATTRIBUTE CODE2")
  
  FROM FindReplace_227_0 AS in0

),

Formula_216_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST('If IsNull([Attribute code3]) then +"A**" else [Attribute code3] endif' AS STRING) AS "ATTRIBUTE CODE3",
    *
  
  FROM FindReplace_227_reorg_0 AS in0

),

FindReplace_182_join AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    in0.F47 AS F47,
    in0."LISTING PRICE" AS "LISTING PRICE",
    in0."AGE GROUP" AS "AGE GROUP",
    in0."DEDICATED FUNCTIONS" AS "DEDICATED FUNCTIONS",
    in0.SKU AS SKU,
    in0."DETAILED PRODUCT GROUP" AS "DETAILED PRODUCT GROUP",
    in0."19" AS "19",
    in0."23" AS "23",
    in0.COLLECTION AS COLLECTION,
    in0.SIZE AS SIZE,
    in0."NAME EXPLANATION" AS "NAME EXPLANATION",
    in0."ATTRIBUTE CODE" AS "ATTRIBUTE CODE",
    in0."15" AS "15",
    in0."PRODUCT GROUP" AS "PRODUCT GROUP",
    in0."SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE" AS "SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE",
    in0."SPECIALIZED FUNCTION" AS "SPECIALIZED FUNCTION",
    in0.STYLE AS STYLE,
    in0."LIFESTYLE GROUP" AS "LIFESTYLE GROUP",
    in0."22" AS "22",
    in0."SIZE RANGE" AS "SIZE RANGE",
    in0."16" AS "16",
    in0."HEEL HEIGHT" AS "HEEL HEIGHT",
    in0.SOLE_TYPE AS SOLE_TYPE,
    in0.GTCL AS GTCL,
    in0."MCH GROUP" AS "MCH GROUP",
    in0.COLOR AS COLOR,
    in0."DESIGN INSPIRATION" AS "DESIGN INSPIRATION",
    in0."21" AS "21",
    in0.BRAND AS BRAND,
    in0."43" AS "43",
    in0."PRICE GROUP" AS "PRICE GROUP",
    in0."IMAGE COPYRIGHT" AS "IMAGE COPYRIGHT",
    in0."SHOE STYLE" AS "SHOE STYLE",
    in0."PRODUCT LINE" AS "PRODUCT LINE",
    in0."COPYRIGHT GROUP" AS "COPYRIGHT GROUP",
    in0."42" AS "42",
    in0."ATTRIBUTE CODE2" AS "ATTRIBUTE CODE2",
    in0."MC EXPLANATION" AS "MC EXPLANATION",
    in0."20" AS "20",
    in0.UPPER_MATERIAL AS UPPER_MATERIAL,
    in1._RULES AS _RULES,
    in0."SIMILAR GROUP" AS "SIMILAR GROUP",
    in0."MOLD CODE" AS "MOLD CODE",
    in0.F45 AS F45,
    in0."STYLE COLOR" AS "STYLE COLOR",
    in0.UNIT AS UNIT,
    in0."LAUNCH SEASON" AS "LAUNCH SEASON",
    in0."18" AS "18",
    in0."COLOR GROUP" AS "COLOR GROUP",
    in0."41" AS "41",
    in0."SIZE GROUP" AS "SIZE GROUP",
    in0."MG GROUP" AS "MG GROUP",
    in0."MCH EXPLANATION" AS "MCH EXPLANATION",
    in0."ATTRIBUTE CODE3" AS "ATTRIBUTE CODE3",
    in0."ACTIVITY GROUP" AS "ACTIVITY GROUP",
    in0.GENDER AS GENDER
  
  FROM Formula_216_0 AS in0
  FULL JOIN FindReplace_182_allRules AS in1
     ON TRUE

),

FindReplace_182_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract("LIFESTYLE GROUP", rule['ATTRIBUTE'], 0)) > 0), 1)), 
      '{}') AS _EXTRACTED_RULE,
    *
  
  FROM FindReplace_182_join AS in0

),

FindReplace_182_reorg_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (GET_JSON_OBJECT(_EXTRACTED_RULE, '$.Attribute code')) AS "ATTRIBUTE CODE2",
    * EXCLUDE ("_RULES", "_EXTRACTED_RULE", "ATTRIBUTE CODE2")
  
  FROM FindReplace_182_0 AS in0

),

Formula_217_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST('If IsNull([Attribute code4]) then +"S**" else [Attribute code4] endif' AS STRING) AS "ATTRIBUTE CODE4",
    *
  
  FROM FindReplace_182_reorg_0 AS in0

),

FindReplace_180_join AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    in0.F47 AS F47,
    in0."LISTING PRICE" AS "LISTING PRICE",
    in0."AGE GROUP" AS "AGE GROUP",
    in0."DEDICATED FUNCTIONS" AS "DEDICATED FUNCTIONS",
    in0.SKU AS SKU,
    in0."DETAILED PRODUCT GROUP" AS "DETAILED PRODUCT GROUP",
    in0."19" AS "19",
    in0."23" AS "23",
    in0.COLLECTION AS COLLECTION,
    in0.SIZE AS SIZE,
    in0."NAME EXPLANATION" AS "NAME EXPLANATION",
    in0."ATTRIBUTE CODE" AS "ATTRIBUTE CODE",
    in0."15" AS "15",
    in0."PRODUCT GROUP" AS "PRODUCT GROUP",
    in0."SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE" AS "SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE",
    in0."ATTRIBUTE CODE4" AS "ATTRIBUTE CODE4",
    in0."SPECIALIZED FUNCTION" AS "SPECIALIZED FUNCTION",
    in0.STYLE AS STYLE,
    in0."LIFESTYLE GROUP" AS "LIFESTYLE GROUP",
    in0."22" AS "22",
    in0."SIZE RANGE" AS "SIZE RANGE",
    in0."16" AS "16",
    in0."HEEL HEIGHT" AS "HEEL HEIGHT",
    in0.SOLE_TYPE AS SOLE_TYPE,
    in0.GTCL AS GTCL,
    in0."MCH GROUP" AS "MCH GROUP",
    in0.COLOR AS COLOR,
    in0."DESIGN INSPIRATION" AS "DESIGN INSPIRATION",
    in0."21" AS "21",
    in0.BRAND AS BRAND,
    in0."43" AS "43",
    in0."PRICE GROUP" AS "PRICE GROUP",
    in0."IMAGE COPYRIGHT" AS "IMAGE COPYRIGHT",
    in0."SHOE STYLE" AS "SHOE STYLE",
    in0."PRODUCT LINE" AS "PRODUCT LINE",
    in0."COPYRIGHT GROUP" AS "COPYRIGHT GROUP",
    in0."42" AS "42",
    in0."ATTRIBUTE CODE2" AS "ATTRIBUTE CODE2",
    in0."MC EXPLANATION" AS "MC EXPLANATION",
    in0."20" AS "20",
    in0.UPPER_MATERIAL AS UPPER_MATERIAL,
    in1._RULES AS _RULES,
    in0."SIMILAR GROUP" AS "SIMILAR GROUP",
    in0."MOLD CODE" AS "MOLD CODE",
    in0.F45 AS F45,
    in0."STYLE COLOR" AS "STYLE COLOR",
    in0.UNIT AS UNIT,
    in0."LAUNCH SEASON" AS "LAUNCH SEASON",
    in0."18" AS "18",
    in0."COLOR GROUP" AS "COLOR GROUP",
    in0."41" AS "41",
    in0."SIZE GROUP" AS "SIZE GROUP",
    in0."MG GROUP" AS "MG GROUP",
    in0."MCH EXPLANATION" AS "MCH EXPLANATION",
    in0."ATTRIBUTE CODE3" AS "ATTRIBUTE CODE3",
    in0."ACTIVITY GROUP" AS "ACTIVITY GROUP",
    in0.GENDER AS GENDER
  
  FROM Formula_217_0 AS in0
  FULL JOIN FindReplace_180_allRules AS in1
     ON TRUE

),

FindReplace_180_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract("HEEL HEIGHT", rule['ATTRIBUTE'], 0)) > 0), 1)), 
      '{}') AS _EXTRACTED_RULE,
    *
  
  FROM FindReplace_180_join AS in0

),

FindReplace_180_reorg_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (GET_JSON_OBJECT(_EXTRACTED_RULE, '$.Attribute code')) AS "ATTRIBUTE CODE2",
    * EXCLUDE ("_RULES", "_EXTRACTED_RULE", "ATTRIBUTE CODE2")
  
  FROM FindReplace_180_0 AS in0

),

Formula_218_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST('If IsNull([Attribute code5]) then +"H**" else [Attribute code5] endif' AS STRING) AS "ATTRIBUTE CODE5",
    *
  
  FROM FindReplace_180_reorg_0 AS in0

),

FindReplace_181_allRules AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT array_agg(struct("ATTRIBUTE CODE" AS "ATTRIBUTE CODE", ATTRIBUTE AS ATTRIBUTE)) AS _RULES
  
  FROM Attributecodes__225 AS in0

),

FindReplace_181_join AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    in0.F47 AS F47,
    in0."LISTING PRICE" AS "LISTING PRICE",
    in0."AGE GROUP" AS "AGE GROUP",
    in0."DEDICATED FUNCTIONS" AS "DEDICATED FUNCTIONS",
    in0.SKU AS SKU,
    in0."DETAILED PRODUCT GROUP" AS "DETAILED PRODUCT GROUP",
    in0."19" AS "19",
    in0."23" AS "23",
    in0.COLLECTION AS COLLECTION,
    in0.SIZE AS SIZE,
    in0."NAME EXPLANATION" AS "NAME EXPLANATION",
    in0."ATTRIBUTE CODE" AS "ATTRIBUTE CODE",
    in0."15" AS "15",
    in0."PRODUCT GROUP" AS "PRODUCT GROUP",
    in0."SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE" AS "SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE",
    in0."ATTRIBUTE CODE4" AS "ATTRIBUTE CODE4",
    in0."SPECIALIZED FUNCTION" AS "SPECIALIZED FUNCTION",
    in0.STYLE AS STYLE,
    in0."LIFESTYLE GROUP" AS "LIFESTYLE GROUP",
    in0."22" AS "22",
    in0."SIZE RANGE" AS "SIZE RANGE",
    in0."16" AS "16",
    in0."HEEL HEIGHT" AS "HEEL HEIGHT",
    in0.SOLE_TYPE AS SOLE_TYPE,
    in0.GTCL AS GTCL,
    in0."MCH GROUP" AS "MCH GROUP",
    in0.COLOR AS COLOR,
    in0."DESIGN INSPIRATION" AS "DESIGN INSPIRATION",
    in0."21" AS "21",
    in0.BRAND AS BRAND,
    in0."43" AS "43",
    in0."PRICE GROUP" AS "PRICE GROUP",
    in0."ATTRIBUTE CODE5" AS "ATTRIBUTE CODE5",
    in0."IMAGE COPYRIGHT" AS "IMAGE COPYRIGHT",
    in0."SHOE STYLE" AS "SHOE STYLE",
    in0."PRODUCT LINE" AS "PRODUCT LINE",
    in0."COPYRIGHT GROUP" AS "COPYRIGHT GROUP",
    in0."42" AS "42",
    in0."ATTRIBUTE CODE2" AS "ATTRIBUTE CODE2",
    in0."MC EXPLANATION" AS "MC EXPLANATION",
    in0."20" AS "20",
    in0.UPPER_MATERIAL AS UPPER_MATERIAL,
    in1._RULES AS _RULES,
    in0."SIMILAR GROUP" AS "SIMILAR GROUP",
    in0."MOLD CODE" AS "MOLD CODE",
    in0.F45 AS F45,
    in0."STYLE COLOR" AS "STYLE COLOR",
    in0.UNIT AS UNIT,
    in0."LAUNCH SEASON" AS "LAUNCH SEASON",
    in0."18" AS "18",
    in0."COLOR GROUP" AS "COLOR GROUP",
    in0."41" AS "41",
    in0."SIZE GROUP" AS "SIZE GROUP",
    in0."MG GROUP" AS "MG GROUP",
    in0."MCH EXPLANATION" AS "MCH EXPLANATION",
    in0."ATTRIBUTE CODE3" AS "ATTRIBUTE CODE3",
    in0."ACTIVITY GROUP" AS "ACTIVITY GROUP",
    in0.GENDER AS GENDER
  
  FROM Formula_218_0 AS in0
  FULL JOIN FindReplace_181_allRules AS in1
     ON TRUE

),

FindReplace_181_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    coalesce(
      to_json(
        element_at(
          filter(
            _rules, 
            rule -> length(regexp_extract("SPECIALIZED FUNCTION", rule['ATTRIBUTE'], 0)) > 0), 
          1)), 
      '{}') AS _EXTRACTED_RULE,
    *
  
  FROM FindReplace_181_join AS in0

),

FindReplace_181_reorg_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (GET_JSON_OBJECT(_EXTRACTED_RULE, '$.Attribute code')) AS "ATTRIBUTE CODE2",
    * EXCLUDE ("_RULES", "_EXTRACTED_RULE", "ATTRIBUTE CODE2")
  
  FROM FindReplace_181_0 AS in0

),

Formula_219_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST('If IsNull([Attribute code6]) then +"C**" else [Attribute code6] endif' AS STRING) AS "ATTRIBUTE CODE6",
    *
  
  FROM FindReplace_181_reorg_0 AS in0

),

FindReplace_228_join AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    in0.F47 AS F47,
    in0."LISTING PRICE" AS "LISTING PRICE",
    in0."AGE GROUP" AS "AGE GROUP",
    in0."DEDICATED FUNCTIONS" AS "DEDICATED FUNCTIONS",
    in0.SKU AS SKU,
    in0."DETAILED PRODUCT GROUP" AS "DETAILED PRODUCT GROUP",
    in0."19" AS "19",
    in0."23" AS "23",
    in0.COLLECTION AS COLLECTION,
    in0.SIZE AS SIZE,
    in0."NAME EXPLANATION" AS "NAME EXPLANATION",
    in0."ATTRIBUTE CODE" AS "ATTRIBUTE CODE",
    in0."15" AS "15",
    in0."PRODUCT GROUP" AS "PRODUCT GROUP",
    in0."SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE" AS "SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE",
    in0."ATTRIBUTE CODE4" AS "ATTRIBUTE CODE4",
    in0."SPECIALIZED FUNCTION" AS "SPECIALIZED FUNCTION",
    in0.STYLE AS STYLE,
    in0."LIFESTYLE GROUP" AS "LIFESTYLE GROUP",
    in0."22" AS "22",
    in0."SIZE RANGE" AS "SIZE RANGE",
    in0."16" AS "16",
    in0."HEEL HEIGHT" AS "HEEL HEIGHT",
    in0.SOLE_TYPE AS SOLE_TYPE,
    in0.GTCL AS GTCL,
    in0."MCH GROUP" AS "MCH GROUP",
    in0.COLOR AS COLOR,
    in0."DESIGN INSPIRATION" AS "DESIGN INSPIRATION",
    in0."21" AS "21",
    in0.BRAND AS BRAND,
    in0."43" AS "43",
    in0."PRICE GROUP" AS "PRICE GROUP",
    in0."ATTRIBUTE CODE5" AS "ATTRIBUTE CODE5",
    in0."IMAGE COPYRIGHT" AS "IMAGE COPYRIGHT",
    in0."SHOE STYLE" AS "SHOE STYLE",
    in0."PRODUCT LINE" AS "PRODUCT LINE",
    in0."COPYRIGHT GROUP" AS "COPYRIGHT GROUP",
    in0."42" AS "42",
    in0."ATTRIBUTE CODE2" AS "ATTRIBUTE CODE2",
    in0."MC EXPLANATION" AS "MC EXPLANATION",
    in0."20" AS "20",
    in0.UPPER_MATERIAL AS UPPER_MATERIAL,
    in1._RULES AS _RULES,
    in0."SIMILAR GROUP" AS "SIMILAR GROUP",
    in0."ATTRIBUTE CODE6" AS "ATTRIBUTE CODE6",
    in0."MOLD CODE" AS "MOLD CODE",
    in0.F45 AS F45,
    in0."STYLE COLOR" AS "STYLE COLOR",
    in0.UNIT AS UNIT,
    in0."LAUNCH SEASON" AS "LAUNCH SEASON",
    in0."18" AS "18",
    in0."COLOR GROUP" AS "COLOR GROUP",
    in0."41" AS "41",
    in0."SIZE GROUP" AS "SIZE GROUP",
    in0."MG GROUP" AS "MG GROUP",
    in0."MCH EXPLANATION" AS "MCH EXPLANATION",
    in0."ATTRIBUTE CODE3" AS "ATTRIBUTE CODE3",
    in0."ACTIVITY GROUP" AS "ACTIVITY GROUP",
    in0.GENDER AS GENDER
  
  FROM Formula_219_0 AS in0
  FULL JOIN FindReplace_228_allRules AS in1
     ON TRUE

),

FindReplace_228_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract("PRODUCT LINE", rule['ATTRIBUTE'], 0)) > 0), 1)), 
      '{}') AS _EXTRACTED_RULE,
    *
  
  FROM FindReplace_228_join AS in0

),

FindReplace_228_reorg_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (GET_JSON_OBJECT(_EXTRACTED_RULE, '$.Attribute code')) AS "ATTRIBUTE CODE2",
    * EXCLUDE ("_RULES", "_EXTRACTED_RULE", "ATTRIBUTE CODE2")
  
  FROM FindReplace_228_0 AS in0

),

Formula_220_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST('If IsNull([Attribute code7]) then +"L**" else [Attribute code7] endif' AS STRING) AS "ATTRIBUTE CODE7",
    *
  
  FROM FindReplace_228_reorg_0 AS in0

),

FindReplace_183_allRules AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT array_agg(struct("ATTRIBUTE CODE" AS "ATTRIBUTE CODE", ATTRIBUTE AS ATTRIBUTE)) AS _RULES
  
  FROM Attributecodes__225 AS in0

),

FindReplace_183_join AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    in0.F47 AS F47,
    in0."LISTING PRICE" AS "LISTING PRICE",
    in0."AGE GROUP" AS "AGE GROUP",
    in0."DEDICATED FUNCTIONS" AS "DEDICATED FUNCTIONS",
    in0.SKU AS SKU,
    in0."DETAILED PRODUCT GROUP" AS "DETAILED PRODUCT GROUP",
    in0."19" AS "19",
    in0."23" AS "23",
    in0.COLLECTION AS COLLECTION,
    in0.SIZE AS SIZE,
    in0."NAME EXPLANATION" AS "NAME EXPLANATION",
    in0."ATTRIBUTE CODE" AS "ATTRIBUTE CODE",
    in0."15" AS "15",
    in0."PRODUCT GROUP" AS "PRODUCT GROUP",
    in0."SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE" AS "SOLE_CODE PARANTHESESOPENSAPPARANTHESESCLOSE",
    in0."ATTRIBUTE CODE4" AS "ATTRIBUTE CODE4",
    in0."SPECIALIZED FUNCTION" AS "SPECIALIZED FUNCTION",
    in0.STYLE AS STYLE,
    in0."LIFESTYLE GROUP" AS "LIFESTYLE GROUP",
    in0."22" AS "22",
    in0."SIZE RANGE" AS "SIZE RANGE",
    in0."16" AS "16",
    in0."HEEL HEIGHT" AS "HEEL HEIGHT",
    in0.SOLE_TYPE AS SOLE_TYPE,
    in0.GTCL AS GTCL,
    in0."MCH GROUP" AS "MCH GROUP",
    in0.COLOR AS COLOR,
    in0."DESIGN INSPIRATION" AS "DESIGN INSPIRATION",
    in0."21" AS "21",
    in0.BRAND AS BRAND,
    in0."43" AS "43",
    in0."PRICE GROUP" AS "PRICE GROUP",
    in0."ATTRIBUTE CODE5" AS "ATTRIBUTE CODE5",
    in0."IMAGE COPYRIGHT" AS "IMAGE COPYRIGHT",
    in0."SHOE STYLE" AS "SHOE STYLE",
    in0."PRODUCT LINE" AS "PRODUCT LINE",
    in0."COPYRIGHT GROUP" AS "COPYRIGHT GROUP",
    in0."42" AS "42",
    in0."ATTRIBUTE CODE2" AS "ATTRIBUTE CODE2",
    in0."MC EXPLANATION" AS "MC EXPLANATION",
    in0."20" AS "20",
    in0.UPPER_MATERIAL AS UPPER_MATERIAL,
    in1._RULES AS _RULES,
    in0."SIMILAR GROUP" AS "SIMILAR GROUP",
    in0."ATTRIBUTE CODE6" AS "ATTRIBUTE CODE6",
    in0."MOLD CODE" AS "MOLD CODE",
    in0.F45 AS F45,
    in0."STYLE COLOR" AS "STYLE COLOR",
    in0.UNIT AS UNIT,
    in0."LAUNCH SEASON" AS "LAUNCH SEASON",
    in0."18" AS "18",
    in0."COLOR GROUP" AS "COLOR GROUP",
    in0."41" AS "41",
    in0."SIZE GROUP" AS "SIZE GROUP",
    in0."MG GROUP" AS "MG GROUP",
    in0."MCH EXPLANATION" AS "MCH EXPLANATION",
    in0."ATTRIBUTE CODE3" AS "ATTRIBUTE CODE3",
    in0."ACTIVITY GROUP" AS "ACTIVITY GROUP",
    in0."ATTRIBUTE CODE7" AS "ATTRIBUTE CODE7",
    in0.GENDER AS GENDER
  
  FROM Formula_220_0 AS in0
  FULL JOIN FindReplace_183_allRules AS in1
     ON TRUE

),

FindReplace_183_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    coalesce(
      to_json(
        element_at(filter(
          _rules, 
          rule -> length(regexp_extract(UPPER_MATERIAL, rule['ATTRIBUTE'], 0)) > 0), 1)), 
      '{}') AS _EXTRACTED_RULE,
    *
  
  FROM FindReplace_183_join AS in0

),

FindReplace_183_reorg_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (GET_JSON_OBJECT(_EXTRACTED_RULE, '$.Attribute code')) AS "ATTRIBUTE CODE2",
    * EXCLUDE ("_RULES", "_EXTRACTED_RULE", "ATTRIBUTE CODE2")
  
  FROM FindReplace_183_0 AS in0

),

Formula_221_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST('If IsNull([Attribute code8]) then +"U**" else [Attribute code8] endif' AS STRING) AS "ATTRIBUTE CODE8",
    *
  
  FROM FindReplace_183_reorg_0 AS in0

),

Unique_184 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM Formula_221_0 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY SKU, GTCL ORDER BY SKU, GTCL) = 1

),

AlteryxSelect_246 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    "ATTRIBUTE CODE" AS "A CODE 1",
    "ATTRIBUTE CODE2" AS "A CODE 2",
    "ATTRIBUTE CODE3" AS "A CODE 3",
    "ATTRIBUTE CODE4" AS "A CODE 4",
    "ATTRIBUTE CODE5" AS "A CODE 5",
    "ATTRIBUTE CODE6" AS "A CODE 6",
    "ATTRIBUTE CODE7" AS "A CODE 7",
    "ATTRIBUTE CODE8" AS "A CODE 8",
    * EXCLUDE ("ATTRIBUTE CODE", 
    "ATTRIBUTE CODE2", 
    "ATTRIBUTE CODE3", 
    "ATTRIBUTE CODE4", 
    "ATTRIBUTE CODE5", 
    "ATTRIBUTE CODE6", 
    "ATTRIBUTE CODE7", 
    "ATTRIBUTE CODE8")
  
  FROM Unique_184 AS in0

),

DynamicSelect_248 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    "AGE GROUP" AS "AGE GROUP",
    "COLOR GROUP" AS "COLOR GROUP",
    UPPER_MATERIAL AS UPPER_MATERIAL,
    "PRICE GROUP" AS "PRICE GROUP",
    "SPECIALIZED FUNCTION" AS "SPECIALIZED FUNCTION",
    "HEEL HEIGHT" AS "HEEL HEIGHT",
    "A CODE 6" AS "A CODE 6",
    "LIFESTYLE GROUP" AS "LIFESTYLE GROUP",
    "A CODE 4" AS "A CODE 4",
    SKU AS SKU,
    "A CODE 7" AS "A CODE 7",
    "A CODE 3" AS "A CODE 3",
    "PRODUCT LINE" AS "PRODUCT LINE",
    "A CODE 2" AS "A CODE 2",
    "A CODE 8" AS "A CODE 8",
    GENDER AS GENDER,
    "A CODE 1" AS "A CODE 1",
    "SHOE STYLE" AS "SHOE STYLE",
    "A CODE 5" AS "A CODE 5"
  
  FROM AlteryxSelect_246 AS in0

),

Filter_187 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (GENDER IN ('BOY', 'GIR'))

),

Formula_188_to_Formula_189_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST((CONCAT("A CODE 1", '-', "A CODE 2", '-', "A CODE 3")) AS STRING) AS "OPTION CODE",
    CAST((CONCAT("PRICE GROUP", '-', "COLOR GROUP", '-', "AGE GROUP")) AS STRING) AS OPTION,
    *
  
  FROM Filter_187 AS in0

),

Filter_198 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'TD')
          AND ((SUBSTRING("SHOE STYLE", 1, 1)) = 'G')
        )

),

Formula_199_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST((CONCAT("A CODE 1", '-', "A CODE 8")) AS STRING) AS "OPTION CODE",
    CAST((CONCAT("PRICE GROUP", '-', UPPER_MATERIAL)) AS STRING) AS OPTION,
    *
  
  FROM Filter_198 AS in0

),

Filter_191_reject_to_Filter_185 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          (
            (
              (
                (
                  NOT(
                    (SUBSTRING("SHOE STYLE", 1, 1)) = 'D')
                ) OR ((SUBSTRING("SHOE STYLE", 1, 1)) IS NULL)
              )
              OR (((SUBSTRING("SHOE STYLE", 1, 1)) = 'D') IS NULL)
            )
            AND ((NOT(GENDER IN ('BOY', 'GIR'))) OR (((GENDER = 'BOY') OR (GENDER = 'GIR')) IS NULL))
          )
          AND ("SHOE STYLE" = 'DLA')
        )

),

Formula_186_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST((CONCAT("A CODE 1", '-', "A CODE 2")) AS STRING) AS "OPTION CODE",
    CAST((CONCAT("PRICE GROUP", '-', "COLOR GROUP")) AS STRING) AS OPTION,
    *
  
  FROM Filter_191_reject_to_Filter_185 AS in0

),

Filter_196 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'SK')
          AND ((SUBSTRING("SHOE STYLE", 1, 1)) = 'G')
        )

),

Formula_197_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST((CONCAT("A CODE 1", '-', "A CODE 2", '-', "A CODE 7")) AS STRING) AS "OPTION CODE",
    CAST((CONCAT("PRICE GROUP", '-', "COLOR GROUP", '-', "PRODUCT LINE")) AS STRING) AS OPTION,
    *
  
  FROM Filter_196 AS in0

),

Filter_202 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'TT')
          AND ((SUBSTRING("SHOE STYLE", 1, 1)) = 'S')
        )

),

Formula_203_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST((CONCAT("A CODE 1", '-', "A CODE 2")) AS STRING) AS "OPTION CODE",
    CAST((CONCAT("PRICE GROUP", '-', "COLOR GROUP")) AS STRING) AS OPTION,
    *
  
  FROM Filter_202 AS in0

),

Formula_188_to_Formula_189_1 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (
      CASE
        WHEN ("SHOE STYLE" = 'DLA')
          THEN CAST((CONCAT("A CODE 1", '-', "A CODE 2")) AS string)
        ELSE "OPTION CODE"
      END
    ) AS "OPTION CODE",
    (
      CASE
        WHEN ("SHOE STYLE" = 'DLA')
          THEN CAST((CONCAT("PRICE GROUP", '-', "COLOR GROUP")) AS string)
        ELSE OPTION
      END
    ) AS OPTION,
    * EXCLUDE ("OPTION CODE", "OPTION")
  
  FROM Formula_188_to_Formula_189_0 AS in0

),

Filter_191 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          ((SUBSTRING("SHOE STYLE", 1, 1)) = 'D')
          AND ((NOT(GENDER IN ('BOY', 'GIR'))) OR (((GENDER = 'BOY') OR (GENDER = 'GIR')) IS NULL))
        )

),

Formula_190_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (
      CASE
        WHEN ((CAST((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) AS string) = 'TD') OR 'XP')
          THEN (CONCAT("A CODE 1", '-', "A CODE 3"))
        ELSE (CONCAT("A CODE 1", '-', "A CODE 2"))
      END
    ) AS "OPTION CODE",
    (
      CASE
        WHEN ((CAST((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) AS string) = 'TD') OR 'XP')
          THEN (CONCAT("PRICE GROUP", '-', "AGE GROUP"))
        ELSE (CONCAT("PRICE GROUP", '-', "COLOR GROUP"))
      END
    ) AS OPTION,
    *
  
  FROM Filter_191 AS in0

),

Filter_208 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'XP')
          AND ((SUBSTRING("SHOE STYLE", 1, 1)) = 'S')
        )

),

Formula_209_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (
      CASE
        WHEN (GENDER = 'WOM')
          THEN (CONCAT("A CODE 1", '-', "A CODE 2", '-', "A CODE 3"))
        ELSE (CONCAT("A CODE 1", '-', "A CODE 3"))
      END
    ) AS "OPTION CODE",
    (
      CASE
        WHEN (GENDER = 'WOM')
          THEN (CONCAT("PRICE GROUP", '-', "COLOR GROUP", '-', "AGE GROUP"))
        ELSE (CONCAT("PRICE GROUP", '-', "AGE GROUP"))
      END
    ) AS OPTION,
    *
  
  FROM Filter_208 AS in0

),

Filter_206 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'TR')
          AND ((SUBSTRING("SHOE STYLE", 1, 1)) = 'S')
        )

),

Formula_207_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST((CONCAT("A CODE 1", '-', "A CODE 4", '-', "A CODE 5")) AS STRING) AS "OPTION CODE",
    CAST((CONCAT("PRICE GROUP", '-', "LIFESTYLE GROUP", '-', "HEEL HEIGHT")) AS STRING) AS OPTION,
    *
  
  FROM Filter_206 AS in0

),

Filter_192 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          (
            (
              ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'CH')
              OR ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'FB')
            )
            OR ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'TS')
          )
          AND ((SUBSTRING("SHOE STYLE", 1, 1)) = 'G')
        )

),

Formula_193_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST((CONCAT("A CODE 1", '-', "A CODE 6")) AS STRING) AS "OPTION CODE",
    CAST((CONCAT("PRICE GROUP", '-', "SPECIALIZED FUNCTION")) AS STRING) AS OPTION,
    *
  
  FROM Filter_192 AS in0

),

Filter_200 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'TY')
          AND ((SUBSTRING("SHOE STYLE", 1, 1)) = 'G')
        )

),

Formula_201_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST((CONCAT("A CODE 1", '-', "A CODE 4")) AS STRING) AS "OPTION CODE",
    CAST((CONCAT("PRICE GROUP", '-', "LIFESTYLE GROUP")) AS STRING) AS OPTION,
    *
  
  FROM Filter_200 AS in0

),

Filter_204 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'TD')
          AND ((SUBSTRING("SHOE STYLE", 1, 1)) = 'S')
        )

),

Formula_205_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (
      CASE
        WHEN (GENDER = 'WOM')
          THEN (CONCAT("A CODE 1", '-', "A CODE 2"))
        ELSE (CONCAT("A CODE 1", '-', "A CODE 3"))
      END
    ) AS "OPTION CODE",
    (
      CASE
        WHEN (GENDER = 'WOM')
          THEN (CONCAT("PRICE GROUP", '-', "COLOR GROUP"))
        ELSE (CONCAT("PRICE GROUP", '-', "AGE GROUP"))
      END
    ) AS OPTION,
    *
  
  FROM Filter_204 AS in0

),

Filter_210 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM DynamicSelect_248 AS in0
  
  WHERE (
          (
            (
              ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'BB')
              OR ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'BI')
            )
            OR ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'BT')
          )
          AND ((SUBSTRING("SHOE STYLE", 1, 1)) = 'G')
        )

),

Formula_211_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    (
      CASE
        WHEN ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'BB')
          THEN (CONCAT("A CODE 1", '-', "A CODE 4"))
        WHEN ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'BI')
          THEN (CONCAT("A CODE 1", '-', "A CODE 4", '-', "A CODE 5"))
        ELSE (CONCAT("A CODE 1", '-', "A CODE 2", '-', "A CODE 5"))
      END
    ) AS "OPTION CODE",
    (
      CASE
        WHEN ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'BB')
          THEN (CONCAT("PRICE GROUP", '-', "LIFESTYLE GROUP"))
        WHEN ((SUBSTRING("SHOE STYLE", (((LENGTH("SHOE STYLE")) - 2) + 1), 2)) = 'BI')
          THEN (CONCAT("PRICE GROUP", '-', "LIFESTYLE GROUP", '-', "HEEL HEIGHT"))
        ELSE (CONCAT("PRICE GROUP", '-', "COLOR GROUP", '-', "HEEL HEIGHT"))
      END
    ) AS OPTION,
    *
  
  FROM Filter_210 AS in0

),

Union_212 AS (

  {#VisualGroup: SKUOptionmapping#}
  {{
    prophecy_basics.UnionByName(
      [
        'Formula_201_0', 
        'Formula_197_0', 
        'Formula_207_0', 
        'Formula_193_0', 
        'Formula_190_0', 
        'Formula_211_0', 
        'Formula_188_to_Formula_189_1', 
        'Formula_205_0', 
        'Formula_203_0', 
        'Formula_199_0', 
        'Formula_209_0', 
        'Formula_186_0'
      ], 
      [
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]', 
        '[{"name": "PRODUCT LINE", "dataType": "String"}, {"name": "A CODE 5", "dataType": "String"}, {"name": "OPTION", "dataType": "String"}, {"name": "PRICE GROUP", "dataType": "String"}, {"name": "LIFESTYLE GROUP", "dataType": "String"}, {"name": "A CODE 1", "dataType": "String"}, {"name": "GENDER", "dataType": "String"}, {"name": "A CODE 8", "dataType": "String"}, {"name": "SKU", "dataType": "String"}, {"name": "AGE GROUP", "dataType": "String"}, {"name": "HEEL HEIGHT", "dataType": "String"}, {"name": "A CODE 2", "dataType": "String"}, {"name": "SPECIALIZED FUNCTION", "dataType": "String"}, {"name": "OPTION CODE", "dataType": "String"}, {"name": "A CODE 4", "dataType": "String"}, {"name": "A CODE 7", "dataType": "String"}, {"name": "SHOE STYLE", "dataType": "String"}, {"name": "A CODE 3", "dataType": "String"}, {"name": "UPPER_MATERIAL", "dataType": "String"}, {"name": "A CODE 6", "dataType": "String"}, {"name": "COLOR GROUP", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_250_left_UnionLeftOuter AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    in0.*,
    in1.* EXCLUDE ("SKU", 
    "COLOR GROUP", 
    "PRICE GROUP", 
    "GENDER", 
    "SHOE STYLE", 
    "SPECIALIZED FUNCTION", 
    "AGE GROUP", 
    "PRODUCT LINE", 
    "LIFESTYLE GROUP", 
    "HEEL HEIGHT", 
    "UPPER_MATERIAL", 
    "A CODE 1", 
    "A CODE 2", 
    "A CODE 3", 
    "A CODE 4", 
    "A CODE 5", 
    "A CODE 6", 
    "A CODE 7", 
    "A CODE 8")
  
  FROM AlteryxSelect_246 AS in0
  LEFT JOIN Union_212 AS in1
     ON (in0.SKU = in1.SKU)

),

AlteryxSelect_247 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * EXCLUDE ("15", 
         "16", 
         "18", 
         "19", 
         "F45", 
         "20", 
         "F47", 
         "21", 
         "22", 
         "23", 
         "41", 
         "42", 
         "43", 
         "GTCL", 
         "A CODE 1", 
         "A CODE 2", 
         "A CODE 3", 
         "A CODE 4", 
         "A CODE 5", 
         "A CODE 6", 
         "A CODE 7", 
         "A CODE 8")
  
  FROM Join_250_left_UnionLeftOuter AS in0

),

Formula_243_0 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    CAST((
      CONCAT(
        'Options', 
        (
          REGEXP_REPLACE(
            (REGEXP_REPLACE((TO_CHAR(CAST(CURRENT_DATE AS DOUBLE), 'FM999999999999999990')), ',', '__THS__')), 
            '__THS__', 
            '')
        ), 
        '.xlsx|SKU')
    ) AS STRING) AS FILENAME,
    *
  
  FROM AlteryxSelect_247 AS in0

)

SELECT *

FROM Formula_243_0
