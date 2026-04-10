{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Download_21_21 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('Billboard_Webscraping_App_1_', 'Download_21_21') }}

),

Download_21_21_cleanup_0 AS (

  SELECT 
    API_DATA AS DOWNLOADDATA,
    NULL AS DOWNLOADHEADERS,
    * EXCLUDE ("_PARAMSORDATA", "_PROCESSEDURL", "API_DATA")
  
  FROM Download_21_21 AS in0

),

RegEx_22 AS (

  {{
    prophecy_basics.Regex(
      ['Download_21_21_cleanup_0'], 
      [{ 'columnName': 'HASH1 SONG', 'dataType': 'String', 'rgxExpression': '(.+?)' }], 
      '[{"name": "DOWNLOADDATA", "dataType": "String"}, {"name": "DOWNLOADHEADERS", "dataType": "String"}, {"name": "URL", "dataType": "String"}]', 
      'DOWNLOADDATA', 
      'u-letter-spacing-0028@tablet">(.+?)</h3>', 
      'parse', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      '', 
      false
    )
  }}

),

RegEx_23 AS (

  {{
    prophecy_basics.Regex(
      ['RegEx_22'], 
      [{ 'columnName': 'HASH1 ARTIST', 'dataType': 'String', 'rgxExpression': '(.+?)' }], 
      '[{"name": "DOWNLOADDATA", "dataType": "String"}, {"name": "DOWNLOADHEADERS", "dataType": "String"}, {"name": "URL", "dataType": "String"}, {"name": "HASH1 SONG", "dataType": "String"}]', 
      'DOWNLOADDATA', 
      'u-font-size-20@tablet"\s>(.+?)</span>', 
      'parse', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      '', 
      false
    )
  }}

),

RegEx_24 AS (

  {{
    prophecy_basics.Regex(
      ['RegEx_23'], 
      [{ 'columnName': 'regex_col1', 'dataType': 'string', 'rgxExpression': '(.+?)' }], 
      '[{"name": "DOWNLOADDATA", "dataType": "String"}, {"name": "DOWNLOADHEADERS", "dataType": "String"}, {"name": "URL", "dataType": "String"}, {"name": "HASH1 SONG", "dataType": "String"}, {"name": "HASH1 ARTIST", "dataType": "String"}]', 
      'DOWNLOADDATA', 
      'u-max-width-230@tablet-only">(.+?)</h3>', 
      'tokenize', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      99, 
      'dropExtraWithoutWarning', 
      'Song', 
      '', 
      false
    )
  }}

),

RegEx_25 AS (

  {{
    prophecy_basics.Regex(
      ['RegEx_24'], 
      [{ 'columnName': 'regex_col1', 'dataType': 'string', 'rgxExpression': '(.+?)' }], 
      '[{"name": "DOWNLOADDATA", "dataType": "String"}, {"name": "DOWNLOADHEADERS", "dataType": "String"}, {"name": "URL", "dataType": "String"}, {"name": "HASH1 SONG", "dataType": "String"}, {"name": "HASH1 ARTIST", "dataType": "String"}, {"name": "Song1", "dataType": "String"}, {"name": "Song2", "dataType": "String"}, {"name": "Song3", "dataType": "String"}, {"name": "Song4", "dataType": "String"}, {"name": "Song5", "dataType": "String"}, {"name": "Song6", "dataType": "String"}, {"name": "Song7", "dataType": "String"}, {"name": "Song8", "dataType": "String"}, {"name": "Song9", "dataType": "String"}, {"name": "Song10", "dataType": "String"}, {"name": "Song11", "dataType": "String"}, {"name": "Song12", "dataType": "String"}, {"name": "Song13", "dataType": "String"}, {"name": "Song14", "dataType": "String"}, {"name": "Song15", "dataType": "String"}, {"name": "Song16", "dataType": "String"}, {"name": "Song17", "dataType": "String"}, {"name": "Song18", "dataType": "String"}, {"name": "Song19", "dataType": "String"}, {"name": "Song20", "dataType": "String"}, {"name": "Song21", "dataType": "String"}, {"name": "Song22", "dataType": "String"}, {"name": "Song23", "dataType": "String"}, {"name": "Song24", "dataType": "String"}, {"name": "Song25", "dataType": "String"}, {"name": "Song26", "dataType": "String"}, {"name": "Song27", "dataType": "String"}, {"name": "Song28", "dataType": "String"}, {"name": "Song29", "dataType": "String"}, {"name": "Song30", "dataType": "String"}, {"name": "Song31", "dataType": "String"}, {"name": "Song32", "dataType": "String"}, {"name": "Song33", "dataType": "String"}, {"name": "Song34", "dataType": "String"}, {"name": "Song35", "dataType": "String"}, {"name": "Song36", "dataType": "String"}, {"name": "Song37", "dataType": "String"}, {"name": "Song38", "dataType": "String"}, {"name": "Song39", "dataType": "String"}, {"name": "Song40", "dataType": "String"}, {"name": "Song41", "dataType": "String"}, {"name": "Song42", "dataType": "String"}, {"name": "Song43", "dataType": "String"}, {"name": "Song44", "dataType": "String"}, {"name": "Song45", "dataType": "String"}, {"name": "Song46", "dataType": "String"}, {"name": "Song47", "dataType": "String"}, {"name": "Song48", "dataType": "String"}, {"name": "Song49", "dataType": "String"}, {"name": "Song50", "dataType": "String"}, {"name": "Song51", "dataType": "String"}, {"name": "Song52", "dataType": "String"}, {"name": "Song53", "dataType": "String"}, {"name": "Song54", "dataType": "String"}, {"name": "Song55", "dataType": "String"}, {"name": "Song56", "dataType": "String"}, {"name": "Song57", "dataType": "String"}, {"name": "Song58", "dataType": "String"}, {"name": "Song59", "dataType": "String"}, {"name": "Song60", "dataType": "String"}, {"name": "Song61", "dataType": "String"}, {"name": "Song62", "dataType": "String"}, {"name": "Song63", "dataType": "String"}, {"name": "Song64", "dataType": "String"}, {"name": "Song65", "dataType": "String"}, {"name": "Song66", "dataType": "String"}, {"name": "Song67", "dataType": "String"}, {"name": "Song68", "dataType": "String"}, {"name": "Song69", "dataType": "String"}, {"name": "Song70", "dataType": "String"}, {"name": "Song71", "dataType": "String"}, {"name": "Song72", "dataType": "String"}, {"name": "Song73", "dataType": "String"}, {"name": "Song74", "dataType": "String"}, {"name": "Song75", "dataType": "String"}, {"name": "Song76", "dataType": "String"}, {"name": "Song77", "dataType": "String"}, {"name": "Song78", "dataType": "String"}, {"name": "Song79", "dataType": "String"}, {"name": "Song80", "dataType": "String"}, {"name": "Song81", "dataType": "String"}, {"name": "Song82", "dataType": "String"}, {"name": "Song83", "dataType": "String"}, {"name": "Song84", "dataType": "String"}, {"name": "Song85", "dataType": "String"}, {"name": "Song86", "dataType": "String"}, {"name": "Song87", "dataType": "String"}, {"name": "Song88", "dataType": "String"}, {"name": "Song89", "dataType": "String"}, {"name": "Song90", "dataType": "String"}, {"name": "Song91", "dataType": "String"}, {"name": "Song92", "dataType": "String"}, {"name": "Song93", "dataType": "String"}, {"name": "Song94", "dataType": "String"}, {"name": "Song95", "dataType": "String"}, {"name": "Song96", "dataType": "String"}, {"name": "Song97", "dataType": "String"}, {"name": "Song98", "dataType": "String"}, {"name": "Song99", "dataType": "String"}]', 
      'DOWNLOADDATA', 
      'u-max-width-230@tablet-only"\s>(.+?)</span>', 
      'tokenize', 
      true, 
      false, 
      '', 
      false, 
      'splitColumns', 
      99, 
      'dropExtraWithoutWarning', 
      'Artist', 
      '', 
      false
    )
  }}

)

SELECT *

FROM RegEx_25
