{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH JupyterCode_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('IPL_Python_web_scraping', 'JupyterCode_1', 'out0') }}

),

AlteryxSelect_5 AS (

  SELECT CAST(NULL AS STRING) AS variableDesc
  
  FROM JupyterCode_1 AS in0

),

Filter_6 AS (

  SELECT * 
  
  FROM AlteryxSelect_5 AS in0
  
  WHERE (
          (
            CAST(((STRPOS((coalesce(LOWER(variableDesc), '')), LOWER('run out'))) > 0) AS BOOL)
            OR (CAST((SUBSTRING(variableDesc, 1, 1)) AS STRING) = 'c')
          )
          OR (CAST((SUBSTRING(variableDesc, 1, 2)) AS STRING) = 'st')
        )

),

Filter_7 AS (

  SELECT * 
  
  FROM Filter_6 AS in0
  
  WHERE ((STRPOS(variableDesc, 'run out')) > 0)

),

Formula_8_0 AS (

  SELECT 
    CAST((
      SUBSTRING(
        variableDesc, 
        (((LENGTH(variableDesc)) - ((LENGTH(variableDesc)) - 9)) + 1), 
        ((LENGTH(variableDesc)) - 9))
    ) AS STRING) AS `variableDesc`,
    *
  
  FROM Filter_7

),

Formula_8_1 AS (

  SELECT 
    CAST((SUBSTRING(variableDesc, 1, ((LENGTH(variableDesc)) - 1))) AS STRING) AS `variableDesc`,
    *
  
  FROM Formula_8_0

),

TextToColumns_9 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Formula_8_1'], 
      'variableDesc', 
      "/", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'variableDesc', 
      'variableDesc', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_9_dropGem_0 AS (

  SELECT 
    VARIABLEDESC_1_VARIABLEDESC AS `1`,
    VARIABLEDESC_2_VARIABLEDESC AS `2`,
    *
  
  FROM TextToColumns_9 AS in0

),

Cleanse_10 AS (

  {{
    prophecy_basics.DataCleansing(
      ['TextToColumns_9_dropGem_0'], 
      [
        { "name": "1", "dataType": "String" }, 
        { "name": "2", "dataType": "String" }, 
        { "name": "variableDesc", "dataType": "String" }, 
        { "name": "variableDesc_2", "dataType": "String" }, 
        { "name": "variableDesc_1", "dataType": "String" }, 
        { "name": "variableDesc_1_variableDesc", "dataType": "String" }, 
        { "name": "variableDesc_2_variableDesc", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['1', '2'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      false, 
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

Formula_11_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((`2` IS NULL) OR ((LENGTH(CAST(`2` AS STRING))) = 0))
          THEN 1
        ELSE 0.5
      END
    ) AS FLOAT64) AS `Run Out Points`,
    *
  
  FROM Cleanse_10 AS in0

),

AlteryxSelect_17 AS (

  SELECT * EXCEPT (`variableDesc`)
  
  FROM Formula_11_0 AS in0

),

Filter_18_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_17 AS in0
  
  WHERE (
          (NOT(((`2` IS NULL) OR ((LENGTH(CAST(`2` AS STRING))) = 0)) OR (`2` IS NULL)))
          OR ((((`2` IS NULL) OR ((LENGTH(CAST(`2` AS STRING))) = 0)) OR (`2` IS NULL)) IS NULL)
        )

),

Filter_7_reject AS (

  SELECT * 
  
  FROM Filter_6 AS in0
  
  WHERE (
          (NOT CAST(((STRPOS(variableDesc, 'run out')) > 0) AS BOOL))
          OR (CAST(((STRPOS(variableDesc, 'run out')) > 0) AS BOOL) IS NULL)
        )

),

Filter_55_reject AS (

  SELECT * 
  
  FROM Filter_7_reject AS in0
  
  WHERE (
          (
            (CAST((SUBSTRING(variableDesc, 1, 2)) AS STRING) <> 'st')
            OR ((SUBSTRING(variableDesc, 1, 2)) IS NULL)
          )
          OR ((CAST((SUBSTRING(variableDesc, 1, 2)) AS STRING) = 'st') IS NULL)
        )

),

RegEx_13 AS (

  {{
    prophecy_basics.Regex(
      ['Filter_55_reject'], 
      [], 
      '[{"name": "variableDesc", "dataType": "String"}]', 
      'variableDesc', 
      'b', 
      'replace', 
      true, 
      false, 
      '-', 
      true, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      '', 
      false
    )
  }}

),

RegEx_13_rename_0 AS (

  SELECT 
    Desc_replaced AS `variableDesc`,
    *
  
  FROM RegEx_13

),

TextToColumns_12 AS (

  {{
    prophecy_basics.TextToColumns(
      ['RegEx_13_rename_0'], 
      'variableDesc', 
      "\\\-", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'variableDesc', 
      'variableDesc', 
      'generatedColumnName'
    )
  }}

),

AlteryxSelect_47 AS (

  SELECT 
    CAST(NULL AS STRING) AS F1,
    CAST(NULL AS STRING) AS Name,
    CAST(NULL AS STRING) AS variableDesc
  
  FROM JupyterCode_1 AS in0

),

Filter_18 AS (

  SELECT * 
  
  FROM AlteryxSelect_17 AS in0
  
  WHERE (((`2` IS NULL) OR ((LENGTH(CAST(`2` AS STRING))) = 0)) OR (`2` IS NULL))

),

AlteryxSelect_22 AS (

  SELECT 
    `1` AS Player,
    * EXCEPT (`2`, `1`)
  
  FROM Filter_18 AS in0

),

Union_21_1 AS (

  SELECT 
    CAST(Player AS STRING) AS prophecy_column_1,
    CAST(`Run Out Points` AS STRING) AS prophecy_column_2
  
  FROM AlteryxSelect_22 AS in0

),

Arrange_19_consolidatedDataCol_0 AS (

  SELECT 
    (
      array(
        (
          NAMED_STRUCT(
            'Player', 
            CAST(`1` AS STRING), 
            'Points', 
            CAST(`Run Out Points` AS STRING), 
            'Description', 
            'TextToColumns: Parsed from Desc')
        ), 
        (
          NAMED_STRUCT(
            'Player', 
            CAST(`2` AS STRING), 
            'Points', 
            CAST(`Run Out Points` AS STRING), 
            'Description', 
            'TextToColumns: Parsed from Desc')
        ))
    ) AS _consolidated_data_col,
    *
  
  FROM Filter_18_reject AS in0

),

Arrange_19_explode_0 AS (

  SELECT 
    (EXPLODE(`_consolidated_data_col`)) AS _exploded_data_col,
    *
  
  FROM Arrange_19_consolidatedDataCol_0 AS in0

),

Arrange_19_0 AS (

  SELECT 
    _exploded_data_col.`Description` AS Description,
    _exploded_data_col.`Player` AS Player,
    _exploded_data_col.`Points` AS Points,
    *
  
  FROM Arrange_19_explode_0 AS in0

),

Arrange_19_selectCols AS (

  SELECT * 
  
  FROM Arrange_19_0 AS in0

),

AlteryxSelect_20 AS (

  SELECT * EXCEPT (`Description`)
  
  FROM Arrange_19_selectCols AS in0

),

Union_21_0 AS (

  SELECT 
    CAST(Player AS STRING) AS prophecy_column_1,
    CAST(Points AS STRING) AS prophecy_column_2
  
  FROM AlteryxSelect_20 AS in0

),

Filter_55 AS (

  SELECT * 
  
  FROM Filter_7_reject AS in0
  
  WHERE (CAST((SUBSTRING(variableDesc, 1, 2)) AS STRING) = 'st')

),

Formula_58_0 AS (

  SELECT 
    CAST((
      SUBSTRING(
        variableDesc, 
        (((LENGTH(variableDesc)) - ((LENGTH(variableDesc)) - 4)) + 1), 
        ((LENGTH(variableDesc)) - 4))
    ) AS STRING) AS `variableDesc`,
    '1' AS `Stmupings`,
    *
  
  FROM Filter_55

),

RegEx_57 AS (

  {{
    prophecy_basics.Regex(
      ['Formula_58_0'], 
      [], 
      '[{"name": "variableDesc", "dataType": "String"}, {"name": "Stmupings", "dataType": "String"}, {"name": "variableDesc_1", "dataType": "String"}]', 
      'variableDesc', 
      'b', 
      'replace', 
      true, 
      false, 
      '_', 
      true, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      '', 
      false
    )
  }}

),

RegEx_57_rename_0 AS (

  SELECT 
    Desc_replaced AS variableDesc,
    * EXCEPT (`Desc_replaced`, `variabledesc`)
  
  FROM RegEx_57 AS in0

),

TextToColumns_56 AS (

  {{
    prophecy_basics.TextToColumns(
      ['RegEx_57_rename_0'], 
      'variableDesc', 
      "\\\ _", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'variableDesc', 
      'variableDesc', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_56_dropGem_0 AS (

  SELECT 
    VARIABLEDESC_1_VARIABLEDESC AS `1`,
    VARIABLEDESC_2_VARIABLEDESC AS `2`,
    *
  
  FROM TextToColumns_56 AS in0

),

AlteryxSelect_59 AS (

  SELECT 
    `1` AS Name,
    Stmupings AS Stmupings,
    * EXCEPT (`variableDesc`, `2`, `Stmupings`, `1`)
  
  FROM TextToColumns_56_dropGem_0 AS in0

),

Union_21_2 AS (

  SELECT 
    CAST(Name AS STRING) AS prophecy_column_1,
    CAST(Stmupings AS STRING) AS prophecy_column_2
  
  FROM AlteryxSelect_59 AS in0

),

Union_21 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_21_0', 'Union_21_1', 'Union_21_2'], 
      [
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_21_postRename AS (

  SELECT 
    prophecy_column_1 AS Player,
    prophecy_column_2 AS `Run Out Points`
  
  FROM Union_21 AS in0

),

Summarize_23 AS (

  SELECT 
    SUM(CAST(`Run Out Points` AS NUMERIC)) AS `Run Outs`,
    Player AS Player
  
  FROM Union_21_postRename AS in0
  
  GROUP BY Player

),

TextToColumns_12_dropGem_0 AS (

  SELECT 
    VARIABLEDESC_1_VARIABLEDESC AS `1`,
    VARIABLEDESC_2_VARIABLEDESC AS `2`,
    *
  
  FROM TextToColumns_12 AS in0

),

Formula_14_0 AS (

  SELECT 
    (
      SUBSTRING(
        CAST(`1` AS STRING), 
        (((LENGTH(CAST(`1` AS STRING))) - ((LENGTH(CAST(`1` AS STRING))) - 2)) + 1), 
        ((LENGTH(CAST(`1` AS STRING))) - 2))
    ) AS `1`,
    * EXCEPT (`1`)
  
  FROM TextToColumns_12_dropGem_0 AS in0

),

Formula_14_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`1` = '&')
          THEN `2`
        ELSE `1`
      END
    ) AS STRING) AS `1`,
    1 AS `Catch Points`,
    * EXCEPT (`1`)
  
  FROM Formula_14_0 AS in0

),

AlteryxSelect_15 AS (

  SELECT 
    `1` AS Catcher,
    `Catch Points` AS Catches,
    * EXCEPT (`variableDesc`, `2`, `1`, `Catch Points`)
  
  FROM Formula_14_1 AS in0

),

Cleanse_16 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_15'], 
      [
        { "name": "Desc_2_Desc", "dataType": "String" }, 
        { "name": "Desc_1_Desc", "dataType": "String" }, 
        { "name": "Catcher", "dataType": "String" }, 
        { "name": "Catches", "dataType": "Double" }
      ], 
      'keepOriginal', 
      ['Catcher'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      false, 
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

Summarize_24 AS (

  SELECT 
    SUM(Catches) AS Catches,
    Catcher AS Catcher
  
  FROM Cleanse_16 AS in0
  
  GROUP BY Catcher

),

Join_25_right AS (

  SELECT in0.*
  
  FROM Summarize_24 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM Summarize_23 AS in1
    
    WHERE in1.Player IS NOT NULL
  ) AS in1_keys
     ON (in1_keys.Player = in0.Catcher)
  
  WHERE (in1_keys.Player IS NULL)

),

Union_26_1 AS (

  SELECT 
    CAST(Catcher AS STRING) AS prophecy_column_1,
    Catches AS prophecy_column_3
  
  FROM Join_25_right AS in0

),

Join_25_left AS (

  SELECT in0.*
  
  FROM Summarize_23 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM Summarize_24 AS in1
    
    WHERE in1.Catcher IS NOT NULL
  ) AS in1_keys
     ON (in0.Player = in1_keys.Catcher)
  
  WHERE (in1_keys.Catcher IS NULL)

),

Union_26_0 AS (

  SELECT 
    CAST(Player AS STRING) AS prophecy_column_1,
    CAST(`Run Outs` AS FLOAT64) AS prophecy_column_2
  
  FROM Join_25_left AS in0

),

Join_25_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Catcher`)
  
  FROM Summarize_23 AS in0
  INNER JOIN Summarize_24 AS in1
     ON (in0.Player = in1.Catcher)

),

Union_26_2 AS (

  SELECT 
    CAST(Player AS STRING) AS prophecy_column_1,
    CAST(`Run Outs` AS FLOAT64) AS prophecy_column_2,
    Catches AS prophecy_column_3
  
  FROM Join_25_inner AS in0

),

Union_26 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_26_2', 'Union_26_0', 'Union_26_1'], 
      [
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "Double"}, {"name": "prophecy_column_3", "dataType": "Double"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "Double"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_3", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_26_postRename AS (

  SELECT 
    prophecy_column_1 AS Player,
    prophecy_column_2 AS `Run Outs`,
    prophecy_column_3 AS Catches
  
  FROM Union_26 AS in0

),

RecordID_60 AS (

  SELECT 
    *,
    (row_number() OVER (ORDER BY 1)) + (0) AS `F1`
  
  FROM JupyterCode_1

),

AlteryxSelect_4 AS (

  SELECT *
  
  FROM RecordID_60 AS in0

),

JupyterCode_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('IPL_Python_web_scraping', 'JupyterCode_1', 'out1') }}

),

Join_28_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_4 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM JupyterCode_1 AS in1
    
    WHERE in1.Name IS NOT NULL
  ) AS in1_keys
     ON (in0.Name = in1_keys.Name)
  
  WHERE (in1_keys.Name IS NULL)

),

Union_29_0 AS (

  SELECT 
    F1 AS prophecy_column_18,
    CAST(NULL AS STRING) AS prophecy_column_5,
    CAST(NULL AS STRING) AS prophecy_column_1,
    CAST(NULL AS STRING) AS prophecy_column_6,
    CAST(NULL AS STRING) AS prophecy_column_2,
    CAST(NULL AS STRING) AS prophecy_column_7,
    CAST(NULL AS STRING) AS prophecy_column_3,
    CAST(NULL AS STRING) AS prophecy_column_4
  
  FROM Join_28_left AS in0

),

Join_28_inner AS (

  SELECT 
    in0.* EXCEPT (`4`, `1`, `0`, `2`, `3`),
    in1.*
  
  FROM AlteryxSelect_4 AS in0
  INNER JOIN JupyterCode_1 AS in1
     ON (in0.Name = in1.Name)

),

Union_29_2 AS (

  SELECT 
    F1 AS prophecy_column_18,
    CAST(NULL AS STRING) AS prophecy_column_5,
    CAST(NULL AS STRING) AS prophecy_column_10,
    CAST(NULL AS STRING) AS prophecy_column_14,
    CAST(NULL AS STRING) AS prophecy_column_1,
    CAST(NULL AS STRING) AS prophecy_column_6,
    CAST(NULL AS STRING) AS prophecy_column_9,
    CAST(NULL AS STRING) AS prophecy_column_13,
    CAST(NULL AS STRING) AS prophecy_column_2,
    CAST(NULL AS STRING) AS prophecy_column_17,
    CAST(NULL AS STRING) AS prophecy_column_12,
    CAST(NULL AS STRING) AS prophecy_column_7,
    CAST(NULL AS STRING) AS prophecy_column_3,
    CAST(NULL AS STRING) AS prophecy_column_16,
    CAST(NULL AS STRING) AS prophecy_column_11,
    CAST(NULL AS STRING) AS prophecy_column_8,
    CAST(NULL AS STRING) AS prophecy_column_4,
    CAST(NULL AS STRING) AS prophecy_column_15
  
  FROM Join_28_inner AS in0

),

Join_28_right AS (

  SELECT in0.*
  
  FROM JupyterCode_1 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM AlteryxSelect_4 AS in1
    
    WHERE in1.Name IS NOT NULL
  ) AS in1_keys
     ON (in1_keys.Name = in0.Name)
  
  WHERE (in1_keys.Name IS NULL)

),

Union_29_1 AS (

  SELECT 
    CAST(NULL AS STRING) AS prophecy_column_10,
    CAST(NULL AS STRING) AS prophecy_column_14,
    CAST(NULL AS STRING) AS prophecy_column_1,
    CAST(NULL AS STRING) AS prophecy_column_9,
    CAST(NULL AS STRING) AS prophecy_column_13,
    CAST(NULL AS STRING) AS prophecy_column_17,
    CAST(NULL AS STRING) AS prophecy_column_12,
    CAST(NULL AS STRING) AS prophecy_column_7,
    CAST(NULL AS STRING) AS prophecy_column_16,
    CAST(NULL AS STRING) AS prophecy_column_11,
    CAST(NULL AS STRING) AS prophecy_column_8,
    CAST(NULL AS STRING) AS prophecy_column_15
  
  FROM Join_28_right AS in0

),

Union_29 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_29_2', 'Union_29_0', 'Union_29_1'], 
      [
        '[{"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "Integer"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}]', 
        '[{"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "Integer"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "String"}]', 
        '[{"name": "prophecy_column_15", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_29_postRename AS (

  SELECT 
    prophecy_column_8 AS Overs,
    prophecy_column_1 AS Name,
    prophecy_column_17 AS Nb,
    prophecy_column_4 AS `4s`,
    prophecy_column_7 AS Team,
    prophecy_column_11 AS Wickets,
    prophecy_column_16 AS Wd,
    prophecy_column_3 AS Balls,
    prophecy_column_15 AS Right_6s,
    prophecy_column_18 AS F1,
    prophecy_column_10 AS Right_Runs,
    prophecy_column_13 AS Dots,
    prophecy_column_9 AS Maidens,
    prophecy_column_5 AS `6s`,
    prophecy_column_2 AS Runs,
    prophecy_column_14 AS Right_4s,
    prophecy_column_12 AS Econ,
    prophecy_column_6 AS SR
  
  FROM Union_29 AS in0

),

Join_30_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Player`)
  
  FROM Union_29_postRename AS in0
  INNER JOIN Union_26_postRename AS in1
     ON (in0.Name = in1.Player)

),

Union_39_3 AS (

  SELECT 
    CAST(`6s` AS STRING) AS prophecy_column_5,
    CAST(Right_Runs AS STRING) AS prophecy_column_10,
    CAST(Right_4s AS STRING) AS prophecy_column_14,
    Catches AS prophecy_column_20,
    CAST(Name AS STRING) AS prophecy_column_1,
    CAST(SR AS STRING) AS prophecy_column_6,
    CAST(Maidens AS STRING) AS prophecy_column_9,
    CAST(Dots AS STRING) AS prophecy_column_13,
    CAST(Runs AS STRING) AS prophecy_column_2,
    CAST(Nb AS STRING) AS prophecy_column_17,
    CAST(Econ AS STRING) AS prophecy_column_12,
    CAST(Team AS STRING) AS prophecy_column_7,
    CAST(Balls AS STRING) AS prophecy_column_3,
    F1 AS prophecy_column_18,
    CAST(Wd AS STRING) AS prophecy_column_16,
    CAST(Wickets AS STRING) AS prophecy_column_11,
    CAST(Overs AS STRING) AS prophecy_column_8,
    `Run Outs` AS prophecy_column_19,
    CAST(`4s` AS STRING) AS prophecy_column_4,
    CAST(Right_6s AS STRING) AS prophecy_column_15
  
  FROM Join_30_inner AS in0

),

Join_30_right AS (

  SELECT in0.*
  
  FROM Union_26_postRename AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM Union_29_postRename AS in1
    
    WHERE in1.Name IS NOT NULL
  ) AS in1_keys
     ON (in1_keys.Name = in0.Player)
  
  WHERE (in1_keys.Name IS NULL)

),

Join_30_left AS (

  SELECT in0.*
  
  FROM Union_29_postRename AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM Union_26_postRename AS in1
    
    WHERE in1.Player IS NOT NULL
  ) AS in1_keys
     ON (in0.Name = in1_keys.Player)
  
  WHERE (in1_keys.Player IS NULL)

),

TextToColumns_31 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Join_30_left'], 
      'Name', 
      "\\\ ", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'Name', 
      'Name', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_31_dropGem_0 AS (

  SELECT 
    Name_1_Name AS Name1,
    Name_2_Name AS Name2,
    * EXCEPT (`Name_1_Name`, `Name_2_Name`)
  
  FROM TextToColumns_31 AS in0

),

Join_32_left AS (

  SELECT in0.*
  
  FROM TextToColumns_31_dropGem_0 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM Join_30_right AS in1
    
    WHERE in1.Player IS NOT NULL
  ) AS in1_keys
     ON (in0.Name2 = in1_keys.Player)
  
  WHERE (in1_keys.Player IS NULL)

),

AlteryxSelect_40 AS (

  SELECT * EXCEPT (`Name1`, `Name2`)
  
  FROM Join_32_left AS in0

),

Union_39_2 AS (

  SELECT 
    CAST(`6s` AS STRING) AS prophecy_column_5,
    CAST(Right_Runs AS STRING) AS prophecy_column_10,
    CAST(Right_4s AS STRING) AS prophecy_column_14,
    CAST(Name AS STRING) AS prophecy_column_1,
    CAST(SR AS STRING) AS prophecy_column_6,
    CAST(Maidens AS STRING) AS prophecy_column_9,
    CAST(Dots AS STRING) AS prophecy_column_13,
    CAST(Runs AS STRING) AS prophecy_column_2,
    CAST(Nb AS STRING) AS prophecy_column_17,
    CAST(Econ AS STRING) AS prophecy_column_12,
    CAST(Team AS STRING) AS prophecy_column_7,
    CAST(Balls AS STRING) AS prophecy_column_3,
    F1 AS prophecy_column_18,
    CAST(Wd AS STRING) AS prophecy_column_16,
    CAST(Wickets AS STRING) AS prophecy_column_11,
    CAST(Overs AS STRING) AS prophecy_column_8,
    CAST(`4s` AS STRING) AS prophecy_column_4,
    CAST(Right_6s AS STRING) AS prophecy_column_15
  
  FROM AlteryxSelect_40 AS in0

),

Join_32_inner AS (

  SELECT 
    in0.* EXCEPT (`Name1`, `Name2`),
    in1.*
  
  FROM TextToColumns_31_dropGem_0 AS in0
  INNER JOIN Join_30_right AS in1
     ON (in0.Name2 = in1.Player)

),

Summarize_33 AS (

  SELECT 
    (
      COUNT(
        (
          CASE
            WHEN ((Player IS NULL) OR (CAST(CAST(Player AS STRING) AS STRING) = ''))
              THEN NULL
            ELSE 1
          END
        )) OVER (PARTITION BY Player ORDER BY 1 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
    ) AS `Count`,
    *
  
  FROM Join_32_inner AS in0

),

Join_34_inner_formula AS (

  SELECT *
  
  FROM Summarize_33

),

Filter_35_reject AS (

  SELECT * 
  
  FROM Join_34_inner_formula AS in0
  
  WHERE (
          (
            NOT(
              COUNT > 1)
          ) OR ((COUNT > 1) IS NULL)
        )

),

AlteryxSelect_37 AS (

  SELECT * EXCEPT (`Player`, `Count`)
  
  FROM Filter_35_reject AS in0

),

Union_39_0 AS (

  SELECT 
    CAST(`6s` AS STRING) AS prophecy_column_5,
    CAST(Right_Runs AS STRING) AS prophecy_column_10,
    CAST(Right_4s AS STRING) AS prophecy_column_14,
    Catches AS prophecy_column_20,
    CAST(Name AS STRING) AS prophecy_column_1,
    CAST(SR AS STRING) AS prophecy_column_6,
    CAST(Maidens AS STRING) AS prophecy_column_9,
    CAST(Dots AS STRING) AS prophecy_column_13,
    CAST(Runs AS STRING) AS prophecy_column_2,
    CAST(Nb AS STRING) AS prophecy_column_17,
    CAST(Econ AS STRING) AS prophecy_column_12,
    CAST(Team AS STRING) AS prophecy_column_7,
    CAST(Balls AS STRING) AS prophecy_column_3,
    F1 AS prophecy_column_18,
    CAST(Wd AS STRING) AS prophecy_column_16,
    CAST(Wickets AS STRING) AS prophecy_column_11,
    CAST(Overs AS STRING) AS prophecy_column_8,
    `Run Outs` AS prophecy_column_19,
    CAST(`4s` AS STRING) AS prophecy_column_4,
    CAST(Right_6s AS STRING) AS prophecy_column_15
  
  FROM AlteryxSelect_37 AS in0

),

Filter_35 AS (

  SELECT * 
  
  FROM Join_34_inner_formula AS in0
  
  WHERE (COUNT > 1)

),

Formula_36_0 AS (

  SELECT 
    (
      CASE
        WHEN (`Run Outs` IS NOT NULL)
          THEN 100
        ELSE `Run Outs`
      END
    ) AS `Run Outs`,
    (
      CASE
        WHEN (Catches IS NOT NULL)
          THEN 100
        ELSE Catches
      END
    ) AS Catches,
    * EXCEPT (`run outs`, `catches`)
  
  FROM Filter_35 AS in0

),

AlteryxSelect_38 AS (

  SELECT * EXCEPT (`Player`, `Count`)
  
  FROM Formula_36_0 AS in0

),

Union_39_1 AS (

  SELECT 
    CAST(`6s` AS STRING) AS prophecy_column_5,
    CAST(Right_Runs AS STRING) AS prophecy_column_10,
    CAST(Right_4s AS STRING) AS prophecy_column_14,
    Catches AS prophecy_column_20,
    CAST(Name AS STRING) AS prophecy_column_1,
    CAST(SR AS STRING) AS prophecy_column_6,
    CAST(Maidens AS STRING) AS prophecy_column_9,
    CAST(Dots AS STRING) AS prophecy_column_13,
    CAST(Runs AS STRING) AS prophecy_column_2,
    CAST(Nb AS STRING) AS prophecy_column_17,
    CAST(Econ AS STRING) AS prophecy_column_12,
    CAST(Team AS STRING) AS prophecy_column_7,
    CAST(Balls AS STRING) AS prophecy_column_3,
    F1 AS prophecy_column_18,
    CAST(Wd AS STRING) AS prophecy_column_16,
    CAST(Wickets AS STRING) AS prophecy_column_11,
    CAST(Overs AS STRING) AS prophecy_column_8,
    `Run Outs` AS prophecy_column_19,
    CAST(`4s` AS STRING) AS prophecy_column_4,
    CAST(Right_6s AS STRING) AS prophecy_column_15
  
  FROM AlteryxSelect_38 AS in0

),

Union_39 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_39_3', 'Union_39_0', 'Union_39_1', 'Union_39_2'], 
      [
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_20", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "Integer"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}]', 
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_20", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "Integer"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}]', 
        '[{"name": "prophecy_column_19", "dataType": "Double"}, {"name": "prophecy_column_20", "dataType": "Double"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "Integer"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}]', 
        '[{"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "String"}, {"name": "prophecy_column_18", "dataType": "Integer"}, {"name": "prophecy_column_3", "dataType": "String"}, {"name": "prophecy_column_15", "dataType": "String"}, {"name": "prophecy_column_7", "dataType": "String"}, {"name": "prophecy_column_4", "dataType": "String"}, {"name": "prophecy_column_9", "dataType": "String"}, {"name": "prophecy_column_12", "dataType": "String"}, {"name": "prophecy_column_16", "dataType": "String"}, {"name": "prophecy_column_11", "dataType": "String"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "String"}, {"name": "prophecy_column_10", "dataType": "String"}, {"name": "prophecy_column_13", "dataType": "String"}, {"name": "prophecy_column_17", "dataType": "String"}, {"name": "prophecy_column_5", "dataType": "String"}, {"name": "prophecy_column_14", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_39_postRename AS (

  SELECT 
    prophecy_column_8 AS Overs,
    prophecy_column_1 AS Name,
    prophecy_column_17 AS Nb,
    prophecy_column_19 AS `Run Outs`,
    prophecy_column_4 AS `4s`,
    prophecy_column_7 AS Team,
    prophecy_column_11 AS Wickets,
    prophecy_column_16 AS Wd,
    prophecy_column_3 AS Balls,
    prophecy_column_15 AS Right_6s,
    prophecy_column_18 AS F1,
    prophecy_column_20 AS Catches,
    prophecy_column_10 AS Right_Runs,
    prophecy_column_13 AS Dots,
    prophecy_column_9 AS Maidens,
    prophecy_column_5 AS `6s`,
    prophecy_column_2 AS Runs,
    prophecy_column_14 AS Right_4s,
    prophecy_column_12 AS Econ,
    prophecy_column_6 AS SR
  
  FROM Union_39 AS in0

),

Formula_43_0 AS (

  SELECT 
    (
      CASE
        WHEN (F1 IS NULL)
          THEN 100
        ELSE F1
      END
    ) AS F1,
    * EXCEPT (`f1`)
  
  FROM Union_39_postRename AS in0

),

Sort_41 AS (

  SELECT * 
  
  FROM Formula_43_0 AS in0
  
  ORDER BY Team ASC, F1 ASC

),

AlteryxSelect_42 AS (

  select  CAST (`4s` AS FLOAT64) as `4s` ,  CAST (`6s` AS FLOAT64) as `6s` ,  `Run Outs` as `Run Outs` , *   REPLACE( Name as `Name` ,  Team as `Team` ,  CAST (Runs AS FLOAT64) as `Runs` ,  CAST (Balls AS FLOAT64) as `Balls` ,  CAST (SR AS FLOAT64) as `SR` ,  CAST (Overs AS FLOAT64) as `Overs` ,  CAST (Maidens AS FLOAT64) as `Maidens` ,  CAST (Wickets AS FLOAT64) as `Wickets` ,  CAST (Econ AS FLOAT64) as `Econ` ,  CAST (Dots AS FLOAT64) as `Dots` ,  CAST (Wd AS FLOAT64) as `Wd` ,  CAST (Nb AS FLOAT64) as `Nb` ,  Catches as `Catches` ,  F1 as `F1` ) from Sort_41

),

Cleanse_44 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_42'], 
      [
        { "name": "Overs", "dataType": "Double" }, 
        { "name": "Name", "dataType": "String" }, 
        { "name": "Nb", "dataType": "Double" }, 
        { "name": "Run Outs", "dataType": "Double" }, 
        { "name": "4s", "dataType": "Double" }, 
        { "name": "Team", "dataType": "String" }, 
        { "name": "Wickets", "dataType": "Double" }, 
        { "name": "Wd", "dataType": "Double" }, 
        { "name": "Balls", "dataType": "Double" }, 
        { "name": "F1", "dataType": "Integer" }, 
        { "name": "Catches", "dataType": "Double" }, 
        { "name": "Dots", "dataType": "Double" }, 
        { "name": "Maidens", "dataType": "Double" }, 
        { "name": "6s", "dataType": "Double" }, 
        { "name": "Runs", "dataType": "Double" }, 
        { "name": "Econ", "dataType": "Double" }, 
        { "name": "SR", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'Name', 
        'Team', 
        'Runs', 
        'Balls', 
        '4s', 
        '6s', 
        'SR', 
        'Overs', 
        'Maidens', 
        'Wickets', 
        'Econ', 
        'Dots', 
        'Wd', 
        'Nb', 
        'Run Outs', 
        'Catches'
      ], 
      true, 
      '', 
      true, 
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

Formula_45_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((Runs = 0) AND (Balls > 0))
          THEN 1
        ELSE 0
      END
    ) AS FLOAT64) AS DuckquesMark,
    *
  
  FROM Cleanse_44 AS in0

),

Join_48_left AS (

  SELECT in0.*
  
  FROM Formula_45_0 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM AlteryxSelect_47 AS in1
    
    WHERE in1.Name IS NOT NULL
  ) AS in1_keys
     ON (in0.Name = in1_keys.Name)
  
  WHERE (in1_keys.Name IS NULL)

),

Union_61_split_1 AS (

  SELECT 
    DuckquesMark AS prophecy_column_5,
    Maidens AS prophecy_column_10,
    Wd AS prophecy_column_14,
    CAST(Name AS STRING) AS prophecy_column_1,
    `4s` AS prophecy_column_6,
    Overs AS prophecy_column_9,
    Dots AS prophecy_column_13,
    CAST(Team AS STRING) AS prophecy_column_2,
    Catches AS prophecy_column_17,
    Econ AS prophecy_column_12,
    `6s` AS prophecy_column_7,
    Runs AS prophecy_column_3,
    F1 AS prophecy_column_18,
    `Run Outs` AS prophecy_column_16,
    Wickets AS prophecy_column_11,
    SR AS prophecy_column_8,
    Balls AS prophecy_column_4,
    Nb AS prophecy_column_15
  
  FROM Join_48_left AS in0

),

Join_48_inner AS (

  SELECT 
    in1.F1 AS Right_F1,
    in0.*,
    in1.* EXCEPT (`Name`, `F1`)
  
  FROM Formula_45_0 AS in0
  INNER JOIN AlteryxSelect_47 AS in1
     ON (in0.Name = in1.Name)

),

Formula_49_0 AS (

  SELECT 
    (
      CASE
        WHEN ((DuckquesMark = 1) AND ((STRPOS((coalesce(LOWER(variableDesc), '')), LOWER('not out'))) > 0))
          THEN 0
        ELSE DuckquesMark
      END
    ) AS DuckquesMark,
    * EXCEPT (`duckquesMark`)
  
  FROM Join_48_inner AS in0

),

Union_61_split_0 AS (

  SELECT 
    DuckquesMark AS prophecy_column_5,
    Maidens AS prophecy_column_10,
    Wd AS prophecy_column_14,
    CAST(Name AS STRING) AS prophecy_column_1,
    `4s` AS prophecy_column_6,
    Overs AS prophecy_column_9,
    Dots AS prophecy_column_13,
    CAST(Team AS STRING) AS prophecy_column_2,
    Catches AS prophecy_column_17,
    Econ AS prophecy_column_12,
    `6s` AS prophecy_column_7,
    Runs AS prophecy_column_3,
    F1 AS prophecy_column_18,
    `Run Outs` AS prophecy_column_16,
    Wickets AS prophecy_column_11,
    SR AS prophecy_column_8,
    CAST(variableDesc AS STRING) AS prophecy_column_19,
    Balls AS prophecy_column_4,
    Nb AS prophecy_column_15
  
  FROM Formula_49_0 AS in0

),

Union_61_split AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_61_split_0', 'Union_61_split_1'], 
      [
        '[{"name": "prophecy_column_19", "dataType": "String"}, {"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Double"}, {"name": "prophecy_column_18", "dataType": "Integer"}, {"name": "prophecy_column_3", "dataType": "Double"}, {"name": "prophecy_column_15", "dataType": "Double"}, {"name": "prophecy_column_7", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Double"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "Double"}, {"name": "prophecy_column_16", "dataType": "Double"}, {"name": "prophecy_column_11", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "Double"}, {"name": "prophecy_column_10", "dataType": "Double"}, {"name": "prophecy_column_13", "dataType": "Double"}, {"name": "prophecy_column_17", "dataType": "Double"}, {"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Double"}]', 
        '[{"name": "prophecy_column_2", "dataType": "String"}, {"name": "prophecy_column_6", "dataType": "Double"}, {"name": "prophecy_column_18", "dataType": "Integer"}, {"name": "prophecy_column_3", "dataType": "Double"}, {"name": "prophecy_column_15", "dataType": "Double"}, {"name": "prophecy_column_7", "dataType": "Double"}, {"name": "prophecy_column_4", "dataType": "Double"}, {"name": "prophecy_column_9", "dataType": "Double"}, {"name": "prophecy_column_12", "dataType": "Double"}, {"name": "prophecy_column_16", "dataType": "Double"}, {"name": "prophecy_column_11", "dataType": "Double"}, {"name": "prophecy_column_1", "dataType": "String"}, {"name": "prophecy_column_8", "dataType": "Double"}, {"name": "prophecy_column_10", "dataType": "Double"}, {"name": "prophecy_column_13", "dataType": "Double"}, {"name": "prophecy_column_17", "dataType": "Double"}, {"name": "prophecy_column_5", "dataType": "Double"}, {"name": "prophecy_column_14", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_61_split_postRename AS (

  SELECT 
    prophecy_column_9 AS Overs,
    prophecy_column_1 AS Name,
    prophecy_column_15 AS Nb,
    prophecy_column_16 AS `Run Outs`,
    prophecy_column_6 AS `4s`,
    prophecy_column_2 AS Team,
    prophecy_column_19 AS variableDesc,
    prophecy_column_11 AS Wickets,
    prophecy_column_5 AS DuckquesMark,
    prophecy_column_14 AS Wd,
    prophecy_column_4 AS Balls,
    prophecy_column_18 AS F1,
    prophecy_column_17 AS Catches,
    prophecy_column_13 AS Dots,
    prophecy_column_10 AS Maidens,
    prophecy_column_7 AS `6s`,
    prophecy_column_3 AS Runs,
    prophecy_column_12 AS Econ,
    prophecy_column_8 AS SR
  
  FROM Union_61_split AS in0

),

Union_61_1 AS (

  SELECT * 
  
  FROM Union_61_split_postRename AS in0

),

Join_32_right AS (

  SELECT in0.*
  
  FROM Join_30_right AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM TextToColumns_31_dropGem_0 AS in1
    
    WHERE in1.Name2 IS NOT NULL
  ) AS in1_keys
     ON (in1_keys.Name2 = in0.Player)
  
  WHERE (in1_keys.Name2 IS NULL)

),

Union_61_0 AS (

  SELECT 
    CAST(Player AS STRING) AS prophecy_column_1,
    `Run Outs` AS prophecy_column_16,
    Catches AS prophecy_column_17
  
  FROM Join_32_right AS in0

),

Union_61 AS (

  {{ prophecy_basics.UnionByName(['Union_61_0', 'Union_61_1'], ['[]', '[]'], 'allowMissingColumns') }}

),

Union_61_postRename AS (

  SELECT 
    CAST(NULL AS string) AS Overs,
    CAST(NULL AS string) AS Name,
    CAST(NULL AS string) AS Nb,
    CAST(NULL AS string) AS `Run Outs`,
    CAST(NULL AS string) AS `4s`,
    CAST(NULL AS string) AS Team,
    CAST(NULL AS string) AS variableDesc,
    CAST(NULL AS string) AS Wickets,
    CAST(NULL AS string) AS DuckquesMark,
    CAST(NULL AS string) AS Wd,
    CAST(NULL AS string) AS Balls,
    CAST(NULL AS string) AS F1,
    CAST(NULL AS string) AS Catches,
    CAST(NULL AS string) AS Dots,
    CAST(NULL AS string) AS Maidens,
    CAST(NULL AS string) AS `6s`,
    CAST(NULL AS string) AS Runs,
    CAST(NULL AS string) AS Econ,
    CAST(NULL AS string) AS SR
  
  FROM Union_61 AS in0

),

Sort_51 AS (

  SELECT * 
  
  FROM Union_61_postRename AS in0
  
  ORDER BY TEAM ASC, F1 ASC, NAME ASC

),

AlteryxSelect_52 AS (

  SELECT 
    CAST(NULL AS string) AS `Run OutsslashStumpings`,
    *
  
  FROM Sort_51

),

Cleanse_53 AS (

  {{ prophecy_basics.DataCleansing(['AlteryxSelect_52'],,'keepOriginal',['Name', 'Team', 'Runs', 'Balls', 'DuckquesMark', '4s', '6s', 'SR', 'Overs', 'Maidens', 'Wickets', 'Econ', 'Dots', 'Wd', 'Nb', 'Run OutsslashStumpings', 'Catches'],true,'',true,0,true,false,false,false,false,false,false,false,'1970-01-01',false,'1970-01-01 00:00:00.0') }}

),

Formula_54_0 AS (

  SELECT 
    CAST(CURRENT_DATE AS STRING) AS `variableDate`,
    *
  
  FROM Cleanse_53

)

SELECT *

FROM Formula_54_0
