{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_42 AS (

  SELECT * 
  
  FROM {{ ref('seed_42')}}

),

TextInput_42_cast AS (

  SELECT 
    YEAR AS Year,
    CAST(`Winning team` AS STRING) AS `Winning team`,
    CAST(Manager AS STRING) AS Manager,
    CAST(Games AS STRING) AS Games,
    CAST(`Losing team` AS STRING) AS `Losing team`,
    CAST(Manager2 AS STRING) AS Manager2,
    CAST(Ref__ AS STRING) AS Ref__
  
  FROM TextInput_42 AS in0

),

Formula_68_0 AS (

  SELECT 
    ((INSTR(`Winning team`, '[')) - 1) AS `Winning Team_1`,
    ((INSTR(`Winning team`, '(')) - 1) AS `Winning Team_2`,
    *
  
  FROM TextInput_42_cast AS in0

),

Formula_68_1 AS (

  SELECT 
    (
      CASE
        WHEN (`Winning Team_1` <= 0)
          THEN `Winning Team_2`
        ELSE `Winning Team_1`
      END
    ) AS FIND_W,
    *
  
  FROM Formula_68_0 AS in0

),

Formula_68_2 AS (

  SELECT 
    CAST((SUBSTRING(`Winning team`, 1, FIND_W)) AS STRING) AS Winning_Team,
    ((INSTR(`Losing team`, '[')) - 1) AS `Losing Team_1`,
    ((INSTR(`Losing team`, '(')) - 1) AS `Losing Team_2`,
    *
  
  FROM Formula_68_1 AS in0

),

Formula_68_3 AS (

  SELECT 
    (
      CASE
        WHEN (`Losing Team_1` <= 0)
          THEN `Losing Team_2`
        ELSE `Losing Team_1`
      END
    ) AS FIND_L,
    *
  
  FROM Formula_68_2 AS in0

),

Formula_68_4 AS (

  SELECT 
    CAST((SUBSTRING(`Losing team`, 1, FIND_L)) AS STRING) AS Losing_Team,
    *
  
  FROM Formula_68_3 AS in0

),

Cleanse_69 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_68_4'], 
      [
        { "name": "Losing_Team", "dataType": "String" }, 
        { "name": "FIND_L", "dataType": "Integer" }, 
        { "name": "Winning_Team", "dataType": "String" }, 
        { "name": "Losing Team_1", "dataType": "Integer" }, 
        { "name": "Losing Team_2", "dataType": "Integer" }, 
        { "name": "FIND_W", "dataType": "Integer" }, 
        { "name": "Winning Team_1", "dataType": "Integer" }, 
        { "name": "Winning Team_2", "dataType": "Integer" }, 
        { "name": "Year", "dataType": "Integer" }, 
        { "name": "Winning team", "dataType": "String" }, 
        { "name": "Manager", "dataType": "String" }, 
        { "name": "Games", "dataType": "String" }, 
        { "name": "Losing team", "dataType": "String" }, 
        { "name": "Manager2", "dataType": "String" }, 
        { "name": "Ref__", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['Winning_Team', 'Losing_Team'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
      false, 
      false, 
      true, 
      true, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

AlteryxSelect_47 AS (

  SELECT 
    CAST(YEAR AS STRING) AS Year,
    * EXCEPT (`Winning team`, 
    `Manager`, 
    `Games`, 
    `Losing team`, 
    `Manager2`, 
    `Ref__`, 
    `Winning Team_1`, 
    `Winning Team_2`, 
    `FIND_W`, 
    `Losing Team_1`, 
    `Losing Team_2`, 
    `FIND_L`, 
    `Losing_Team`, 
    `Year`)
  
  FROM Cleanse_69 AS in0

),

Formula_52_0 AS (

  SELECT 
    1 AS Winning,
    *
  
  FROM AlteryxSelect_47 AS in0

),

Summarize_56 AS (

  SELECT 
    (ARRAY_TO_STRING((ARRAY_AGG(YEAR)), ',')) AS `Winning Years`,
    SUM(Winning) AS Appearances_W,
    Winning_Team AS Winning_Team
  
  FROM Formula_52_0 AS in0
  
  GROUP BY Winning_Team

),

Formula_70_0 AS (

  SELECT 
    CAST((LENGTH(Winning_Team)) AS STRING) AS length,
    *
  
  FROM Summarize_56 AS in0

),

AlteryxSelect_48 AS (

  SELECT 
    CAST(YEAR AS STRING) AS Year,
    * EXCEPT (`Winning team`, 
    `Manager`, 
    `Games`, 
    `Losing team`, 
    `Manager2`, 
    `Ref__`, 
    `Winning Team_1`, 
    `Winning Team_2`, 
    `FIND_W`, 
    `Winning_Team`, 
    `Losing Team_1`, 
    `Losing Team_2`, 
    `FIND_L`, 
    `Year`)
  
  FROM Cleanse_69 AS in0

),

Formula_53_0 AS (

  SELECT 
    1 AS Losing,
    *
  
  FROM AlteryxSelect_48 AS in0

),

Summarize_55 AS (

  SELECT 
    SUM(Losing) AS Appearances_L,
    Losing_Team AS Losing_Team
  
  FROM Formula_53_0 AS in0
  
  GROUP BY Losing_Team

),

Join_57_left_UnionFullOuter AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Formula_70_0 AS in0
  FULL JOIN Summarize_55 AS in1
     ON (in0.Winning_Team = in1.Losing_Team)

),

Cleanse_60 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_57_left_UnionFullOuter'], 
      [
        { "name": "length", "dataType": "String" }, 
        { "name": "Winning Years", "dataType": "String" }, 
        { "name": "Appearances_W", "dataType": "Integer" }, 
        { "name": "Winning_Team", "dataType": "String" }, 
        { "name": "Appearances_L", "dataType": "Integer" }, 
        { "name": "Losing_Team", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['Appearances_W', 'Appearances_L'], 
      false, 
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

Formula_59_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (Winning_Team IS NULL)
          THEN Losing_Team
        ELSE Winning_Team
      END
    ) AS STRING) AS Team,
    CAST((Appearances_W + Appearances_L) AS INT64) AS Appearances,
    *
  
  FROM Cleanse_60 AS in0

),

Formula_59_1 AS (

  SELECT 
    CAST(((Appearances_W / Appearances) * 100) AS INT64) AS `Winning Percent`,
    *
  
  FROM Formula_59_0 AS in0

),

Formula_59_2 AS (

  SELECT 
    CAST((
      CONCAT(
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT('%.0f', CAST(`Winning Percent` AS FLOAT64))), ',', '__THS__')), '__THS__', '')), 
        '%')
    ) AS STRING) AS `Winning Percentage`,
    *
  
  FROM Formula_59_1 AS in0

),

Sort_61 AS (

  SELECT * 
  
  FROM Formula_59_2 AS in0
  
  ORDER BY CAST(Team AS STRING) ASC

),

AlteryxSelect_62 AS (

  select *   REPLACE( Team as `Team` ,  Appearances as `Appearances` ,  `Winning Years` as `Winning Years` ,  `Winning Percentage` as `Winning Percentage` ) from Sort_61

)

SELECT *

FROM AlteryxSelect_62
