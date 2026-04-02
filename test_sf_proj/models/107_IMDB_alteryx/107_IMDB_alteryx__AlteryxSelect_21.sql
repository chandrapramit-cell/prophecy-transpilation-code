{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH IMDb_TopVoted_G_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('107_IMDB_alteryx', 'IMDb_TopVoted_G_1') }}

),

Cleanse_2 AS (

  {{
    prophecy_basics.DataCleansing(
      ['IMDb_TopVoted_G_1'], 
      [
        { "name": "RATING", "dataType": "String" }, 
        { "name": "VOTES", "dataType": "String" }, 
        { "name": "TITLE", "dataType": "String" }, 
        { "name": "RANK", "dataType": "String" }, 
        { "name": "YEAR", "dataType": "String" }, 
        { "name": "MOVIE_ID", "dataType": "String" }, 
        { "name": "RUNTIME", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['RANK', 'VOTES'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      false, 
      true, 
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

TextToColumns_5 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Cleanse_2'], 
      'RUNTIME', 
      "\\\\\\\\\\\\s", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'RUNTIME', 
      'RUNTIME', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_5_dropGem_0 AS (

  SELECT 
    RUNTIME_1_RUNTIME AS HOURS1,
    RUNTIME_2_RUNTIME AS HOURS2,
    *
  
  FROM TextToColumns_5 AS in0

),

Formula_7_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (CONTAINS((coalesce(LOWER(HOURS1), '')), LOWER('m')))
          THEN (
            coalesce(
              CAST((REGEXP_REPLACE(HOURS1, '[m]', SUBSTRING('', 1, 1))) AS DOUBLE), 
              CAST((REGEXP_SUBSTR((REGEXP_REPLACE(HOURS1, '[m]', SUBSTRING('', 1, 1))), '^[0-9]+')) AS INTEGER), 
              0)
          )
        ELSE (
          (
            60
            * (
                coalesce(
                  CAST((REGEXP_REPLACE(HOURS1, '[h]', SUBSTRING('', 1, 1))) AS DOUBLE), 
                  CAST((REGEXP_SUBSTR((REGEXP_REPLACE(HOURS1, '[h]', SUBSTRING('', 1, 1))), '^[0-9]+')) AS INTEGER), 
                  0)
              )
          )
          + (
              coalesce(
                CAST((REGEXP_REPLACE(HOURS2, '[m]', SUBSTRING('', 1, 1))) AS DOUBLE), 
                CAST((REGEXP_SUBSTR((REGEXP_REPLACE(HOURS2, '[m]', SUBSTRING('', 1, 1))), '^[0-9]+')) AS INTEGER), 
                0)
            )
        )
      END
    ) AS string) AS RUNTIME,
    * EXCLUDE ("RUNTIME")
  
  FROM TextToColumns_5_dropGem_0 AS in0

),

AlteryxSelect_9 AS (

  SELECT 
    CAST(RANK AS INTEGER) AS RANK,
    RUNTIME AS RUNTIMEMINUTES,
    CAST("YEAR" AS INTEGER) AS YEAR,
    CAST(RATING AS DOUBLE) AS RATING,
    CAST(VOTES AS INTEGER) AS VOTES,
    * EXCLUDE ("HOURS1", "HOURS2", "RANK", "YEAR", "RATING", "VOTES", "RUNTIME")
  
  FROM Formula_7_0 AS in0

),

Movies_csv_12 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('107_IMDB_alteryx', 'Movies_csv_12') }}

),

Join_11_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("MOVIE_ID")
  
  FROM AlteryxSelect_9 AS in0
  INNER JOIN Movies_csv_12 AS in1
     ON (in0.MOVIE_ID = in1.MOVIE_ID)

),

TextToColumns_16 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Join_11_inner'], 
      'GENRES', 
      ",", 
      'splitColumns', 
      3, 
      'leaveExtraCharLastCol', 
      'GENRES', 
      'GENRES', 
      'GENERATEDCOLUMNNAME'
    )
  }}

),

TextToColumns_16_dropGem_0 AS (

  SELECT 
    GENRES_1_GENRES AS GENRE1,
    GENRES_2_GENRES AS GENRE2,
    GENRES_3_GENRES AS GENRE3,
    *
  
  FROM TextToColumns_16 AS in0

),

Formula_18_to_Formula_25_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (GENRE2 IS NULL)
          THEN '-'
        ELSE GENRE2
      END
    ) AS string) AS GENRE2,
    CAST((
      CASE
        WHEN (GENRE3 IS NULL)
          THEN '-'
        ELSE GENRE3
      END
    ) AS string) AS GENRE3,
    CAST((
      CASE
        WHEN ((RATINGCATEGORY IS NULL) OR (RATINGCATEGORY = 'Unwanted'))
          THEN 'Not Rated'
        WHEN (RATINGCATEGORY = 'Approved')
          THEN 'G'
        ELSE RATINGCATEGORY
      END
    ) AS string) AS RATINGCATEGORY,
    CAST((
      CASE
        WHEN (GENRE1 = '\\N')
          THEN '-'
        ELSE GENRE1
      END
    ) AS string) AS GENRE1,
    * EXCLUDE ("GENRE3", "GENRE2", "RATINGCATEGORY", "GENRE1")
  
  FROM TextToColumns_16_dropGem_0 AS in0

),

AlteryxSelect_21 AS (

  SELECT 
    MOVIE_ID AS MOVIE_ID,
    RANK AS RANK,
    TITLE AS TITLE,
    ORIGINALTITLE AS ORIGINALTITLE,
    DESCRIPTION AS DESCRIPTION,
    "YEAR" AS YEAR,
    VOTES AS VOTES,
    RATING AS RATING,
    CAST(RUNTIMEMINUTES AS INTEGER) AS RUNTIMEMINUTES,
    RATINGCATEGORY AS RATINGCATEGORY,
    GENRE1 AS GENRE01,
    GENRE2 AS GENRE02,
    GENRE3 AS GENRE03,
    * EXCLUDE ("GENRES", 
    "MOVIE_ID", 
    "RANK", 
    "TITLE", 
    "ORIGINALTITLE", 
    "DESCRIPTION", 
    "YEAR", 
    "VOTES", 
    "RATING", 
    "RUNTIMEMINUTES", 
    "RATINGCATEGORY", 
    "GENRE1", 
    "GENRE2", 
    "GENRE3")
  
  FROM Formula_18_to_Formula_25_0 AS in0

)

SELECT *

FROM AlteryxSelect_21
