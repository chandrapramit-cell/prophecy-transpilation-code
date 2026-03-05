{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
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
        { "name": "votes", "dataType": "String" }, 
        { "name": "rank", "dataType": "String" }, 
        { "name": "year", "dataType": "String" }, 
        { "name": "rating", "dataType": "String" }, 
        { "name": "title", "dataType": "String" }, 
        { "name": "movie_id", "dataType": "String" }, 
        { "name": "runtime", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['rank', 'votes'], 
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
      'runtime', 
      "\\\\\\\\\\\\s", 
      'splitColumns', 
      2, 
      'leaveExtraCharLastCol', 
      'runtime', 
      'runtime', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_5_dropGem_0 AS (

  SELECT 
    runtime_1_runtime AS hours1,
    runtime_2_runtime AS hours2,
    * EXCEPT (`runtime_1_runtime`, `runtime_2_runtime`)
  
  FROM TextToColumns_5 AS in0

),

Formula_7_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((STRPOS((coalesce(LOWER(hours1), '')), LOWER('m'))) > 0)
          THEN (
            coalesce(
              CAST((REGEXP_REPLACE(hours1, '[m]', SUBSTRING('' FROM 0 FOR 1))) AS FLOAT64), 
              CAST((REGEXP_EXTRACT((REGEXP_REPLACE(hours1, '[m]', SUBSTRING('' FROM 0 FOR 1))), '^[0-9]+', 0)) AS INT64), 
              0)
          )
        ELSE (
          (
            60
            * (
                coalesce(
                  CAST((REGEXP_REPLACE(hours1, '[h]', SUBSTRING('' FROM 0 FOR 1))) AS FLOAT64), 
                  CAST((REGEXP_EXTRACT((REGEXP_REPLACE(hours1, '[h]', SUBSTRING('' FROM 0 FOR 1))), '^[0-9]+', 0)) AS INT64), 
                  0)
              )
          )
          + (
              coalesce(
                CAST((REGEXP_REPLACE(hours2, '[m]', SUBSTRING('' FROM 0 FOR 1))) AS FLOAT64), 
                CAST((REGEXP_EXTRACT((REGEXP_REPLACE(hours2, '[m]', SUBSTRING('' FROM 0 FOR 1))), '^[0-9]+', 0)) AS INT64), 
                0)
            )
        )
      END
    ) AS STRING) AS runtime,
    * EXCEPT (`runtime`)
  
  FROM TextToColumns_5_dropGem_0 AS in0

),

AlteryxSelect_9 AS (

  SELECT 
    CAST(rank AS INT64) AS rank,
    runtime AS runtimeMinutes,
    CAST(YEAR AS INT64) AS year,
    CAST(rating AS FLOAT64) AS rating,
    CAST(votes AS INT64) AS votes,
    * EXCEPT (`hours1`, `hours2`, `rank`, `year`, `rating`, `votes`, `runtime`)
  
  FROM Formula_7_0 AS in0

),

Movies_csv_12 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('107_IMDB_alteryx', 'Movies_csv_12') }}

),

Join_11_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`movie_id`)
  
  FROM AlteryxSelect_9 AS in0
  INNER JOIN Movies_csv_12 AS in1
     ON (in0.movie_id = in1.movie_id)

),

Sort_13 AS (

  SELECT * 
  
  FROM Join_11_inner AS in0
  
  ORDER BY rank ASC

),

TextToColumns_16 AS (

  {{
    prophecy_basics.TextToColumns(
      ['Sort_13'], 
      'genres', 
      ",", 
      'splitColumns', 
      3, 
      'leaveExtraCharLastCol', 
      'genres', 
      'genres', 
      'generatedColumnName'
    )
  }}

),

TextToColumns_16_dropGem_0 AS (

  SELECT 
    genres_1_genres AS genre1,
    genres_2_genres AS genre2,
    genres_3_genres AS genre3,
    * EXCEPT (`genres_1_genres`, `genres_2_genres`, `genres_3_genres`)
  
  FROM TextToColumns_16 AS in0

),

Formula_18_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (genre2 IS NULL)
          THEN '-'
        ELSE genre2
      END
    ) AS STRING) AS genre2,
    * EXCEPT (`genre2`)
  
  FROM TextToColumns_16_dropGem_0 AS in0

),

Formula_19_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (genre3 IS NULL)
          THEN '-'
        ELSE genre3
      END
    ) AS STRING) AS genre3,
    * EXCEPT (`genre3`)
  
  FROM Formula_18_0 AS in0

),

Formula_25_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((ratingCategory IS NULL) OR (ratingCategory = 'Unwanted'))
          THEN 'Not Rated'
        WHEN (ratingCategory = 'Approved')
          THEN 'G'
        ELSE ratingCategory
      END
    ) AS STRING) AS ratingCategory,
    CAST((
      CASE
        WHEN (genre1 = '\\N')
          THEN '-'
        ELSE genre1
      END
    ) AS STRING) AS genre1,
    * EXCEPT (`ratingcategory`, `genre1`)
  
  FROM Formula_19_0 AS in0

),

AlteryxSelect_21 AS (

  select  genre1 as `genre01` ,  genre2 as `genre02` ,  genre3 as `genre03` , *   REPLACE( movie_id as `movie_id` ,  rank as `rank` ,  title as `title` ,  originalTitle as `originalTitle` ,  description as `description` ,  YEAR as `year` ,  votes as `votes` ,  rating as `rating` ,  CAST (runtimeMinutes AS INT64) as `runtimeMinutes` ,  ratingCategory as `ratingCategory` ) from Formula_25_0

)

SELECT *

FROM AlteryxSelect_21
