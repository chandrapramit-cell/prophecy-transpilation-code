{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH Movies_csv_12 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('107_IMDB_alteryx', 'Movies_csv_12') }}

),

IMDb_TopVoted_G_1 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('107_IMDB_alteryx', 'IMDb_TopVoted_G_1') }}

),

Cleanse_2 AS (

  {{
    prophecy_basics.DataCleansing(
      ['IMDb_TopVoted_G_1'], 
      [
        { "name": "votes", "dataType": "String" }, 
        { "name": "variableyear", "dataType": "String" }, 
        { "name": "rank", "dataType": "String" }, 
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
    CAST(CASE
      WHEN contains(coalesce(lower(hours1), ''), lower('m'))
        THEN coalesce(
          CAST(regexp_replace(hours1, '[m]', substring('', 1, 1)) AS DOUBLE), 
          CAST(regexp_extract(regexp_replace(hours1, '[m]', substring('', 1, 1)), '^[0-9]+', 0) AS INT), 
          0)
      ELSE (
        (
          60
          * coalesce(
              CAST(regexp_replace(hours1, '[h]', substring('', 1, 1)) AS DOUBLE), 
              CAST(regexp_extract(regexp_replace(hours1, '[h]', substring('', 1, 1)), '^[0-9]+', 0) AS INT), 
              0)
        )
        + coalesce(
            CAST(regexp_replace(hours2, '[m]', substring('', 1, 1)) AS DOUBLE), 
            CAST(regexp_extract(regexp_replace(hours2, '[m]', substring('', 1, 1)), '^[0-9]+', 0) AS INT), 
            0)
      )
    END AS STRING) AS runtime,
    CAST(rank AS INTEGER) AS rank,
    * EXCEPT (`rank`, `runtime`)
  
  FROM TextToColumns_5_dropGem_0 AS in0

),

Formula_7_1 AS (

  SELECT 
    runtime AS runtimeMinutes,
    CAST(variableyear AS INTEGER) AS variableyear,
    CAST(rating AS FLOAT64) AS rating,
    CAST(votes AS INTEGER) AS votes,
    * EXCEPT (`votes`, `variableyear`, `rating`)
  
  FROM Formula_7_0 AS in0

),

Join_11_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`movie_id`)
  
  FROM Formula_7_1 AS in0
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
        WHEN CAST((genre2 IS NULL) AS BOOLEAN)
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
        WHEN CAST((genre3 IS NULL) AS BOOLEAN)
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
        WHEN CAST((ratingCategory IS NULL) AS BOOLEAN)
          THEN 'Not Rated'
        WHEN (ratingCategory = 'Unwanted')
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
    movie_id AS movie_id,
    rank AS rank,
    title AS title,
    originalTitle AS originalTitle,
    description AS description,
    variableyear AS variableyear,
    votes AS votes,
    rating AS rating,
    CAST(runtimeMinutes AS INTEGER) AS runtimeMinutes,
    * EXCEPT (`runtimeminutes`, 
    `votes`, 
    `description`, 
    `rank`, 
    `originaltitle`, 
    `variableyear`, 
    `ratingcategory`, 
    `rating`, 
    `genre1`, 
    `title`, 
    `movie_id`)
  
  FROM Formula_19_0 AS in0

),

Formula_25_1 AS (

  SELECT 
    genre1 AS genre01,
    genre2 AS genre02,
    genre3 AS genre03,
    *
  
  FROM Formula_25_0 AS in0

)

SELECT *

FROM Formula_25_1
