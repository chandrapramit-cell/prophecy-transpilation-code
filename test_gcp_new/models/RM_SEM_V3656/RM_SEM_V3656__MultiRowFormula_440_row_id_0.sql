{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Database__LOADI_341 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3656', 'Database__LOADI_341') }}

),

Summarize_343 AS (

  SELECT DISTINCT NDOD AS NDOD
  
  FROM Database__LOADI_341 AS in0

),

Formula_344_0 AS (

  SELECT 
    CAST((SUBSTRING(NDOD, 1, 3)) AS string) AS ORIG,
    CAST((SUBSTRING(NDOD, (((LENGTH(NDOD)) - 3) + 1), 3)) AS string) AS DEST,
    *
  
  FROM Summarize_343 AS in0

),

AlteryxSelect_346 AS (

  SELECT * EXCEPT (`NDOD`, `ORIG`)
  
  FROM Formula_344_0 AS in0

),

Union_345_0 AS (

  SELECT CAST(DEST AS string) AS prophecy_column_1
  
  FROM AlteryxSelect_346 AS in0

),

AlteryxSelect_347 AS (

  SELECT * EXCEPT (`NDOD`, `DEST`)
  
  FROM Formula_344_0 AS in0

),

Union_345_1 AS (

  SELECT CAST(ORIG AS string) AS prophecy_column_1
  
  FROM AlteryxSelect_347 AS in0

),

Union_345 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_345_0', 'Union_345_1'], 
      [
        '[{"name": "prophecy_column_1", "dataType": "String"}]', 
        '[{"name": "prophecy_column_1", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Union_345_postRename AS (

  SELECT prophecy_column_1 AS ORIG
  
  FROM Union_345 AS in0

),

Database__LOADI_283 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3656', 'Database__LOADI_283') }}

),

Summarize_436 AS (

  SELECT 
    SUM(`SUM(REV.LEG_PAX_CNT)`) AS PAX,
    LEG_NDOD AS LEG_NDOD,
    TT_NDOD AS TT_NDOD
  
  FROM Database__LOADI_283 AS in0
  
  GROUP BY 
    LEG_NDOD, TT_NDOD

),

Filter_288 AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3656__Filter_288')}}

),

Summarize_348 AS (

  SELECT DISTINCT ORIG AS ORIG
  
  FROM Union_345_postRename AS in0

),

Join_438_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`LEG_NDOD`)
  
  FROM Summarize_436 AS in0
  INNER JOIN Filter_288 AS in1
     ON (in0.LEG_NDOD = in1.LEG_NDOD)

),

Filter_439 AS (

  SELECT * 
  
  FROM Join_438_inner AS in0
  
  WHERE (PAX >= 365)

),

Formula_442_0 AS (

  SELECT 
    CAST((SUBSTRING(TT_NDOD, 1, 3)) AS string) AS TRIP_ORIG,
    CAST((SUBSTRING(TT_NDOD, (((LENGTH(TT_NDOD)) - 3) + 1), 3)) AS string) AS TRIP_DEST,
    *
  
  FROM Filter_439 AS in0

),

Join_444_inner AS (

  SELECT 
    in0.*,
    in1.*
  
  FROM Formula_442_0 AS in0
  INNER JOIN Summarize_348 AS in1
     ON (in0.TRIP_ORIG = in1.ORIG)

),

Join_445_inner AS (

  SELECT 
    in0.* EXCEPT (`ORIG`),
    in1.*
  
  FROM Join_444_inner AS in0
  INNER JOIN Summarize_348 AS in1
     ON (in0.TRIP_DEST = in1.ORIG)

),

AlteryxSelect_443 AS (

  SELECT * EXCEPT (`TRIP_ORIG`, `TRIP_DEST`, `ORIG`)
  
  FROM Join_445_inner AS in0

),

Filter_441 AS (

  SELECT * 
  
  FROM AlteryxSelect_443 AS in0
  
  WHERE (
          (
            (
              NOT(
                LEG_NDOD = TT_NDOD)
            ) OR (LEG_NDOD IS NULL)
          ) OR (TT_NDOD IS NULL)
        )

),

MultiRowFormula_440_row_id_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_row_id,
    *
  
  FROM Filter_441 AS in0

)

SELECT *

FROM MultiRowFormula_440_row_id_0
