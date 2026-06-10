{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Filter_288 AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__Filter_288')}}

),

Database__LOADI_283 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_283') }}

),

Summarize_284 AS (

  SELECT 
    SUM(`SUM(REV.LEG_PAX_CNT)`) AS BKGS,
    LEG_NDOD AS NDOD,
    DPTR_WEEK AS DPTR_WEEK
  
  FROM Database__LOADI_283 AS in0
  
  GROUP BY 
    LEG_NDOD, DPTR_WEEK

),

Join_286_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`LEG_NDOD`)
  
  FROM Summarize_284 AS in0
  INNER JOIN Filter_288 AS in1
     ON (in0.NDOD = in1.LEG_NDOD)

),

Formula_287_0 AS (

  SELECT 
    CAST(((100 * BKGS) / TTL_BKGS) AS DOUBLE) AS INTAKE_SHARE,
    *
  
  FROM Join_286_inner AS in0

),

Database__LOADI_363 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_363') }}

),

Summarize_371 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((DPTR_DATE IS NULL) OR (CAST(DPTR_DATE AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    NDOD AS NDOD
  
  FROM Database__LOADI_363 AS in0
  
  GROUP BY NDOD

),

Filter_375 AS (

  SELECT * 
  
  FROM Summarize_371 AS in0
  
  WHERE (Count >= 10)

),

Join_374_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`NDOD`, `Count`)
  
  FROM Database__LOADI_363 AS in0
  INNER JOIN Filter_375 AS in1
     ON (in0.NDOD = in1.NDOD)

),

Join_373_inner AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__Join_373_inner')}}

),

Join_364_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN (
          (((in0.OD = in1.OD) AND (in0.DPTR_DATE = in1.DPTR_DATE)) AND (in0.FLT_NBR = in1.FLT_NBR))
          AND (in0.LEG_CABIN_CD = in1.CABIN_CODE)
        )
          THEN NULL
        ELSE in1.CABIN_CODE
      END
    ) AS CABIN_CODE,
    (
      CASE
        WHEN (
          (((in0.OD = in1.OD) AND (in0.DPTR_DATE = in1.DPTR_DATE)) AND (in0.FLT_NBR = in1.FLT_NBR))
          AND (in0.LEG_CABIN_CD = in1.CABIN_CODE)
        )
          THEN NULL
        ELSE in1.FLT_NBR
      END
    ) AS FLT_NBR,
    (
      CASE
        WHEN (
          (((in0.OD = in1.OD) AND (in0.DPTR_DATE = in1.DPTR_DATE)) AND (in0.FLT_NBR = in1.FLT_NBR))
          AND (in0.LEG_CABIN_CD = in1.CABIN_CODE)
        )
          THEN NULL
        ELSE in1.OD
      END
    ) AS OD,
    (
      CASE
        WHEN (
          (((in0.OD = in1.OD) AND (in0.DPTR_DATE = in1.DPTR_DATE)) AND (in0.FLT_NBR = in1.FLT_NBR))
          AND (in0.LEG_CABIN_CD = in1.CABIN_CODE)
        )
          THEN NULL
        ELSE in1.DPTR_DATE
      END
    ) AS DPTR_DATE,
    (
      CASE
        WHEN (
          (((in0.OD = in1.OD) AND (in0.DPTR_DATE = in1.DPTR_DATE)) AND (in0.FLT_NBR = in1.FLT_NBR))
          AND (in0.LEG_CABIN_CD = in1.CABIN_CODE)
        )
          THEN NULL
        ELSE in1.NDOD
      END
    ) AS NDOD,
    in0.* EXCEPT (`FLT_NBR`, `OD`, `DPTR_DATE`, `NDOD`, `CAP`),
    in1.* EXCEPT (`OD`, `NDOD`, `DPTR_DATE`, `FLT_NBR`, `CABIN_CODE`)
  
  FROM Join_373_inner AS in0
  FULL JOIN Join_374_inner AS in1
     ON (
      (((in0.OD = in1.OD) AND (in0.DPTR_DATE = in1.DPTR_DATE)) AND (in0.FLT_NBR = in1.FLT_NBR))
      AND (in0.LEG_CABIN_CD = in1.CABIN_CODE)
    )

),

Formula_366_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((NBR_ZERO_BP IS NULL) AS BOOLEAN)
          THEN 0
        ELSE NBR_ZERO_BP
      END
    ) AS DOUBLE) AS SLACK,
    *
  
  FROM Join_364_left_UnionFullOuter AS in0

),

Summarize_281 AS (

  SELECT 
    SUM(SLACK) AS SLACK,
    SUM(CAP) AS SHIP_CAP,
    DPTR_WEEK_START_DATE AS DPTR_WEEK_START_DATE,
    NDOD AS NDOD,
    SEAT_INDEX_CURR AS SEAT_INDEX_CURR,
    MILES AS MILES,
    DPTR_DAY_OF_WEEK AS DPTR_DAY_OF_WEEK,
    DPTR_DATE AS DPTR_DATE,
    OD AS OD,
    CAP AS CAP,
    PLNG_REG_SHORT_NAME AS PLNG_REG_SHORT_NAME
  
  FROM Formula_366_0 AS in0
  
  GROUP BY 
    DPTR_WEEK_START_DATE, NDOD, SEAT_INDEX_CURR, MILES, DPTR_DAY_OF_WEEK, DPTR_DATE, OD, CAP, PLNG_REG_SHORT_NAME

),

Join_289_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`NDOD`, `DPTR_WEEK`, `BKGS`, `TTL_BKGS`)
  
  FROM Summarize_281 AS in0
  LEFT JOIN Formula_287_0 AS in1
     ON ((in0.NDOD = in1.NDOD) AND (in0.DPTR_WEEK_START_DATE = in1.DPTR_WEEK))

),

Database__LOADI_292 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_292') }}

),

Join_358_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`NDOD`, `AS_IND`, `DL_IND`, `UA_IND`, `AA_IND`, `B6_IND`, `WN_IND`, `HA_IND`)
  
  FROM Join_289_left_UnionLeftOuter AS in0
  LEFT JOIN Database__LOADI_292 AS in1
     ON (in0.NDOD = in1.NDOD)

),

Formula_362_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((INTAKE_SHARE IS NULL) AS BOOLEAN)
          THEN 0
        ELSE INTAKE_SHARE
      END
    ) AS DOUBLE) AS INTAKE_SHARE,
    * EXCEPT (`intake_share`)
  
  FROM Join_358_left_UnionLeftOuter AS in0

),

Database__LOADI_394 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_394') }}

),

Database__LOADI_395 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_395') }}

),

Join_393_inner AS (

  SELECT 
    in0.* EXCEPT (`OD`, `DPTR_DATE`),
    in1.*
  
  FROM Database__LOADI_395 AS in0
  INNER JOIN Database__LOADI_394 AS in1
     ON ((in0.OD = in1.OD) AND (in0.DPTR_DATE = in1.DPTR_DATE))

),

Formula_392_0 AS (

  SELECT 
    CAST((ARRAY_MAX((ARRAY((REVENUE - TOTAL_BP), 0)))) AS DOUBLE) AS POTENTIAL_REVENUE,
    *
  
  FROM Join_393_inner AS in0

),

Join_391_left_UnionFullOuter AS (

  SELECT 
    in0.* EXCEPT (`OD`, `DPTR_DATE`),
    in1.*
  
  FROM Formula_392_0 AS in0
  FULL JOIN Formula_362_0 AS in1
     ON ((in0.OD = in1.OD) AND (in0.DPTR_DATE = in1.DPTR_DATE))

),

Formula_361_0 AS (

  SELECT 
    CAST((((SEAT_INDEX_CURR / CAP) * (INTAKE_SHARE * POTENTIAL_REVENUE)) / SEAT_INDEX_CURR) AS DOUBLE) AS SCORE,
    *
  
  FROM Join_391_left_UnionFullOuter AS in0

),

Summarize_293 AS (

  SELECT 
    SUM(SCORE) AS AD_SCORE,
    SUM(SLACK) AS SLACK_SEATS,
    SUM(SHIP_CAP) AS SHIP_CAP,
    PLNG_REG_SHORT_NAME AS PLNG_REG_SHORT_NAME,
    NDOD AS NDOD,
    CARRIER_CNT AS CARRIER_CNT,
    MILES AS MILES
  
  FROM Formula_361_0 AS in0
  
  GROUP BY 
    PLNG_REG_SHORT_NAME, NDOD, CARRIER_CNT, MILES

),

Database__REPOR_351 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__REPOR_351') }}

),

Join_349_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.NDOD = in1.NDOD)
          THEN in1.NDOD
        ELSE NULL
      END
    ) AS Right_NDOD,
    in0.*,
    in1.* EXCEPT (`NDOD`)
  
  FROM Summarize_293 AS in0
  LEFT JOIN Database__REPOR_351 AS in1
     ON (in0.NDOD = in1.NDOD)

),

Formula_352_0 AS (

  SELECT 
    CAST((AS_QSI_PTS / TTL_QSI_PTS) AS DOUBLE) AS AS_QSI,
    *
  
  FROM Join_349_left_UnionLeftOuter AS in0

),

RecordID_295 AS (

  {{
    prophecy_basics.RecordID(
      ['Formula_352_0'], 
      'incremental_id', 
      'RANK', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Summarize_296 AS (

  SELECT 
    DISTINCT RANK AS RANK,
    NDOD AS NDOD
  
  FROM RecordID_295 AS in0

),

MultiRowFormula_440 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'MultiRowFormula_440') }}

),

MultiRowFormula_440_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_440 AS in0

),

Join_297_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`LEG_NDOD`)
  
  FROM Summarize_296 AS in0
  INNER JOIN MultiRowFormula_440_row_id_drop_0 AS in1
     ON (in0.NDOD = in1.LEG_NDOD)

),

AlteryxSelect_298 AS (

  SELECT 
    RANK AS RANK,
    FLOW_RANK AS FLOW_RANK,
    TT_NDOD AS TT_NDOD
  
  FROM Join_297_inner AS in0

),

Database__LOADI_341 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_341') }}

),

Join_342_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`NDOD`)
  
  FROM RecordID_295 AS in0
  INNER JOIN Database__LOADI_341 AS in1
     ON (in0.NDOD = in1.NDOD)

),

Join_384_left AS (

  SELECT in0.*
  
  FROM Join_373_inner AS in0
  ANTI JOIN Database__LOADI_283 AS in1
     ON (in0.NDOD = in1.LEG_NDOD)

),

Summarize_386 AS (

  SELECT DISTINCT NDOD AS NDOD
  
  FROM Join_384_left AS in0

),

Formula_385_0 AS (

  SELECT 
    CAST('YES' AS string) AS NEW_MARKET,
    *
  
  FROM Summarize_386 AS in0

),

Join_387_right_UnionRightOuter AS (

  SELECT 
    in0.* EXCEPT (`NDOD`),
    in1.*
  
  FROM Formula_385_0 AS in0
  RIGHT JOIN Join_342_inner AS in1
     ON (in0.NDOD = in1.NDOD)

),

Formula_353_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (NEW_MARKET = 'YES')
          THEN 'HIGH'
        WHEN (((RANK <= 25) AND (PCT_SYS_ASMS > 0.005)) AND (AS_QSI < 0.85))
          THEN 'EXTREME'
        WHEN ((RANK <= 86) AND (AS_QSI < 0.85))
          THEN 'HIGH'
        WHEN (((RANK > 86) AND (RANK <= 147)) AND (AS_QSI < 0.85))
          THEN 'MEDIUM'
        ELSE 'LOW'
      END
    ) AS string) AS SUPPORT_PROPOSED,
    *
  
  FROM Join_387_right_UnionRightOuter AS in0

),

Join_299_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`RANK`)
  
  FROM Formula_353_0 AS in0
  LEFT JOIN AlteryxSelect_298 AS in1
     ON (in0.RANK = in1.RANK)

),

Filter_303 AS (

  SELECT * 
  
  FROM Join_299_left_UnionLeftOuter AS in0
  
  WHERE (FLOW_RANK <= 3)

),

CrossTab_307 AS (

  SELECT *
  
  FROM (
    SELECT 
      RANK,
      SUPPORT_PROPOSED,
      FLOW_RANK,
      TT_NDOD
    
    FROM Filter_303 AS in0
  )
  PIVOT (
    FIRST(TT_NDOD) AS First
    FOR FLOW_RANK
    IN (
      '1', '2', '3'
    )
  )

),

Filter_309 AS (

  SELECT * 
  
  FROM CrossTab_307 AS in0
  
  WHERE (CAST(SUPPORT_PROPOSED AS string) IN ('HIGH', 'EXTREME'))

),

AlteryxSelect_310 AS (

  SELECT 
    `1` AS FLOW_1,
    `2` AS FLOW_2,
    `3` AS FLOW_3,
    * EXCEPT (`1`, `2`, `3`)
  
  FROM Filter_309 AS in0

),

Join_311_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`RANK`, `SUPPORT_PROPOSED`)
  
  FROM Formula_353_0 AS in0
  LEFT JOIN AlteryxSelect_310 AS in1
     ON (in0.RANK = in1.RANK)

)

SELECT *

FROM Join_311_left_UnionLeftOuter
