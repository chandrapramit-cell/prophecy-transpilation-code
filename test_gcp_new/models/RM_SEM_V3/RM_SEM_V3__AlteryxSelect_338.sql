{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Formula_321_0 AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__Formula_321_0')}}

),

Summarize_323 AS (

  SELECT *
  
  FROM {{ ref('RM_SEM_V3__Summarize_323')}}

),

Join_329_left AS (

  SELECT in0.*
  
  FROM Summarize_323 AS in0
  ANTI JOIN Formula_321_0 AS in1
     ON (in0.NDOD = in1.NDOD)

),

Formula_319_0 AS (

  SELECT 
    CAST('CNX' AS string) AS variableTYPE,
    CAST('MEDIUM' AS string) AS Support,
    *
  
  FROM Join_329_left AS in0

),

Union_320 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_319_0', 'Formula_321_0'], 
      [
        '[{"name": "variableTYPE", "dataType": "String"}, {"name": "Support", "dataType": "String"}, {"name": "NDOD", "dataType": "String"}]', 
        '[{"name": "variableTYPE", "dataType": "String"}, {"name": "SUPPORT", "dataType": "String"}, {"name": "NEW_MARKET", "dataType": "String"}, {"name": "AS_QSI", "dataType": "Double"}, {"name": "Right_NDOD", "dataType": "String"}, {"name": "NDOD", "dataType": "String"}, {"name": "MILES", "dataType": "Double"}, {"name": "AS_QSI_PTS", "dataType": "Double"}, {"name": "TTL_QSI_PTS", "dataType": "Double"}, {"name": "PCT_SYS_ASMS", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_324 AS (

  SELECT * 
  
  FROM Union_320 AS in0
  
  WHERE (NOT(NDOD IS NULL))

),

AlteryxSelect_325 AS (

  SELECT 
    NDOD AS OD,
    variableTYPE AS variableTYPE,
    * EXCEPT (`variableTYPE`, `NDOD`)
  
  FROM Filter_324 AS in0

),

Formula_326_0 AS (

  SELECT 
    CAST((
      CAST((SUBSTRING(OD, (((LENGTH(OD)) - 3) + 1), 3)) AS DECIMAL (19, 9))
      + CAST((SUBSTRING(OD, 1, 3)) AS DECIMAL (19, 9))
    ) AS string) AS OD,
    * EXCEPT (`od`)
  
  FROM AlteryxSelect_325 AS in0

),

Database__LOADI_330 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_330') }}

),

Database__LOADI_313 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_313') }}

),

Formula_357_to_Formula_474_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(ORIG_CITY_NAME, '(?i)/', ' ')) AS string) AS ORIG_CITY_NAME,
    CAST((REGEXP_REPLACE(ORIG_AIRPORT_NAME, '(?i)/', ' ')) AS string) AS ORIG_AIRPORT_NAME,
    CAST((REGEXP_REPLACE(DEST_CITY_NAME, '(?i)/', ' ')) AS string) AS DEST_CITY_NAME,
    CAST((REGEXP_REPLACE(DEST_AIRPORT_NAME, '(?i)/', ' ')) AS string) AS DEST_AIRPORT_NAME,
    * EXCEPT (`orig_airport_name`, `orig_city_name`, `dest_city_name`, `dest_airport_name`)
  
  FROM Database__LOADI_313 AS in0

),

Formula_357_to_Formula_474_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN ((ORIG_CITY_NAME = 'Washington') AND (ORIG_STATE = 'DC'))
          THEN 'Washington DC'
        ELSE ORIG_CITY_NAME
      END
    ) AS string) AS ORIG_CITY_NAME,
    CAST((
      CASE
        WHEN ((DEST_CITY_NAME = 'Washington') AND (DEST_STATE = 'DC'))
          THEN 'Washington DC'
        ELSE DEST_CITY_NAME
      END
    ) AS string) AS DEST_CITY_NAME,
    * EXCEPT (`orig_city_name`, `dest_city_name`)
  
  FROM Formula_357_to_Formula_474_0 AS in0

),

AlteryxSelect_318 AS (

  SELECT 
    CAST(OD AS string) AS OD,
    CAST(ORIG AS string) AS ORIG,
    CAST(DEST AS string) AS DEST,
    CAST(ORIG_CITY_NAME AS string) AS ORIG_CITY_NAME,
    CAST(DEST_CITY_NAME AS string) AS DEST_CITY_NAME,
    ORIG_AIRPORT_NAME AS ORIG_AIRPORT_NAME,
    DEST_AIRPORT_NAME AS DEST_AIRPORT_NAME,
    CAST(ORIG_STATE AS string) AS ORIG_STATE,
    CAST(DEST_STATE AS string) AS DEST_STATE,
    * EXCEPT (`OD`, 
    `ORIG`, 
    `DEST`, 
    `ORIG_CITY_NAME`, 
    `DEST_CITY_NAME`, 
    `ORIG_AIRPORT_NAME`, 
    `DEST_AIRPORT_NAME`, 
    `ORIG_STATE`, 
    `DEST_STATE`)
  
  FROM Formula_357_to_Formula_474_1 AS in0

),

Union_327 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_325', 'Formula_326_0'], 
      [
        '[{"name": "OD", "dataType": "String"}, {"name": "variableTYPE", "dataType": "String"}, {"name": "Support", "dataType": "String"}, {"name": "NEW_MARKET", "dataType": "String"}, {"name": "AS_QSI", "dataType": "Double"}, {"name": "Right_NDOD", "dataType": "String"}, {"name": "MILES", "dataType": "Double"}, {"name": "AS_QSI_PTS", "dataType": "Double"}, {"name": "TTL_QSI_PTS", "dataType": "Double"}, {"name": "PCT_SYS_ASMS", "dataType": "Double"}]', 
        '[{"name": "OD", "dataType": "String"}, {"name": "variableTYPE", "dataType": "String"}, {"name": "Support", "dataType": "String"}, {"name": "NEW_MARKET", "dataType": "String"}, {"name": "AS_QSI", "dataType": "Double"}, {"name": "Right_NDOD", "dataType": "String"}, {"name": "MILES", "dataType": "Double"}, {"name": "AS_QSI_PTS", "dataType": "Double"}, {"name": "TTL_QSI_PTS", "dataType": "Double"}, {"name": "PCT_SYS_ASMS", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_328_inner AS (

  SELECT 
    in0.OD AS OD,
    in1.ORIG AS ORIG,
    in1.DEST AS DEST,
    in1.ORIG_CITY_NAME AS ORIG_CITY_NAME,
    in1.DEST_CITY_NAME AS DEST_CITY_NAME,
    in1.ORIG_AIRPORT_NAME AS ORIG_AIRPORT_NAME,
    in1.DEST_AIRPORT_NAME AS DEST_AIRPORT_NAME,
    in1.ORIG_STATE AS ORIG_STATE,
    in1.DEST_STATE AS DEST_STATE,
    in0.variableTYPE AS variableTYPE,
    in0.Support AS Support,
    in0.MILES AS MILES,
    in0.AS_QSI_PTS AS AS_QSI_PTS,
    in0.TTL_QSI_PTS AS TTL_QSI_PTS,
    in0.AS_QSI AS AS_QSI,
    in0.PCT_SYS_ASMS AS PCT_SYS_ASMS,
    in0.* EXCEPT (`OD`, `variableTYPE`, `Support`, `MILES`, `AS_QSI_PTS`, `TTL_QSI_PTS`, `AS_QSI`, `PCT_SYS_ASMS`),
    in1.* EXCEPT (`OD`, 
    `ORIG`, 
    `DEST`, 
    `ORIG_CITY_NAME`, 
    `DEST_CITY_NAME`, 
    `ORIG_AIRPORT_NAME`, 
    `DEST_AIRPORT_NAME`, 
    `ORIG_STATE`, 
    `DEST_STATE`)
  
  FROM Union_327 AS in0
  INNER JOIN AlteryxSelect_318 AS in1
     ON (in0.OD = in1.OD)

),

Join_331_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN (in0.OD = in1.OD)
          THEN in1.FLT_CNT
        ELSE NULL
      END
    ) AS FLTS_PER_MTH,
    in0.*,
    in1.* EXCEPT (`OD`, `FLT_CNT`)
  
  FROM Join_328_inner AS in0
  LEFT JOIN Database__LOADI_330 AS in1
     ON (in0.OD = in1.OD)

),

Database__LOADI_403 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('RM_SEM_V3', 'Database__LOADI_403') }}

),

Join_405_left_UnionLeftOuter AS (

  SELECT 
    in0.* EXCEPT (`OD`),
    in1.*
  
  FROM Join_331_left_UnionLeftOuter AS in0
  LEFT JOIN Database__LOADI_403 AS in1
     ON (in0.OD = in1.OD)

),

Formula_471_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (((variableTYPE = 'DIRECT') AND (NEW_MARKET = 'YES')) AND (FLT_CNT_NEXT_3_MTH IS NULL))
          THEN 'OFF'
        WHEN (((variableTYPE = 'DIRECT') AND (NEW_MARKET IS NULL)) AND (FLT_CNT_NEXT_MTH IS NULL))
          THEN 'OFF'
        ELSE NULL
      END
    ) AS string) AS NO_FLIGHT_OVERRIDER,
    *
  
  FROM Join_405_left_UnionLeftOuter AS in0

),

AlteryxSelect_470 AS (

  SELECT *
  
  FROM Formula_471_0 AS in0

),

Filter_369 AS (

  SELECT * 
  
  FROM AlteryxSelect_470 AS in0
  
  WHERE (
          (NOT((variableTYPE = 'DIRECT') AND (MILES IS NULL)))
          OR (((variableTYPE = 'DIRECT') AND (MILES IS NULL)) IS NULL)
        )

),

Formula_339_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (NOT(NO_FLIGHT_OVERRIDER IS NULL))
          THEN NO_FLIGHT_OVERRIDER
        ELSE Support
      END
    ) AS string) AS Support,
    * EXCEPT (`support`)
  
  FROM Filter_369 AS in0

),

AlteryxSelect_338 AS (

  SELECT 
    OD AS OD,
    ORIG AS ORIG,
    DEST AS DEST,
    ORIG_CITY_NAME AS ORIG_CITY_NAME,
    DEST_CITY_NAME AS DEST_CITY_NAME,
    ORIG_AIRPORT_NAME AS ORIG_AIRPORT_NAME,
    DEST_AIRPORT_NAME AS DEST_AIRPORT_NAME,
    ORIG_STATE AS ORIG_STATE,
    DEST_STATE AS DEST_STATE,
    variableTYPE AS variableTYPE,
    Support AS Support,
    MILES AS MILES,
    PCT_SYS_ASMS AS PCT_SYS_ASMS,
    FLTS_PER_MTH AS FLTS_PER_MTH
  
  FROM Formula_339_0 AS in0

)

SELECT *

FROM AlteryxSelect_338
