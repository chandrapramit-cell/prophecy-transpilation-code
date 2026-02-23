{{
  config({    
    "materialized": "table",
    "alias": "Database__loadi_234_ref",
    "database": "{{ var('db_name') }}",
    "schema": "{{ var('schema_name') }}"
  })
}}

WITH Database__repor_31 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_31_ref') }}

),

Filter_34 AS (

  SELECT * 
  
  FROM Database__repor_31 AS in0
  
  WHERE (
          ((PARSE_DATE('%Y-%m-%d', CPN_DPTR_DATE)) > (PARSE_DATE('%Y-%m-%d', (DATE_ADD(CAST(CURRENT_DATE AS STRING), INTERVAL CAST(-98 AS INTEGER) DAY)))))
          OR ((PARSE_DATE('%Y-%m-%d', CPN_DPTR_DATE)) >= (PARSE_DATE('%Y-%m-%d', (DATE_ADD(DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL CAST(0 AS INTEGER) DAY)))))
        )

),

Database__repor_49 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_49_ref') }}

),

Formula_62_0 AS (

  select  '1' as `1` , *  REPLACE( (PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', (DATE_ADD(WKLY_SNAP_DT  ,INTERVAL -7  DAY)))) as `WKLY_SNAP_DT` ,  (CC_MILES / 7) as `CC_MILES` ) from Database__repor_49

),

Formula_159_0 AS (

  SELECT 
    '1' AS `1`,
    *
  
  FROM Database__repor_31 AS in0

),

Join_160_inner AS (

  SELECT 
    in1.CPN_DPTR_DATE AS CPN_DPTR_DATE,
    in0.WKLY_SNAP_DT AS WKLY_SNAP_DT,
    in0.CC_MILES AS CC_MILES
  
  FROM Formula_62_0 AS in0
  INNER JOIN Formula_159_0 AS in1
     ON (in0.`1` = in1.`1`)

),

Formula_163_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (CPN_DPTR_DATE >= WKLY_SNAP_DT)
          AND ((PARSE_DATE('%Y-%m-%d', CPN_DPTR_DATE)) < (DATE_ADD(WKLY_SNAP_DT, INTERVAL 7 DAY)))
        )
          THEN 'YES'
        ELSE 'NO '
      END
    ) AS STRING) AS KEEP,
    *
  
  FROM Join_160_inner AS in0

),

Filter_164 AS (

  SELECT * 
  
  FROM Formula_163_0 AS in0
  
  WHERE (KEEP = 'Yes')

),

Formula_165_0 AS (

  SELECT 
    (PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', (DATE_ADD(CPN_DPTR_DATE, INTERVAL 12 MONTH)))) AS CPN_DPTR_DATE,
    * EXCEPT (`cpn_dptr_date`)
  
  FROM Filter_164 AS in0

),

Database__repor_32 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_32_ref') }}

),

AlteryxSelect_169 AS (

  SELECT 
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(MONTH AS STRING)) AS DATE)
      END
    ) AS MONTH,
    CY_MP_US_CC_COMISSION AS CY_MP_US_CC_COMISSION,
    variableTYPE AS variableTYPE
  
  FROM Database__repor_32 AS in0

),

Filter_158 AS (

  SELECT * 
  
  FROM Formula_62_0 AS in0
  
  WHERE (
          ((PARSE_DATE('%Y-%m-%d', WKLY_SNAP_DT)) > (PARSE_DATE('%Y-%m-%d', (DATE_ADD(CAST(CURRENT_DATE AS STRING), INTERVAL CAST(-98 AS INTEGER) DAY)))))
          OR ((PARSE_DATE('%Y-%m-%d', WKLY_SNAP_DT)) >= (PARSE_DATE('%Y-%m-%d', (DATE_ADD(DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL CAST(-7 AS INTEGER) DAY)))))
        )

),

Formula_42_0 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(CPN_DPTR_DATE, MONTH)))) AS MONTH,
    '1' AS `1`,
    *
  
  FROM Filter_34 AS in0

),

Join_63_inner AS (

  SELECT 
    in0.* EXCEPT (`1`),
    in1.* EXCEPT (`RDMD_PTS`, `MONTH`, `1`)
  
  FROM Filter_158 AS in0
  INNER JOIN Formula_42_0 AS in1
     ON (in0.`1` = in1.`1`)

),

Formula_64_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          (CPN_DPTR_DATE >= WKLY_SNAP_DT)
          AND ((PARSE_DATE('%Y-%m-%d', CPN_DPTR_DATE)) < (DATE_ADD(WKLY_SNAP_DT, INTERVAL 7 DAY)))
        )
          THEN 'YES'
        ELSE 'NO '
      END
    ) AS STRING) AS KEEP,
    *
  
  FROM Join_63_inner AS in0

),

Filter_65 AS (

  SELECT * 
  
  FROM Formula_64_0 AS in0
  
  WHERE (KEEP = 'Yes')

),

Formula_68_0 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(CPN_DPTR_DATE, MONTH)))) AS MONTH,
    '1' AS `1`,
    *
  
  FROM Filter_65 AS in0

),

AlteryxSelect_48 AS (

  SELECT 
    MONTH AS MONTH,
    CY_MP_US_CC_COMISSION AS CY_MP_US_CC_COMISSION,
    variableTYPE AS variableTYPE
  
  FROM Database__repor_32 AS in0

),

Filter_51 AS (

  SELECT * 
  
  FROM AlteryxSelect_48 AS in0
  
  WHERE (variableTYPE = 'FORECAST')

),

Database__repor_50 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_50_ref') }}

),

Join_52_inner AS (

  SELECT 
    in0.RPT_MONTH AS RPT_MONTH,
    in0.* EXCEPT (`RPT_MONTH`),
    in1.* EXCEPT (`MONTH`, `variableTYPE`)
  
  FROM Database__repor_50 AS in0
  INNER JOIN Filter_51 AS in1
     ON (in0.RPT_MONTH = in1.MONTH)

),

Summarize_54 AS (

  SELECT 
    SUM(CC_MILES) AS CC_MILES,
    SUM(CY_MP_US_CC_COMISSION) AS CY_MP_US_CC_COMISSION,
    RPT_MONTH AS RPT_MONTH
  
  FROM Join_52_inner AS in0
  
  GROUP BY RPT_MONTH

),

Formula_60_0 AS (

  SELECT 
    (CY_MP_US_CC_COMISSION / CC_MILES) AS CC_VALUE,
    *
  
  FROM Summarize_54 AS in0

),

Join_67_left AS (

  SELECT in0.*
  
  FROM Formula_68_0 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.RPT_MONTH
    
    FROM Formula_60_0 AS in1
    
    WHERE in1.RPT_MONTH IS NOT NULL
  ) AS in1_keys
     ON (t.MONTH0 = in1_keys.RPT_MONTH)
  
  WHERE (in1_keys.RPT_MONTH IS NULL)

),

Database__repor_118 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_118_ref') }}

),

AlteryxSelect_120 AS (

  SELECT 
    PLNG_REG_NAME AS REGION,
    * EXCEPT (`PLNG_REG_NAME`)
  
  FROM Database__repor_118 AS in0

),

Database__repor_6 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_6_ref') }}

),

Summarize_182 AS (

  SELECT 
    SUM(CAP) AS CAP,
    SUM(ASM) AS ASM,
    SUM(PAX) AS PAX,
    SUM(RPM) AS RPM,
    SUM(SAVER_BUYUP_PAX) AS SAVER_BUYUP_PAX,
    SUM(SAVER_OFFER_PAX) AS SAVER_OFFER_PAX,
    MONTH AS MONTH,
    NDOD AS NDOD,
    REGION AS REGION
  
  FROM Database__repor_6 AS in0
  
  GROUP BY 
    MONTH, NDOD, REGION

),

Database__repor_12 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_12_ref') }}

),

Database__repor_88 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_88_ref') }}

),

Filter_275 AS (

  SELECT * 
  
  FROM Database__repor_88 AS in0
  
  WHERE (
          ((PY_REV > 0) AND ((PY_F_REV > 0) AND (PY_MAIN_REV > 0)))
          AND (((PY_RPM > 0) AND (PY_F_RPM > 0)) AND ((PY_MAIN_RPM > 0) AND (PY_SAVER_RPM > 0)))
        )

),

Formula_93_0 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(DPTR_DATE, MONTH)))) AS MONTH,
    *
  
  FROM Filter_275 AS in0

),

Summarize_90 AS (

  SELECT 
    SUM(PY_REV) AS PY_REV,
    SUM(PY_F_REV) AS PY_F_REV,
    SUM(PY_MAIN_REV) AS PY_MAIN_REV,
    SUM(PY_RPM) AS PY_RPM,
    SUM(PY_F_RPM) AS PY_F_RPM,
    SUM(PY_MAIN_RPM) AS PY_MAIN_RPM,
    SUM(PY_SAVER_RPM) AS PY_SAVER_RPM,
    MONTH AS MONTH,
    DPTR_DATE AS DPTR_DATE,
    REGION AS REGION
  
  FROM Formula_93_0 AS in0
  
  GROUP BY 
    MONTH, DPTR_DATE, REGION

),

Summarize_95 AS (

  SELECT 
    (SUM(PY_REV) OVER (PARTITION BY MONTH, REGION ORDER BY 1 NULLS FIRST)) AS MTH_PY_REV,
    (SUM(PY_F_REV) OVER (PARTITION BY MONTH, REGION ORDER BY 1 NULLS FIRST)) AS MTH_PY_F_REV,
    (SUM(PY_MAIN_REV) OVER (PARTITION BY MONTH, REGION ORDER BY 1 NULLS FIRST)) AS MTH_PY_MAIN_REV,
    (SUM(PY_RPM) OVER (PARTITION BY MONTH, REGION ORDER BY 1 NULLS FIRST)) AS MTH_PY_RPM,
    (SUM(PY_F_RPM) OVER (PARTITION BY MONTH, REGION ORDER BY 1 NULLS FIRST)) AS MTH_PY_F_RPM,
    (SUM(PY_MAIN_RPM) OVER (PARTITION BY MONTH, REGION ORDER BY 1 NULLS FIRST)) AS MTH_PY_MAIN_RPM,
    (SUM(PY_SAVER_RPM) OVER (PARTITION BY MONTH, REGION ORDER BY 1 NULLS FIRST)) AS MTH_PY_SAVER_RPM,
    *
  
  FROM Summarize_90 AS in0

),

Join_97_inner_formula_to_Formula_101_0 AS (

  select *  REPLACE( MONTH as `MONTH` ,  DPTR_DATE as `DPTR_DATE` ,  REGION as `REGION` ,  PY_REV as `PY_REV` ,  PY_F_REV as `PY_F_REV` ,  PY_MAIN_REV as `PY_MAIN_REV` ,  PY_RPM as `PY_RPM` ,  PY_F_RPM as `PY_F_RPM` ,  PY_MAIN_RPM as `PY_MAIN_RPM` ,  MTH_PY_REV as `MTH_PY_REV` ,  MTH_PY_F_REV as `MTH_PY_F_REV` ,  MTH_PY_MAIN_REV as `MTH_PY_MAIN_REV` ,  MTH_PY_RPM as `MTH_PY_RPM` ,  MTH_PY_F_RPM as `MTH_PY_F_RPM` ,  MTH_PY_MAIN_RPM as `MTH_PY_MAIN_RPM` ,  PY_SAVER_RPM as `PY_SAVER_RPM` ,  MTH_PY_SAVER_RPM as `MTH_PY_SAVER_RPM` ) from Summarize_95

),

Join_97_inner_formula_to_Formula_101_1 AS (

  SELECT 
    CAST((CAST(PY_REV AS NUMERIC) / CAST(MTH_PY_REV AS NUMERIC)) AS FLOAT64) AS REV_PCT,
    CAST((CAST(PY_F_REV AS NUMERIC) / CAST(MTH_PY_F_REV AS NUMERIC)) AS FLOAT64) AS F_REV_PCT,
    CAST((CAST(PY_MAIN_REV AS NUMERIC) / CAST(MTH_PY_MAIN_REV AS NUMERIC)) AS FLOAT64) AS MAIN_REV_PCT,
    CAST((CAST(PY_F_RPM AS NUMERIC) / CAST(MTH_PY_F_RPM AS NUMERIC)) AS FLOAT64) AS F_RPM_PCT,
    CAST((CAST(PY_MAIN_RPM AS NUMERIC) / CAST(MTH_PY_MAIN_RPM AS NUMERIC)) AS FLOAT64) AS MAIN_RPM_PCT,
    CAST((CAST(PY_RPM AS NUMERIC) / CAST(MTH_PY_RPM AS NUMERIC)) AS FLOAT64) AS RPM_PCT,
    CAST((CAST(PY_SAVER_RPM AS NUMERIC) / CAST(MTH_PY_SAVER_RPM AS NUMERIC)) AS FLOAT64) AS SAVER_RPM_PCT,
    *
  
  FROM Join_97_inner_formula_to_Formula_101_0 AS in0

),

AlteryxSelect_102 AS (

  select *  REPLACE( MONTH as `MONTH` ,  (CASE WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(DPTR_DATE AS string)) IS NOT NULL) THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(DPTR_DATE AS string)) AS DATE) WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(DPTR_DATE AS string)) IS NOT NULL) THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(DPTR_DATE AS string)) AS DATE) ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(DPTR_DATE AS string)) AS DATE) END) as `DPTR_DATE` ,  REGION as `REGION` ,  REV_PCT as `REV_PCT` ,  F_REV_PCT as `F_REV_PCT` ,  MAIN_REV_PCT as `MAIN_REV_PCT` ,  RPM_PCT as `RPM_PCT` ,  F_RPM_PCT as `F_RPM_PCT` ,  MAIN_RPM_PCT as `MAIN_RPM_PCT` ,  SAVER_RPM_PCT as `SAVER_RPM_PCT` ) from Join_97_inner_formula_to_Formula_101_1

),

Summarize_94 AS (

  SELECT 
    (SUM(PY_REV) OVER (PARTITION BY MONTH, NDOD ORDER BY 1 NULLS FIRST)) AS MTH_PY_REV,
    (SUM(PY_F_REV) OVER (PARTITION BY MONTH, NDOD ORDER BY 1 NULLS FIRST)) AS MTH_PY_F_REV,
    (SUM(PY_MAIN_REV) OVER (PARTITION BY MONTH, NDOD ORDER BY 1 NULLS FIRST)) AS MTH_PY_MAIN_REV,
    (SUM(PY_RPM) OVER (PARTITION BY MONTH, NDOD ORDER BY 1 NULLS FIRST)) AS MTH_PY_RPM,
    (SUM(PY_F_RPM) OVER (PARTITION BY MONTH, NDOD ORDER BY 1 NULLS FIRST)) AS MTH_PY_F_RPM,
    (SUM(PY_MAIN_RPM) OVER (PARTITION BY MONTH, NDOD ORDER BY 1 NULLS FIRST)) AS MTH_PY_MAIN_RPM,
    (SUM(PY_SAVER_RPM) OVER (PARTITION BY MONTH, NDOD ORDER BY 1 NULLS FIRST)) AS MTH_PY_SAVER_RPM,
    *
  
  FROM Formula_93_0 AS in0

),

Join_96_inner_formula_to_Formula_98_0 AS (

  select *  REPLACE( MONTH as `MONTH` ,  DPTR_DATE as `DPTR_DATE` ,  NDOD as `NDOD` ,  REGION as `REGION` ,  PY_REV as `PY_REV` ,  PY_F_REV as `PY_F_REV` ,  PY_MAIN_REV as `PY_MAIN_REV` ,  PY_RPM as `PY_RPM` ,  PY_F_RPM as `PY_F_RPM` ,  PY_MAIN_RPM as `PY_MAIN_RPM` ,  MTH_PY_REV as `MTH_PY_REV` ,  MTH_PY_F_REV as `MTH_PY_F_REV` ,  MTH_PY_MAIN_REV as `MTH_PY_MAIN_REV` ,  MTH_PY_RPM as `MTH_PY_RPM` ,  MTH_PY_F_RPM as `MTH_PY_F_RPM` ,  MTH_PY_MAIN_RPM as `MTH_PY_MAIN_RPM` ,  PY_SAVER_RPM as `PY_SAVER_RPM` ,  MTH_PY_SAVER_RPM as `MTH_PY_SAVER_RPM` ) from Summarize_94

),

Join_96_inner_formula_to_Formula_98_1 AS (

  SELECT 
    CAST((CAST(PY_REV AS NUMERIC) / CAST(MTH_PY_REV AS NUMERIC)) AS FLOAT64) AS REV_PCT,
    CAST((CAST(PY_F_REV AS NUMERIC) / CAST(MTH_PY_F_REV AS NUMERIC)) AS FLOAT64) AS F_REV_PCT,
    CAST((CAST(PY_MAIN_REV AS NUMERIC) / CAST(MTH_PY_MAIN_REV AS NUMERIC)) AS FLOAT64) AS MAIN_REV_PCT,
    CAST((CAST(PY_F_RPM AS NUMERIC) / CAST(MTH_PY_F_RPM AS NUMERIC)) AS FLOAT64) AS F_RPM_PCT,
    CAST((CAST(PY_MAIN_RPM AS NUMERIC) / CAST(MTH_PY_MAIN_RPM AS NUMERIC)) AS FLOAT64) AS MAIN_RPM_PCT,
    CAST((CAST(PY_RPM AS NUMERIC) / CAST(MTH_PY_RPM AS NUMERIC)) AS FLOAT64) AS RPM_PCT,
    CAST((CAST(PY_SAVER_RPM AS NUMERIC) / CAST(MTH_PY_SAVER_RPM AS NUMERIC)) AS FLOAT64) AS SAVER_RPM_PCT,
    *
  
  FROM Join_96_inner_formula_to_Formula_98_0 AS in0

),

AlteryxSelect_100 AS (

  select *  REPLACE( MONTH as `MONTH` ,  (CASE WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(DPTR_DATE AS string)) IS NOT NULL) THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(DPTR_DATE AS string)) AS DATE) WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(DPTR_DATE AS string)) IS NOT NULL) THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(DPTR_DATE AS string)) AS DATE) ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(DPTR_DATE AS string)) AS DATE) END) as `DPTR_DATE` ,  NDOD as `NDOD` ,  REV_PCT as `REV_PCT` ,  F_REV_PCT as `F_REV_PCT` ,  MAIN_REV_PCT as `MAIN_REV_PCT` ,  RPM_PCT as `RPM_PCT` ,  F_RPM_PCT as `F_RPM_PCT` ,  MAIN_RPM_PCT as `MAIN_RPM_PCT` ,  SAVER_RPM_PCT as `SAVER_RPM_PCT` ) from Join_96_inner_formula_to_Formula_98_1

),

AlteryxSelect_73 AS (

  SELECT 
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(MONTH AS STRING)) AS DATE)
      END
    ) AS MONTH,
    * EXCEPT (`CAP`, `ASM`, `MONTH`)
  
  FROM Database__repor_6 AS in0

),

CrossTab_179 AS (

  SELECT *
  
  FROM (
    SELECT 
      MONTH,
      NDOD,
      REGION,
      PRODUCT,
      UPG_FEE
    
    FROM AlteryxSelect_73 AS in0
  )
  PIVOT (
    SUM(UPG_FEE) AS Sum
    FOR PRODUCT
    IN (
      'PC', 'ER', 'FIRST', 'Q', 'SAVER', 'MAIN'
    )
  )

),

AlteryxSelect_180 AS (

  SELECT 
    FIRST AS F_UPG_REV,
    PC AS PC_UPG_REV,
    * EXCEPT (`ER`, `MAIN`, `SAVER`, `FIRST`, `PC`)
  
  FROM CrossTab_179 AS in0

),

CrossTab_74 AS (

  SELECT *
  
  FROM (
    SELECT 
      MONTH,
      NDOD,
      REGION,
      PRODUCT,
      CPN_REV
    
    FROM AlteryxSelect_73 AS in0
  )
  PIVOT (
    SUM(CPN_REV) AS Sum
    FOR PRODUCT
    IN (
      'PC', 'ER', 'FIRST', 'Q', 'SAVER', 'MAIN'
    )
  )

),

AlteryxSelect_76 AS (

  SELECT 
    ER AS ER_REV,
    FIRST AS FIRST_REV,
    MAIN AS MAIN_REV,
    PC AS PC_REV,
    SAVER AS SAVER_REV,
    * EXCEPT (`ER`, `FIRST`, `MAIN`, `PC`, `SAVER`)
  
  FROM CrossTab_74 AS in0

),

Summarize_75 AS (

  SELECT 
    SUM(PAX) AS TTL_PAX,
    SUM(CPN_REV) AS TTL_CPN_REV,
    SUM(RPM) AS TTL_RPM,
    SUM(SAVER_BUYUP_PAX) AS TTL_SAVER_BUYUP_PAX,
    SUM(SAVER_OFFER_PAX) AS TTL_SAVER_OFFER_PAX,
    MONTH AS MONTH,
    NDOD AS NDOD,
    REGION AS REGION
  
  FROM AlteryxSelect_73 AS in0
  
  GROUP BY 
    MONTH, NDOD, REGION

),

Join_77_inner AS (

  SELECT 
    in0.* EXCEPT (`Q`),
    in1.* EXCEPT (`MONTH`, `NDOD`, `REGION`, `TTL_PAX`, `TTL_RPM`, `TTL_SAVER_BUYUP_PAX`, `TTL_SAVER_OFFER_PAX`)
  
  FROM AlteryxSelect_76 AS in0
  INNER JOIN Summarize_75 AS in1
     ON ((t.MONTH0 = in1.MONTH) AND (t.NDOD = in1.NDOD))

),

Join_181_inner AS (

  SELECT 
    in1.Q AS Right_Q,
    in0.*,
    in1.* EXCEPT (`MONTH`, `NDOD`, `REGION`, `Q`)
  
  FROM Join_77_inner AS in0
  INNER JOIN AlteryxSelect_180 AS in1
     ON ((in0.MONTH = in1.MONTH) AND (in0.NDOD = in1.NDOD))

),

AlteryxSelect_184 AS (

  SELECT 
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(MONTH AS STRING)) AS DATE)
      END
    ) AS MONTH,
    * EXCEPT (`MONTH`)
  
  FROM Summarize_182 AS in0

),

Join_183_inner AS (

  SELECT 
    in0.MONTH AS MONTH,
    in0.NDOD AS NDOD,
    in0.REGION AS REGION,
    in1.PAX AS PAX,
    in1.RPM AS RPM,
    in0.FIRST_REV AS FIRST_REV,
    in0.MAIN_REV AS MAIN_REV,
    in0.PC_REV AS PC_REV,
    in0.SAVER_REV AS SAVER_REV,
    in0.TTL_CPN_REV AS TTL_CPN_REV,
    in0.F_UPG_REV AS F_UPG_REV,
    in0.PC_UPG_REV AS PC_UPG_REV,
    in1.SAVER_BUYUP_PAX AS SAVER_BUYUP_PAX,
    in1.SAVER_OFFER_PAX AS SAVER_OFFER_PAX,
    in0.ER_REV AS ER_REV,
    in0.Right_Q AS Right_Q
  
  FROM Join_181_inner AS in0
  INNER JOIN AlteryxSelect_184 AS in1
     ON ((t.MONTH0 = in1.MONTH) AND (t.NDOD = in1.NDOD))

),

Join_176_left AS (

  SELECT in0.*
  
  FROM Join_183_inner AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM AlteryxSelect_100 AS in1
  ) AS in1_keys
     ON ((in0.MONTH = t.MONTH0) AND (in0.NDOD = t.NDOD))

),

Join_175_inner AS (

  SELECT 
    in1.DPTR_DATE AS DPTR_DATE,
    in0.MONTH AS MONTH,
    in0.NDOD AS NDOD,
    in0.REGION AS REGION,
    in0.PAX AS PAX,
    in0.RPM AS RPM,
    in0.FIRST_REV AS FIRST_REV,
    in0.PC_REV AS PC_REV,
    in0.MAIN_REV AS MAIN_REV,
    in0.SAVER_REV AS SAVER_REV,
    in0.F_UPG_REV AS F_UPG_REV,
    in0.PC_UPG_REV AS PC_UPG_REV,
    in1.REV_PCT AS REV_PCT,
    in1.F_REV_PCT AS F_REV_PCT,
    in1.MAIN_REV_PCT AS MAIN_REV_PCT,
    in1.RPM_PCT AS RPM_PCT,
    in1.F_RPM_PCT AS F_RPM_PCT,
    in1.MAIN_RPM_PCT AS MAIN_RPM_PCT,
    in0.TTL_CPN_REV AS TTL_CPN_REV,
    in1.SAVER_RPM_PCT AS SAVER_RPM_PCT,
    in0.SAVER_BUYUP_PAX AS SAVER_BUYUP_PAX,
    in0.SAVER_OFFER_PAX AS SAVER_OFFER_PAX,
    in0.ER_REV AS ER_REV,
    in0.Right_Q AS Right_Q
  
  FROM Join_176_left AS in0
  INNER JOIN AlteryxSelect_102 AS in1
     ON ((in0.MONTH = t.MONTH0) AND (in0.REGION = t.REGION))

),

Join_176_inner AS (

  SELECT 
    in1.DPTR_DATE AS DPTR_DATE,
    in0.MONTH AS MONTH,
    in0.NDOD AS NDOD,
    in0.REGION AS REGION,
    in0.PAX AS PAX,
    in0.RPM AS RPM,
    in0.FIRST_REV AS FIRST_REV,
    in0.PC_REV AS PC_REV,
    in0.MAIN_REV AS MAIN_REV,
    in0.SAVER_REV AS SAVER_REV,
    in0.F_UPG_REV AS F_UPG_REV,
    in0.PC_UPG_REV AS PC_UPG_REV,
    in1.REV_PCT AS REV_PCT,
    in1.F_REV_PCT AS F_REV_PCT,
    in1.MAIN_REV_PCT AS MAIN_REV_PCT,
    in1.RPM_PCT AS RPM_PCT,
    in1.F_RPM_PCT AS F_RPM_PCT,
    in1.MAIN_RPM_PCT AS MAIN_RPM_PCT,
    in0.TTL_CPN_REV AS TTL_CPN_REV,
    in1.SAVER_RPM_PCT AS SAVER_RPM_PCT,
    in0.SAVER_BUYUP_PAX AS SAVER_BUYUP_PAX,
    in0.SAVER_OFFER_PAX AS SAVER_OFFER_PAX,
    in0.* EXCEPT (`MONTH`, 
    `NDOD`, 
    `REGION`, 
    `PAX`, 
    `RPM`, 
    `FIRST_REV`, 
    `PC_REV`, 
    `MAIN_REV`, 
    `SAVER_REV`, 
    `F_UPG_REV`, 
    `PC_UPG_REV`, 
    `TTL_CPN_REV`, 
    `SAVER_BUYUP_PAX`, 
    `SAVER_OFFER_PAX`)
  
  FROM Join_183_inner AS in0
  INNER JOIN AlteryxSelect_100 AS in1
     ON ((in0.MONTH = t.MONTH0) AND (in0.NDOD = t.NDOD))

),

Union_177 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_175_inner', 'Join_176_inner'], 
      [
        '[{"name": "RPM_PCT", "dataType": "Double"}, {"name": "MAIN_REV", "dataType": "Double"}, {"name": "NDOD", "dataType": "String"}, {"name": "MAIN_RPM_PCT", "dataType": "Double"}, {"name": "TTL_CPN_REV", "dataType": "Double"}, {"name": "PC_UPG_REV", "dataType": "Double"}, {"name": "SAVER_RPM_PCT", "dataType": "Double"}, {"name": "F_UPG_REV", "dataType": "Double"}, {"name": "MONTH", "dataType": "Date"}, {"name": "MAIN_REV_PCT", "dataType": "Double"}, {"name": "REGION", "dataType": "String"}, {"name": "PAX", "dataType": "Double"}, {"name": "REV_PCT", "dataType": "Double"}, {"name": "SAVER_REV", "dataType": "Double"}, {"name": "FIRST_REV", "dataType": "Double"}, {"name": "Right_Q", "dataType": "Double"}, {"name": "F_REV_PCT", "dataType": "Double"}, {"name": "DPTR_DATE", "dataType": "Timestamp"}, {"name": "RPM", "dataType": "Double"}, {"name": "PC_REV", "dataType": "Double"}, {"name": "SAVER_OFFER_PAX", "dataType": "Double"}, {"name": "ER_REV", "dataType": "Double"}, {"name": "SAVER_BUYUP_PAX", "dataType": "Double"}, {"name": "F_RPM_PCT", "dataType": "Double"}]', 
        '[{"name": "RPM_PCT", "dataType": "Double"}, {"name": "MAIN_REV", "dataType": "Double"}, {"name": "NDOD", "dataType": "String"}, {"name": "MAIN_RPM_PCT", "dataType": "Double"}, {"name": "TTL_CPN_REV", "dataType": "Double"}, {"name": "PC_UPG_REV", "dataType": "Double"}, {"name": "SAVER_RPM_PCT", "dataType": "Double"}, {"name": "F_UPG_REV", "dataType": "Double"}, {"name": "MONTH", "dataType": "Date"}, {"name": "MAIN_REV_PCT", "dataType": "Double"}, {"name": "REGION", "dataType": "String"}, {"name": "PAX", "dataType": "Double"}, {"name": "REV_PCT", "dataType": "Double"}, {"name": "SAVER_REV", "dataType": "Double"}, {"name": "FIRST_REV", "dataType": "Double"}, {"name": "Right_Q", "dataType": "Double"}, {"name": "F_REV_PCT", "dataType": "Double"}, {"name": "DPTR_DATE", "dataType": "Timestamp"}, {"name": "RPM", "dataType": "Double"}, {"name": "PC_REV", "dataType": "Double"}, {"name": "SAVER_OFFER_PAX", "dataType": "Double"}, {"name": "ER_REV", "dataType": "Double"}, {"name": "SAVER_BUYUP_PAX", "dataType": "Double"}, {"name": "F_RPM_PCT", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_283 AS (

  select  NULL as `CAP` ,  NULL as `ASM` ,  NULL as `ASM_PCT` ,  NULL as `CAP_PCT` , *  REPLACE( DPTR_DATE as `DPTR_DATE` ,  MONTH as `MONTH` ,  NDOD as `NDOD` ,  REGION as `REGION` ,  PAX as `PAX` ,  RPM as `RPM` ,  FIRST_REV as `FIRST_REV` ,  PC_REV as `PC_REV` ,  MAIN_REV as `MAIN_REV` ,  SAVER_REV as `SAVER_REV` ,  F_UPG_REV as `F_UPG_REV` ,  PC_UPG_REV as `PC_UPG_REV` ,  REV_PCT as `REV_PCT` ,  F_REV_PCT as `F_REV_PCT` ,  MAIN_REV_PCT as `MAIN_REV_PCT` ,  RPM_PCT as `RPM_PCT` ,  F_RPM_PCT as `F_RPM_PCT` ,  MAIN_RPM_PCT as `MAIN_RPM_PCT` ,  SAVER_RPM_PCT as `SAVER_RPM_PCT` ,  SAVER_BUYUP_PAX as `SAVER_BUYUP_PAX` ,  SAVER_OFFER_PAX as `SAVER_OFFER_PAX` ,  ER_REV as `ER_REV` ,  Right_Q as `Right_Q` ) from Union_177

),

Cleanse_284 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_283'], 
      [
        { "name": "RPM_PCT", "dataType": "Double" }, 
        { "name": "MAIN_REV", "dataType": "Double" }, 
        { "name": "NDOD", "dataType": "String" }, 
        { "name": "MAIN_RPM_PCT", "dataType": "Double" }, 
        { "name": "PC_UPG_REV", "dataType": "Double" }, 
        { "name": "SAVER_RPM_PCT", "dataType": "Double" }, 
        { "name": "F_UPG_REV", "dataType": "Double" }, 
        { "name": "MONTH", "dataType": "Date" }, 
        { "name": "MAIN_REV_PCT", "dataType": "Double" }, 
        { "name": "REGION", "dataType": "String" }, 
        { "name": "PAX", "dataType": "Double" }, 
        { "name": "REV_PCT", "dataType": "Double" }, 
        { "name": "SAVER_REV", "dataType": "Double" }, 
        { "name": "CAP_PCT", "dataType": "String" }, 
        { "name": "ASM", "dataType": "String" }, 
        { "name": "FIRST_REV", "dataType": "Double" }, 
        { "name": "Right_Q", "dataType": "Double" }, 
        { "name": "F_REV_PCT", "dataType": "Double" }, 
        { "name": "DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "RPM", "dataType": "Double" }, 
        { "name": "PC_REV", "dataType": "Double" }, 
        { "name": "ASM_PCT", "dataType": "String" }, 
        { "name": "SAVER_OFFER_PAX", "dataType": "Double" }, 
        { "name": "ER_REV", "dataType": "Double" }, 
        { "name": "CAP", "dataType": "String" }, 
        { "name": "SAVER_BUYUP_PAX", "dataType": "Double" }, 
        { "name": "F_RPM_PCT", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'DPTR_DATE', 
        'MONTH', 
        'NDOD', 
        'REGION', 
        'PAX', 
        'RPM', 
        'FIRST_REV', 
        'PC_REV', 
        'MAIN_REV', 
        'SAVER_REV', 
        'F_UPG_REV', 
        'PC_UPG_REV', 
        'REV_PCT', 
        'F_REV_PCT', 
        'MAIN_REV_PCT', 
        'RPM_PCT', 
        'F_RPM_PCT', 
        'MAIN_RPM_PCT'
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

Formula_186_0 AS (

  SELECT 
    (RPM * RPM_PCT) AS RPM,
    (PAX * RPM_PCT) AS PAX,
    (FIRST_REV * F_REV_PCT) AS FIRST_REV,
    (PC_REV * MAIN_REV_PCT) AS PC_REV,
    (MAIN_REV * MAIN_REV_PCT) AS MAIN_REV,
    (SAVER_REV * MAIN_REV_PCT) AS SAVER_REV,
    (F_UPG_REV * F_REV_PCT) AS F_UPG_REV,
    (PC_UPG_REV * MAIN_REV_PCT) AS PC_UPG_REV,
    (SAVER_BUYUP_PAX * SAVER_RPM_PCT) AS SAVER_BUYUP_PAX,
    (SAVER_OFFER_PAX * SAVER_RPM_PCT) AS SAVER_OFFER_PAX,
    (ER_REV * MAIN_REV_PCT) AS ER_REV,
    * EXCEPT (`main_rev`, 
    `pc_upg_rev`, 
    `f_upg_rev`, 
    `pax`, 
    `saver_rev`, 
    `first_rev`, 
    `rpm`, 
    `pc_rev`, 
    `saver_offer_pax`, 
    `er_rev`, 
    `saver_buyup_pax`)
  
  FROM Cleanse_284 AS in0

),

Summarize_189 AS (

  SELECT 
    SUM(PAX) AS PAX,
    SUM(RPM) AS RPM,
    SUM(FIRST_REV) AS FIRST_REV,
    SUM(PC_REV) AS PC_REV,
    SUM(MAIN_REV) AS MAIN_REV,
    SUM(SAVER_REV) AS SAVER_REV,
    SUM(F_UPG_REV) AS F_UPG_REV,
    SUM(PC_UPG_REV) AS PC_UPG_REV,
    SUM(SAVER_BUYUP_PAX) AS SAVER_BUYUP_PAX,
    SUM(SAVER_OFFER_PAX) AS SAVER_OFFER_PAX,
    SUM(ER_REV) AS ER_REV,
    DPTR_DATE AS DPTR_DATE,
    MONTH AS MONTH
  
  FROM Formula_186_0 AS in0
  
  GROUP BY 
    DPTR_DATE, MONTH

),

Summarize_294 AS (

  SELECT 
    SUM(CAP) AS CAP,
    SUM(ASM) AS ASM,
    MONTH AS MONTH
  
  FROM AlteryxSelect_184 AS in0
  
  GROUP BY MONTH

),

Join_295_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MONTH`)
  
  FROM Summarize_189 AS in0
  INNER JOIN Summarize_294 AS in1
     ON (t.MONTH0 = in1.MONTH)

),

Formula_121_0 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(DT, MONTH)))) AS MONTH,
    *
  
  FROM AlteryxSelect_120 AS in0

),

Summarize_119 AS (

  SELECT 
    SUM(ASM) AS ASM,
    SUM(CAP) AS CAP,
    MONTH AS MONTH,
    DT AS DT
  
  FROM Formula_121_0 AS in0
  
  GROUP BY 
    MONTH, DT

),

Summarize_123 AS (

  SELECT 
    (SUM(ASM) OVER (PARTITION BY MONTH ORDER BY 1 NULLS FIRST)) AS MTH_ASM,
    (SUM(CAP) OVER (PARTITION BY MONTH ORDER BY 1 NULLS FIRST)) AS MTH_CAP,
    *
  
  FROM Summarize_119 AS in0

),

Join_125_inner_formula_to_Formula_132_0 AS (

  select *  REPLACE( DT as `DT` ,  MONTH as `MONTH` ,  ASM as `ASM` ,  CAP as `CAP` ,  MTH_ASM as `MTH_ASM` ,  MTH_CAP as `MTH_CAP` ) from Summarize_123

),

Join_125_inner_formula_to_Formula_132_1 AS (

  SELECT 
    CAST((CAST(ASM AS NUMERIC) / CAST(MTH_ASM AS NUMERIC)) AS FLOAT64) AS ASM_PCT,
    CAST((CAST(CAP AS NUMERIC) / CAST(MTH_CAP AS NUMERIC)) AS FLOAT64) AS CAP_PCT,
    *
  
  FROM Join_125_inner_formula_to_Formula_132_0 AS in0

),

AlteryxSelect_133 AS (

  select  NULL as `REGION` , *  REPLACE( (CASE WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(DT AS string)) IS NOT NULL) THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(DT AS string)) AS DATE) WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(DT AS string)) IS NOT NULL) THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(DT AS string)) AS DATE) ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(DT AS string)) AS DATE) END) as `DT` ,  MONTH as `MONTH` ,  ASM_PCT as `ASM_PCT` ,  CAP_PCT as `CAP_PCT` ) from Join_125_inner_formula_to_Formula_132_1

),

Join_286_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`DT`, `MONTH`)
  
  FROM Join_295_inner AS in0
  INNER JOIN AlteryxSelect_133 AS in1
     ON (in0.DPTR_DATE = in1.DT)

),

Formula_287_0 AS (

  SELECT 
    (CAP * CAP_PCT) AS CAP,
    (ASM * ASM_PCT) AS ASM,
    * EXCEPT (`asm`, `cap`)
  
  FROM Join_286_inner AS in0

),

CrossTab_115_rename AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('CALM_Dashboard_Workflow', 'CrossTab_115_rename') }}

),

Formula_143_0 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(DT, MONTH)))) AS MONTH,
    *
  
  FROM CrossTab_115_rename AS in0

),

AlteryxSelect_139 AS (

  SELECT 
    Avg_BAG AS BAG_PCT,
    Avg_OTHER AS OTHER_PCT,
    * EXCEPT (`Avg_BAG`, `Avg_OTHER`)
  
  FROM Formula_143_0 AS in0

),

Database__repor_9 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_9_ref') }}

),

AlteryxSelect_190 AS (

  SELECT 
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(BDGT_MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(BDGT_MONTH AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(BDGT_MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(BDGT_MONTH AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(BDGT_MONTH AS STRING)) AS DATE)
      END
    ) AS BDGT_MONTH,
    * EXCEPT (`ER_UPG_FEE`, `BDGT_MONTH`)
  
  FROM Database__repor_9 AS in0

),

Join_199_inner AS (

  SELECT 
    in1.DT AS DT,
    in0.BAG_REV AS BAG_REV,
    in0.OTHER_REV AS OTHER_REV,
    in1.BAG_PCT AS BAG_PCT,
    in1.OTHER_PCT AS OTHER_PCT
  
  FROM AlteryxSelect_190 AS in0
  INNER JOIN AlteryxSelect_139 AS in1
     ON (in0.BDGT_MONTH = t.MONTH0)

),

Formula_200_0 AS (

  SELECT 
    (BAG_REV * BAG_PCT) AS BAG_REV,
    (OTHER_REV * OTHER_PCT) AS OTHER_REV,
    * EXCEPT (`bag_rev`, `other_rev`)
  
  FROM Join_199_inner AS in0

),

Join_198_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`DT`, `BAG_PCT`, `OTHER_PCT`)
  
  FROM Formula_287_0 AS in0
  LEFT JOIN Formula_200_0 AS in1
     ON (in0.DPTR_DATE = t.DT0)

),

AlteryxSelect_191 AS (

  SELECT 
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(BDGT_MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(BDGT_MONTH AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(BDGT_MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(BDGT_MONTH AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(BDGT_MONTH AS STRING)) AS DATE)
      END
    ) AS BDGT_MONTH,
    * EXCEPT (`BAG_REV`, `OTHER_REV`, `BDGT_MONTH`)
  
  FROM Database__repor_9 AS in0

),

Summarize_192 AS (

  SELECT 
    SUM(PY_MAIN_REV) AS PY_MAIN_REV,
    DPTR_DATE AS DPTR_DATE,
    MONTH AS MONTH
  
  FROM Formula_93_0 AS in0
  
  GROUP BY 
    DPTR_DATE, MONTH

),

Summarize_193 AS (

  SELECT 
    (SUM(PY_MAIN_REV) OVER (PARTITION BY MONTH ORDER BY 1 NULLS FIRST)) AS MTH_MAIN_REV,
    *
  
  FROM Summarize_192 AS in0

),

Join_194_inner_formula_to_Formula_195_0 AS (

  SELECT 
    DPTR_DATE AS DPTR_DATE,
    (PY_MAIN_REV / MTH_MAIN_REV) AS MAIN_PCT,
    * EXCEPT (`dptr_date`)
  
  FROM Summarize_193 AS in0

),

Join_196_inner AS (

  SELECT 
    in1.DPTR_DATE AS DPTR_DATE,
    in0.ER_UPG_FEE AS ER_UPG_FEE,
    in1.MAIN_PCT AS MAIN_PCT
  
  FROM AlteryxSelect_191 AS in0
  INNER JOIN Join_194_inner_formula_to_Formula_195_0 AS in1
     ON (in0.BDGT_MONTH = t.MONTH0)

),

Formula_197_0 AS (

  SELECT 
    (ER_UPG_FEE * MAIN_PCT) AS ER_UPG_REV,
    *
  
  FROM Join_196_inner AS in0

),

Join_201_left_UnionLeftOuter AS (

  SELECT 
    in0.DPTR_DATE AS DPTR_DATE,
    in0.MONTH AS MONTH,
    in0.CAP AS CAP,
    in0.ASM AS ASM,
    in0.PAX AS PAX,
    in0.RPM AS RPM,
    in0.FIRST_REV AS FIRST_REV,
    in0.PC_REV AS PC_REV,
    in0.MAIN_REV AS MAIN_REV,
    in0.SAVER_REV AS SAVER_REV,
    in0.F_UPG_REV AS F_UPG_REV,
    in0.PC_UPG_REV AS PC_UPG_REV,
    in1.ER_UPG_REV AS ER_UPG_REV,
    in0.BAG_REV AS BAG_REV,
    in0.OTHER_REV AS OTHER_REV,
    in0.SAVER_BUYUP_PAX AS SAVER_BUYUP_PAX,
    in0.SAVER_OFFER_PAX AS SAVER_OFFER_PAX,
    in0.ASM_PCT AS ASM_PCT,
    in0.CAP_PCT AS CAP_PCT,
    in0.ER_REV AS ER_REV,
    in0.* EXCEPT (`DPTR_DATE`, 
    `MONTH`, 
    `CAP`, 
    `ASM`, 
    `PAX`, 
    `RPM`, 
    `FIRST_REV`, 
    `PC_REV`, 
    `MAIN_REV`, 
    `SAVER_REV`, 
    `F_UPG_REV`, 
    `PC_UPG_REV`, 
    `BAG_REV`, 
    `OTHER_REV`, 
    `SAVER_BUYUP_PAX`, 
    `SAVER_OFFER_PAX`, 
    `ASM_PCT`, 
    `CAP_PCT`, 
    `ER_REV`)
  
  FROM Join_198_left_UnionLeftOuter AS in0
  LEFT JOIN Formula_197_0 AS in1
     ON (in0.DPTR_DATE = t.DPTR_DATE0)

),

AlteryxSelect_154 AS (

  SELECT 
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(MONTH AS STRING)) AS DATE)
      END
    ) AS MONTH,
    CY_ONLINE_REDEMPTION AS CY_ONLINE_REDEMPTION,
    variableTYPE AS variableTYPE
  
  FROM Database__repor_32 AS in0

),

Filter_155_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_154 AS in0
  
  WHERE ((variableTYPE <> 'FORECAST') OR ((variableTYPE = 'FORECAST') IS NULL))

),

Formula_148_0 AS (

  SELECT 
    (PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', (DATE_ADD(CPN_DPTR_DATE, INTERVAL 12 MONTH)))) AS CPN_DPTR_DATE,
    * EXCEPT (`cpn_dptr_date`)
  
  FROM Database__repor_31 AS in0

),

Formula_148_1 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(CPN_DPTR_DATE, MONTH)))) AS MONTH,
    *
  
  FROM Formula_148_0 AS in0

),

Summarize_150 AS (

  SELECT 
    (SUM(RDMD_PTS) OVER (PARTITION BY MONTH ORDER BY 1 NULLS FIRST)) AS MTH_RDMD_PTS,
    *
  
  FROM Formula_148_1 AS in0

),

Join_151_inner_formula_to_Formula_152_0 AS (

  select *  REPLACE( CPN_DPTR_DATE as `CPN_DPTR_DATE` ,  MONTH as `MONTH` ,  RDMD_PTS as `RDMD_PTS` ,  MTH_RDMD_PTS as `MTH_RDMD_PTS` ) from Summarize_150

),

Join_151_inner_formula_to_Formula_152_1 AS (

  SELECT 
    CAST((CAST(RDMD_PTS AS NUMERIC) / CAST(MTH_RDMD_PTS AS NUMERIC)) AS FLOAT64) AS MP_PCT,
    *
  
  FROM Join_151_inner_formula_to_Formula_152_0 AS in0

),

Join_202_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MONTH`, `variableTYPE`)
  
  FROM Join_151_inner_formula_to_Formula_152_1 AS in0
  INNER JOIN Filter_155_reject AS in1
     ON (t.MONTH0 = in1.MONTH)

),

Formula_203_0 AS (

  SELECT 
    (CY_ONLINE_REDEMPTION * MP_PCT) AS ONLN_MP_REDEEM,
    *
  
  FROM Join_202_inner AS in0

),

Join_204_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CPN_DPTR_DATE`, `MONTH`, `RDMD_PTS`, `MTH_RDMD_PTS`, `MP_PCT`, `CY_ONLINE_REDEMPTION`)
  
  FROM Join_201_left_UnionLeftOuter AS in0
  LEFT JOIN Formula_203_0 AS in1
     ON (in0.DPTR_DATE = t.CPN_DPTR_DATE0)

),

Filter_170_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_169 AS in0
  
  WHERE ((variableTYPE <> 'FORECAST') OR ((variableTYPE = 'FORECAST') IS NULL))

),

Formula_165_1 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(CPN_DPTR_DATE, MONTH)))) AS MONTH,
    (CC_MILES / 7) AS CC_MILES,
    * EXCEPT (`cc_miles`)
  
  FROM Formula_165_0 AS in0

),

Summarize_166 AS (

  SELECT 
    (SUM(CC_MILES) OVER (PARTITION BY MONTH ORDER BY 1 NULLS FIRST)) AS MTH_CC_MILES,
    *
  
  FROM Formula_165_1 AS in0

),

Join_167_inner_formula_to_Formula_168_0 AS (

  SELECT 
    CPN_DPTR_DATE AS CPN_DPTR_DATE,
    MONTH AS MONTH,
    CC_MILES AS CC_MILES,
    MTH_CC_MILES AS MTH_CC_MILES,
    * EXCEPT (`cpn_dptr_date`, `mth_cc_miles`, `month`, `cc_miles`)
  
  FROM Summarize_166 AS in0

),

Join_167_inner_formula_to_Formula_168_1 AS (

  SELECT 
    CAST((CAST(CC_MILES AS NUMERIC) / CAST(MTH_CC_MILES AS NUMERIC)) AS FLOAT64) AS CC_PCT,
    *
  
  FROM Join_167_inner_formula_to_Formula_168_0 AS in0

),

Join_206_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MONTH`, `variableTYPE`)
  
  FROM Join_167_inner_formula_to_Formula_168_1 AS in0
  INNER JOIN Filter_170_reject AS in1
     ON (t.MONTH0 = in1.MONTH)

),

Formula_205_0 AS (

  SELECT 
    (CY_MP_US_CC_COMISSION * CC_PCT) AS CC_COMMISSION,
    *
  
  FROM Join_206_inner AS in0

),

Join_207_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CPN_DPTR_DATE`, `MONTH`, `CC_MILES`, `MTH_CC_MILES`, `CC_PCT`, `CY_MP_US_CC_COMISSION`)
  
  FROM Join_204_left_UnionLeftOuter AS in0
  LEFT JOIN Formula_205_0 AS in1
     ON (in0.DPTR_DATE = t.CPN_DPTR_DATE0)

),

Cleanse_304 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_207_left_UnionLeftOuter'], 
      [
        { "name": "CC_COMMISSION", "dataType": "Double" }, 
        { "name": "MAIN_REV", "dataType": "Double" }, 
        { "name": "PC_UPG_REV", "dataType": "Double" }, 
        { "name": "WKLY_SNAP_DT", "dataType": "Timestamp" }, 
        { "name": "ER_UPG_REV", "dataType": "Double" }, 
        { "name": "F_UPG_REV", "dataType": "Double" }, 
        { "name": "BAG_REV", "dataType": "Double" }, 
        { "name": "MONTH", "dataType": "Date" }, 
        { "name": "KEEP", "dataType": "String" }, 
        { "name": "REGION", "dataType": "String" }, 
        { "name": "PAX", "dataType": "Double" }, 
        { "name": "SAVER_REV", "dataType": "Double" }, 
        { "name": "CAP_PCT", "dataType": "Double" }, 
        { "name": "ASM", "dataType": "Double" }, 
        { "name": "OTHER_REV", "dataType": "Double" }, 
        { "name": "FIRST_REV", "dataType": "Double" }, 
        { "name": "DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "RPM", "dataType": "Double" }, 
        { "name": "PC_REV", "dataType": "Double" }, 
        { "name": "ASM_PCT", "dataType": "Double" }, 
        { "name": "SAVER_OFFER_PAX", "dataType": "Double" }, 
        { "name": "ER_REV", "dataType": "Double" }, 
        { "name": "CAP", "dataType": "Double" }, 
        { "name": "SAVER_BUYUP_PAX", "dataType": "Double" }, 
        { "name": "ONLN_MP_REDEEM", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'DPTR_DATE', 
        'MONTH', 
        'PAX', 
        'RPM', 
        'FIRST_REV', 
        'PC_REV', 
        'MAIN_REV', 
        'SAVER_REV', 
        'F_UPG_REV', 
        'PC_UPG_REV', 
        'SAVER_BUYUP_PAX', 
        'SAVER_OFFER_PAX', 
        'ER_REV', 
        'CAP', 
        'ASM', 
        'ASM_PCT', 
        'CAP_PCT', 
        'BAG_REV', 
        'OTHER_REV', 
        'ER_UPG_REV', 
        'ONLN_MP_REDEEM', 
        'CC_COMMISSION'
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

Formula_214_0 AS (

  SELECT 
    'BUDGET' AS variableTYPE,
    (
      (
        (
          (
            (
              ((((((FIRST_REV + PC_REV) + MAIN_REV) + SAVER_REV) + F_UPG_REV) + PC_UPG_REV) + ER_UPG_REV)
              + BAG_REV
            )
            + OTHER_REV
          )
          + ONLN_MP_REDEEM
        )
        + CC_COMMISSION
      )
      + ER_REV
    ) AS TTL_REV,
    ((((FIRST_REV + PC_REV) + MAIN_REV) + SAVER_REV) + ER_REV) AS CPN_REV,
    *
  
  FROM Cleanse_304 AS in0

),

Query_SELECTF___4 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Query_SELECTF___4_ref') }}

),

AlteryxSelect_72 AS (

  SELECT 
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(MONTH AS STRING)) AS DATE)
      END
    ) AS MONTH,
    * EXCEPT (`MONTH`)
  
  FROM Query_SELECTF___4 AS in0

),

Formula_78_0 AS (

  SELECT 
    (FIRST_REV / TTL_CPN_REV) AS F_BGT_REV_PCT,
    (PC_REV / TTL_CPN_REV) AS PC_BGT_REV_PCT,
    (MAIN_REV / TTL_CPN_REV) AS MAIN_BGT_REV_PCT,
    (SAVER_REV / TTL_CPN_REV) AS SAVER_BGT_REV_PCT,
    (ER_REV / TTL_CPN_REV) AS ER_BGT_REV_PCT,
    *
  
  FROM Join_77_inner AS in0

),

Join_82_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MONTH`, `NDOD`, `REGION`, `FIRST_REV`, `MAIN_REV`, `PC_REV`, `SAVER_REV`, `TTL_CPN_REV`)
  
  FROM AlteryxSelect_72 AS in0
  INNER JOIN Formula_78_0 AS in1
     ON ((in0.MONTH = t.MONTH0) AND (in0.NDOD = t.NDOD))

),

Join_82_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_72 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM Formula_78_0 AS in1
  ) AS in1_keys
     ON ((in0.MONTH = t.MONTH0) AND (in0.NDOD = t.NDOD))

),

Summarize_80 AS (

  SELECT 
    SUM(FIRST_REV) AS FIRST_REV,
    SUM(MAIN_REV) AS MAIN_REV,
    SUM(PC_REV) AS PC_REV,
    SUM(SAVER_REV) AS SAVER_REV,
    SUM(TTL_CPN_REV) AS TTL_CPN_REV,
    SUM(ER_REV) AS ER_REV,
    MONTH AS MONTH,
    REGION AS REGION
  
  FROM Join_77_inner AS in0
  
  GROUP BY 
    MONTH, REGION

),

Formula_79_0 AS (

  SELECT 
    (FIRST_REV / TTL_CPN_REV) AS F_BGT_REV_PCT,
    (PC_REV / TTL_CPN_REV) AS PC_BGT_REV_PCT,
    (MAIN_REV / TTL_CPN_REV) AS MAIN_BGT_REV_PCT,
    (SAVER_REV / TTL_CPN_REV) AS SAVER_BGT_REV_PCT,
    (ER_REV / TTL_CPN_REV) AS ER_BGT_REV_PCT,
    *
  
  FROM Summarize_80 AS in0

),

Join_83_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MONTH`, `REGION`, `FIRST_REV`, `MAIN_REV`, `PC_REV`, `SAVER_REV`, `TTL_CPN_REV`)
  
  FROM Join_82_left AS in0
  INNER JOIN Formula_79_0 AS in1
     ON ((in0.MONTH = t.MONTH0) AND (in0.REGION = t.REGION))

),

Union_84 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_82_inner', 'Join_83_inner'], 
      [
        '[{"name": "ER_BGT_REV_PCT", "dataType": "Double"}, {"name": "F_BGT_REV_PCT", "dataType": "Double"}, {"name": "NDOD", "dataType": "String"}, {"name": "PC_UPG_REV", "dataType": "Double"}, {"name": "ER_UPG_REV", "dataType": "Double"}, {"name": "F_UPG_REV", "dataType": "Double"}, {"name": "BAG_REV", "dataType": "Double"}, {"name": "CPN_REV", "dataType": "Double"}, {"name": "MONTH", "dataType": "Timestamp"}, {"name": "SAVER_BGT_REV_PCT", "dataType": "Double"}, {"name": "REGION", "dataType": "String"}, {"name": "PAX", "dataType": "Double"}, {"name": "PC_BGT_REV_PCT", "dataType": "Double"}, {"name": "ASM", "dataType": "Double"}, {"name": "OTHER_REV", "dataType": "Double"}, {"name": "MAIN_BGT_REV_PCT", "dataType": "Double"}, {"name": "RPM", "dataType": "Double"}, {"name": "ER_REV", "dataType": "Double"}, {"name": "CAP", "dataType": "Double"}]', 
        '[{"name": "ER_BGT_REV_PCT", "dataType": "Double"}, {"name": "F_BGT_REV_PCT", "dataType": "Double"}, {"name": "NDOD", "dataType": "String"}, {"name": "PC_UPG_REV", "dataType": "Double"}, {"name": "ER_UPG_REV", "dataType": "Double"}, {"name": "F_UPG_REV", "dataType": "Double"}, {"name": "BAG_REV", "dataType": "Double"}, {"name": "CPN_REV", "dataType": "Double"}, {"name": "MONTH", "dataType": "Timestamp"}, {"name": "SAVER_BGT_REV_PCT", "dataType": "Double"}, {"name": "REGION", "dataType": "String"}, {"name": "PAX", "dataType": "Double"}, {"name": "PC_BGT_REV_PCT", "dataType": "Double"}, {"name": "ASM", "dataType": "Double"}, {"name": "OTHER_REV", "dataType": "Double"}, {"name": "MAIN_BGT_REV_PCT", "dataType": "Double"}, {"name": "RPM", "dataType": "Double"}, {"name": "ER_REV", "dataType": "Double"}, {"name": "CAP", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_170 AS (

  SELECT * 
  
  FROM AlteryxSelect_169 AS in0
  
  WHERE (variableTYPE = 'FORECAST')

),

AlteryxSelect_33 AS (

  SELECT 
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(MONTH AS STRING)) AS DATE)
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) IS NOT NULL)
          THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(MONTH AS STRING)) AS DATE)
        ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(MONTH AS STRING)) AS DATE)
      END
    ) AS MONTH,
    CY_ONLINE_REDEMPTION AS CY_ONLINE_REDEMPTION,
    variableTYPE AS variableTYPE
  
  FROM Database__repor_32 AS in0

),

Filter_44 AS (

  SELECT * 
  
  FROM AlteryxSelect_33 AS in0
  
  WHERE (variableTYPE = 'FORECAST')

),

Filter_146 AS (

  SELECT * 
  
  FROM Database__repor_31 AS in0
  
  WHERE ((PARSE_DATE('%Y-%m-%d', CPN_DPTR_DATE)) >= (PARSE_DATE('%Y-%m-%d', (DATE_ADD((DATE_TRUNC(CURRENT_DATE, MONTH)), INTERVAL -12 MONTH)))))

),

Formula_35_0 AS (

  SELECT 
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(CPN_DPTR_DATE, MONTH)))) AS MONTH,
    (PARSE_DATE('%Y-%m-%d', (DATE_TRUNC(CURRENT_DATE, MONTH)))) AS CM,
    *
  
  FROM Filter_146 AS in0

),

Summarize_36 AS (

  SELECT 
    SUM((
      CASE
        WHEN (MONTH < CM)
          THEN RDMD_PTS
        ELSE NULL
      END
    )) AS RDMD_PTS,
    MONTH AS MONTH
  
  FROM Formula_35_0 AS in0
  
  GROUP BY MONTH

),

Join_38_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MONTH`, `variableTYPE`)
  
  FROM Summarize_36 AS in0
  INNER JOIN Filter_44 AS in1
     ON (t.MONTH0 = in1.MONTH)

),

Summarize_57 AS (

  SELECT 
    SUM(RDMD_PTS) AS RDMD_PTS,
    SUM(CY_ONLINE_REDEMPTION) AS CY_ONLINE_REDEMPTION,
    MONTH AS MONTH
  
  FROM Join_38_inner AS in0
  
  GROUP BY MONTH

),

Formula_56_0 AS (

  SELECT 
    (CY_ONLINE_REDEMPTION / RDMD_PTS) AS PTS_VALUE,
    *
  
  FROM Summarize_57 AS in0

),

Join_41_left AS (

  SELECT in0.*
  
  FROM Formula_42_0 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.MONTH
    
    FROM Formula_56_0 AS in1
    
    WHERE in1.MONTH IS NOT NULL
  ) AS in1_keys
     ON (in0.MONTH = in1_keys.MONTH)
  
  WHERE (in1_keys.MONTH IS NULL)

),

Summarize_40 AS (

  SELECT 
    SUM(RDMD_PTS) AS RDMD_PTS,
    SUM(CY_ONLINE_REDEMPTION) AS CY_ONLINE_REDEMPTION
  
  FROM Join_38_inner AS in0

),

Formula_39_0 AS (

  SELECT 
    (CY_ONLINE_REDEMPTION / RDMD_PTS) AS PTS_VALUE,
    '1' AS `1`,
    *
  
  FROM Summarize_40 AS in0

),

Join_58_inner AS (

  SELECT 
    in0.* EXCEPT (`1`),
    in1.* EXCEPT (`RDMD_PTS`, `CY_ONLINE_REDEMPTION`, `1`)
  
  FROM Join_41_left AS in0
  INNER JOIN Formula_39_0 AS in1
     ON (in0.`1` = in1.`1`)

),

Join_41_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MONTH`, `RDMD_PTS`, `CY_ONLINE_REDEMPTION`)
  
  FROM Formula_42_0 AS in0
  INNER JOIN Formula_56_0 AS in1
     ON (in0.MONTH = in1.MONTH)

),

Union_59 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_41_inner', 'Join_58_inner'], 
      [
        '[{"name": "CPN_DPTR_DATE", "dataType": "Timestamp"}, {"name": "MONTH", "dataType": "Date"}, {"name": "1", "dataType": "String"}, {"name": "PTS_VALUE", "dataType": "Double"}, {"name": "RDMD_PTS", "dataType": "Double"}]', 
        '[{"name": "CPN_DPTR_DATE", "dataType": "Timestamp"}, {"name": "RDMD_PTS", "dataType": "Double"}, {"name": "MONTH", "dataType": "Date"}, {"name": "PTS_VALUE", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_43_0 AS (

  SELECT 
    (RDMD_PTS * PTS_VALUE) AS ONLN_MP_REDEEM,
    *
  
  FROM Union_59 AS in0

),

Database__repor_11 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_11_ref') }}

),

Summarize_24 AS (

  SELECT 
    SUM(F_UPG_FEE) AS F_UPG_FEE,
    SUM(PC_UPG_FEE) AS PC_UPG_FEE,
    SUM(ER_UPG_FEE) AS ER_UPG_FEE,
    LEG_DPTR_DATE AS LEG_DPTR_DATE
  
  FROM Database__repor_11 AS in0
  
  GROUP BY LEG_DPTR_DATE

),

Database__repor_1 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__repor_1_ref') }}

),

Filter_109 AS (

  SELECT * 
  
  FROM Database__repor_1 AS in0
  
  WHERE (
          ((PARSE_DATE('%Y-%m-%d', DT)) > (PARSE_DATE('%Y-%m-%d', (DATE_ADD(CAST(CURRENT_DATE AS STRING), INTERVAL CAST(-98 AS INTEGER) DAY)))))
          OR ((PARSE_DATE('%Y-%m-%d', DT)) >= (PARSE_DATE('%Y-%m-%d', (DATE_ADD(DATE_TRUNC(CURRENT_DATE, YEAR), INTERVAL CAST(0 AS INTEGER) DAY)))))
        )

),

CrossTab_21 AS (

  SELECT *
  
  FROM (
    SELECT 
      DT,
      KPI,
      ANC_REV
    
    FROM Filter_109 AS in0
  )
  PIVOT (
    SUM(ANC_REV) AS Sum
    FOR KPI
    IN (
      'BAG', 'OTHER'
    )
  )

),

Sort_17 AS (

  SELECT * 
  
  FROM Database__repor_12 AS in0
  
  ORDER BY CAST(FLT_ORIG_DATE AS STRING) ASC

),

AlteryxSelect_15 AS (

  SELECT 
    LEG_DPTR_DATE AS LEG_DPTR_DATE,
    PRODUCT AS PRODUCT,
    CPN_REV AS CPN_REV
  
  FROM Database__repor_11 AS in0

),

CrossTab_20 AS (

  SELECT *
  
  FROM (
    SELECT 
      LEG_DPTR_DATE,
      PRODUCT,
      CPN_REV
    
    FROM AlteryxSelect_15 AS in0
  )
  PIVOT (
    SUM(CPN_REV) AS Sum
    FOR PRODUCT
    IN (
      'EXIT_ROW', 'PC', 'FIRST', 'SAVER', 'MAIN'
    )
  )

),

Join_67_inner AS (

  SELECT 
    in0.CPN_DPTR_DATE AS CPN_DPTR_DATE,
    in0.CC_MILES AS CC_MILES,
    in0.`1` AS `1`,
    in1.CC_VALUE AS CC_VALUE
  
  FROM Formula_68_0 AS in0
  INNER JOIN Formula_60_0 AS in1
     ON (t.MONTH0 = in1.RPT_MONTH)

),

Summarize_55 AS (

  SELECT 
    SUM(CC_MILES) AS CC_MILES,
    SUM(CY_MP_US_CC_COMISSION) AS CY_MP_US_CC_COMISSION
  
  FROM Join_52_inner AS in0

),

Formula_61_0 AS (

  SELECT 
    (CY_MP_US_CC_COMISSION / CC_MILES) AS CC_VALUE,
    '1' AS `1`,
    *
  
  FROM Summarize_55 AS in0

),

Join_69_inner AS (

  SELECT 
    in0.CPN_DPTR_DATE AS CPN_DPTR_DATE,
    in0.CC_MILES AS CC_MILES,
    in1.CC_VALUE AS CC_VALUE
  
  FROM Join_67_left AS in0
  INNER JOIN Formula_61_0 AS in1
     ON (in0.`1` = in1.`1`)

),

Union_70 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_67_inner', 'Join_69_inner'], 
      [
        '[{"name": "CPN_DPTR_DATE", "dataType": "Timestamp"}, {"name": "CC_MILES", "dataType": "Double"}, {"name": "1", "dataType": "String"}, {"name": "CC_VALUE", "dataType": "Double"}]', 
        '[{"name": "CPN_DPTR_DATE", "dataType": "Timestamp"}, {"name": "CC_MILES", "dataType": "Double"}, {"name": "CC_VALUE", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_71_0 AS (

  SELECT 
    (CC_MILES * CC_VALUE) AS CC_COMMISSION,
    *
  
  FROM Union_70 AS in0

),

Database__REPOR_301 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__REPOR_301_ref') }}

),

JoinMultiple_22 AS (

  SELECT 
    in0.FLT_ORIG_DATE AS DT,
    in0.CAP AS CAP,
    in0.ASM AS ASM,
    in0.RPM AS RPM,
    in0.PAX AS PAX,
    in1.LEG_DPTR_DATE AS LEG_DPTR_DATE,
    in1.EXIT_ROW AS EXIT_ROW,
    in1.FIRST AS FIRST,
    in1.MAIN AS MAIN,
    in1.PC AS PC,
    in1.SAVER AS SAVER,
    in2.LEG_DPTR_DATE AS Input_hash3_LEG_DPTR_DATE,
    in2.F_UPG_FEE AS F_UPG_FEE,
    in2.PC_UPG_FEE AS PC_UPG_FEE,
    in2.ER_UPG_FEE AS ER_UPG_FEE,
    in3.DT AS Input_hash4_DT,
    in3.BAG AS BAG,
    in3.OTHER AS OTHER,
    in4.CPN_DPTR_DATE AS CPN_DPTR_DATE,
    in4.ONLN_MP_REDEEM AS ONLN_MP_REDEEM,
    in5.CC_COMMISSION AS CC_COMMISSION,
    in6.DPTR_DATE AS DPTR_DATE,
    in6.SAVER_BUYUP_PAX AS SAVER_BUYUP_PAX,
    in6.SAVER_OFFER_PAX AS SAVER_OFFER_PAX
  
  FROM Sort_17 AS in0
  FULL JOIN CrossTab_20 AS in1
     ON (in0.FLT_ORIG_DATE = in1.LEG_DPTR_DATE)
  FULL JOIN Summarize_24 AS in2
     ON ((coalesce(IN0.FLT_ORIG_DATE, IN1.LEG_DPTR_DATE)) = IN2.LEG_DPTR_DATE)
  FULL JOIN CrossTab_21 AS in3
     ON ((coalesce(IN0.FLT_ORIG_DATE, IN1.LEG_DPTR_DATE, IN2.LEG_DPTR_DATE)) = IN3.DT)
  FULL JOIN Formula_43_0 AS in4
     ON ((coalesce(IN0.FLT_ORIG_DATE, IN1.LEG_DPTR_DATE, IN2.LEG_DPTR_DATE, IN3.DT)) = IN4.CPN_DPTR_DATE)
  FULL JOIN Formula_71_0 AS in5
     ON ((coalesce(IN0.FLT_ORIG_DATE, IN1.LEG_DPTR_DATE, IN2.LEG_DPTR_DATE, IN3.DT, IN4.CPN_DPTR_DATE)) = IN5.CPN_DPTR_DATE)
  FULL JOIN Database__REPOR_301 AS in6
     ON ((coalesce(IN0.FLT_ORIG_DATE, IN1.LEG_DPTR_DATE, IN2.LEG_DPTR_DATE, IN3.DT, IN4.CPN_DPTR_DATE, IN5.CPN_DPTR_DATE)) = IN6.DPTR_DATE)

),

Formula_28_0 AS (

  SELECT 
    (
      PARSE_TIMESTAMP(
        '%Y-%m-%d %H:%M:%%f', 
        (
          CASE
            WHEN (DT IS NULL)
              THEN (
                CASE
                  WHEN (LEG_DPTR_DATE IS NULL)
                    THEN (
                      CASE
                        WHEN (Input_hash3_LEG_DPTR_DATE IS NULL)
                          THEN (
                            CASE
                              WHEN (Input_hash4_DT IS NULL)
                                THEN CPN_DPTR_DATE
                              ELSE Input_hash4_DT
                            END
                          )
                        ELSE Input_hash3_LEG_DPTR_DATE
                      END
                    )
                  ELSE LEG_DPTR_DATE
                END
              )
            ELSE DT
          END
        ))
    ) AS DT,
    (((FIRST + PC) + MAIN) + SAVER) AS CPN_REV,
    * EXCEPT (`dt`)
  
  FROM JoinMultiple_22 AS in0

),

Cleanse_233 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_28_0'], 
      [
        { "name": "CC_COMMISSION", "dataType": "Double" }, 
        { "name": "OTHER", "dataType": "Double" }, 
        { "name": "CPN_DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "LEG_DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "BAG", "dataType": "Double" }, 
        { "name": "EXIT_ROW", "dataType": "Double" }, 
        { "name": "ER_UPG_FEE", "dataType": "Double" }, 
        { "name": "PC", "dataType": "Double" }, 
        { "name": "CPN_REV", "dataType": "Double" }, 
        { "name": "PAX", "dataType": "Double" }, 
        { "name": "FIRST", "dataType": "Double" }, 
        { "name": "PC_UPG_FEE", "dataType": "Double" }, 
        { "name": "SAVER", "dataType": "Double" }, 
        { "name": "ASM", "dataType": "Double" }, 
        { "name": "DT", "dataType": "Timestamp" }, 
        { "name": "DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "RPM", "dataType": "Double" }, 
        { "name": "F_UPG_FEE", "dataType": "Double" }, 
        { "name": "SAVER_OFFER_PAX", "dataType": "Double" }, 
        { "name": "CAP", "dataType": "Double" }, 
        { "name": "MAIN", "dataType": "Double" }, 
        { "name": "SAVER_BUYUP_PAX", "dataType": "Double" }, 
        { "name": "Input_hash3_LEG_DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "ONLN_MP_REDEEM", "dataType": "Double" }, 
        { "name": "Input_hash4_DT", "dataType": "Timestamp" }
      ], 
      'keepOriginal', 
      [
        'DT', 
        'CAP', 
        'ASM', 
        'RPM', 
        'PAX', 
        'LEG_DPTR_DATE', 
        'EXIT_ROW', 
        'FIRST', 
        'MAIN', 
        'PC', 
        'SAVER', 
        'Input_hash3_LEG_DPTR_DATE', 
        'F_UPG_FEE', 
        'PC_UPG_FEE', 
        'ER_UPG_FEE', 
        'BAG', 
        'OTHER', 
        'CPN_DPTR_DATE', 
        'ONLN_MP_REDEEM', 
        'CC_COMMISSION', 
        'CPN_REV'
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

Formula_212_0 AS (

  SELECT 
    'ACTUAL' AS variableTYPE,
    (
      (
        (
          ((((((((FIRST + PC) + EXIT_ROW) + MAIN) + SAVER) + F_UPG_FEE) + PC_UPG_FEE) + ER_UPG_FEE) + BAG)
          + OTHER
        )
        + ONLN_MP_REDEEM
      )
      + CC_COMMISSION
    ) AS TTL_REV,
    ((((FIRST + PC) + EXIT_ROW) + MAIN) + SAVER) AS CPN_REV,
    * EXCEPT (`cpn_rev`)
  
  FROM Cleanse_233 AS in0

),

AlteryxSelect_30 AS (

  SELECT 
    DT AS DPTR_DATE,
    variableTYPE AS variableTYPE,
    CAP AS CAP,
    ASM AS ASM,
    PAX AS PAX,
    RPM AS RPM,
    TTL_REV AS TTL_REV,
    CPN_REV AS CPN_REV,
    FIRST AS FIRST_REV,
    PC AS PC_REV,
    EXIT_ROW AS ER_REV,
    MAIN AS MAIN_REV,
    SAVER AS SAVER_REV,
    F_UPG_FEE AS F_UPG_REV,
    PC_UPG_FEE AS PC_UPG_REV,
    ER_UPG_FEE AS ER_UPG_REV,
    BAG AS BAG_REV,
    OTHER AS OTHER_REV,
    ONLN_MP_REDEEM AS ONLN_MP_REDEEM,
    CC_COMMISSION AS CC_COMMISSION,
    SAVER_BUYUP_PAX AS SAVER_BUYUP_PAX,
    SAVER_OFFER_PAX AS SAVER_OFFER_PAX
  
  FROM Formula_212_0 AS in0

),

Transpose_218 AS (

  {{
    prophecy_basics.Transpose(
      ['AlteryxSelect_30'], 
      ['DPTR_DATE', 'variableTYPE'], 
      [
        'CAP', 
        'ASM', 
        'PAX', 
        'RPM', 
        'TTL_REV', 
        'CPN_REV', 
        'FIRST_REV', 
        'PC_REV', 
        'ER_REV', 
        'MAIN_REV', 
        'SAVER_REV', 
        'F_UPG_REV', 
        'PC_UPG_REV', 
        'ER_UPG_REV', 
        'BAG_REV', 
        'OTHER_REV', 
        'ONLN_MP_REDEEM', 
        'CC_COMMISSION', 
        'SAVER_BUYUP_PAX', 
        'SAVER_OFFER_PAX'
      ], 
      'Name', 
      'Value', 
      [
        'CC_COMMISSION', 
        'variableTYPE', 
        'MAIN_REV', 
        'PC_UPG_REV', 
        'ER_UPG_REV', 
        'F_UPG_REV', 
        'BAG_REV', 
        'TTL_REV', 
        'CPN_REV', 
        'PAX', 
        'SAVER_REV', 
        'ASM', 
        'OTHER_REV', 
        'FIRST_REV', 
        'DPTR_DATE', 
        'RPM', 
        'PC_REV', 
        'SAVER_OFFER_PAX', 
        'ER_REV', 
        'CAP', 
        'SAVER_BUYUP_PAX', 
        'ONLN_MP_REDEEM'
      ], 
      true
    )
  }}

),

AlteryxSelect_219 AS (

  select  Name as `KPI` ,  VALUE as `ACTUAL` , *  REPLACE( (CASE WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(DPTR_DATE AS string)) IS NOT NULL) THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(DPTR_DATE AS string)) AS DATE) WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(DPTR_DATE AS string)) IS NOT NULL) THEN CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(DPTR_DATE AS string)) AS DATE) ELSE CAST(SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(DPTR_DATE AS string)) AS DATE) END) as `DPTR_DATE` ) from Transpose_218

),

Summarize_296 AS (

  SELECT 
    SUM(CAP) AS CAP,
    SUM(ASM) AS ASM,
    MONTH AS MONTH
  
  FROM AlteryxSelect_72 AS in0
  
  GROUP BY MONTH

),

Formula_85_0 AS (

  SELECT 
    (CPN_REV * F_BGT_REV_PCT) AS FIRST_REV,
    (CPN_REV * PC_BGT_REV_PCT) AS PC_REV,
    (CPN_REV * MAIN_BGT_REV_PCT) AS MAIN_REV,
    (CPN_REV * SAVER_BGT_REV_PCT) AS SAVER_REV,
    (CPN_REV * ER_BGT_REV_PCT) AS ER_REV,
    * EXCEPT (`er_rev`)
  
  FROM Union_84 AS in0

),

AlteryxSelect_87 AS (

  select *  REPLACE( MONTH as `MONTH` ,  NDOD as `NDOD` ,  REGION as `REGION` ,  PAX as `PAX` ,  RPM as `RPM` ,  FIRST_REV as `FIRST_REV` ,  PC_REV as `PC_REV` ,  MAIN_REV as `MAIN_REV` ,  SAVER_REV as `SAVER_REV` ,  F_UPG_REV as `F_UPG_REV` ,  PC_UPG_REV as `PC_UPG_REV` ,  ER_UPG_REV as `ER_UPG_REV` ,  ER_REV as `ER_REV` ) from Formula_85_0

),

Join_105_inner AS (

  SELECT 
    in1.DPTR_DATE AS DPTR_DATE,
    in0.MONTH AS MONTH,
    in0.NDOD AS NDOD,
    in0.REGION AS REGION,
    in0.PAX AS PAX,
    in0.RPM AS RPM,
    in0.FIRST_REV AS FIRST_REV,
    in0.PC_REV AS PC_REV,
    in0.MAIN_REV AS MAIN_REV,
    in0.SAVER_REV AS SAVER_REV,
    in0.F_UPG_REV AS F_UPG_REV,
    in0.PC_UPG_REV AS PC_UPG_REV,
    in0.ER_UPG_REV AS ER_UPG_REV,
    in1.REV_PCT AS REV_PCT,
    in1.F_REV_PCT AS F_REV_PCT,
    in1.MAIN_REV_PCT AS MAIN_REV_PCT,
    in1.RPM_PCT AS RPM_PCT,
    in1.F_RPM_PCT AS F_RPM_PCT,
    in1.MAIN_RPM_PCT AS MAIN_RPM_PCT,
    in1.SAVER_RPM_PCT AS SAVER_RPM_PCT,
    in0.ER_REV AS ER_REV
  
  FROM AlteryxSelect_87 AS in0
  INNER JOIN AlteryxSelect_100 AS in1
     ON ((in0.MONTH = t.MONTH0) AND (in0.NDOD = t.NDOD))

),

Join_105_left AS (

  SELECT in0.*
  
  FROM AlteryxSelect_87 AS in0
  LEFT JOIN (
    SELECT * 
    
    FROM AlteryxSelect_100 AS in1
  ) AS in1_keys
     ON ((in0.MONTH = t.MONTH0) AND (in0.NDOD = t.NDOD))

),

Join_106_inner AS (

  SELECT 
    in1.DPTR_DATE AS DPTR_DATE,
    in0.MONTH AS MONTH,
    in0.NDOD AS NDOD,
    in0.REGION AS REGION,
    in0.PAX AS PAX,
    in0.RPM AS RPM,
    in0.FIRST_REV AS FIRST_REV,
    in0.PC_REV AS PC_REV,
    in0.MAIN_REV AS MAIN_REV,
    in0.SAVER_REV AS SAVER_REV,
    in0.F_UPG_REV AS F_UPG_REV,
    in0.PC_UPG_REV AS PC_UPG_REV,
    in0.ER_UPG_REV AS ER_UPG_REV,
    in1.REV_PCT AS REV_PCT,
    in1.F_REV_PCT AS F_REV_PCT,
    in1.MAIN_REV_PCT AS MAIN_REV_PCT,
    in1.RPM_PCT AS RPM_PCT,
    in1.F_RPM_PCT AS F_RPM_PCT,
    in1.MAIN_RPM_PCT AS MAIN_RPM_PCT,
    in1.SAVER_RPM_PCT AS SAVER_RPM_PCT,
    in0.* EXCEPT (`MONTH`, 
    `NDOD`, 
    `REGION`, 
    `PAX`, 
    `RPM`, 
    `FIRST_REV`, 
    `PC_REV`, 
    `MAIN_REV`, 
    `SAVER_REV`, 
    `F_UPG_REV`, 
    `PC_UPG_REV`, 
    `ER_UPG_REV`)
  
  FROM Join_105_left AS in0
  INNER JOIN AlteryxSelect_102 AS in1
     ON ((in0.MONTH = t.MONTH0) AND (in0.REGION = t.REGION))

),

Union_107 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_105_inner', 'Join_106_inner'], 
      [
        '[{"name": "RPM_PCT", "dataType": "Double"}, {"name": "MAIN_REV", "dataType": "Double"}, {"name": "NDOD", "dataType": "String"}, {"name": "MAIN_RPM_PCT", "dataType": "Double"}, {"name": "PC_UPG_REV", "dataType": "Double"}, {"name": "SAVER_RPM_PCT", "dataType": "Double"}, {"name": "ER_UPG_REV", "dataType": "Double"}, {"name": "F_UPG_REV", "dataType": "Double"}, {"name": "MONTH", "dataType": "Timestamp"}, {"name": "MAIN_REV_PCT", "dataType": "Double"}, {"name": "REGION", "dataType": "String"}, {"name": "PAX", "dataType": "Double"}, {"name": "REV_PCT", "dataType": "Double"}, {"name": "SAVER_REV", "dataType": "Double"}, {"name": "FIRST_REV", "dataType": "Double"}, {"name": "F_REV_PCT", "dataType": "Double"}, {"name": "DPTR_DATE", "dataType": "Timestamp"}, {"name": "RPM", "dataType": "Double"}, {"name": "PC_REV", "dataType": "Double"}, {"name": "ER_REV", "dataType": "Double"}, {"name": "F_RPM_PCT", "dataType": "Double"}]', 
        '[{"name": "RPM_PCT", "dataType": "Double"}, {"name": "MAIN_REV", "dataType": "Double"}, {"name": "NDOD", "dataType": "String"}, {"name": "MAIN_RPM_PCT", "dataType": "Double"}, {"name": "PC_UPG_REV", "dataType": "Double"}, {"name": "SAVER_RPM_PCT", "dataType": "Double"}, {"name": "ER_UPG_REV", "dataType": "Double"}, {"name": "F_UPG_REV", "dataType": "Double"}, {"name": "MONTH", "dataType": "Timestamp"}, {"name": "MAIN_REV_PCT", "dataType": "Double"}, {"name": "REGION", "dataType": "String"}, {"name": "PAX", "dataType": "Double"}, {"name": "REV_PCT", "dataType": "Double"}, {"name": "SAVER_REV", "dataType": "Double"}, {"name": "FIRST_REV", "dataType": "Double"}, {"name": "F_REV_PCT", "dataType": "Double"}, {"name": "DPTR_DATE", "dataType": "Timestamp"}, {"name": "RPM", "dataType": "Double"}, {"name": "PC_REV", "dataType": "Double"}, {"name": "ER_REV", "dataType": "Double"}, {"name": "F_RPM_PCT", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_278 AS (

  select *  REPLACE( DPTR_DATE as `DPTR_DATE` ,  MONTH as `MONTH` ,  NDOD as `NDOD` ,  REGION as `REGION` ,  PAX as `PAX` ,  RPM as `RPM` ,  FIRST_REV as `FIRST_REV` ,  PC_REV as `PC_REV` ,  MAIN_REV as `MAIN_REV` ,  SAVER_REV as `SAVER_REV` ,  F_UPG_REV as `F_UPG_REV` ,  PC_UPG_REV as `PC_UPG_REV` ,  ER_UPG_REV as `ER_UPG_REV` ,  REV_PCT as `REV_PCT` ,  F_REV_PCT as `F_REV_PCT` ,  MAIN_REV_PCT as `MAIN_REV_PCT` ,  RPM_PCT as `RPM_PCT` ,  F_RPM_PCT as `F_RPM_PCT` ,  MAIN_RPM_PCT as `MAIN_RPM_PCT` ,  SAVER_RPM_PCT as `SAVER_RPM_PCT` ,  ER_REV as `ER_REV` ) from Union_107

),

Cleanse_279 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_278'], 
      [
        { "name": "RPM_PCT", "dataType": "Double" }, 
        { "name": "MAIN_REV", "dataType": "Double" }, 
        { "name": "NDOD", "dataType": "String" }, 
        { "name": "MAIN_RPM_PCT", "dataType": "Double" }, 
        { "name": "PC_UPG_REV", "dataType": "Double" }, 
        { "name": "SAVER_RPM_PCT", "dataType": "Double" }, 
        { "name": "ER_UPG_REV", "dataType": "Double" }, 
        { "name": "F_UPG_REV", "dataType": "Double" }, 
        { "name": "MONTH", "dataType": "Timestamp" }, 
        { "name": "MAIN_REV_PCT", "dataType": "Double" }, 
        { "name": "REGION", "dataType": "String" }, 
        { "name": "PAX", "dataType": "Double" }, 
        { "name": "REV_PCT", "dataType": "Double" }, 
        { "name": "SAVER_REV", "dataType": "Double" }, 
        { "name": "FIRST_REV", "dataType": "Double" }, 
        { "name": "F_REV_PCT", "dataType": "Double" }, 
        { "name": "DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "RPM", "dataType": "Double" }, 
        { "name": "PC_REV", "dataType": "Double" }, 
        { "name": "ER_REV", "dataType": "Double" }, 
        { "name": "F_RPM_PCT", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'DPTR_DATE', 
        'MONTH', 
        'NDOD', 
        'REGION', 
        'PAX', 
        'RPM', 
        'FIRST_REV', 
        'PC_REV', 
        'MAIN_REV', 
        'SAVER_REV', 
        'F_UPG_REV', 
        'PC_UPG_REV', 
        'ER_UPG_REV', 
        'REV_PCT', 
        'F_REV_PCT', 
        'MAIN_REV_PCT', 
        'RPM_PCT', 
        'F_RPM_PCT', 
        'MAIN_RPM_PCT'
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

Formula_108_0 AS (

  SELECT 
    (RPM * RPM_PCT) AS RPM,
    (PAX * RPM_PCT) AS PAX,
    (FIRST_REV * F_REV_PCT) AS FIRST_REV,
    (PC_REV * MAIN_REV_PCT) AS PC_REV,
    (MAIN_REV * MAIN_REV_PCT) AS MAIN_REV,
    (SAVER_REV * MAIN_REV_PCT) AS SAVER_REV,
    (F_UPG_REV * F_REV_PCT) AS F_UPG_REV,
    (PC_UPG_REV * MAIN_REV_PCT) AS PC_UPG_REV,
    (ER_UPG_REV * MAIN_REV_PCT) AS ER_UPG_REV,
    * EXCEPT (`main_rev`, `pc_upg_rev`, `er_upg_rev`, `f_upg_rev`, `pax`, `saver_rev`, `first_rev`, `rpm`, `pc_rev`)
  
  FROM Cleanse_279 AS in0

),

Summarize_116 AS (

  SELECT 
    SUM(PAX) AS PAX,
    SUM(RPM) AS RPM,
    SUM(FIRST_REV) AS FIRST_REV,
    SUM(PC_REV) AS PC_REV,
    SUM(MAIN_REV) AS MAIN_REV,
    SUM(SAVER_REV) AS SAVER_REV,
    SUM(F_UPG_REV) AS F_UPG_REV,
    SUM(PC_UPG_REV) AS PC_UPG_REV,
    SUM(ER_UPG_REV) AS ER_UPG_REV,
    SUM(ER_REV) AS ER_REV,
    DPTR_DATE AS DPTR_DATE,
    MONTH AS MONTH
  
  FROM Formula_108_0 AS in0
  
  GROUP BY 
    DPTR_DATE, MONTH

),

Join_297_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MONTH`)
  
  FROM Summarize_116 AS in0
  INNER JOIN Summarize_296 AS in1
     ON (in0.MONTH = in1.MONTH)

),

Join_288_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`DT`, `MONTH`)
  
  FROM Join_297_inner AS in0
  INNER JOIN AlteryxSelect_133 AS in1
     ON (in0.DPTR_DATE = in1.DT)

),

Formula_289_0 AS (

  SELECT 
    (CAP * CAP_PCT) AS CAP,
    (ASM * ASM_PCT) AS ASM,
    * EXCEPT (`asm`, `cap`)
  
  FROM Join_288_inner AS in0

),

Summarize_141 AS (

  SELECT 
    SUM(BAG_REV) AS BAG_REV,
    SUM(OTHER_REV) AS OTHER_REV,
    MONTH AS MONTH
  
  FROM Formula_85_0 AS in0
  
  GROUP BY MONTH

),

Join_142_inner AS (

  SELECT 
    in1.DT AS DT,
    in0.MONTH AS MONTH,
    in0.BAG_REV AS BAG_REV,
    in0.OTHER_REV AS OTHER_REV,
    in1.BAG_PCT AS BAG_PCT,
    in1.OTHER_PCT AS OTHER_PCT
  
  FROM Summarize_141 AS in0
  INNER JOIN AlteryxSelect_139 AS in1
     ON (in0.MONTH = t.MONTH0)

),

Formula_144_0 AS (

  SELECT 
    (BAG_REV * BAG_PCT) AS BAG_REV,
    (OTHER_REV * OTHER_PCT) AS OTHER_REV,
    * EXCEPT (`bag_rev`, `other_rev`)
  
  FROM Join_142_inner AS in0

),

Join_145_inner AS (

  SELECT 
    in0.* EXCEPT (`ASM_PCT`, `CAP_PCT`),
    in1.* EXCEPT (`DT`, `MONTH`, `BAG_PCT`, `OTHER_PCT`)
  
  FROM Formula_289_0 AS in0
  INNER JOIN Formula_144_0 AS in1
     ON (in0.DPTR_DATE = t.DT0)

),

Filter_155 AS (

  SELECT * 
  
  FROM AlteryxSelect_154 AS in0
  
  WHERE (variableTYPE = 'FORECAST')

),

Join_153_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MONTH`, `variableTYPE`)
  
  FROM Join_151_inner_formula_to_Formula_152_1 AS in0
  INNER JOIN Filter_155 AS in1
     ON (t.MONTH0 = in1.MONTH)

),

Formula_156_0 AS (

  SELECT 
    (CY_ONLINE_REDEMPTION * MP_PCT) AS ONLN_MP_REDEEM,
    *
  
  FROM Join_153_inner AS in0

),

Join_157_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CPN_DPTR_DATE`, `MONTH`, `RDMD_PTS`, `MTH_RDMD_PTS`, `MP_PCT`, `CY_ONLINE_REDEMPTION`)
  
  FROM Join_145_inner AS in0
  INNER JOIN Formula_156_0 AS in1
     ON (in0.DPTR_DATE = t.CPN_DPTR_DATE0)

),

Join_171_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`MONTH`, `variableTYPE`)
  
  FROM Join_167_inner_formula_to_Formula_168_1 AS in0
  INNER JOIN Filter_170 AS in1
     ON (t.MONTH0 = in1.MONTH)

),

Formula_172_0 AS (

  SELECT 
    (CY_MP_US_CC_COMISSION * CC_PCT) AS CC_COMMISSION,
    *
  
  FROM Join_171_inner AS in0

),

Join_173_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`CPN_DPTR_DATE`, `MONTH`, `CC_MILES`, `MTH_CC_MILES`, `CC_PCT`, `CY_MP_US_CC_COMISSION`)
  
  FROM Join_157_inner AS in0
  INNER JOIN Formula_172_0 AS in1
     ON (in0.DPTR_DATE = t.CPN_DPTR_DATE0)

),

Cleanse_305 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_173_inner'], 
      [
        { "name": "CC_COMMISSION", "dataType": "Double" }, 
        { "name": "MAIN_REV", "dataType": "Double" }, 
        { "name": "PC_UPG_REV", "dataType": "Double" }, 
        { "name": "WKLY_SNAP_DT", "dataType": "Timestamp" }, 
        { "name": "ER_UPG_REV", "dataType": "Double" }, 
        { "name": "F_UPG_REV", "dataType": "Double" }, 
        { "name": "BAG_REV", "dataType": "Double" }, 
        { "name": "MONTH", "dataType": "Timestamp" }, 
        { "name": "KEEP", "dataType": "String" }, 
        { "name": "REGION", "dataType": "String" }, 
        { "name": "PAX", "dataType": "Double" }, 
        { "name": "SAVER_REV", "dataType": "Double" }, 
        { "name": "ASM", "dataType": "Double" }, 
        { "name": "OTHER_REV", "dataType": "Double" }, 
        { "name": "FIRST_REV", "dataType": "Double" }, 
        { "name": "DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "RPM", "dataType": "Double" }, 
        { "name": "PC_REV", "dataType": "Double" }, 
        { "name": "ER_REV", "dataType": "Double" }, 
        { "name": "CAP", "dataType": "Double" }, 
        { "name": "ONLN_MP_REDEEM", "dataType": "Double" }
      ], 
      'keepOriginal', 
      [
        'DPTR_DATE', 
        'MONTH', 
        'PAX', 
        'RPM', 
        'FIRST_REV', 
        'PC_REV', 
        'MAIN_REV', 
        'SAVER_REV', 
        'F_UPG_REV', 
        'PC_UPG_REV', 
        'ER_UPG_REV', 
        'ER_REV', 
        'CAP', 
        'ASM', 
        'BAG_REV', 
        'OTHER_REV', 
        'ONLN_MP_REDEEM', 
        'CC_COMMISSION'
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

Formula_213_0 AS (

  SELECT 
    'FORECAST' AS variableTYPE,
    (
      (
        (
          (
            (
              ((((((FIRST_REV + PC_REV) + MAIN_REV) + SAVER_REV) + F_UPG_REV) + PC_UPG_REV) + ER_UPG_REV)
              + BAG_REV
            )
            + OTHER_REV
          )
          + ONLN_MP_REDEEM
        )
        + CC_COMMISSION
      )
      + ER_REV
    ) AS TTL_REV,
    ((((FIRST_REV + PC_REV) + MAIN_REV) + SAVER_REV) + ER_REV) AS CPN_REV,
    *
  
  FROM Cleanse_305 AS in0

),

AlteryxSelect_211 AS (

  SELECT 
    DPTR_DATE AS DPTR_DATE,
    variableTYPE AS variableTYPE,
    CAP AS CAP,
    ASM AS ASM,
    PAX AS PAX,
    RPM AS RPM,
    TTL_REV AS TTL_REV,
    CPN_REV AS CPN_REV,
    FIRST_REV AS FIRST_REV,
    PC_REV AS PC_REV,
    MAIN_REV AS MAIN_REV,
    SAVER_REV AS SAVER_REV,
    F_UPG_REV AS F_UPG_REV,
    PC_UPG_REV AS PC_UPG_REV,
    ER_UPG_REV AS ER_UPG_REV,
    BAG_REV AS BAG_REV,
    OTHER_REV AS OTHER_REV,
    ONLN_MP_REDEEM AS ONLN_MP_REDEEM,
    CC_COMMISSION AS CC_COMMISSION,
    ER_REV AS ER_REV
  
  FROM Formula_213_0 AS in0

),

Transpose_217 AS (

  {{
    prophecy_basics.Transpose(
      ['AlteryxSelect_211'], 
      ['DPTR_DATE', 'variableTYPE'], 
      [
        'CAP', 
        'ASM', 
        'PAX', 
        'RPM', 
        'TTL_REV', 
        'CPN_REV', 
        'FIRST_REV', 
        'PC_REV', 
        'MAIN_REV', 
        'SAVER_REV', 
        'F_UPG_REV', 
        'PC_UPG_REV', 
        'ER_UPG_REV', 
        'BAG_REV', 
        'OTHER_REV', 
        'ONLN_MP_REDEEM', 
        'CC_COMMISSION', 
        'ER_REV'
      ], 
      'Name', 
      'Value', 
      [
        'CC_COMMISSION', 
        'variableTYPE', 
        'MAIN_REV', 
        'PC_UPG_REV', 
        'ER_UPG_REV', 
        'F_UPG_REV', 
        'BAG_REV', 
        'TTL_REV', 
        'CPN_REV', 
        'PAX', 
        'SAVER_REV', 
        'ASM', 
        'OTHER_REV', 
        'FIRST_REV', 
        'DPTR_DATE', 
        'RPM', 
        'PC_REV', 
        'ER_REV', 
        'CAP', 
        'ONLN_MP_REDEEM'
      ], 
      true
    )
  }}

),

AlteryxSelect_220 AS (

  SELECT 
    Name AS KPI,
    VALUE AS FORECAST,
    * EXCEPT (`variableTYPE`, `Name`, `Value`)
  
  FROM Transpose_217 AS in0

),

Join_222_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.DPTR_DATE = in1.DPTR_DATE) AND (in0.KPI = in1.KPI))
          THEN NULL
        ELSE in1.DPTR_DATE
      END
    ) AS DPTR_DATE,
    (
      CASE
        WHEN ((in0.DPTR_DATE = in1.DPTR_DATE) AND (in0.KPI = in1.KPI))
          THEN NULL
        ELSE in1.KPI
      END
    ) AS KPI,
    (
      CASE
        WHEN ((in0.DPTR_DATE = in1.DPTR_DATE) AND (in0.KPI = in1.KPI))
          THEN in1.KPI
        ELSE NULL
      END
    ) AS Right_KPI,
    (
      CASE
        WHEN ((in0.DPTR_DATE = in1.DPTR_DATE) AND (in0.KPI = in1.KPI))
          THEN in1.DPTR_DATE
        ELSE NULL
      END
    ) AS Right_DPTR_DATE,
    in0.* EXCEPT (`DPTR_DATE`, `KPI`),
    in1.* EXCEPT (`DPTR_DATE`, `KPI`)
  
  FROM AlteryxSelect_219 AS in0
  FULL JOIN AlteryxSelect_220 AS in1
     ON ((in0.DPTR_DATE = in1.DPTR_DATE) AND (in0.KPI = in1.KPI))

),

Formula_224_0 AS (

  SELECT 
    (
      PARSE_DATE(
        '%Y-%m-%d', 
        (
          CASE
            WHEN (DPTR_DATE IS NULL)
              THEN Right_DPTR_DATE
            ELSE CAST(DPTR_DATE AS STRING)
          END
        ))
    ) AS DPTR_DATE,
    CAST((
      CASE
        WHEN (KPI IS NULL)
          THEN Right_KPI
        ELSE KPI
      END
    ) AS STRING) AS KPI,
    * EXCEPT (`kpi`, `dptr_date`)
  
  FROM Join_222_left_UnionFullOuter AS in0

),

AlteryxSelect_228 AS (

  SELECT * EXCEPT (`Right_DPTR_DATE`, `Right_KPI`)
  
  FROM Formula_224_0 AS in0

),

AlteryxSelect_210 AS (

  SELECT 
    DPTR_DATE AS DPTR_DATE,
    variableTYPE AS variableTYPE,
    CAP AS CAP,
    ASM AS ASM,
    PAX AS PAX,
    RPM AS RPM,
    TTL_REV AS TTL_REV,
    CPN_REV AS CPN_REV,
    FIRST_REV AS FIRST_REV,
    PC_REV AS PC_REV,
    MAIN_REV AS MAIN_REV,
    SAVER_REV AS SAVER_REV,
    F_UPG_REV AS F_UPG_REV,
    PC_UPG_REV AS PC_UPG_REV,
    ER_UPG_REV AS ER_UPG_REV,
    BAG_REV AS BAG_REV,
    OTHER_REV AS OTHER_REV,
    ONLN_MP_REDEEM AS ONLN_MP_REDEEM,
    CC_COMMISSION AS CC_COMMISSION,
    SAVER_BUYUP_PAX AS SAVER_BUYUP_PAX,
    SAVER_OFFER_PAX AS SAVER_OFFER_PAX,
    ER_REV AS ER_REV
  
  FROM Formula_214_0 AS in0

),

Transpose_215 AS (

  {{
    prophecy_basics.Transpose(
      ['AlteryxSelect_210'], 
      ['DPTR_DATE', 'variableTYPE'], 
      [
        'CAP', 
        'ASM', 
        'PAX', 
        'RPM', 
        'TTL_REV', 
        'CPN_REV', 
        'FIRST_REV', 
        'PC_REV', 
        'MAIN_REV', 
        'SAVER_REV', 
        'F_UPG_REV', 
        'PC_UPG_REV', 
        'ER_UPG_REV', 
        'BAG_REV', 
        'OTHER_REV', 
        'ONLN_MP_REDEEM', 
        'CC_COMMISSION', 
        'SAVER_BUYUP_PAX', 
        'SAVER_OFFER_PAX', 
        'ER_REV'
      ], 
      'Name', 
      'Value', 
      [
        'CC_COMMISSION', 
        'variableTYPE', 
        'MAIN_REV', 
        'PC_UPG_REV', 
        'ER_UPG_REV', 
        'F_UPG_REV', 
        'BAG_REV', 
        'TTL_REV', 
        'CPN_REV', 
        'PAX', 
        'SAVER_REV', 
        'ASM', 
        'OTHER_REV', 
        'FIRST_REV', 
        'DPTR_DATE', 
        'RPM', 
        'PC_REV', 
        'SAVER_OFFER_PAX', 
        'ER_REV', 
        'CAP', 
        'SAVER_BUYUP_PAX', 
        'ONLN_MP_REDEEM'
      ], 
      true
    )
  }}

),

AlteryxSelect_221 AS (

  SELECT 
    Name AS KPI,
    VALUE AS BUDGET,
    * EXCEPT (`variableTYPE`, `Name`, `Value`)
  
  FROM Transpose_215 AS in0

),

Join_225_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN ((CAST(in0.DPTR_DATE AS TIMESTAMP) = in1.DPTR_DATE) AND (in0.KPI = in1.KPI))
          THEN NULL
        ELSE in1.DPTR_DATE
      END
    ) AS DPTR_DATE,
    (
      CASE
        WHEN ((CAST(in0.DPTR_DATE AS TIMESTAMP) = in1.DPTR_DATE) AND (in0.KPI = in1.KPI))
          THEN NULL
        ELSE in1.KPI
      END
    ) AS KPI,
    (
      CASE
        WHEN ((CAST(in0.DPTR_DATE AS TIMESTAMP) = in1.DPTR_DATE) AND (in0.KPI = in1.KPI))
          THEN in1.KPI
        ELSE NULL
      END
    ) AS Right_KPI,
    (
      CASE
        WHEN ((CAST(in0.DPTR_DATE AS TIMESTAMP) = in1.DPTR_DATE) AND (in0.KPI = in1.KPI))
          THEN in1.DPTR_DATE
        ELSE NULL
      END
    ) AS Right_DPTR_DATE,
    in0.* EXCEPT (`DPTR_DATE`, `KPI`),
    in1.* EXCEPT (`DPTR_DATE`, `KPI`)
  
  FROM AlteryxSelect_228 AS in0
  FULL JOIN AlteryxSelect_221 AS in1
     ON ((t.DPTR_DATE0 = in1.DPTR_DATE) AND (t.KPI = in1.KPI))

),

Formula_227_0 AS (

  SELECT 
    (
      PARSE_DATE(
        '%Y-%m-%d', 
        (
          CASE
            WHEN (DPTR_DATE IS NULL)
              THEN Right_DPTR_DATE
            ELSE CAST(DPTR_DATE AS STRING)
          END
        ))
    ) AS DPTR_DATE,
    CAST((
      CASE
        WHEN (KPI IS NULL)
          THEN Right_KPI
        ELSE KPI
      END
    ) AS STRING) AS KPI,
    CAST((
      CASE
        WHEN (FORECAST IS NULL)
          THEN BUDGET
        ELSE FORECAST
      END
    ) AS FLOAT64) AS FORECAST,
    * EXCEPT (`kpi`, `forecast`, `dptr_date`)
  
  FROM Join_225_left_UnionFullOuter AS in0

),

Formula_227_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (BUDGET IS NULL)
          THEN CAST(FORECAST AS STRING)
        ELSE BUDGET
      END
    ) AS FLOAT64) AS BUDGET,
    * EXCEPT (`budget`)
  
  FROM Formula_227_0 AS in0

),

Database__loadi_236 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__loadi_236_ref') }}

),

AlteryxSelect_241 AS (

  select  DTE as `DPTR_DATE` , *  REPLACE( KPI as `KPI` ,  ACTUAL as `ACTUAL` ,  FORECAST as `FORECAST` ,  BUDGET as `BUDGET` ) from Database__loadi_236

),

Union_245_reformat_0 AS (

  SELECT 
    CAST(ACTUAL AS STRING) AS ACTUAL,
    BUDGET AS BUDGET,
    (PARSE_DATE('%Y-%m-%d', DPTR_DATE)) AS DPTR_DATE,
    FORECAST AS FORECAST,
    KPI AS KPI
  
  FROM AlteryxSelect_241 AS in0

),

Cleanse_240 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Formula_227_1'], 
      [
        { "name": "KPI", "dataType": "String" }, 
        { "name": "FORECAST", "dataType": "Double" }, 
        { "name": "ACTUAL", "dataType": "String" }, 
        { "name": "Right_DPTR_DATE", "dataType": "String" }, 
        { "name": "DPTR_DATE", "dataType": "Date" }, 
        { "name": "Right_KPI", "dataType": "String" }, 
        { "name": "BUDGET", "dataType": "Double" }
      ], 
      'keepOriginal', 
      ['DPTR_DATE', 'KPI', 'ACTUAL', 'FORECAST', 'Right_DPTR_DATE', 'Right_KPI', 'BUDGET'], 
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

AlteryxSelect_229 AS (

  SELECT * EXCEPT (`Right_DPTR_DATE`, `Right_KPI`)
  
  FROM Cleanse_240 AS in0

),

Union_245_reformat_1 AS (

  SELECT 
    CAST(ACTUAL AS STRING) AS ACTUAL,
    BUDGET AS BUDGET,
    (PARSE_DATE('%Y-%m-%d', DPTR_DATE)) AS DPTR_DATE,
    FORECAST AS FORECAST,
    KPI AS KPI
  
  FROM AlteryxSelect_229 AS in0

),

Database__loadi_237 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__loadi_237_ref') }}

),

AlteryxSelect_242 AS (

  select  SNAPSHOT_DATE as `DPTR_DATE` ,  CY_LF_BUILD as `ACTUAL` ,  FCST_LF_BUILD as `FORECAST` ,  BDGT_LF_BUILD as `BUDGET` , *  REPLACE() from Database__loadi_237

),

Formula_243_0 AS (

  SELECT 
    'Future LF Build' AS KPI,
    *
  
  FROM AlteryxSelect_242 AS in0

),

Union_245_reformat_2 AS (

  SELECT 
    CAST(ACTUAL AS STRING) AS ACTUAL,
    BUDGET AS BUDGET,
    (PARSE_DATE('%Y-%m-%d', DPTR_DATE)) AS DPTR_DATE,
    FORECAST AS FORECAST,
    KPI AS KPI
  
  FROM Formula_243_0 AS in0

),

Union_245 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_245_reformat_0', 'Union_245_reformat_2', 'Union_245_reformat_1'], 
      [
        '[{"name": "KPI", "dataType": "String"}, {"name": "FORECAST", "dataType": "Double"}, {"name": "ACTUAL", "dataType": "Double"}, {"name": "DPTR_DATE", "dataType": "Timestamp"}, {"name": "BUDGET", "dataType": "Double"}]', 
        '[{"name": "KPI", "dataType": "String"}, {"name": "FORECAST", "dataType": "Double"}, {"name": "ACTUAL", "dataType": "Double"}, {"name": "DPTR_DATE", "dataType": "Timestamp"}, {"name": "BUDGET", "dataType": "Double"}]', 
        '[{"name": "KPI", "dataType": "String"}, {"name": "FORECAST", "dataType": "Double"}, {"name": "ACTUAL", "dataType": "String"}, {"name": "DPTR_DATE", "dataType": "Date"}, {"name": "BUDGET", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_246_0 AS (

  SELECT 
    CAST(CURRENT_DATE AS STRING) AS REFRESH,
    *
  
  FROM Union_245 AS in0

)

SELECT *

FROM Formula_246_0
