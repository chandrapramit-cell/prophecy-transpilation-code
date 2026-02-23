{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH Database__REPOR_84 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__REPOR_84_ref') }}

),

AlteryxSelect_90 AS (

  SELECT 
    FLT_SCHED_DPTR_DATE_DAY AS BASE_DPTR_DATE,
    LEG_OPER_FLT_NBR AS FLT_NBR,
    LEG_COS_CODE AS CLASS_OF_SERVICE_CODE,
    * EXCEPT (`FLT_SCHED_DPTR_DATE_DAY`, `LEG_OPER_FLT_NBR`, `LEG_COS_CODE`)
  
  FROM Database__REPOR_84 AS in0

),

Database__REPOR_1 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__REPOR_1_ref') }}

),

Formula_4_0 AS (

  SELECT 
    CAST((CONCAT(BASE_ORIG, BASE_DEST)) AS STRING) AS LEG_OD,
    *
  
  FROM Database__REPOR_1 AS in0

),

Join_85_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN (
          (
            ((in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))
            AND (in0.FLT_NBR = in1.FLT_NBR)
          )
          AND (in0.CLASS_OF_SERVICE_CODE = in1.CLASS_OF_SERVICE_CODE)
        )
          THEN NULL
        ELSE in1.CLASS_OF_SERVICE_CODE
      END
    ) AS CLASS_OF_SERVICE_CODE,
    (
      CASE
        WHEN (
          (
            ((in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))
            AND (in0.FLT_NBR = in1.FLT_NBR)
          )
          AND (in0.CLASS_OF_SERVICE_CODE = in1.CLASS_OF_SERVICE_CODE)
        )
          THEN NULL
        ELSE in1.FLT_NBR
      END
    ) AS FLT_NBR,
    (
      CASE
        WHEN (
          (
            ((in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))
            AND (in0.FLT_NBR = in1.FLT_NBR)
          )
          AND (in0.CLASS_OF_SERVICE_CODE = in1.CLASS_OF_SERVICE_CODE)
        )
          THEN NULL
        ELSE in0.BASE_ORIG
      END
    ) AS BASE_ORIG,
    (
      CASE
        WHEN (
          (
            ((in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))
            AND (in0.FLT_NBR = in1.FLT_NBR)
          )
          AND (in0.CLASS_OF_SERVICE_CODE = in1.CLASS_OF_SERVICE_CODE)
        )
          THEN NULL
        ELSE in1.LEG_OD
      END
    ) AS LEG_OD,
    (
      CASE
        WHEN (
          (
            ((in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))
            AND (in0.FLT_NBR = in1.FLT_NBR)
          )
          AND (in0.CLASS_OF_SERVICE_CODE = in1.CLASS_OF_SERVICE_CODE)
        )
          THEN NULL
        ELSE in0.BASE_DEST
      END
    ) AS BASE_DEST,
    (
      CASE
        WHEN (
          (
            ((in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))
            AND (in0.FLT_NBR = in1.FLT_NBR)
          )
          AND (in0.CLASS_OF_SERVICE_CODE = in1.CLASS_OF_SERVICE_CODE)
        )
          THEN NULL
        ELSE in1.BASE_DPTR_DATE
      END
    ) AS BASE_DPTR_DATE,
    (
      CASE
        WHEN (
          (
            ((in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))
            AND (in0.FLT_NBR = in1.FLT_NBR)
          )
          AND (in0.CLASS_OF_SERVICE_CODE = in1.CLASS_OF_SERVICE_CODE)
        )
          THEN NULL
        ELSE in1.CARRIER_CD
      END
    ) AS CARRIER_CD,
    in0.* EXCEPT (`BASE_ORIG`, `BASE_DEST`, `CLASS_OF_SERVICE_CODE`, `FLT_NBR`, `LEG_OD`, `BASE_DPTR_DATE`),
    in1.* EXCEPT (`LEG_OD`, `BASE_DPTR_DATE`, `CARRIER_CD`, `FLT_NBR`, `CLASS_OF_SERVICE_CODE`)
  
  FROM Formula_4_0 AS in0
  FULL JOIN AlteryxSelect_90 AS in1
     ON (
      (
        ((in0.BASE_DPTR_DATE = in1.BASE_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))
        AND (in0.FLT_NBR = in1.FLT_NBR)
      )
      AND (in0.CLASS_OF_SERVICE_CODE = in1.CLASS_OF_SERVICE_CODE)
    )

),

Database__REPOR_2 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Database__REPOR_2_ref') }}

),

Formula_48_0 AS (

  SELECT 
    (REV / PAX) AS FARE,
    *
  
  FROM Database__REPOR_2 AS in0

),

TextInput_41 AS (

  SELECT * 
  
  FROM {{ ref('seed_41')}}

),

TextInput_41_cast AS (

  SELECT 
    CAST(COS AS STRING) AS COS,
    CAST(L1_COS AS STRING) AS L1_COS,
    CAST(L2_COS AS STRING) AS L2_COS,
    CAST(L3_COS AS STRING) AS L3_COS
  
  FROM TextInput_41 AS in0

),

Join_42_inner AS (

  SELECT 
    in1.LEG_SC_DPTR_DATE AS LEG_SC_DPTR_DATE,
    in1.LEG_OD AS LEG_OD,
    in1.LEG_AS_MKTG_FLT_NBR AS LEG_AS_MKTG_FLT_NBR,
    in1.PURCHASED_COS_CODE AS PURCHASED_COS_CODE,
    in0.L1_COS AS L1_COS,
    in0.L2_COS AS L2_COS,
    in0.L3_COS AS L3_COS,
    in1.LEG_OPER_CARR_AIRLINE_CODE AS LEG_OPER_CARR_AIRLINE_CODE,
    in1.FARE AS FARE
  
  FROM TextInput_41_cast AS in0
  INNER JOIN Formula_48_0 AS in1
     ON (in0.COS = in1.PURCHASED_COS_CODE)

),

Cleanse_88 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Join_85_left_UnionFullOuter'], 
      [
        { "name": "CLASS_OF_SERVICE_CODE", "dataType": "String" }, 
        { "name": "FLT_NBR", "dataType": "Numeric" }, 
        { "name": "BASE_ORIG", "dataType": "String" }, 
        { "name": "LEG_OD", "dataType": "String" }, 
        { "name": "BASE_DEST", "dataType": "String" }, 
        { "name": "BASE_DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "CARRIER_CD", "dataType": "String" }, 
        { "name": "ALTEA_DMD", "dataType": "Float" }, 
        { "name": "BKGS", "dataType": "Float" }
      ], 
      'keepOriginal', 
      [
        'BASE_DPTR_DATE', 
        'FLT_NBR', 
        'CLASS_OF_SERVICE_CODE', 
        'ALTEA_DMD', 
        'LEG_OD', 
        'BKGS', 
        'BASE_ORIG', 
        'BASE_DEST'
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

Formula_86_0 AS (

  SELECT 
    (ALTEA_DMD - BKGS) AS ALTEA_SPILL,
    *
  
  FROM Cleanse_88 AS in0

),

Join_3_inner AS (

  SELECT 
    in0.* EXCEPT (`BASE_ORIG`, `BASE_DEST`, `CARRIER_CD`),
    in1.* EXCEPT (`LEG_SC_DPTR_DATE`, `LEG_OD`, `LEG_AS_MKTG_FLT_NBR`, `PURCHASED_COS_CODE`)
  
  FROM Formula_86_0 AS in0
  INNER JOIN Join_42_inner AS in1
     ON (
      (
        ((in0.FLT_NBR = in1.LEG_AS_MKTG_FLT_NBR) AND (in0.BASE_DPTR_DATE = in1.LEG_SC_DPTR_DATE))
        AND (in0.LEG_OD = in1.LEG_OD)
      )
      AND (in0.CLASS_OF_SERVICE_CODE = in1.PURCHASED_COS_CODE)
    )

),

Join_3_left AS (

  SELECT in0.*
  
  FROM Formula_86_0 AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.LEG_AS_MKTG_FLT_NBR,
      in1.LEG_SC_DPTR_DATE,
      in1.LEG_OD,
      in1.PURCHASED_COS_CODE
    
    FROM Join_42_inner AS in1
    
    WHERE in1.LEG_AS_MKTG_FLT_NBR IS NOT NULL
          AND in1.LEG_SC_DPTR_DATE IS NOT NULL
          AND in1.LEG_OD IS NOT NULL
          AND in1.PURCHASED_COS_CODE IS NOT NULL
  ) AS in1_keys
     ON (
      (
        ((in0.FLT_NBR = in1_keys.LEG_AS_MKTG_FLT_NBR) AND (in0.BASE_DPTR_DATE = in1_keys.LEG_SC_DPTR_DATE))
        AND (in0.LEG_OD = in1_keys.LEG_OD)
      )
      AND (in0.CLASS_OF_SERVICE_CODE = in1_keys.PURCHASED_COS_CODE)
    )
  
  WHERE (in1_keys.LEG_AS_MKTG_FLT_NBR IS NULL)

),

Join_43_inner AS (

  SELECT 
    in0.* EXCEPT (`BASE_ORIG`, `BASE_DEST`),
    in1.* EXCEPT (`LEG_SC_DPTR_DATE`, `LEG_OD`, `LEG_AS_MKTG_FLT_NBR`, `PURCHASED_COS_CODE`)
  
  FROM Join_3_left AS in0
  INNER JOIN Join_42_inner AS in1
     ON (
      (
        ((in0.FLT_NBR = in1.LEG_AS_MKTG_FLT_NBR) AND (in0.BASE_DPTR_DATE = in1.LEG_SC_DPTR_DATE))
        AND (in0.LEG_OD = in1.LEG_OD)
      )
      AND (in0.CLASS_OF_SERVICE_CODE = in1.L1_COS)
    )

),

Join_43_left AS (

  SELECT in0.*
  
  FROM Join_3_left AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.LEG_AS_MKTG_FLT_NBR,
      in1.LEG_SC_DPTR_DATE,
      in1.LEG_OD,
      in1.L1_COS
    
    FROM Join_42_inner AS in1
    
    WHERE in1.LEG_AS_MKTG_FLT_NBR IS NOT NULL
          AND in1.LEG_SC_DPTR_DATE IS NOT NULL
          AND in1.LEG_OD IS NOT NULL
          AND in1.L1_COS IS NOT NULL
  ) AS in1_keys
     ON (
      (
        ((in0.FLT_NBR = in1_keys.LEG_AS_MKTG_FLT_NBR) AND (in0.BASE_DPTR_DATE = in1_keys.LEG_SC_DPTR_DATE))
        AND (in0.LEG_OD = in1_keys.LEG_OD)
      )
      AND (in0.CLASS_OF_SERVICE_CODE = in1_keys.L1_COS)
    )
  
  WHERE (in1_keys.LEG_AS_MKTG_FLT_NBR IS NULL)

),

Join_44_inner AS (

  SELECT 
    in0.* EXCEPT (`BASE_ORIG`, `BASE_DEST`),
    in1.* EXCEPT (`LEG_SC_DPTR_DATE`, `LEG_OD`, `LEG_AS_MKTG_FLT_NBR`, `PURCHASED_COS_CODE`)
  
  FROM Join_43_left AS in0
  INNER JOIN Join_42_inner AS in1
     ON (
      (
        ((in0.FLT_NBR = in1.LEG_AS_MKTG_FLT_NBR) AND (in0.BASE_DPTR_DATE = in1.LEG_SC_DPTR_DATE))
        AND (in0.LEG_OD = in1.LEG_OD)
      )
      AND (in0.CLASS_OF_SERVICE_CODE = in1.L2_COS)
    )

),

Join_44_left AS (

  SELECT in0.*
  
  FROM Join_43_left AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.LEG_AS_MKTG_FLT_NBR,
      in1.LEG_SC_DPTR_DATE,
      in1.LEG_OD,
      in1.L2_COS
    
    FROM Join_42_inner AS in1
    
    WHERE in1.LEG_AS_MKTG_FLT_NBR IS NOT NULL
          AND in1.LEG_SC_DPTR_DATE IS NOT NULL
          AND in1.LEG_OD IS NOT NULL
          AND in1.L2_COS IS NOT NULL
  ) AS in1_keys
     ON (
      (
        ((in0.FLT_NBR = in1_keys.LEG_AS_MKTG_FLT_NBR) AND (in0.BASE_DPTR_DATE = in1_keys.LEG_SC_DPTR_DATE))
        AND (in0.LEG_OD = in1_keys.LEG_OD)
      )
      AND (in0.CLASS_OF_SERVICE_CODE = in1_keys.L2_COS)
    )
  
  WHERE (in1_keys.LEG_AS_MKTG_FLT_NBR IS NULL)

),

Join_45_inner AS (

  SELECT 
    in0.* EXCEPT (`BASE_ORIG`, `BASE_DEST`),
    in1.* EXCEPT (`LEG_SC_DPTR_DATE`, `LEG_OD`, `LEG_AS_MKTG_FLT_NBR`, `PURCHASED_COS_CODE`)
  
  FROM Join_44_left AS in0
  INNER JOIN Join_42_inner AS in1
     ON (
      (
        ((in0.FLT_NBR = in1.LEG_AS_MKTG_FLT_NBR) AND (in0.BASE_DPTR_DATE = in1.LEG_SC_DPTR_DATE))
        AND (in0.LEG_OD = in1.LEG_OD)
      )
      AND (in0.CLASS_OF_SERVICE_CODE = in1.L3_COS)
    )

),

Join_45_left AS (

  SELECT in0.*
  
  FROM Join_44_left AS in0
  LEFT JOIN (
    SELECT 
      DISTINCT in1.LEG_AS_MKTG_FLT_NBR,
      in1.LEG_SC_DPTR_DATE,
      in1.LEG_OD,
      in1.L3_COS
    
    FROM Join_42_inner AS in1
    
    WHERE in1.LEG_AS_MKTG_FLT_NBR IS NOT NULL
          AND in1.LEG_SC_DPTR_DATE IS NOT NULL
          AND in1.LEG_OD IS NOT NULL
          AND in1.L3_COS IS NOT NULL
  ) AS in1_keys
     ON (
      (
        ((in0.FLT_NBR = in1_keys.LEG_AS_MKTG_FLT_NBR) AND (in0.BASE_DPTR_DATE = in1_keys.LEG_SC_DPTR_DATE))
        AND (in0.LEG_OD = in1_keys.LEG_OD)
      )
      AND (in0.CLASS_OF_SERVICE_CODE = in1_keys.L3_COS)
    )
  
  WHERE (in1_keys.LEG_AS_MKTG_FLT_NBR IS NULL)

),

Summarize_57 AS (

  SELECT 
    MIN(FARE) AS FARE,
    LEG_SC_DPTR_DATE AS LEG_SC_DPTR_DATE,
    LEG_OD AS LEG_OD
  
  FROM Formula_48_0 AS in0
  
  GROUP BY 
    LEG_SC_DPTR_DATE, LEG_OD

),

Join_58_left_UnionLeftOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.BASE_DPTR_DATE = in1.LEG_SC_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))
          THEN NULL
        ELSE in0.BASE_ORIG
      END
    ) AS BASE_ORIG,
    (
      CASE
        WHEN ((in0.BASE_DPTR_DATE = in1.LEG_SC_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))
          THEN NULL
        ELSE in0.BASE_DEST
      END
    ) AS BASE_DEST,
    in0.* EXCEPT (`BASE_ORIG`, `BASE_DEST`),
    in1.* EXCEPT (`LEG_SC_DPTR_DATE`, `LEG_OD`)
  
  FROM Join_45_left AS in0
  LEFT JOIN Summarize_57 AS in1
     ON ((in0.BASE_DPTR_DATE = in1.LEG_SC_DPTR_DATE) AND (in0.LEG_OD = in1.LEG_OD))

),

Union_46 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_43_inner', 'Join_3_inner', 'Join_45_inner', 'Join_44_inner', 'Join_58_left_UnionLeftOuter'], 
      [
        '[{"name": "ALTEA_SPILL", "dataType": "Double"}, {"name": "FLT_NBR", "dataType": "Integer"}, {"name": "LEG_OPER_CARR_AIRLINE_CODE", "dataType": "String"}, {"name": "L1_COS", "dataType": "String"}, {"name": "FARE", "dataType": "Double"}, {"name": "CARRIER_CD", "dataType": "String"}, {"name": "L3_COS", "dataType": "String"}, {"name": "L2_COS", "dataType": "String"}, {"name": "ALTEA_DMD", "dataType": "Double"}, {"name": "LEG_OD", "dataType": "String"}, {"name": "BASE_DPTR_DATE", "dataType": "Timestamp"}, {"name": "BKGS", "dataType": "Double"}, {"name": "CLASS_OF_SERVICE_CODE", "dataType": "String"}]', 
        '[{"name": "ALTEA_SPILL", "dataType": "Double"}, {"name": "FLT_NBR", "dataType": "Integer"}, {"name": "LEG_OPER_CARR_AIRLINE_CODE", "dataType": "String"}, {"name": "L1_COS", "dataType": "String"}, {"name": "FARE", "dataType": "Double"}, {"name": "L3_COS", "dataType": "String"}, {"name": "L2_COS", "dataType": "String"}, {"name": "ALTEA_DMD", "dataType": "Double"}, {"name": "LEG_OD", "dataType": "String"}, {"name": "BASE_DPTR_DATE", "dataType": "Timestamp"}, {"name": "BKGS", "dataType": "Double"}, {"name": "CLASS_OF_SERVICE_CODE", "dataType": "String"}]', 
        '[{"name": "ALTEA_SPILL", "dataType": "Double"}, {"name": "FLT_NBR", "dataType": "Integer"}, {"name": "LEG_OPER_CARR_AIRLINE_CODE", "dataType": "String"}, {"name": "L1_COS", "dataType": "String"}, {"name": "FARE", "dataType": "Double"}, {"name": "CARRIER_CD", "dataType": "String"}, {"name": "L3_COS", "dataType": "String"}, {"name": "L2_COS", "dataType": "String"}, {"name": "ALTEA_DMD", "dataType": "Double"}, {"name": "LEG_OD", "dataType": "String"}, {"name": "BASE_DPTR_DATE", "dataType": "Timestamp"}, {"name": "BKGS", "dataType": "Double"}, {"name": "CLASS_OF_SERVICE_CODE", "dataType": "String"}]', 
        '[{"name": "ALTEA_SPILL", "dataType": "Double"}, {"name": "FLT_NBR", "dataType": "Integer"}, {"name": "LEG_OPER_CARR_AIRLINE_CODE", "dataType": "String"}, {"name": "L1_COS", "dataType": "String"}, {"name": "FARE", "dataType": "Double"}, {"name": "CARRIER_CD", "dataType": "String"}, {"name": "L3_COS", "dataType": "String"}, {"name": "L2_COS", "dataType": "String"}, {"name": "ALTEA_DMD", "dataType": "Double"}, {"name": "LEG_OD", "dataType": "String"}, {"name": "BASE_DPTR_DATE", "dataType": "Timestamp"}, {"name": "BKGS", "dataType": "Double"}, {"name": "CLASS_OF_SERVICE_CODE", "dataType": "String"}]', 
        '[{"name": "ALTEA_SPILL", "dataType": "Double"}, {"name": "FLT_NBR", "dataType": "Integer"}, {"name": "BASE_DEST", "dataType": "String"}, {"name": "FARE", "dataType": "Double"}, {"name": "CARRIER_CD", "dataType": "String"}, {"name": "ALTEA_DMD", "dataType": "Double"}, {"name": "BASE_ORIG", "dataType": "String"}, {"name": "LEG_OD", "dataType": "String"}, {"name": "BASE_DPTR_DATE", "dataType": "Timestamp"}, {"name": "BKGS", "dataType": "Double"}, {"name": "CLASS_OF_SERVICE_CODE", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Cleanse_38 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Union_46'], 
      [
        { "name": "ALTEA_SPILL", "dataType": "Double" }, 
        { "name": "FLT_NBR", "dataType": "Integer" }, 
        { "name": "BASE_DEST", "dataType": "String" }, 
        { "name": "LEG_OPER_CARR_AIRLINE_CODE", "dataType": "String" }, 
        { "name": "L1_COS", "dataType": "String" }, 
        { "name": "FARE", "dataType": "Double" }, 
        { "name": "CARRIER_CD", "dataType": "String" }, 
        { "name": "L3_COS", "dataType": "String" }, 
        { "name": "L2_COS", "dataType": "String" }, 
        { "name": "ALTEA_DMD", "dataType": "Double" }, 
        { "name": "BASE_ORIG", "dataType": "String" }, 
        { "name": "LEG_OD", "dataType": "String" }, 
        { "name": "BASE_DPTR_DATE", "dataType": "Timestamp" }, 
        { "name": "BKGS", "dataType": "Double" }, 
        { "name": "CLASS_OF_SERVICE_CODE", "dataType": "String" }
      ], 
      'keepOriginal', 
      [
        'BASE_DPTR_DATE', 
        'FLT_NBR', 
        'CLASS_OF_SERVICE_CODE', 
        'ALTEA_DMD', 
        'LEG_OD', 
        'L1_COS', 
        'L2_COS', 
        'L3_COS', 
        'LEG_OPER_CARR_AIRLINE_CODE', 
        'FARE', 
        'BASE_ORIG', 
        'BASE_DEST'
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

Formula_7_0 AS (

  SELECT 
    (ALTEA_SPILL * FARE) AS SPILL_REV,
    (BKGS * FARE) AS EST_REV,
    CAST((
      CASE
        WHEN (CAST(CLASS_OF_SERVICE_CODE AS STRING) IN ('C', 'D', 'E', 'I', 'J', 'U'))
          THEN 'J'
        ELSE 'Y'
      END
    ) AS STRING) AS CABIN,
    (
      CASE
        WHEN (ALTEA_SPILL < 0)
          THEN 0
        ELSE ALTEA_SPILL
      END
    ) AS POSIT_ALTEA_SPILL,
    *
  
  FROM Cleanse_38 AS in0

)

SELECT *

FROM Formula_7_0
