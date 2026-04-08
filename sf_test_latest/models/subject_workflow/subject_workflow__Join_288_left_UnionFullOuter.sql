{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_208_0 AS (

  SELECT *
  
  FROM {{ ref('subject_workflow__Formula_208_0')}}

),

AlteryxSelect_213 AS (

  SELECT *
  
  FROM {{ ref('subject_workflow__AlteryxSelect_213')}}

),

Join_210_left AS (

  SELECT in0.*
  
  FROM Formula_208_0 AS in0
  LEFT JOIN AlteryxSelect_213 AS in1
     ON ((in0.SUBJECT_ID = in1.SUBJECT_ID) AND (in0.STUDY_ID = in1.STUDY_ID))

),

Filter_306 AS (

  SELECT * 
  
  FROM Join_210_left AS in0
  
  WHERE (
          (
            NOT(
              SUBJ_STATUS = 'DELETED')
          ) OR (SUBJ_STATUS IS NULL)
        )

),

Filter_308 AS (

  SELECT * 
  
  FROM Filter_306 AS in0
  
  WHERE (NOT(SUBJ_ENTERED_ACTIVE_TRNMT_DT IS NULL))

),

Filter_315 AS (

  SELECT * 
  
  FROM Filter_308 AS in0
  
  WHERE (SRC_SYS_TYPE = 'EDC')

),

Sample_310 AS (

  {{ prophecy_basics.Sample('Filter_315', ['STUDY_ID', 'SUBJECT_ID', 'SRC_SYS_TYPE'], 1002, 'firstN', 1) }}

),

Filter_295 AS (

  SELECT * 
  
  FROM Filter_306 AS in0
  
  WHERE (NOT(SUBJ_ENROLLED_STUDY_DT IS NULL))

),

Filter_302 AS (

  SELECT * 
  
  FROM Filter_295 AS in0
  
  WHERE (SRC_SYS_TYPE = 'IRT')

),

Filter_252 AS (

  SELECT * 
  
  FROM Filter_306 AS in0
  
  WHERE (SRC_SYS_TYPE = 'EDC')

),

Summarize_253 AS (

  SELECT 
    COUNT(DISTINCT STUDY_SITE_NUMBER) AS COUNTDISTINCT_STUDY_SITE_NUMBER,
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID
  
  FROM Filter_252 AS in0
  
  GROUP BY 
    STUDY_ID, SUBJECT_ID

),

Filter_254 AS (

  SELECT * 
  
  FROM Summarize_253 AS in0
  
  WHERE (COUNTDISTINCT_STUDY_SITE_NUMBER > 1)

),

Join_255_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("STUDY_ID", "SUBJECT_ID")
  
  FROM Filter_252 AS in0
  INNER JOIN Filter_254 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

Transpose_256_schemaTransform_0 AS (

  SELECT * EXCLUDE ("CREATION_TS", 
         "END_TS", 
         "SUBJECT_SCREEN_FAILURE_DT", 
         "SBJ_STR_STRATA_NUM", 
         "SBJ_COH_COHORT_NUM", 
         "SBJ_STRATA_CODE_VALUE_DESC", 
         "SBJ_COH_COHORT_DESC", 
         "COUNTDISTINCT_STUDY_SITE_NUMBER")
  
  FROM Join_255_inner AS in0

),

Transpose_256 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_256_schemaTransform_0'], 
      ['SUBJECT_ID', 'STUDY_SITE_NUMBER', 'STUDY_ID'], 
      ['SUBJ_COMPLETED_STUDY_DT', 'SUBJ_DISCONTINUED_DT'], 
      'NAME', 
      '"VALUE"', 
      [
        'STUDY_SITE_NUMBER', 
        'SUBJ_REASON_FOR_WITHDRAWAL', 
        'SUBJ_STATUS', 
        'SUBJ_WITHDRAWAL_TIMEFRAME', 
        'LEGACY_SUBJ_STATUS', 
        'SUBJ_ETHNICITY', 
        'SUBJ_GENDER', 
        'SUBJ_RACE', 
        'SRC_SYS_NAME', 
        'SRC_SYS_TYPE', 
        'SITE_ID', 
        'DELETE_FLAG', 
        'SUBJ_COMPLETED_STUDY_DT', 
        'STUDY_ID', 
        'SUBJ_ENROLLED_STUDY_DT', 
        'LOAD_DATE', 
        'SUBJ_ENTERED_ACTIVE_TRNMT_DT', 
        'ORIGINAL_SUBJ_STATUS', 
        'EMERGENCY_BRK_BLIND_REQTD', 
        'SUBJECT_ID', 
        'SUBJ_DISCONTINUED_DT', 
        'EMERGENCY_BRK_BLIND_PROVIDED', 
        'SDR_REQUIRED'
      ], 
      true
    )
  }}

),

Summarize_257 AS (

  SELECT 
    *,
    count(CASE
      WHEN CAST("VALUE" IS NULL AS boolean)
        THEN ''
      ELSE "VALUE"
    END) OVER (PARTITION BY STUDY_ID, SUBJECT_ID ORDER BY 1 ASC NULLS FIRST) AS COUNTNONNULL_VALUE
  
  FROM Transpose_256 AS in0

),

Filter_270 AS (

  SELECT * 
  
  FROM Filter_306 AS in0
  
  WHERE (SRC_SYS_TYPE = 'IRT')

),

Summarize_271 AS (

  SELECT 
    COUNT(DISTINCT STUDY_SITE_NUMBER) AS COUNTDISTINCT_STUDY_SITE_NUMBER,
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID
  
  FROM Filter_270 AS in0
  
  GROUP BY 
    STUDY_ID, SUBJECT_ID

),

Filter_272 AS (

  SELECT * 
  
  FROM Summarize_271 AS in0
  
  WHERE (COUNTDISTINCT_STUDY_SITE_NUMBER > 1)

),

Join_273_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("STUDY_ID", "SUBJECT_ID")
  
  FROM Filter_270 AS in0
  INNER JOIN Filter_272 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

Transpose_274_schemaTransform_0 AS (

  SELECT * EXCLUDE ("CREATION_TS", 
         "END_TS", 
         "SUBJECT_SCREEN_FAILURE_DT", 
         "SBJ_STR_STRATA_NUM", 
         "SBJ_COH_COHORT_NUM", 
         "SBJ_STRATA_CODE_VALUE_DESC", 
         "SBJ_COH_COHORT_DESC", 
         "COUNTDISTINCT_STUDY_SITE_NUMBER")
  
  FROM Join_273_inner AS in0

),

Transpose_274 AS (

  {{
    prophecy_basics.Transpose(
      ['Transpose_274_schemaTransform_0'], 
      ['SUBJECT_ID', 'STUDY_SITE_NUMBER', 'STUDY_ID'], 
      ['SUBJ_COMPLETED_STUDY_DT', 'SUBJ_DISCONTINUED_DT'], 
      'NAME', 
      '"VALUE"', 
      [
        'STUDY_SITE_NUMBER', 
        'SUBJ_REASON_FOR_WITHDRAWAL', 
        'SUBJ_STATUS', 
        'SUBJ_WITHDRAWAL_TIMEFRAME', 
        'LEGACY_SUBJ_STATUS', 
        'SUBJ_ETHNICITY', 
        'SUBJ_GENDER', 
        'SUBJ_RACE', 
        'SRC_SYS_NAME', 
        'SRC_SYS_TYPE', 
        'SITE_ID', 
        'DELETE_FLAG', 
        'SUBJ_COMPLETED_STUDY_DT', 
        'STUDY_ID', 
        'SUBJ_ENROLLED_STUDY_DT', 
        'LOAD_DATE', 
        'SUBJ_ENTERED_ACTIVE_TRNMT_DT', 
        'ORIGINAL_SUBJ_STATUS', 
        'EMERGENCY_BRK_BLIND_REQTD', 
        'SUBJECT_ID', 
        'SUBJ_DISCONTINUED_DT', 
        'EMERGENCY_BRK_BLIND_PROVIDED', 
        'SDR_REQUIRED'
      ], 
      true
    )
  }}

),

Summarize_275 AS (

  SELECT 
    *,
    count(CASE
      WHEN CAST("VALUE" IS NULL AS boolean)
        THEN ''
      ELSE "VALUE"
    END) OVER (PARTITION BY STUDY_ID, SUBJECT_ID ORDER BY 1 ASC NULLS FIRST) AS COUNTNONNULL_VALUE
  
  FROM Transpose_274 AS in0

),

Join_276_inner_formula AS (

  SELECT * 
  
  FROM Summarize_275 AS in0

),

Filter_277 AS (

  SELECT * 
  
  FROM Join_276_inner_formula AS in0
  
  WHERE (COUNTNONNULL_VALUE > 0)

),

Sample_280 AS (

  {{ prophecy_basics.Sample('Filter_277', ['STUDY_ID', 'SUBJECT_ID'], 1002, 'firstN', 1) }}

),

Join_273_left AS (

  SELECT in0.*
  
  FROM Filter_270 AS in0
  LEFT JOIN Filter_272 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

Sample_279 AS (

  {{ prophecy_basics.Sample('Join_273_left', ['STUDY_ID', 'SUBJECT_ID'], 1002, 'firstN', 1) }}

),

Filter_277_reject AS (

  SELECT * 
  
  FROM Join_276_inner_formula AS in0
  
  WHERE (
          (
            NOT(
              COUNTNONNULL_VALUE > 0)
          ) OR ((COUNTNONNULL_VALUE > 0) IS NULL)
        )

),

Formula_281_0 AS (

  SELECT 
    (
      CASE
        WHEN (
          ((STUDY_SITE_NUMBER IS NULL) OR ((SUBSTRING(SUBJECT_ID, 1, 4)) IS NULL))
          OR (STUDY_SITE_NUMBER <> (SUBSTRING(SUBJECT_ID, 1, 4)))
        )
          THEN 1
        ELSE 0
      END
    ) AS HIERARCHY,
    *
  
  FROM Filter_277_reject AS in0

),

Sample_282 AS (

  {{ prophecy_basics.Sample('Formula_281_0', ['STUDY_ID', 'SUBJECT_ID'], 1002, 'firstN', 1) }}

),

Union_283 AS (

  {{
    prophecy_basics.UnionByName(
      ['Sample_279', 'Sample_280', 'Sample_282'], 
      [
        '[{"name": "STUDY_SITE_NUMBER", "dataType": "String"}, {"name": "SUBJ_REASON_FOR_WITHDRAWAL", "dataType": "String"}, {"name": "SUBJ_STATUS", "dataType": "String"}, {"name": "SUBJ_WITHDRAWAL_TIMEFRAME", "dataType": "String"}, {"name": "SUBJECT_SCREEN_FAILURE_DT", "dataType": "Timestamp"}, {"name": "SBJ_STR_STRATA_NUM", "dataType": "String"}, {"name": "SBJ_COH_COHORT_NUM", "dataType": "Float"}, {"name": "SBJ_STRATA_CODE_VALUE_DESC", "dataType": "String"}, {"name": "SBJ_COH_COHORT_DESC", "dataType": "String"}, {"name": "LEGACY_SUBJ_STATUS", "dataType": "String"}, {"name": "SUBJ_ETHNICITY", "dataType": "String"}, {"name": "SUBJ_GENDER", "dataType": "String"}, {"name": "SUBJ_RACE", "dataType": "String"}, {"name": "SRC_SYS_NAME", "dataType": "String"}, {"name": "SRC_SYS_TYPE", "dataType": "String"}, {"name": "SITE_ID", "dataType": "String"}, {"name": "CREATION_TS", "dataType": "Timestamp"}, {"name": "END_TS", "dataType": "Timestamp"}, {"name": "DELETE_FLAG", "dataType": "String"}, {"name": "SUBJ_COMPLETED_STUDY_DT", "dataType": "Timestamp"}, {"name": "STUDY_ID", "dataType": "String"}, {"name": "SUBJ_ENROLLED_STUDY_DT", "dataType": "Timestamp"}, {"name": "LOAD_DATE", "dataType": "Timestamp"}, {"name": "SUBJ_ENTERED_ACTIVE_TRNMT_DT", "dataType": "Timestamp"}, {"name": "ORIGINAL_SUBJ_STATUS", "dataType": "String"}, {"name": "EMERGENCY_BRK_BLIND_REQTD", "dataType": "Timestamp"}, {"name": "SUBJECT_ID", "dataType": "String"}, {"name": "SUBJ_DISCONTINUED_DT", "dataType": "Timestamp"}, {"name": "EMERGENCY_BRK_BLIND_PROVIDED", "dataType": "Timestamp"}, {"name": "SDR_REQUIRED", "dataType": "String"}]', 
        '[{"name": "SUBJECT_ID", "dataType": "String"}, {"name": "STUDY_SITE_NUMBER", "dataType": "String"}, {"name": "STUDY_ID", "dataType": "String"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "COUNTNONNULL_VALUE", "dataType": "Number"}]', 
        '[{"name": "HIERARCHY", "dataType": "Number"}, {"name": "SUBJECT_ID", "dataType": "String"}, {"name": "STUDY_SITE_NUMBER", "dataType": "String"}, {"name": "STUDY_ID", "dataType": "String"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "COUNTNONNULL_VALUE", "dataType": "Number"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_285 AS (

  SELECT 
    SUBJECT_ID AS SUBJECT_ID,
    STUDY_SITE_NUMBER AS SITE_NUMBER_IRT,
    STUDY_ID AS STUDY_ID
  
  FROM Union_283 AS in0

),

Join_258_inner_formula AS (

  SELECT * 
  
  FROM Summarize_257 AS in0

),

Filter_259 AS (

  SELECT * 
  
  FROM Join_258_inner_formula AS in0
  
  WHERE (COUNTNONNULL_VALUE > 0)

),

Sample_262 AS (

  {{ prophecy_basics.Sample('Filter_259', ['STUDY_ID', 'SUBJECT_ID'], 1002, 'firstN', 1) }}

),

Filter_259_reject AS (

  SELECT * 
  
  FROM Join_258_inner_formula AS in0
  
  WHERE (
          (
            NOT(
              COUNTNONNULL_VALUE > 0)
          ) OR ((COUNTNONNULL_VALUE > 0) IS NULL)
        )

),

Formula_263_0 AS (

  SELECT 
    (
      CASE
        WHEN (
          ((STUDY_SITE_NUMBER IS NULL) OR ((SUBSTRING(SUBJECT_ID, 1, 4)) IS NULL))
          OR (STUDY_SITE_NUMBER <> (SUBSTRING(SUBJECT_ID, 1, 4)))
        )
          THEN 1
        ELSE 0
      END
    ) AS HIERARCHY,
    *
  
  FROM Filter_259_reject AS in0

),

Sample_264 AS (

  {{ prophecy_basics.Sample('Formula_263_0', ['STUDY_ID', 'SUBJECT_ID'], 1002, 'firstN', 1) }}

),

Join_255_left AS (

  SELECT in0.*
  
  FROM Filter_252 AS in0
  LEFT JOIN Filter_254 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

Sample_261 AS (

  {{ prophecy_basics.Sample('Join_255_left', ['STUDY_ID', 'SUBJECT_ID'], 1002, 'firstN', 1) }}

),

Union_265 AS (

  {{
    prophecy_basics.UnionByName(
      ['Sample_261', 'Sample_262', 'Sample_264'], 
      [
        '[{"name": "STUDY_SITE_NUMBER", "dataType": "String"}, {"name": "SUBJ_REASON_FOR_WITHDRAWAL", "dataType": "String"}, {"name": "SUBJ_STATUS", "dataType": "String"}, {"name": "SUBJ_WITHDRAWAL_TIMEFRAME", "dataType": "String"}, {"name": "SUBJECT_SCREEN_FAILURE_DT", "dataType": "Timestamp"}, {"name": "SBJ_STR_STRATA_NUM", "dataType": "String"}, {"name": "SBJ_COH_COHORT_NUM", "dataType": "Float"}, {"name": "SBJ_STRATA_CODE_VALUE_DESC", "dataType": "String"}, {"name": "SBJ_COH_COHORT_DESC", "dataType": "String"}, {"name": "LEGACY_SUBJ_STATUS", "dataType": "String"}, {"name": "SUBJ_ETHNICITY", "dataType": "String"}, {"name": "SUBJ_GENDER", "dataType": "String"}, {"name": "SUBJ_RACE", "dataType": "String"}, {"name": "SRC_SYS_NAME", "dataType": "String"}, {"name": "SRC_SYS_TYPE", "dataType": "String"}, {"name": "SITE_ID", "dataType": "String"}, {"name": "CREATION_TS", "dataType": "Timestamp"}, {"name": "END_TS", "dataType": "Timestamp"}, {"name": "DELETE_FLAG", "dataType": "String"}, {"name": "SUBJ_COMPLETED_STUDY_DT", "dataType": "Timestamp"}, {"name": "STUDY_ID", "dataType": "String"}, {"name": "SUBJ_ENROLLED_STUDY_DT", "dataType": "Timestamp"}, {"name": "LOAD_DATE", "dataType": "Timestamp"}, {"name": "SUBJ_ENTERED_ACTIVE_TRNMT_DT", "dataType": "Timestamp"}, {"name": "ORIGINAL_SUBJ_STATUS", "dataType": "String"}, {"name": "EMERGENCY_BRK_BLIND_REQTD", "dataType": "Timestamp"}, {"name": "SUBJECT_ID", "dataType": "String"}, {"name": "SUBJ_DISCONTINUED_DT", "dataType": "Timestamp"}, {"name": "EMERGENCY_BRK_BLIND_PROVIDED", "dataType": "Timestamp"}, {"name": "SDR_REQUIRED", "dataType": "String"}]', 
        '[{"name": "SUBJECT_ID", "dataType": "String"}, {"name": "STUDY_SITE_NUMBER", "dataType": "String"}, {"name": "STUDY_ID", "dataType": "String"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "COUNTNONNULL_VALUE", "dataType": "Number"}]', 
        '[{"name": "HIERARCHY", "dataType": "Number"}, {"name": "SUBJECT_ID", "dataType": "String"}, {"name": "STUDY_SITE_NUMBER", "dataType": "String"}, {"name": "STUDY_ID", "dataType": "String"}, {"name": "NAME", "dataType": "String"}, {"name": "VALUE", "dataType": "String"}, {"name": "COUNTNONNULL_VALUE", "dataType": "Number"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_267 AS (

  SELECT 
    SUBJECT_ID AS SUBJECT_ID,
    STUDY_SITE_NUMBER AS SITE_NUMBER_EDC,
    STUDY_ID AS STUDY_ID
  
  FROM Union_265 AS in0

),

Join_240_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.STUDY_ID
      END
    ) AS STUDY_ID,
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.SUBJECT_ID
      END
    ) AS SUBJECT_ID,
    in0.* EXCLUDE ("STUDY_ID", "SUBJECT_ID"),
    in1.* EXCLUDE ("STUDY_ID", "SUBJECT_ID")
  
  FROM AlteryxSelect_285 AS in0
  FULL JOIN AlteryxSelect_267 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

Unique_248 AS (

  SELECT * 
  
  FROM Join_240_left_UnionFullOuter AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY SUBJECT_ID, STUDY_ID ORDER BY SUBJECT_ID, STUDY_ID) = 1

),

Filter_245_to_Filter_247 AS (

  SELECT * 
  
  FROM Unique_248 AS in0
  
  WHERE (
          (
            (
              (
                ((LENGTH(SUBJECT_ID)) = 8)
                AND (
                      (
                        NOT(
                          STUDY_ID = 'C5041010')
                      ) OR (STUDY_ID IS NULL)
                    )
              )
              AND ((LENGTH(CAST((REGEXP_SUBSTR(SUBJECT_ID, '\\d*')) AS STRING))) > 0)
            )
            AND (NOT((coalesce(LOWER(SUBJECT_ID), '')) LIKE (CONCAT(LOWER('0'), '%'))))
          )
          AND (NOT((coalesce(LOWER(SUBJECT_ID), '')) LIKE (CONCAT(LOWER('999'), '%'))))
        )

),

Formula_246_to_Formula_250_0 AS (

  SELECT 
    CAST((SUBSTRING(SUBJECT_ID, 1, 4)) AS STRING) AS ORIGINAL_SITE_NUMBER,
    *
  
  FROM Filter_245_to_Filter_247 AS in0

),

Formula_246_to_Formula_250_1 AS (

  SELECT 
    (
      CASE
        WHEN ((STUDY_ID = 'C4591001') AND (ORIGINAL_SITE_NUMBER = '4444'))
          THEN '1231'
        ELSE ORIGINAL_SITE_NUMBER
      END
    ) AS ORIGINAL_SITE_NUMBER,
    * EXCLUDE ("ORIGINAL_SITE_NUMBER")
  
  FROM Formula_246_to_Formula_250_0 AS in0

),

AlteryxSelect_249 AS (

  SELECT * EXCLUDE ("SITE_NUMBER_IRT", "SITE_NUMBER_EDC")
  
  FROM Formula_246_to_Formula_250_1 AS in0

),

aka_GPDIP_EDLUD_368 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('subject_workflow', 'aka_GPDIP_EDLUD_368') }}

),

Join_367_inner AS (

  SELECT 
    in0.*,
    in1.* EXCLUDE ("STUDY_ID", "STUDY_SITE_NUMBER")
  
  FROM AlteryxSelect_249 AS in0
  INNER JOIN aka_GPDIP_EDLUD_368 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.ORIGINAL_SITE_NUMBER = in1.STUDY_SITE_NUMBER))

),

Join_242_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.STUDY_ID
      END
    ) AS STUDY_ID,
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.SUBJECT_ID
      END
    ) AS SUBJECT_ID,
    in0.* EXCLUDE ("STUDY_ID", "SUBJECT_ID"),
    in1.* EXCLUDE ("SUBJECT_ID", "STUDY_ID")
  
  FROM Join_240_left_UnionFullOuter AS in0
  FULL JOIN Join_367_inner AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

Filter_319 AS (

  SELECT * 
  
  FROM Filter_306 AS in0
  
  WHERE (NOT(SUBJ_COMPLETED_STUDY_DT IS NULL))

),

Filter_325 AS (

  SELECT * 
  
  FROM Filter_319 AS in0
  
  WHERE (SRC_SYS_TYPE = 'IRT')

),

Sample_320 AS (

  {{ prophecy_basics.Sample('Filter_325', ['STUDY_ID', 'SUBJECT_ID', 'SRC_SYS_TYPE'], 1002, 'firstN', 1) }}

),

AlteryxSelect_323 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_COMPLETED_STUDY_DT AS SUBJ_COMPLETED_STUDY_DT_IRT
  
  FROM Sample_320 AS in0

),

Filter_326 AS (

  SELECT * 
  
  FROM Filter_319 AS in0
  
  WHERE (SRC_SYS_TYPE = 'EDC')

),

Sample_321 AS (

  {{ prophecy_basics.Sample('Filter_326', ['STUDY_ID', 'SUBJECT_ID', 'SRC_SYS_TYPE'], 1002, 'firstN', 1) }}

),

AlteryxSelect_327 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_COMPLETED_STUDY_DT AS SUBJ_COMPLETED_STUDY_DT_EDC
  
  FROM Sample_321 AS in0

),

Join_324_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.STUDY_ID
      END
    ) AS STUDY_ID,
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.SUBJECT_ID
      END
    ) AS SUBJECT_ID,
    in0.* EXCLUDE ("STUDY_ID", "SUBJECT_ID"),
    in1.* EXCLUDE ("STUDY_ID", "SUBJECT_ID")
  
  FROM AlteryxSelect_327 AS in0
  FULL JOIN AlteryxSelect_323 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

Filter_330 AS (

  SELECT * 
  
  FROM Filter_306 AS in0
  
  WHERE (NOT(SUBJ_DISCONTINUED_DT IS NULL))

),

Filter_337 AS (

  SELECT * 
  
  FROM Filter_330 AS in0
  
  WHERE (SRC_SYS_TYPE = 'EDC')

),

Sample_332 AS (

  {{ prophecy_basics.Sample('Filter_337', ['STUDY_ID', 'SUBJECT_ID', 'SRC_SYS_TYPE'], 1002, 'firstN', 1) }}

),

AlteryxSelect_338 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_DISCONTINUED_DT AS SUBJ_DISCONTINUED_DT_EDC,
    SUBJ_REASON_FOR_WITHDRAWAL AS SUBJ_REASON_FOR_WITHDRAWAL_EDC,
    SUBJ_WITHDRAWAL_TIMEFRAME AS SUBJ_WITHDRAWAL_TIMEFRAME_EDC
  
  FROM Sample_332 AS in0

),

Filter_336 AS (

  SELECT * 
  
  FROM Filter_330 AS in0
  
  WHERE (SRC_SYS_TYPE = 'IRT')

),

Sample_331 AS (

  {{ prophecy_basics.Sample('Filter_336', ['STUDY_ID', 'SUBJECT_ID', 'SRC_SYS_TYPE'], 1002, 'firstN', 1) }}

),

AlteryxSelect_334 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_DISCONTINUED_DT AS SUBJ_DISCONTINUED_DT_IRT,
    SUBJ_REASON_FOR_WITHDRAWAL AS SUBJ_REASON_FOR_WITHDRAWAL_IRT,
    SUBJ_WITHDRAWAL_TIMEFRAME AS SUBJ_WITHDRAWAL_TIMEFRAME_IRT
  
  FROM Sample_331 AS in0

),

Join_335_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.STUDY_ID
      END
    ) AS STUDY_ID,
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.SUBJECT_ID
      END
    ) AS SUBJECT_ID,
    in0.* EXCLUDE ("STUDY_ID", "SUBJECT_ID"),
    in1.* EXCLUDE ("STUDY_ID", "SUBJECT_ID")
  
  FROM AlteryxSelect_338 AS in0
  FULL JOIN AlteryxSelect_334 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

AlteryxSelect_232 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID
  
  FROM Filter_306 AS in0

),

Unique_233 AS (

  SELECT * 
  
  FROM AlteryxSelect_232 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY STUDY_ID, SUBJECT_ID ORDER BY STUDY_ID, SUBJECT_ID) = 1

),

AlteryxSelect_316 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_ENTERED_ACTIVE_TRNMT_DT AS SUBJ_ENTERED_ACTIVE_TRNMT_DT_EDC
  
  FROM Sample_310 AS in0

),

Filter_314 AS (

  SELECT * 
  
  FROM Filter_308 AS in0
  
  WHERE (SRC_SYS_TYPE = 'IRT')

),

Sample_309 AS (

  {{ prophecy_basics.Sample('Filter_314', ['STUDY_ID', 'SUBJECT_ID', 'SRC_SYS_TYPE'], 1002, 'firstN', 1) }}

),

AlteryxSelect_312 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_ENTERED_ACTIVE_TRNMT_DT AS SUBJ_ENTERED_ACTIVE_TRNMT_DT_IRT
  
  FROM Sample_309 AS in0

),

Join_313_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.STUDY_ID
      END
    ) AS STUDY_ID,
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.SUBJECT_ID
      END
    ) AS SUBJECT_ID,
    in0.* EXCLUDE ("STUDY_ID", "SUBJECT_ID"),
    in1.* EXCLUDE ("STUDY_ID", "SUBJECT_ID")
  
  FROM AlteryxSelect_316 AS in0
  FULL JOIN AlteryxSelect_312 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

Filter_237 AS (

  SELECT * 
  
  FROM Filter_306 AS in0
  
  WHERE (NOT(SUBJ_STATUS IS NULL))

),

Filter_151 AS (

  SELECT * 
  
  FROM Filter_237 AS in0
  
  WHERE (SRC_SYS_TYPE = 'EDC')

),

Sample_159 AS (

  {{ prophecy_basics.Sample('Filter_151', ['STUDY_ID', 'SUBJECT_ID', 'SRC_SYS_TYPE'], 1002, 'firstN', 1) }}

),

AlteryxSelect_149 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_STATUS AS SUBJ_STATUS_EDC
  
  FROM Sample_159 AS in0

),

Filter_152 AS (

  SELECT * 
  
  FROM Filter_237 AS in0
  
  WHERE (SRC_SYS_TYPE = 'IRT')

),

Sample_160 AS (

  {{ prophecy_basics.Sample('Filter_152', ['STUDY_ID', 'SUBJECT_ID', 'SRC_SYS_TYPE'], 1002, 'firstN', 1) }}

),

AlteryxSelect_154 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_STATUS AS SUBJ_STATUS_IRT
  
  FROM Sample_160 AS in0

),

Join_153_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.STUDY_ID
      END
    ) AS STUDY_ID,
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.SUBJECT_ID
      END
    ) AS SUBJECT_ID,
    in0.* EXCLUDE ("STUDY_ID", "SUBJECT_ID"),
    in1.* EXCLUDE ("STUDY_ID", "SUBJECT_ID")
  
  FROM AlteryxSelect_149 AS in0
  FULL JOIN AlteryxSelect_154 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

Filter_31_to_Filter_142 AS (

  SELECT * 
  
  FROM Filter_306 AS in0
  
  WHERE (
          (SRC_SYS_TYPE = 'IRT')
          AND (
                (
                  (NOT(SBJ_STR_STRATA_NUM IS NULL))
                  OR ((NOT(SBJ_COH_COHORT_NUM IS NULL)) OR (NOT(SBJ_STRATA_CODE_VALUE_DESC IS NULL)))
                )
                OR (
                     ((NOT(SBJ_COH_COHORT_DESC IS NULL)) OR (NOT(SUBJ_RACE IS NULL)))
                     OR ((NOT(SUBJ_ETHNICITY IS NULL)) OR (NOT(SUBJ_GENDER IS NULL)))
                   )
              )
        )

),

AlteryxSelect_32 AS (

  SELECT 
    SUBJECT_ID AS SUBJECT_ID,
    STUDY_ID AS STUDY_ID,
    SBJ_STR_STRATA_NUM AS SBJ_STR_STRATA_NUM,
    CAST(SBJ_COH_COHORT_NUM AS INTEGER) AS SBJ_COH_COHORT_NUM,
    SBJ_STRATA_CODE_VALUE_DESC AS SBJ_STRATA_CODE_VALUE_DESC,
    SBJ_COH_COHORT_DESC AS SBJ_COH_COHORT_DESC,
    SUBJ_RACE AS SUBJ_RACE,
    SUBJ_ETHNICITY AS SUBJ_ETHNICITY,
    SUBJ_GENDER AS SUBJ_GENDER
  
  FROM Filter_31_to_Filter_142 AS in0

),

Sample_143 AS (

  {{ prophecy_basics.Sample('AlteryxSelect_32', ['SUBJECT_ID', 'STUDY_ID'], 1002, 'firstN', 1) }}

),

Sample_297 AS (

  {{ prophecy_basics.Sample('Filter_302', ['STUDY_ID', 'SUBJECT_ID', 'SRC_SYS_TYPE'], 1002, 'firstN', 1) }}

),

AlteryxSelect_300 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_ENROLLED_STUDY_DT AS SUBJ_ENROLLED_STUDY_DT_IRT
  
  FROM Sample_297 AS in0

),

Filter_303 AS (

  SELECT * 
  
  FROM Filter_295 AS in0
  
  WHERE (SRC_SYS_TYPE = 'EDC')

),

Sample_298 AS (

  {{ prophecy_basics.Sample('Filter_303', ['STUDY_ID', 'SUBJECT_ID', 'SRC_SYS_TYPE'], 1002, 'firstN', 1) }}

),

AlteryxSelect_304 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_ENROLLED_STUDY_DT AS SUBJ_ENROLLED_STUDY_DT_EDC
  
  FROM Sample_298 AS in0

),

Join_301_left_UnionFullOuter AS (

  SELECT 
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.STUDY_ID
      END
    ) AS STUDY_ID,
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.SUBJECT_ID
      END
    ) AS SUBJECT_ID,
    in0.* EXCLUDE ("STUDY_ID", "SUBJECT_ID"),
    in1.* EXCLUDE ("STUDY_ID", "SUBJECT_ID")
  
  FROM AlteryxSelect_304 AS in0
  FULL JOIN AlteryxSelect_300 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

),

JoinMultiple_341 AS (

  SELECT 
    in1.SUBJ_STATUS_IRT AS SUBJ_STATUS_IRT,
    in6.SBJ_STR_STRATA_NUM AS SBJ_STR_STRATA_NUM,
    in5.SUBJ_REASON_FOR_WITHDRAWAL_EDC AS SUBJ_REASON_FOR_WITHDRAWAL_EDC,
    in5.SUBJ_DISCONTINUED_DT_IRT AS SUBJ_DISCONTINUED_DT_IRT,
    in6.SUBJ_RACE AS SUBJ_RACE,
    in4.SUBJ_COMPLETED_STUDY_DT_IRT AS SUBJ_COMPLETED_STUDY_DT_IRT,
    in0.STUDY_ID AS STUDY_ID,
    in5.SUBJ_WITHDRAWAL_TIMEFRAME_EDC AS SUBJ_WITHDRAWAL_TIMEFRAME_EDC,
    in6.SUBJ_GENDER AS SUBJ_GENDER,
    in5.SUBJ_DISCONTINUED_DT_EDC AS SUBJ_DISCONTINUED_DT_EDC,
    in4.SUBJ_COMPLETED_STUDY_DT_EDC AS SUBJ_COMPLETED_STUDY_DT_EDC,
    in6.SBJ_COH_COHORT_NUM AS SBJ_COH_COHORT_NUM,
    in6.SUBJ_ETHNICITY AS SUBJ_ETHNICITY,
    in5.SUBJ_WITHDRAWAL_TIMEFRAME_IRT AS SUBJ_WITHDRAWAL_TIMEFRAME_IRT,
    in3.SUBJ_ENTERED_ACTIVE_TRNMT_DT_EDC AS SUBJ_ENTERED_ACTIVE_TRNMT_DT_EDC,
    in6.SBJ_COH_COHORT_DESC AS SBJ_COH_COHORT_DESC,
    in5.SUBJ_REASON_FOR_WITHDRAWAL_IRT AS SUBJ_REASON_FOR_WITHDRAWAL_IRT,
    in2.SUBJ_ENROLLED_STUDY_DT_IRT AS SUBJ_ENROLLED_STUDY_DT_IRT,
    in3.SUBJ_ENTERED_ACTIVE_TRNMT_DT_IRT AS SUBJ_ENTERED_ACTIVE_TRNMT_DT_IRT,
    in6.SBJ_STRATA_CODE_VALUE_DESC AS SBJ_STRATA_CODE_VALUE_DESC,
    in1.SUBJ_STATUS_EDC AS SUBJ_STATUS_EDC,
    in0.SUBJECT_ID AS SUBJECT_ID,
    in2.SUBJ_ENROLLED_STUDY_DT_EDC AS SUBJ_ENROLLED_STUDY_DT_EDC
  
  FROM Unique_233 AS in0
  FULL JOIN Join_153_left_UnionFullOuter AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
  FULL JOIN Join_301_left_UnionFullOuter AS in2
     ON (
      (coalesce(in0.STUDY_ID, in1.STUDY_ID) = in2.STUDY_ID)
      AND (coalesce(in0.SUBJECT_ID, in1.SUBJECT_ID) = in2.SUBJECT_ID)
    )
  FULL JOIN Join_313_left_UnionFullOuter AS in3
     ON (
      (coalesce(in0.STUDY_ID, in1.STUDY_ID, in2.STUDY_ID) = in3.STUDY_ID)
      AND (coalesce(in0.SUBJECT_ID, in1.SUBJECT_ID, in2.SUBJECT_ID) = in3.SUBJECT_ID)
    )
  FULL JOIN Join_324_left_UnionFullOuter AS in4
     ON (
      (coalesce(in0.STUDY_ID, in1.STUDY_ID, in2.STUDY_ID, in3.STUDY_ID) = in4.STUDY_ID)
      AND (coalesce(in0.SUBJECT_ID, in1.SUBJECT_ID, in2.SUBJECT_ID, in3.SUBJECT_ID) = in4.SUBJECT_ID)
    )
  FULL JOIN Join_335_left_UnionFullOuter AS in5
     ON (
      (coalesce(in0.STUDY_ID, in1.STUDY_ID, in2.STUDY_ID, in3.STUDY_ID, in4.STUDY_ID) = in5.STUDY_ID)
      AND (coalesce(in0.SUBJECT_ID, in1.SUBJECT_ID, in2.SUBJECT_ID, in3.SUBJECT_ID, in4.SUBJECT_ID) = in5.SUBJECT_ID)
    )
  FULL JOIN Sample_143 AS in6
     ON (
      (coalesce(in0.STUDY_ID, in1.STUDY_ID, in2.STUDY_ID, in3.STUDY_ID, in4.STUDY_ID, in5.STUDY_ID) = in6.STUDY_ID)
      AND (coalesce(in0.SUBJECT_ID, in1.SUBJECT_ID, in2.SUBJECT_ID, in3.SUBJECT_ID, in4.SUBJECT_ID, in5.SUBJECT_ID) = in6.SUBJECT_ID)
    )

),

AlteryxSelect_343 AS (

  SELECT 
    STUDY_ID AS STUDY_ID,
    SUBJECT_ID AS SUBJECT_ID,
    SUBJ_STATUS_EDC AS SUBJ_STATUS_EDC,
    SUBJ_STATUS_IRT AS SUBJ_STATUS_IRT,
    SUBJ_ENROLLED_STUDY_DT_EDC AS SUBJ_ENROLLED_STUDY_DT_EDC,
    SUBJ_ENROLLED_STUDY_DT_IRT AS SUBJ_ENROLLED_STUDY_DT_IRT,
    SUBJ_ENTERED_ACTIVE_TRNMT_DT_EDC AS SUBJ_ENTERED_ACTIVE_TRNMT_DT_EDC,
    SUBJ_ENTERED_ACTIVE_TRNMT_DT_IRT AS SUBJ_ENTERED_ACTIVE_TRNMT_DT_IRT,
    SUBJ_COMPLETED_STUDY_DT_EDC AS SUBJ_COMPLETED_STUDY_DT_EDC,
    SUBJ_COMPLETED_STUDY_DT_IRT AS SUBJ_COMPLETED_STUDY_DT_IRT,
    SUBJ_DISCONTINUED_DT_EDC AS SUBJ_DISCONTINUED_DT_EDC,
    SUBJ_REASON_FOR_WITHDRAWAL_EDC AS SUBJ_REASON_FOR_WITHDRAWAL_EDC,
    SUBJ_WITHDRAWAL_TIMEFRAME_EDC AS SUBJ_WITHDRAWAL_TIMEFRAME_EDC,
    SUBJ_DISCONTINUED_DT_IRT AS SUBJ_DISCONTINUED_DT_IRT,
    SUBJ_REASON_FOR_WITHDRAWAL_IRT AS SUBJ_REASON_FOR_WITHDRAWAL_IRT,
    SUBJ_WITHDRAWAL_TIMEFRAME_IRT AS SUBJ_WITHDRAWAL_TIMEFRAME_IRT,
    SBJ_STR_STRATA_NUM AS SBJ_STR_STRATA_NUM,
    SBJ_COH_COHORT_NUM AS SBJ_COH_COHORT_NUM,
    SBJ_STRATA_CODE_VALUE_DESC AS SBJ_STRATA_CODE_VALUE_DESC,
    SBJ_COH_COHORT_DESC AS SBJ_COH_COHORT_DESC,
    SUBJ_RACE AS SUBJ_RACE,
    SUBJ_ETHNICITY AS SUBJ_ETHNICITY,
    SUBJ_GENDER AS SUBJ_GENDER
  
  FROM JoinMultiple_341 AS in0

),

Formula_290_0 AS (

  SELECT 
    (
      CASE
        WHEN (ORIGINAL_SITE_NUMBER IS NOT NULL)
          THEN ORIGINAL_SITE_NUMBER
        WHEN (SITE_NUMBER_IRT IS NOT NULL)
          THEN SITE_NUMBER_IRT
        WHEN (SITE_NUMBER_EDC IS NOT NULL)
          THEN SITE_NUMBER_EDC
        ELSE NULL
      END
    ) AS STUDY_SITE_NUMBER,
    *
  
  FROM Join_242_left_UnionFullOuter AS in0

),

Join_288_left_UnionFullOuter AS (

  SELECT 
    in1.STUDY_SITE_NUMBER AS STUDY_SITE_NUMBER,
    in1.SITE_NUMBER_IRT AS SITE_NUMBER_IRT,
    in1.SITE_NUMBER_EDC AS SITE_NUMBER_EDC,
    in1.ORIGINAL_SITE_NUMBER AS ORIGINAL_SITE_NUMBER,
    in0.SUBJ_STATUS_EDC AS SUBJ_STATUS_EDC,
    in0.SUBJ_STATUS_IRT AS SUBJ_STATUS_IRT,
    in0.SUBJ_ENROLLED_STUDY_DT_EDC AS SUBJ_ENROLLED_STUDY_DT_EDC,
    in0.SUBJ_ENROLLED_STUDY_DT_IRT AS SUBJ_ENROLLED_STUDY_DT_IRT,
    in0.SUBJ_ENTERED_ACTIVE_TRNMT_DT_EDC AS SUBJ_ENTERED_ACTIVE_TRNMT_DT_EDC,
    in0.SUBJ_ENTERED_ACTIVE_TRNMT_DT_IRT AS SUBJ_ENTERED_ACTIVE_TRNMT_DT_IRT,
    in0.SUBJ_DISCONTINUED_DT_EDC AS SUBJ_DISCONTINUED_DT_EDC,
    in0.SUBJ_DISCONTINUED_DT_IRT AS SUBJ_DISCONTINUED_DT_IRT,
    in0.SUBJ_REASON_FOR_WITHDRAWAL_EDC AS SUBJ_REASON_FOR_WITHDRAWAL_EDC,
    in0.SUBJ_REASON_FOR_WITHDRAWAL_IRT AS SUBJ_REASON_FOR_WITHDRAWAL_IRT,
    in0.SUBJ_WITHDRAWAL_TIMEFRAME_EDC AS SUBJ_WITHDRAWAL_TIMEFRAME_EDC,
    in0.SUBJ_WITHDRAWAL_TIMEFRAME_IRT AS SUBJ_WITHDRAWAL_TIMEFRAME_IRT,
    in0.SUBJ_COMPLETED_STUDY_DT_EDC AS SUBJ_COMPLETED_STUDY_DT_EDC,
    in0.SUBJ_COMPLETED_STUDY_DT_IRT AS SUBJ_COMPLETED_STUDY_DT_IRT,
    in0.SBJ_STR_STRATA_NUM AS SBJ_STR_STRATA_NUM,
    in0.SBJ_COH_COHORT_NUM AS SBJ_COH_COHORT_NUM,
    in0.SBJ_STRATA_CODE_VALUE_DESC AS SBJ_STRATA_CODE_VALUE_DESC,
    in0.SBJ_COH_COHORT_DESC AS SBJ_COH_COHORT_DESC,
    in0.SUBJ_RACE AS SUBJ_RACE,
    in0.SUBJ_ETHNICITY AS SUBJ_ETHNICITY,
    in0.SUBJ_GENDER AS SUBJ_GENDER,
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.STUDY_ID
      END
    ) AS STUDY_ID,
    (
      CASE
        WHEN ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))
          THEN NULL
        ELSE in1.SUBJECT_ID
      END
    ) AS SUBJECT_ID,
    in0.* EXCLUDE ("SUBJ_STATUS_EDC", 
    "SUBJ_STATUS_IRT", 
    "SUBJ_ENROLLED_STUDY_DT_EDC", 
    "SUBJ_ENROLLED_STUDY_DT_IRT", 
    "SUBJ_ENTERED_ACTIVE_TRNMT_DT_EDC", 
    "SUBJ_ENTERED_ACTIVE_TRNMT_DT_IRT", 
    "SUBJ_DISCONTINUED_DT_EDC", 
    "SUBJ_DISCONTINUED_DT_IRT", 
    "SUBJ_REASON_FOR_WITHDRAWAL_EDC", 
    "SUBJ_REASON_FOR_WITHDRAWAL_IRT", 
    "SUBJ_WITHDRAWAL_TIMEFRAME_EDC", 
    "SUBJ_WITHDRAWAL_TIMEFRAME_IRT", 
    "SUBJ_COMPLETED_STUDY_DT_EDC", 
    "SUBJ_COMPLETED_STUDY_DT_IRT", 
    "SBJ_STR_STRATA_NUM", 
    "SBJ_COH_COHORT_NUM", 
    "SBJ_STRATA_CODE_VALUE_DESC", 
    "SBJ_COH_COHORT_DESC", 
    "SUBJ_RACE", 
    "SUBJ_ETHNICITY", 
    "SUBJ_GENDER", 
    "STUDY_ID", 
    "SUBJECT_ID"),
    in1.* EXCLUDE ("STUDY_SITE_NUMBER", "SITE_NUMBER_IRT", "SITE_NUMBER_EDC", "ORIGINAL_SITE_NUMBER", "STUDY_ID", "SUBJECT_ID")
  
  FROM AlteryxSelect_343 AS in0
  FULL JOIN Formula_290_0 AS in1
     ON ((in0.STUDY_ID = in1.STUDY_ID) AND (in0.SUBJECT_ID = in1.SUBJECT_ID))

)

SELECT *

FROM Join_288_left_UnionFullOuter
