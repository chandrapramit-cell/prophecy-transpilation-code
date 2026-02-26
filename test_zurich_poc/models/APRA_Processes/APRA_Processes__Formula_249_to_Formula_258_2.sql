{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH DynamicInput_192 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('APRA_Processes', 'DynamicInput_192') }}

),

Formula_249_to_Formula_258_0 AS (

  SELECT 
    (DATE_ADD(CAST(v_EndOfMonth AS DATE), CAST(-182 AS INTEGER))) AS n_6MonthDay,
    CAST((REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(v_DateCalc AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')) AS STRING) AS sDateCalc,
    *
  
  FROM DynamicInput_192 AS in0

),

Formula_249_to_Formula_258_1 AS (

  SELECT 
    (
      TO_DATE(
        (
          CASE
            WHEN (
              (
                CAST((
                  coalesce(
                    CAST((SUBSTRING(sDateCalc, 5, 2)) AS DOUBLE), 
                    CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 5, 2)), '^[0-9]+', 0)) AS INTEGER), 
                    0)
                ) AS DOUBLE) IN (
                  CAST(1 AS DOUBLE),
                  CAST(3 AS DOUBLE),
                  CAST(5 AS DOUBLE),
                  CAST(7 AS DOUBLE),
                  CAST(8 AS DOUBLE),
                  CAST(10 AS DOUBLE),
                  CAST(12 AS DOUBLE)
                )
              )
              AND (
                    (
                      coalesce(
                        CAST((SUBSTRING(sDateCalc, 7, 2)) AS DOUBLE), 
                        CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 7, 2)), '^[0-9]+', 0)) AS INTEGER), 
                        0)
                    ) > 31
                  )
            )
              THEN (CONCAT((SUBSTRING(sDateCalc, 1, 4)), '-', (SUBSTRING(sDateCalc, 5, 2)), '-31'))
            WHEN (
              (
                CAST((
                  coalesce(
                    CAST((SUBSTRING(sDateCalc, 5, 2)) AS DOUBLE), 
                    CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 5, 2)), '^[0-9]+', 0)) AS INTEGER), 
                    0)
                ) AS DOUBLE) IN (CAST(4 AS DOUBLE), CAST(6 AS DOUBLE), CAST(9 AS DOUBLE), CAST(11 AS DOUBLE))
              )
              AND (
                    (
                      coalesce(
                        CAST((SUBSTRING(sDateCalc, 7, 2)) AS DOUBLE), 
                        CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 7, 2)), '^[0-9]+', 0)) AS INTEGER), 
                        0)
                    ) > 30
                  )
            )
              THEN (CONCAT((SUBSTRING(sDateCalc, 1, 4)), '-', (SUBSTRING(sDateCalc, 5, 2)), '-30'))
            WHEN (
              (
                (
                  (
                    (
                      coalesce(
                        CAST((SUBSTRING(sDateCalc, 5, 2)) AS DOUBLE), 
                        CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 5, 2)), '^[0-9]+', 0)) AS INTEGER), 
                        0)
                    ) = 2
                  )
                  AND (
                        (
                          coalesce(
                            CAST((SUBSTRING(sDateCalc, 7, 2)) AS DOUBLE), 
                            CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 7, 2)), '^[0-9]+', 0)) AS INTEGER), 
                            0)
                        ) > 29
                      )
                )
                AND (
                      (
                        PMOD(
                          (
                            coalesce(
                              CAST((SUBSTRING(sDateCalc, 1, 4)) AS DOUBLE), 
                              CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 1, 4)), '^[0-9]+', 0)) AS INTEGER), 
                              0)
                          ), 
                          4)
                      ) = 0
                    )
              )
              AND (
                    (
                      (
                        NOT(
                          (
                            PMOD(
                              (
                                coalesce(
                                  CAST((SUBSTRING(sDateCalc, 1, 4)) AS DOUBLE), 
                                  CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 1, 4)), '^[0-9]+', 0)) AS INTEGER), 
                                  0)
                              ), 
                              100)
                          ) = 0)
                      )
                      OR (
                           (
                             PMOD(
                               (
                                 coalesce(
                                   CAST((SUBSTRING(sDateCalc, 1, 4)) AS DOUBLE), 
                                   CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 1, 4)), '^[0-9]+', 0)) AS INTEGER), 
                                   0)
                               ), 
                               100)
                           ) IS NULL
                         )
                    )
                    OR (
                         (
                           PMOD(
                             (
                               coalesce(
                                 CAST((SUBSTRING(sDateCalc, 1, 4)) AS DOUBLE), 
                                 CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 1, 4)), '^[0-9]+', 0)) AS INTEGER), 
                                 0)
                             ), 
                             400)
                         ) = 0
                       )
                  )
            )
              THEN (CONCAT((SUBSTRING(sDateCalc, 1, 4)), '-', (SUBSTRING(sDateCalc, 5, 2)), '-29'))
            WHEN (
              (
                (
                  (
                    coalesce(
                      CAST((SUBSTRING(sDateCalc, 5, 2)) AS DOUBLE), 
                      CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 5, 2)), '^[0-9]+', 0)) AS INTEGER), 
                      0)
                  ) = 2
                )
                AND (
                      (
                        coalesce(
                          CAST((SUBSTRING(sDateCalc, 7, 2)) AS DOUBLE), 
                          CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 7, 2)), '^[0-9]+', 0)) AS INTEGER), 
                          0)
                      ) > 28
                    )
              )
              AND (
                    NOT(
                      (
                        (
                          PMOD(
                            (
                              coalesce(
                                CAST((SUBSTRING(sDateCalc, 1, 4)) AS DOUBLE), 
                                CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 1, 4)), '^[0-9]+', 0)) AS INTEGER), 
                                0)
                            ), 
                            4)
                        ) = 0
                      )
                      AND (
                            (
                              (
                                NOT(
                                  (
                                    PMOD(
                                      (
                                        coalesce(
                                          CAST((SUBSTRING(sDateCalc, 1, 4)) AS DOUBLE), 
                                          CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 1, 4)), '^[0-9]+', 0)) AS INTEGER), 
                                          0)
                                      ), 
                                      100)
                                  ) = 0)
                              )
                              OR (
                                   (
                                     PMOD(
                                       (
                                         coalesce(
                                           CAST((SUBSTRING(sDateCalc, 1, 4)) AS DOUBLE), 
                                           CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 1, 4)), '^[0-9]+', 0)) AS INTEGER), 
                                           0)
                                       ), 
                                       100)
                                   ) IS NULL
                                 )
                            )
                            OR (
                                 (
                                   PMOD(
                                     (
                                       coalesce(
                                         CAST((SUBSTRING(sDateCalc, 1, 4)) AS DOUBLE), 
                                         CAST((REGEXP_EXTRACT((SUBSTRING(sDateCalc, 1, 4)), '^[0-9]+', 0)) AS INTEGER), 
                                         0)
                                     ), 
                                     400)
                                 ) = 0
                               )
                          ))
                  )
            )
              THEN (CONCAT((SUBSTRING(sDateCalc, 1, 4)), '-', (SUBSTRING(sDateCalc, 5, 2)), '-28'))
            ELSE (CONCAT((SUBSTRING(sDateCalc, 1, 4)), '-', (SUBSTRING(sDateCalc, 5, 2)), '-', (SUBSTRING(sDateCalc, 7, 2))))
          END
        ), 
        'yyyy-MM-dd')
    ) AS sDate,
    *
  
  FROM Formula_249_to_Formula_258_0 AS in0

),

Formula_249_to_Formula_258_2 AS (

  SELECT 
    CAST(CAST(datediff(to_date(v_EndOfMonth), to_date(sDate)) AS INT) AS DOUBLE) AS n_DaysOS,
    CAST(CASE
      WHEN (CAST(datediff(to_date(sDate), to_date(n_6MonthDay)) AS INT) < 0)
        THEN v_OSDebtors
      ELSE 0
    END AS DOUBLE) AS v_Debtors6Months,
    *
  
  FROM Formula_249_to_Formula_258_1 AS in0

)

SELECT *

FROM Formula_249_to_Formula_258_2
