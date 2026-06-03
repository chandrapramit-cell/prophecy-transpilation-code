{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Filter_251 AS (

  SELECT *
  
  FROM {{ ref('FDI_Treasury_Altx_Sol_FAS107_2025_Q1__Filter_251')}}

),

Formula_256_0 AS (

  SELECT 
    (DATE_ADD((DATE_TRUNC('month', PRD_END_DT)), CAST(27 AS INTEGER))) AS PRD_END_DT_FLR,
    (
      TO_DATE(
        (
          CASE
            WHEN (CD_NXTMATDT < PRD_END_DT)
              THEN PRD_END_DT
            ELSE CD_NXTMATDT
          END
        ), 
        'yyyy-MM-dd')
    ) AS CD_NXTMATDT_2,
    CAST((CAST(CD_INTRATE_R AS DECIMAL (19, 9)) * CAST(CD_VALUE AS DECIMAL (19, 9))) AS DOUBLE) AS CD_WEIGHT,
    *
  
  FROM Filter_251 AS in0

),

Formula_256_1 AS (

  SELECT 
    (DATE_ADD((DATE_TRUNC('month', CD_NXTMATDT_2)), CAST(27 AS INTEGER))) AS CD_NXTMATDT_FLR,
    *
  
  FROM Formula_256_0 AS in0

),

Formula_256_2 AS (

  SELECT 
    (ADD_MONTHS(CD_NXTMATDT_FLR, -1)) AS Mat_Dt_Then,
    (DATE_ADD((ADD_MONTHS(CD_NXTMATDT_FLR, 1)), CAST(-1 AS INTEGER))) AS Mat_Dt_Else,
    (
      TO_DATE(
        (
          CASE
            WHEN CAST((CD_NXTMATDT_2 IS NULL) AS BOOLEAN)
              THEN '1970-01-01'
            ELSE CAST(CD_NXTMATDT_2 AS string)
          END
        ), 
        'yyyy-MM-dd')
    ) AS CD_NXTMATDT_2,
    * EXCEPT (`cd_nxtmatdt_2`)
  
  FROM Formula_256_1 AS in0

),

DateTime_255_0 AS (

  SELECT 
    (DATE_FORMAT(CD_NXTMATDT_FLR, 'yyyy-MM-dd')) AS CD_NXTMATDT_FLR_str,
    *
  
  FROM Formula_256_2 AS in0

),

DateTime_254_0 AS (

  SELECT 
    (DATE_FORMAT(Mat_Dt_Then, 'yyyy-MM-dd')) AS Mat_Dt_Then_str,
    *
  
  FROM DateTime_255_0 AS in0

),

DateTime_253_0 AS (

  SELECT 
    (DATE_FORMAT(Mat_Dt_Else, 'yyyy-MM-dd')) AS Mat_Dt_Else_str,
    *
  
  FROM DateTime_254_0 AS in0

),

Formula_263_to_Formula_259_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (CD_NXTMATDT_FLR > CD_NXTMATDT_2)
          THEN (CONCAT(Mat_Dt_Then_str, ' - ', CD_NXTMATDT_FLR_str))
        ELSE (CONCAT(CD_NXTMATDT_FLR_str, ' - ', Mat_Dt_Else_str))
      END
    ) AS string) AS MATURITY_RANGE,
    CAST((
      CASE
        WHEN (CD_NXTMATDT_FLR > CD_NXTMATDT_2)
          THEN CAST((MONTHS_BETWEEN((TO_DATE((DATE_TRUNC('month', CD_NXTMATDT_FLR)))), (TO_DATE((DATE_TRUNC('month', PRD_END_DT_FLR)))))) AS INTEGER)
        ELSE CAST((
          MONTHS_BETWEEN(
            (TO_DATE((ADD_MONTHS((DATE_TRUNC('month', CD_NXTMATDT_FLR)), 1)))), 
            (TO_DATE((DATE_TRUNC('month', PRD_END_DT_FLR)))))
        ) AS INTEGER)
      END
    ) AS INTEGER) AS REMAINING_MONTHS_ACT,
    *
  
  FROM DateTime_253_0 AS in0

),

Formula_263_to_Formula_259_1 AS (

  SELECT 
    CAST((
      CASE
        WHEN (REMAINING_MONTHS_ACT < 1)
          THEN 1
        WHEN (REMAINING_MONTHS_ACT > 120)
          THEN 120
        ELSE REMAINING_MONTHS_ACT
      END
    ) AS INTEGER) AS REMAINING_MONTHS,
    CAST((
      (
        CASE
          WHEN (
            (
              (
                CAST((
                  MONTHS_BETWEEN(
                    (TO_DATE(CD_NXTMATDT_2)), 
                    (
                      TO_DATE(
                        (
                          CASE
                            WHEN CAST((CD_RENDATE IS NULL) AS BOOLEAN)
                              THEN CD_ISSDATE
                            ELSE CD_RENDATE
                          END
                        ))
                    ))
                ) AS INTEGER)
                / 1
              ) < 0
            )
            AND (
                  (
                    (
                      CAST((
                        MONTHS_BETWEEN(
                          (TO_DATE(CD_NXTMATDT_2)), 
                          (
                            TO_DATE(
                              (
                                CASE
                                  WHEN CAST((CD_RENDATE IS NULL) AS BOOLEAN)
                                    THEN CD_ISSDATE
                                  ELSE CD_RENDATE
                                END
                              ))
                          ))
                      ) AS INTEGER)
                      / 1
                    )
                    - FLOOR(
                        (
                          CAST((
                            MONTHS_BETWEEN(
                              (TO_DATE(CD_NXTMATDT_2)), 
                              (
                                TO_DATE(
                                  (
                                    CASE
                                      WHEN CAST((CD_RENDATE IS NULL) AS BOOLEAN)
                                        THEN CD_ISSDATE
                                      ELSE CD_RENDATE
                                    END
                                  ))
                              ))
                          ) AS INTEGER)
                          / 1
                        ))
                  ) = 0.5
                )
          )
            THEN CEIL(
              (
                CAST((
                  MONTHS_BETWEEN(
                    (TO_DATE(CD_NXTMATDT_2)), 
                    (
                      TO_DATE(
                        (
                          CASE
                            WHEN CAST((CD_RENDATE IS NULL) AS BOOLEAN)
                              THEN CD_ISSDATE
                            ELSE CD_RENDATE
                          END
                        ))
                    ))
                ) AS INTEGER)
                / 1
              ))
          ELSE ROUND(
            (
              CAST((
                MONTHS_BETWEEN(
                  (TO_DATE(CD_NXTMATDT_2)), 
                  (
                    TO_DATE(
                      (
                        CASE
                          WHEN CAST((CD_RENDATE IS NULL) AS BOOLEAN)
                            THEN CD_ISSDATE
                          ELSE CD_RENDATE
                        END
                      ))
                  ))
              ) AS INTEGER)
              / 1
            ))
        END
      )
      * 1
    ) AS DOUBLE) AS TERM,
    CAST((
      CASE
        WHEN CAST((CD_RENDATE IS NULL) AS BOOLEAN)
          THEN (
            ((DAY((LAST_DAY(CAST(CD_ISSDATE AS DATE))))) - (DAY(CD_ISSDATE)))
            / (DAY((LAST_DAY(CAST(CD_ISSDATE AS DATE)))))
          )
        ELSE (
          ((DAY((LAST_DAY(CAST(CD_RENDATE AS DATE))))) - (DAY(CD_RENDATE)))
          / (DAY((LAST_DAY(CAST(CD_RENDATE AS DATE)))))
        )
      END
    ) AS DOUBLE) AS `New Term Calc 1st Mo`,
    CAST((
      CASE
        WHEN CAST((CD_RENDATE IS NULL) AS BOOLEAN)
          THEN CAST((
            MONTHS_BETWEEN(
              (TO_DATE((ADD_MONTHS((DATE_TRUNC('month', CD_NXTMATDT_2)), 0)))), 
              (TO_DATE((ADD_MONTHS((DATE_TRUNC('day', CD_ISSDATE)), 1)))))
          ) AS INTEGER)
        ELSE CAST((
          MONTHS_BETWEEN(
            (TO_DATE((ADD_MONTHS((DATE_TRUNC('month', CD_NXTMATDT_2)), 0)))), 
            (TO_DATE((ADD_MONTHS((DATE_TRUNC('day', CD_RENDATE)), 1)))))
        ) AS INTEGER)
      END
    ) AS DOUBLE) AS `New Term Calc Mid Mo`,
    CAST(((DAY(CD_NXTMATDT_2)) / (DAY((LAST_DAY(CAST(CD_NXTMATDT_2 AS DATE)))))) AS DOUBLE) AS `New Term Calc Last Mo`,
    *
  
  FROM Formula_263_to_Formula_259_0 AS in0

),

Formula_263_to_Formula_259_2 AS (

  SELECT 
    CAST(((`New Term Calc 1st Mo` + `New Term Calc Mid Mo`) + `New Term Calc Last Mo`) AS INTEGER) AS `New Term`,
    CAST(((`New Term Calc 1st Mo` + `New Term Calc Mid Mo`) + `New Term Calc Last Mo`) AS DOUBLE) AS `New Term Double`,
    CAST(CASE
      WHEN (CAST(CD_INTRATE_R AS INT) < 2)
        THEN '\'0-1'
      WHEN (CAST(CD_INTRATE_R AS INT) < 5)
        THEN '\'2-4'
      WHEN (CAST(CD_INTRATE_R AS INT) < 9)
        THEN '\'5-8'
      WHEN (CAST(CD_INTRATE_R AS INT) < 12)
        THEN '\'9-11'
      ELSE '12+'
    END AS STRING) AS COUPONAGG,
    CAST((REMAINING_MONTHS - 1) AS DOUBLE) AS REMAINING_MONTHS_ADJ,
    CAST((
      CASE
        WHEN (HCS_LOB_NODE03_NB = 'S577006')
          THEN HCS_LOB_NODE04_NB
        ELSE HCS_LOB_NODE03_NB
      END
    ) AS string) AS `LOB Lvl2`,
    *
  
  FROM Formula_263_to_Formula_259_1 AS in0

),

Formula_263_to_Formula_259_3 AS (

  SELECT 
    CAST((
      CASE
        WHEN (`LOB Lvl2` = 'S570308')
          THEN 'Commercial Banking'
        WHEN (`LOB Lvl2` = 'S572000')
          THEN 'Asset & Wealth Management'
        WHEN (`LOB Lvl2` = 'S573050')
          THEN 'Corporate & Investment Bank'
        WHEN CAST((`LOB Lvl2` IN ('S576344', 'S536099')) AS BOOLEAN)
          THEN 'Consumer & Community Banking'
        ELSE NULL
      END
    ) AS string) AS Node__Description,
    CAST((CAST(CD_VALUE AS DECIMAL (19, 9)) * CAST(CD_INTRATE_R AS DECIMAL (19, 9))) AS DOUBLE) AS WAC1,
    CAST((CAST(CD_VALUE AS DOUBLE) * TERM) AS DOUBLE) AS WAM1,
    CAST((CAST(CD_VALUE AS DOUBLE) * REMAINING_MONTHS_ADJ) AS DOUBLE) AS COUPON_PAYMENTS1,
    CAST(((CAST(CD_VALUE AS DECIMAL (19, 9)) * CAST(CD_INTRATE_R AS DECIMAL (19, 9))) * REMAINING_MONTHS_ADJ) AS DOUBLE) AS Total_Coupon_Payments,
    *
  
  FROM Formula_263_to_Formula_259_2 AS in0

)

SELECT *

FROM Formula_263_to_Formula_259_3
