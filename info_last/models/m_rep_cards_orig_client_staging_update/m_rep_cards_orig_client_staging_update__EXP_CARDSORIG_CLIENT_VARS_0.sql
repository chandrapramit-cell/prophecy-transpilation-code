{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH DBreference AS (

  SELECT * 
  
  FROM {{ ref('dsg')}}

),

DBreference AS (

  SELECT * 
  
  FROM {{ ref('dsg')}}

),

SQ_CLIENT AS (

  SELECT *
  
  FROM (
    SELECT 
      /*+ parallel(cus) parallel(ca) parallel(c) use_hash(cus ca c)*/
      DISTINCT ca.clid AS client_alias_clid,
      ca.merge_clid AS client_alias_merge_clid,
      ca.efrom AS client_alias_efrom,
      cus.customer_number,
      CASE
        WHEN c.clid IS NULL
          THEN cs.clid
        ELSE c.clid
      END AS clid_lkp,
      c.merge_clid AS merge_clid_lkp,
      c.efrom AS efrom_lkp,
      CASE
        WHEN c.title IS NULL
          THEN cs.title
        ELSE c.title
      END AS title_lkp,
      CASE
        WHEN c.forenames IS NULL
          THEN cs.forenames
        ELSE c.forenames
      END AS forenames_lkp,
      CASE
        WHEN c.surname IS NULL
          THEN cs.surname
        ELSE c.surname
      END AS surname_lkp,
      CASE
        WHEN c.gender IS NULL
          THEN cs.gender
        ELSE c.gender
      END AS gender_lkp,
      CASE
        WHEN c.dob IS NULL
          THEN cs.dob
        ELSE c.dob
      END AS dob_lkp,
      CASE
        WHEN c.client_status IS NULL
          THEN cs.client_status
        ELSE c.client_status
      END AS client_status_lkp,
      -- case when cus.title = 'Mr.' then 'Mr' else cus.title end as title_lkp,
      cus.title,
      cus.firstnames,
      cus.lastname,
      cus.date_of_birth,
      CASE
        WHEN cus.gender = 'MALE'
          THEN 'M'
        WHEN cus.gender = 'FEMALE'
          THEN 'F'
        WHEN cus.gender = 'UNKNOWN'
          THEN 'U'
        ELSE CUS.GENDER
      END AS GENDER,
      row_number() OVER (PARTITION BY cus.customer_number ORDER BY last_action_date DESC) row_number
    
    FROM work_cards_orig_staging AS cus, client_alias AS ca, client AS c, client_staging AS cs
    
    WHERE cus.customer_number = ca.source_client_ref (+)
          and cus.customer_number IS NOT NULL
          and ca.source_type (+) = 0
          and ca.source_system_code (+) = 'U'
          and ca.eto (+) IS NULL
          and ca.clid = c.clid (+)
          and c.eto (+) IS NULL
          and ca.clid = cs.clid (+)
  )
  
  WHERE row_number = 1

),

SQ_CLIENT_GENERATE_SK_0 AS (

  SELECT 
    (ROW_NUMBER() OVER ()) AS prophecy_sk,
    *
  
  FROM SQ_CLIENT AS in0

),

SQ_CLIENT_GENERATE_SK_EXPR_29 AS (

  SELECT 
    TITLE AS input_title,
    FORENAMES AS input_forename,
    SURNAME AS input_surname,
    GENDER AS input_gender,
    prophecy_sk AS prophecy_sk,
    CLIENT_STATUS_LKP AS CLIENT_STATUS_LKP,
    DATE_OF_BIRTH AS DATE_OF_BIRTH,
    CLID AS CLID,
    MERGE_CLID AS MERGE_CLID,
    EFROM AS EFROM,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    CLID_LKP AS CLID_LKP,
    MERGE_CLID_LKP AS MERGE_CLID_LKP,
    EFROM_LKP AS EFROM_LKP,
    TITLE_LKP AS TITLE_LKP,
    FORENAMES_LKP AS FORENAMES_LKP,
    SURNAME_LKP AS SURNAME_LKP,
    GENDER_LKP AS GENDER_LKP,
    DOB_LKP AS DOB_LKP
  
  FROM SQ_CLIENT_GENERATE_SK_0 AS in0

),

EXP_GENERIC_NAME_REFORMAT_LOOKUP_merged AS (

  SELECT 
    in0.value2 AS LOOKUP_VARIABLE_5,
    in2.value2 AS LOOKUP_VARIABLE_6,
    in3.value3 AS LOOKUP_VARIABLE_7,
    in3.class AS class,
    in3.value1 AS value1,
    in3.value2 AS value2,
    in3.value3 AS value3,
    'a' AS value4,
    in1.input_title AS input_title,
    in1.input_forename AS input_forename,
    in1.input_surname AS input_surname,
    in1.input_gender AS input_gender,
    in1.prophecy_sk AS prophecy_sk
  
  FROM DBreference AS in0
  LEFT JOIN SQ_CLIENT_GENERATE_SK_EXPR_29 AS in1
     ON ((in0.class = '3000'))
  RIGHT JOIN DBreference AS in2
     ON ((in2.class = '5000'))
  RIGHT JOIN DBreference AS in3
     ON ((in3.class = '2000'))

),

DBATTRIBUTE AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'DBATTRIBUTE') }}

),

EXP_GENERIC_NAME_REFORMAT_VARS_0 AS (

  SELECT 
    LOOKUP_VARIABLE_7 AS work_gender1__lookup_dbref_value3_lkp_2,
    LOOKUP_VARIABLE_6 AS work_surname3__lookup_dbref_value2_lkp_1,
    UPPER(CAST((LTRIM((RTRIM(input_forename)))) AS STRING)) AS check_forename3,
    LOOKUP_VARIABLE_5 AS salacious_surname,
    UPPER(CAST((LTRIM((RTRIM(input_title)))) AS STRING)) AS work_title1,
    *
  
  FROM EXP_GENERIC_NAME_REFORMAT_LOOKUP_merged AS in0

),

EXP_GENERIC_NAME_REFORMAT AS (

  SELECT 
    (
      CASE
        WHEN (
          (
            LENGTH(
              CAST((
                RTRIM(
                  (
                    LTRIM(
                      UPPER(
                        (
                          CONCAT(
                            SUBSTRING(
                              (
                                LTRIM(
                                  (
                                    RTRIM(
                                      (
                                        CASE
                                          WHEN (
                                            (
                                              CASE
                                                WHEN (
                                                  (
                                                    CASE
                                                      WHEN (
                                                        (
                                                          CASE
                                                            WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                              THEN NULL
                                                            WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                              THEN NULL
                                                            ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                          END
                                                        ) IS NULL
                                                      )
                                                        THEN 'Y'
                                                      ELSE 'N'
                                                    END
                                                  ) = 'Y'
                                                )
                                                  THEN 'Y'
                                                WHEN (INPUT_INITIALS_CODE = 'Y')
                                                  THEN 'Y'
                                                ELSE 'N'
                                              END
                                            ) = 'Y'
                                          )
                                            THEN (
                                              CONCAT(
                                                SUBSTRING(INPUT_FORENAME, 0, 1), 
                                                (
                                                  CASE
                                                    WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                      THEN NULL
                                                    WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                      THEN NULL
                                                    ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                  END
                                                ))
                                            )
                                          WHEN (
                                            UPPER(SUBSTRING(INPUT_FORENAME, 0, 1)) = UPPER(
                                              SUBSTRING(
                                                (
                                                  CASE
                                                    WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                      THEN NULL
                                                    WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                      THEN NULL
                                                    ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                  END
                                                ), 
                                                0, 
                                                1))
                                          )
                                            THEN (
                                              CASE
                                                WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                  THEN NULL
                                                WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                  THEN NULL
                                                ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                              END
                                            )
                                          ELSE (
                                            CONCAT(
                                              SUBSTRING(INPUT_FORENAME, 0, 1), 
                                              (
                                                CASE
                                                  WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                    THEN NULL
                                                  WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                    THEN NULL
                                                  ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                END
                                              ))
                                          )
                                        END
                                      ))
                                  ))
                              ), 
                              0, 
                              1), 
                            ' ', 
                            SUBSTRING(
                              (
                                LTRIM(
                                  (
                                    RTRIM(
                                      (
                                        CASE
                                          WHEN (
                                            (
                                              CASE
                                                WHEN (
                                                  (
                                                    CASE
                                                      WHEN (
                                                        (
                                                          CASE
                                                            WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                              THEN NULL
                                                            WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                              THEN NULL
                                                            ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                          END
                                                        ) IS NULL
                                                      )
                                                        THEN 'Y'
                                                      ELSE 'N'
                                                    END
                                                  ) = 'Y'
                                                )
                                                  THEN 'Y'
                                                WHEN (INPUT_INITIALS_CODE = 'Y')
                                                  THEN 'Y'
                                                ELSE 'N'
                                              END
                                            ) = 'Y'
                                          )
                                            THEN (
                                              CONCAT(
                                                SUBSTRING(INPUT_FORENAME, 0, 1), 
                                                (
                                                  CASE
                                                    WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                      THEN NULL
                                                    WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                      THEN NULL
                                                    ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                  END
                                                ))
                                            )
                                          WHEN (
                                            UPPER(SUBSTRING(INPUT_FORENAME, 0, 1)) = UPPER(
                                              SUBSTRING(
                                                (
                                                  CASE
                                                    WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                      THEN NULL
                                                    WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                      THEN NULL
                                                    ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                  END
                                                ), 
                                                0, 
                                                1))
                                          )
                                            THEN (
                                              CASE
                                                WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                  THEN NULL
                                                WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                  THEN NULL
                                                ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                              END
                                            )
                                          ELSE (
                                            CONCAT(
                                              SUBSTRING(INPUT_FORENAME, 0, 1), 
                                              (
                                                CASE
                                                  WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                    THEN NULL
                                                  WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                    THEN NULL
                                                  ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                END
                                              ))
                                          )
                                        END
                                      ))
                                  ))
                              ), 
                              1, 
                              1), 
                            ' ', 
                            SUBSTRING(
                              (
                                LTRIM(
                                  (
                                    RTRIM(
                                      (
                                        CASE
                                          WHEN (
                                            (
                                              CASE
                                                WHEN (
                                                  (
                                                    CASE
                                                      WHEN (
                                                        (
                                                          CASE
                                                            WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                              THEN NULL
                                                            WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                              THEN NULL
                                                            ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                          END
                                                        ) IS NULL
                                                      )
                                                        THEN 'Y'
                                                      ELSE 'N'
                                                    END
                                                  ) = 'Y'
                                                )
                                                  THEN 'Y'
                                                WHEN (INPUT_INITIALS_CODE = 'Y')
                                                  THEN 'Y'
                                                ELSE 'N'
                                              END
                                            ) = 'Y'
                                          )
                                            THEN (
                                              CONCAT(
                                                SUBSTRING(INPUT_FORENAME, 0, 1), 
                                                (
                                                  CASE
                                                    WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                      THEN NULL
                                                    WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                      THEN NULL
                                                    ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                  END
                                                ))
                                            )
                                          WHEN (
                                            UPPER(SUBSTRING(INPUT_FORENAME, 0, 1)) = UPPER(
                                              SUBSTRING(
                                                (
                                                  CASE
                                                    WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                      THEN NULL
                                                    WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                      THEN NULL
                                                    ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                  END
                                                ), 
                                                0, 
                                                1))
                                          )
                                            THEN (
                                              CASE
                                                WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                  THEN NULL
                                                WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                  THEN NULL
                                                ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                              END
                                            )
                                          ELSE (
                                            CONCAT(
                                              SUBSTRING(INPUT_FORENAME, 0, 1), 
                                              (
                                                CASE
                                                  WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                    THEN NULL
                                                  WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                    THEN NULL
                                                  ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                END
                                              ))
                                          )
                                        END
                                      ))
                                  ))
                              ), 
                              2, 
                              1), 
                            ' ', 
                            SUBSTRING(
                              (
                                LTRIM(
                                  (
                                    RTRIM(
                                      (
                                        CASE
                                          WHEN (
                                            (
                                              CASE
                                                WHEN (
                                                  (
                                                    CASE
                                                      WHEN (
                                                        (
                                                          CASE
                                                            WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                              THEN NULL
                                                            WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                              THEN NULL
                                                            ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                          END
                                                        ) IS NULL
                                                      )
                                                        THEN 'Y'
                                                      ELSE 'N'
                                                    END
                                                  ) = 'Y'
                                                )
                                                  THEN 'Y'
                                                WHEN (INPUT_INITIALS_CODE = 'Y')
                                                  THEN 'Y'
                                                ELSE 'N'
                                              END
                                            ) = 'Y'
                                          )
                                            THEN (
                                              CONCAT(
                                                SUBSTRING(INPUT_FORENAME, 0, 1), 
                                                (
                                                  CASE
                                                    WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                      THEN NULL
                                                    WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                      THEN NULL
                                                    ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                  END
                                                ))
                                            )
                                          WHEN (
                                            UPPER(SUBSTRING(INPUT_FORENAME, 0, 1)) = UPPER(
                                              SUBSTRING(
                                                (
                                                  CASE
                                                    WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                      THEN NULL
                                                    WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                      THEN NULL
                                                    ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                  END
                                                ), 
                                                0, 
                                                1))
                                          )
                                            THEN (
                                              CASE
                                                WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                  THEN NULL
                                                WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                  THEN NULL
                                                ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                              END
                                            )
                                          ELSE (
                                            CONCAT(
                                              SUBSTRING(INPUT_FORENAME, 0, 1), 
                                              (
                                                CASE
                                                  WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                    THEN NULL
                                                  WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                    THEN NULL
                                                  ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                END
                                              ))
                                          )
                                        END
                                      ))
                                  ))
                              ), 
                              3, 
                              1))
                        )))
                  ))
              ) AS STRING))
          ) = 0
        )
          THEN NULL
        ELSE (
          RTRIM(
            (
              LTRIM(
                UPPER(
                  (
                    CONCAT(
                      SUBSTRING(
                        (
                          LTRIM(
                            (
                              RTRIM(
                                (
                                  CASE
                                    WHEN (
                                      (
                                        CASE
                                          WHEN (
                                            (
                                              CASE
                                                WHEN (
                                                  (
                                                    CASE
                                                      WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                        THEN NULL
                                                      WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                        THEN NULL
                                                      ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                    END
                                                  ) IS NULL
                                                )
                                                  THEN 'Y'
                                                ELSE 'N'
                                              END
                                            ) = 'Y'
                                          )
                                            THEN 'Y'
                                          WHEN (INPUT_INITIALS_CODE = 'Y')
                                            THEN 'Y'
                                          ELSE 'N'
                                        END
                                      ) = 'Y'
                                    )
                                      THEN (
                                        CONCAT(
                                          SUBSTRING(INPUT_FORENAME, 0, 1), 
                                          (
                                            CASE
                                              WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                THEN NULL
                                              WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                THEN NULL
                                              ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                            END
                                          ))
                                      )
                                    WHEN (
                                      UPPER(SUBSTRING(INPUT_FORENAME, 0, 1)) = UPPER(
                                        SUBSTRING(
                                          (
                                            CASE
                                              WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                THEN NULL
                                              WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                THEN NULL
                                              ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                            END
                                          ), 
                                          0, 
                                          1))
                                    )
                                      THEN (
                                        CASE
                                          WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                            THEN NULL
                                          WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                            THEN NULL
                                          ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                        END
                                      )
                                    ELSE (
                                      CONCAT(
                                        SUBSTRING(INPUT_FORENAME, 0, 1), 
                                        (
                                          CASE
                                            WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                              THEN NULL
                                            WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                              THEN NULL
                                            ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                          END
                                        ))
                                    )
                                  END
                                ))
                            ))
                        ), 
                        0, 
                        1), 
                      ' ', 
                      SUBSTRING(
                        (
                          LTRIM(
                            (
                              RTRIM(
                                (
                                  CASE
                                    WHEN (
                                      (
                                        CASE
                                          WHEN (
                                            (
                                              CASE
                                                WHEN (
                                                  (
                                                    CASE
                                                      WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                        THEN NULL
                                                      WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                        THEN NULL
                                                      ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                    END
                                                  ) IS NULL
                                                )
                                                  THEN 'Y'
                                                ELSE 'N'
                                              END
                                            ) = 'Y'
                                          )
                                            THEN 'Y'
                                          WHEN (INPUT_INITIALS_CODE = 'Y')
                                            THEN 'Y'
                                          ELSE 'N'
                                        END
                                      ) = 'Y'
                                    )
                                      THEN (
                                        CONCAT(
                                          SUBSTRING(INPUT_FORENAME, 0, 1), 
                                          (
                                            CASE
                                              WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                THEN NULL
                                              WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                THEN NULL
                                              ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                            END
                                          ))
                                      )
                                    WHEN (
                                      UPPER(SUBSTRING(INPUT_FORENAME, 0, 1)) = UPPER(
                                        SUBSTRING(
                                          (
                                            CASE
                                              WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                THEN NULL
                                              WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                THEN NULL
                                              ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                            END
                                          ), 
                                          0, 
                                          1))
                                    )
                                      THEN (
                                        CASE
                                          WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                            THEN NULL
                                          WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                            THEN NULL
                                          ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                        END
                                      )
                                    ELSE (
                                      CONCAT(
                                        SUBSTRING(INPUT_FORENAME, 0, 1), 
                                        (
                                          CASE
                                            WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                              THEN NULL
                                            WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                              THEN NULL
                                            ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                          END
                                        ))
                                    )
                                  END
                                ))
                            ))
                        ), 
                        1, 
                        1), 
                      ' ', 
                      SUBSTRING(
                        (
                          LTRIM(
                            (
                              RTRIM(
                                (
                                  CASE
                                    WHEN (
                                      (
                                        CASE
                                          WHEN (
                                            (
                                              CASE
                                                WHEN (
                                                  (
                                                    CASE
                                                      WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                        THEN NULL
                                                      WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                        THEN NULL
                                                      ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                    END
                                                  ) IS NULL
                                                )
                                                  THEN 'Y'
                                                ELSE 'N'
                                              END
                                            ) = 'Y'
                                          )
                                            THEN 'Y'
                                          WHEN (INPUT_INITIALS_CODE = 'Y')
                                            THEN 'Y'
                                          ELSE 'N'
                                        END
                                      ) = 'Y'
                                    )
                                      THEN (
                                        CONCAT(
                                          SUBSTRING(INPUT_FORENAME, 0, 1), 
                                          (
                                            CASE
                                              WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                THEN NULL
                                              WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                THEN NULL
                                              ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                            END
                                          ))
                                      )
                                    WHEN (
                                      UPPER(SUBSTRING(INPUT_FORENAME, 0, 1)) = UPPER(
                                        SUBSTRING(
                                          (
                                            CASE
                                              WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                THEN NULL
                                              WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                THEN NULL
                                              ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                            END
                                          ), 
                                          0, 
                                          1))
                                    )
                                      THEN (
                                        CASE
                                          WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                            THEN NULL
                                          WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                            THEN NULL
                                          ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                        END
                                      )
                                    ELSE (
                                      CONCAT(
                                        SUBSTRING(INPUT_FORENAME, 0, 1), 
                                        (
                                          CASE
                                            WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                              THEN NULL
                                            WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                              THEN NULL
                                            ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                          END
                                        ))
                                    )
                                  END
                                ))
                            ))
                        ), 
                        2, 
                        1), 
                      ' ', 
                      SUBSTRING(
                        (
                          LTRIM(
                            (
                              RTRIM(
                                (
                                  CASE
                                    WHEN (
                                      (
                                        CASE
                                          WHEN (
                                            (
                                              CASE
                                                WHEN (
                                                  (
                                                    CASE
                                                      WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                        THEN NULL
                                                      WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                        THEN NULL
                                                      ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                                    END
                                                  ) IS NULL
                                                )
                                                  THEN 'Y'
                                                ELSE 'N'
                                              END
                                            ) = 'Y'
                                          )
                                            THEN 'Y'
                                          WHEN (INPUT_INITIALS_CODE = 'Y')
                                            THEN 'Y'
                                          ELSE 'N'
                                        END
                                      ) = 'Y'
                                    )
                                      THEN (
                                        CONCAT(
                                          SUBSTRING(INPUT_FORENAME, 0, 1), 
                                          (
                                            CASE
                                              WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                THEN NULL
                                              WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                THEN NULL
                                              ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                            END
                                          ))
                                      )
                                    WHEN (
                                      UPPER(SUBSTRING(INPUT_FORENAME, 0, 1)) = UPPER(
                                        SUBSTRING(
                                          (
                                            CASE
                                              WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                                THEN NULL
                                              WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                                THEN NULL
                                              ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                            END
                                          ), 
                                          0, 
                                          1))
                                    )
                                      THEN (
                                        CASE
                                          WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                            THEN NULL
                                          WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                            THEN NULL
                                          ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                        END
                                      )
                                    ELSE (
                                      CONCAT(
                                        SUBSTRING(INPUT_FORENAME, 0, 1), 
                                        (
                                          CASE
                                            WHEN CAST((INPUT_INITIALS IS NULL) AS BOOL)
                                              THEN NULL
                                            WHEN ((LENGTH(INPUT_INITIALS)) = 0)
                                              THEN NULL
                                            ELSE (LTRIM((RTRIM(INPUT_INITIALS))))
                                          END
                                        ))
                                    )
                                  END
                                ))
                            ))
                        ), 
                        3, 
                        1))
                  )))
            ))
        )
      END
    ) AS output_initials,
    (
      CASE
        WHEN CAST((WORK_TITLE2 IS NULL) AS BOOL)
          THEN INITCAP(UPPER((LTRIM((RTRIM(INPUT_TITLE))))))
        ELSE WORK_TITLE2
      END
    ) AS output_title,
    (
      CASE
        WHEN CAST((WORK_TITLE2 IS NULL) AS BOOL)
          THEN (CONCAT('Unknown title', WORK_TITLE2))
        ELSE NULL
      END
    ) AS error_title,
    (
      CASE
        WHEN (SALACIOUS_FORENAME IS NOT NULL)
          THEN NULL
        WHEN (
          (
            CASE
              WHEN (
                (
                  ((LENGTH(CAST((RTRIM((LTRIM(INPUT_FORENAME)))) AS STRING))) = 1)
                  OR (
                       ((IF((0 < 1), 0, (INSTR((RTRIM((LTRIM(INPUT_FORENAME)))), ' ', 0)))) = 1)
                       AND ((LENGTH(CAST(SUBSTRING(INPUT_FORENAME, 0, (IF((1 < 1), 0, (INSTR(INPUT_FORENAME, ' ', 1))))) AS STRING))) = 1)
                     )
                )
                OR ((RTRIM((LTRIM(INPUT_INITIALS)))) = (RTRIM((LTRIM(INPUT_FORENAME)))))
              )
                THEN 1
              ELSE 0
            END
          ) = CAST(1 AS STRING)
        )
          THEN NULL
        WHEN (
          (
            CASE
              WHEN ((REGEXP_INSTR((RTRIM((LTRIM(INPUT_FORENAME)))), ' ', 1, 1)) >= 1)
                THEN (
                  CASE
                    WHEN ((REGEXP_INSTR((LTRIM((RTRIM(INPUT_FORENAME)))), ' ', 1, 1)) = ((LENGTH(CAST((LTRIM((RTRIM(INPUT_FORENAME)))) AS STRING))) - 1))
                      THEN 2
                    WHEN ((REGEXP_INSTR((RTRIM((LTRIM(INPUT_FORENAME)))), ' ', 1, 2)) > 1)
                      THEN (
                        CASE
                          WHEN ((REGEXP_INSTR((LTRIM((RTRIM(INPUT_FORENAME)))), ' ', 1, 2)) = ((LENGTH(CAST((LTRIM((RTRIM(INPUT_FORENAME)))) AS STRING))) - 1))
                            THEN 0
                          ELSE NULL
                        END
                      )
                    ELSE NULL
                  END
                )
              ELSE NULL
            END
          ) = CAST(0 AS STRING)
        )
          THEN INITCAP((RTRIM((LTRIM(INPUT_FORENAME)))))
        WHEN (
          (
            CASE
              WHEN ((REGEXP_INSTR((RTRIM((LTRIM(INPUT_FORENAME)))), ' ', 1, 1)) >= 1)
                THEN (
                  CASE
                    WHEN ((REGEXP_INSTR((LTRIM((RTRIM(INPUT_FORENAME)))), ' ', 1, 1)) = ((LENGTH(CAST((LTRIM((RTRIM(INPUT_FORENAME)))) AS STRING))) - 1))
                      THEN 2
                    WHEN ((REGEXP_INSTR((RTRIM((LTRIM(INPUT_FORENAME)))), ' ', 1, 2)) > 1)
                      THEN (
                        CASE
                          WHEN ((REGEXP_INSTR((LTRIM((RTRIM(INPUT_FORENAME)))), ' ', 1, 2)) = ((LENGTH(CAST((LTRIM((RTRIM(INPUT_FORENAME)))) AS STRING))) - 1))
                            THEN 0
                          ELSE NULL
                        END
                      )
                    ELSE NULL
                  END
                )
              ELSE NULL
            END
          ) = CAST(2 AS STRING)
        )
          THEN INITCAP(SUBSTRING((RTRIM(INPUT_FORENAME)), 0, ((REGEXP_INSTR((RTRIM(INPUT_FORENAME)), ' ', -1, 1)) - 1)))
        ELSE NULL
      END
    ) AS output_forename,
    CASE
      WHEN CAST(isnull(salacious_surname) AS BOOLEAN)
        THEN CASE
          WHEN CAST(isnull(
            CASE
              WHEN (
                substring(
                  upper(
                    CASE
                      WHEN (
                        CASE
                          WHEN (
                            CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                            OR (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                   OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                   )
                                   OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                 ) > 0
                               )
                          )
                            THEN 'Y'
                          ELSE 'N'
                        END = 'N'
                      )
                        THEN input_surname
                      ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                    END), 
                  1, 
                  3) = 'MAC'
              )
                THEN work_surname3__lookup_dbref_value2_lkp_1
              ELSE NULL
            END) AS BOOLEAN)
            THEN CASE
              WHEN (
                substring(
                  upper(
                    CASE
                      WHEN (
                        CASE
                          WHEN (
                            CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                            OR (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                   OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                   )
                                   OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                 ) > 0
                               )
                          )
                            THEN 'Y'
                          ELSE 'N'
                        END = 'N'
                      )
                        THEN input_surname
                      ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                    END), 
                  1, 
                  3) = 'MC-'
              )
                THEN concat(
                  initcap(
                    substring(
                      CASE
                        WHEN (
                          CASE
                            WHEN (
                              CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                              OR (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                     OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                   )
                                                   OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                   ) > 0
                                 )
                            )
                              THEN 'Y'
                            ELSE 'N'
                          END = 'N'
                        )
                          THEN input_surname
                        ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                      END, 
                      1, 
                      2)), 
                  initcap(
                    ltrim(
                      rtrim(
                        substring(
                          CASE
                            WHEN (
                              CASE
                                WHEN (
                                  CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                  OR (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     (
                                                       (
                                                         CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                         OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                       )
                                                       OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                                     )
                                                     OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                                   )
                                                   OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                       ) > 0
                                     )
                                )
                                  THEN 'Y'
                                ELSE 'N'
                              END = 'N'
                            )
                              THEN input_surname
                            ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                          END, 
                          4, 
                          47)))))
              WHEN (
                substring(
                  upper(
                    CASE
                      WHEN (
                        CASE
                          WHEN (
                            CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                            OR (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                   OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                   )
                                   OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                 ) > 0
                               )
                          )
                            THEN 'Y'
                          ELSE 'N'
                        END = 'N'
                      )
                        THEN input_surname
                      ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                    END), 
                  1, 
                  4) = 'MAC-'
              )
                THEN concat(
                  initcap(
                    substring(
                      CASE
                        WHEN (
                          CASE
                            WHEN (
                              CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                              OR (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                     OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                   )
                                                   OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                   ) > 0
                                 )
                            )
                              THEN 'Y'
                            ELSE 'N'
                          END = 'N'
                        )
                          THEN input_surname
                        ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                      END, 
                      1, 
                      3)), 
                  initcap(
                    ltrim(
                      rtrim(
                        substring(
                          CASE
                            WHEN (
                              CASE
                                WHEN (
                                  CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                  OR (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     (
                                                       (
                                                         CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                         OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                       )
                                                       OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                                     )
                                                     OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                                   )
                                                   OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                       ) > 0
                                     )
                                )
                                  THEN 'Y'
                                ELSE 'N'
                              END = 'N'
                            )
                              THEN input_surname
                            ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                          END, 
                          5, 
                          47)))))
              WHEN (
                substring(
                  upper(
                    CASE
                      WHEN (
                        CASE
                          WHEN (
                            CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                            OR (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                   OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                   )
                                   OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                 ) > 0
                               )
                          )
                            THEN 'Y'
                          ELSE 'N'
                        END = 'N'
                      )
                        THEN input_surname
                      ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                    END), 
                  1, 
                  2) = 'MC'
              )
                THEN concat(
                  initcap(
                    substring(
                      CASE
                        WHEN (
                          CASE
                            WHEN (
                              CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                              OR (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                     OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                   )
                                                   OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                   ) > 0
                                 )
                            )
                              THEN 'Y'
                            ELSE 'N'
                          END = 'N'
                        )
                          THEN input_surname
                        ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                      END, 
                      1, 
                      2)), 
                  initcap(
                    ltrim(
                      rtrim(
                        substring(
                          CASE
                            WHEN (
                              CASE
                                WHEN (
                                  CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                  OR (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     (
                                                       (
                                                         CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                         OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                       )
                                                       OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                                     )
                                                     OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                                   )
                                                   OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                       ) > 0
                                     )
                                )
                                  THEN 'Y'
                                ELSE 'N'
                              END = 'N'
                            )
                              THEN input_surname
                            ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                          END, 
                          3, 
                          47)))))
              WHEN (
                substring(
                  upper(
                    CASE
                      WHEN (
                        CASE
                          WHEN (
                            CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                            OR (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                   OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                   )
                                   OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                 ) > 0
                               )
                          )
                            THEN 'Y'
                          ELSE 'N'
                        END = 'N'
                      )
                        THEN input_surname
                      ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                    END), 
                  1, 
                  3) = 'MAC'
              )
                THEN concat(
                  initcap(
                    substring(
                      CASE
                        WHEN (
                          CASE
                            WHEN (
                              CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                              OR (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                     OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                   )
                                                   OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                   ) > 0
                                 )
                            )
                              THEN 'Y'
                            ELSE 'N'
                          END = 'N'
                        )
                          THEN input_surname
                        ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                      END, 
                      1, 
                      3)), 
                  initcap(
                    ltrim(
                      rtrim(
                        substring(
                          CASE
                            WHEN (
                              CASE
                                WHEN (
                                  CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                  OR (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     (
                                                       (
                                                         CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                         OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                       )
                                                       OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                                     )
                                                     OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                                   )
                                                   OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                       ) > 0
                                     )
                                )
                                  THEN 'Y'
                                ELSE 'N'
                              END = 'N'
                            )
                              THEN input_surname
                            ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                          END, 
                          4, 
                          47)))))
              ELSE ltrim(
                rtrim(
                  initcap(
                    CASE
                      WHEN (
                        CASE
                          WHEN (
                            CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                            OR (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                   OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                   )
                                   OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                 ) > 0
                               )
                          )
                            THEN 'Y'
                          ELSE 'N'
                        END = 'N'
                      )
                        THEN input_surname
                      ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                    END)))
            END
          ELSE initcap(
            CASE
              WHEN (
                substring(
                  upper(
                    CASE
                      WHEN (
                        CASE
                          WHEN (
                            CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
                            OR (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                                   OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                                 )
                                                 OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                                               )
                                               OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                                             )
                                             OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                                           )
                                           OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                         )
                                         OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                                       )
                                       OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                                     )
                                     OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                                   )
                                   OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                                 ) > 0
                               )
                          )
                            THEN 'Y'
                          ELSE 'N'
                        END = 'N'
                      )
                        THEN input_surname
                      ELSE substring(input_surname, 1, (locate(' ', input_surname, 0) - 1))
                    END), 
                  1, 
                  3) = 'MAC'
              )
                THEN work_surname3__lookup_dbref_value2_lkp_1
              ELSE NULL
            END)
        END
      ELSE 'Customer'
    END AS output_surname,
    CASE
      WHEN (
        CASE
          WHEN (
            CAST(locate('DECEASED', upper(input_suffix), 0) AS BOOLEAN)
            OR (
                 (
                   (
                     (
                       (
                         (
                           (
                             (
                               (
                                 (
                                   CAST(locate('(DECEASED)', upper(input_suffix), 0) AS BOOLEAN)
                                   OR CAST(locate('DECSD', upper(input_suffix), 0) AS BOOLEAN)
                                 )
                                 OR CAST(locate('(DECSD)', upper(input_suffix), 0) AS BOOLEAN)
                               )
                               OR CAST(locate('DECD', upper(input_suffix), 0) AS BOOLEAN)
                             )
                             OR CAST(locate('(DECD)', upper(input_suffix), 0) AS BOOLEAN)
                           )
                           OR CAST(locate('-DECEASED', upper(input_suffix), 0) AS BOOLEAN)
                         )
                         OR CAST(locate('- DECEASED', upper(input_suffix), 0) AS BOOLEAN)
                       )
                       OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_suffix), 0) AS BOOLEAN)
                     )
                     OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_suffix), 0) AS BOOLEAN)
                   )
                   OR CAST(locate('DECEA', upper(input_suffix), 0) AS BOOLEAN)
                 ) > 0
               )
          )
            THEN 'Y'
          ELSE 'N'
        END = 'Y'
      )
        THEN NULL
      WHEN (input_suffix = 'PHD')
        THEN 'PhD'
      WHEN (input_suffix = 'MSC')
        THEN 'MSc'
      WHEN (input_suffix = 'BSC')
        THEN 'BSc'
      WHEN (input_suffix = 'MA')
        THEN 'MA'
      WHEN (input_suffix = 'BA')
        THEN 'BA'
      WHEN (input_suffix = 'MBA')
        THEN 'MBA'
      WHEN (input_suffix = 'OBE')
        THEN 'OBE'
      WHEN (input_suffix = 'MBE')
        THEN 'MBE'
      WHEN (input_suffix = 'CBE')
        THEN 'CBE'
      WHEN (input_suffix = 'O.B.E')
        THEN 'OBE'
      WHEN (input_suffix = 'M.B.E')
        THEN 'MBE'
      WHEN (input_suffix = 'M.B.A')
        THEN 'MBA'
      WHEN (input_suffix = 'C.B.E')
        THEN 'CBE'
      ELSE input_suffix
    END AS output_suffix,
    (
      CASE
        WHEN (
          (
            CASE
              WHEN (CAST(UPPER(CAST((LTRIM((RTRIM(input_gender)))) AS STRING)) AS STRING) = 'M')
                THEN 'M'
              WHEN (CAST(UPPER(CAST((LTRIM((RTRIM(input_gender)))) AS STRING)) AS STRING) = 'F')
                THEN 'F'
              ELSE work_gender1__lookup_dbref_value3_lkp_2
            END
          ) = 'M'
        )
          THEN 'M'
        WHEN (
          (
            CASE
              WHEN (CAST(UPPER(CAST((LTRIM((RTRIM(input_gender)))) AS STRING)) AS STRING) = 'M')
                THEN 'M'
              WHEN (CAST(UPPER(CAST((LTRIM((RTRIM(input_gender)))) AS STRING)) AS STRING) = 'F')
                THEN 'F'
              ELSE work_gender1__lookup_dbref_value3_lkp_2
            END
          ) = 'F'
        )
          THEN 'F'
        ELSE 'U'
      END
    ) AS output_gender,
    CASE
      WHEN (
        CASE
          WHEN (
            CAST(locate('DECEASED', upper(input_surname), 0) AS BOOLEAN)
            OR (
                 (
                   (
                     (
                       (
                         (
                           (
                             (
                               (
                                 (
                                   CAST(locate('(DECEASED)', upper(input_surname), 0) AS BOOLEAN)
                                   OR CAST(locate('DECSD', upper(input_surname), 0) AS BOOLEAN)
                                 )
                                 OR CAST(locate('(DECSD)', upper(input_surname), 0) AS BOOLEAN)
                               )
                               OR CAST(locate('DECD', upper(input_surname), 0) AS BOOLEAN)
                             )
                             OR CAST(locate('(DECD)', upper(input_surname), 0) AS BOOLEAN)
                           )
                           OR CAST(locate('-DECEASED', upper(input_surname), 0) AS BOOLEAN)
                         )
                         OR CAST(locate('- DECEASED', upper(input_surname), 0) AS BOOLEAN)
                       )
                       OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_surname), 0) AS BOOLEAN)
                     )
                     OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_surname), 0) AS BOOLEAN)
                   )
                   OR CAST(locate('DECEA', upper(input_surname), 0) AS BOOLEAN)
                 ) > 0
               )
          )
            THEN 'Y'
          ELSE 'N'
        END = 'Y'
      )
        THEN 'D'
      WHEN (
        CASE
          WHEN (
            CAST(locate('DECEASED', upper(input_suffix), 0) AS BOOLEAN)
            OR (
                 (
                   (
                     (
                       (
                         (
                           (
                             (
                               (
                                 (
                                   CAST(locate('(DECEASED)', upper(input_suffix), 0) AS BOOLEAN)
                                   OR CAST(locate('DECSD', upper(input_suffix), 0) AS BOOLEAN)
                                 )
                                 OR CAST(locate('(DECSD)', upper(input_suffix), 0) AS BOOLEAN)
                               )
                               OR CAST(locate('DECD', upper(input_suffix), 0) AS BOOLEAN)
                             )
                             OR CAST(locate('(DECD)', upper(input_suffix), 0) AS BOOLEAN)
                           )
                           OR CAST(locate('-DECEASED', upper(input_suffix), 0) AS BOOLEAN)
                         )
                         OR CAST(locate('- DECEASED', upper(input_suffix), 0) AS BOOLEAN)
                       )
                       OR CAST(locate(_concat('DEC', '\'', 'D'), upper(input_suffix), 0) AS BOOLEAN)
                     )
                     OR CAST(locate(_concat('(DEC', '\'', 'D)'), upper(input_suffix), 0) AS BOOLEAN)
                   )
                   OR CAST(locate('DECEA', upper(input_suffix), 0) AS BOOLEAN)
                 ) > 0
               )
          )
            THEN 'Y'
          ELSE 'N'
        END = 'Y'
      )
        THEN 'D'
      ELSE NULL
    END AS output_deceased_flag,
    prophecy_sk AS prophecy_sk
  
  FROM EXP_GENERIC_NAME_REFORMAT_VARS_0 AS in0

),

EXP_CARDSORIG_CLIENT_JOIN AS (

  SELECT 
    in0.output_forename AS output_forename,
    in1.MERGE_CLID_LKP AS MERGE_CLID_LKP,
    in1.EFROM_LKP AS EFROM_LKP,
    in1.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    in1.EFROM AS EFROM,
    in1.GENDER_LKP AS GENDER_LKP,
    in0.output_title AS output_title,
    in1.MERGE_CLID AS MERGE_CLID,
    in1.DOB_LKP AS DOB_LKP,
    in1.CLIENT_STATUS_LKP AS CLIENT_STATUS_LKP,
    in0.output_gender AS output_gender,
    in1.DATE_OF_BIRTH AS DATE_OF_BIRTH,
    in1.SURNAME_LKP AS SURNAME_LKP,
    in1.CLID AS CLID,
    in0.prophecy_sk AS prophecy_sk,
    in0.output_initials AS output_initials,
    in1.FORENAMES_LKP AS FORENAMES_LKP,
    in0.output_surname AS output_surname,
    in1.TITLE_LKP AS TITLE_LKP,
    in1.CLID_LKP AS CLID_LKP
  
  FROM EXP_GENERIC_NAME_REFORMAT AS in0
  INNER JOIN SQ_CLIENT_GENERATE_SK_0 AS in1
     ON (in0.prophecy_sk = in1.prophecy_sk)

),

EXP_CARDSORIG_CLIENT_JOIN_EXPR_26 AS (

  SELECT 
    output_initials AS INITIALS,
    output_title AS TITLE,
    output_forename AS FORENAMES,
    output_surname AS SURNAME,
    output_gender AS GENDER,
    prophecy_sk AS prophecy_sk,
    CLIENT_STATUS_LKP AS CLIENT_STATUS_LKP,
    DATE_OF_BIRTH AS DOB,
    CLID AS CLID,
    MERGE_CLID AS MERGE_CLID,
    EFROM AS EFROM,
    CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    CLID_LKP AS CLID_LKP,
    MERGE_CLID_LKP AS MERGE_CLID_LKP,
    EFROM_LKP AS EFROM_LKP,
    TITLE_LKP AS TITLE_LKP,
    FORENAMES_LKP AS FORENAMES_LKP,
    SURNAME_LKP AS SURNAME_LKP,
    GENDER_LKP AS GENDER_LKP,
    DOB_LKP AS DOB_LKP
  
  FROM EXP_CARDSORIG_CLIENT_JOIN AS in0

),

EXP_CARDSORIG_CLIENT_LOOKUP_41 AS (

  SELECT 
    in0.CURRVALUE AS LOOKUP_VARIABLE_8,
    in0.NAME AS NAME,
    in0.CURRVALUE AS CURRVALUE,
    in1.INITIALS AS INITIALS,
    in1.TITLE AS TITLE,
    in1.FORENAMES AS FORENAMES,
    in1.SURNAME AS SURNAME,
    in1.GENDER AS GENDER,
    in1.prophecy_sk AS prophecy_sk,
    in1.CLIENT_STATUS_LKP AS CLIENT_STATUS_LKP,
    in1.DOB AS DOB,
    in1.CLID AS CLID,
    in1.MERGE_CLID AS MERGE_CLID,
    in1.EFROM AS EFROM,
    in1.CUSTOMER_NUMBER AS CUSTOMER_NUMBER,
    in1.CLID_LKP AS CLID_LKP,
    in1.MERGE_CLID_LKP AS MERGE_CLID_LKP,
    in1.EFROM_LKP AS EFROM_LKP,
    in1.TITLE_LKP AS TITLE_LKP,
    in1.FORENAMES_LKP AS FORENAMES_LKP,
    in1.SURNAME_LKP AS SURNAME_LKP,
    in1.GENDER_LKP AS GENDER_LKP,
    in1.DOB_LKP AS DOB_LKP
  
  FROM DBATTRIBUTE AS in0
  LEFT JOIN EXP_CARDSORIG_CLIENT_JOIN_EXPR_26 AS in1
     ON (in0.NAME = 'businessdate')

),

EXP_CARDSORIG_CLIENT_VARS_0 AS (

  SELECT 
    'businessdate' AS lookup_string,
    LOOKUP_VARIABLE_8 AS business_date_string__find_business_date_lkp_3,
    *
  
  FROM EXP_CARDSORIG_CLIENT_LOOKUP_41 AS in0

)

SELECT *

FROM EXP_CARDSORIG_CLIENT_VARS_0
