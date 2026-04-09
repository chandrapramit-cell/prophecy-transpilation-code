{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Error_63 AS (

  SELECT CASE
           WHEN (
             NOT(
               (
                 (
                   coalesce(
                     CAST((
                       SUBSTRING(
                         (SUBSTRING({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, 1, 7)), 
                         (
                           (
                             (LENGTH((SUBSTRING({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, 1, 7))))
                             - 2
                           )
                           + 1
                         ), 
                         2)
                     ) AS DOUBLE), 
                     CAST((
                       REGEXP_SUBSTR(
                         (
                           SUBSTRING(
                             (SUBSTRING({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, 1, 7)), 
                             (
                               (
                                 (LENGTH((SUBSTRING({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, 1, 7))))
                                 - 2
                               )
                               + 1
                             ), 
                             2)
                         ), 
                         '^[0-9]+')
                     ) AS INTEGER), 
                     0)
                 ) > 12
               )
               OR (
                    (
                      coalesce(
                        CAST((
                          SUBSTRING(
                            {{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, 
                            (((LENGTH({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }})) - 2) + 1), 
                            2)
                        ) AS DOUBLE), 
                        CAST((
                          REGEXP_SUBSTR(
                            (
                              SUBSTRING(
                                {{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, 
                                (((LENGTH({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }})) - 2) + 1), 
                                2)
                            ), 
                            '^[0-9]+')
                        ) AS INTEGER), 
                        0)
                    ) > 31
                  ))
           )
             THEN TRUE
           ELSE RAISE_ERROR('Error validating config for tool: 63')
         END AS check_config63

)

SELECT *

FROM Error_63
