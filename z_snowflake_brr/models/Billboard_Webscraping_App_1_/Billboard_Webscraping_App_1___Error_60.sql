{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Error_60 AS (

  SELECT CASE
           WHEN (
             NOT(
               (
                 (
                   (
                     coalesce(
                       CAST((SUBSTRING({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, 1, 4)) AS DOUBLE), 
                       CAST((REGEXP_SUBSTR((SUBSTRING({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, 1, 4)), '^[0-9]+')) AS INTEGER), 
                       0)
                   ) <= 1958
                 )
                 AND (
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
                       ) < 8
                     )
               )
               AND (
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
                     ) > 0
                   ))
           )
             THEN TRUE
           ELSE 1 / 0
         END AS check_config60

)

SELECT *

FROM Error_60
