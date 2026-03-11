{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_60 AS (

  SELECT CASE
           WHEN (
             NOT CAST((
               (
                 (
                   (
                     coalesce(
                       CAST((SUBSTRING({{ var('TEXT_BOX_42') }}, 1, 4)) AS FLOAT64), 
                       CAST((REGEXP_EXTRACT((SUBSTRING({{ var('TEXT_BOX_42') }}, 1, 4)), '^[0-9]+', 0)) AS INT64), 
                       0)
                   ) <= 1958
                 )
                 AND (
                       (
                         coalesce(
                           CAST((
                             SUBSTRING(
                               (SUBSTRING({{ var('TEXT_BOX_42') }}, 1, 7)), 
                               (((LENGTH((SUBSTRING({{ var('TEXT_BOX_42') }}, 1, 7)))) - 2) + 1), 
                               2)
                           ) AS FLOAT64), 
                           CAST((
                             REGEXP_EXTRACT(
                               (
                                 SUBSTRING(
                                   (SUBSTRING({{ var('TEXT_BOX_42') }}, 1, 7)), 
                                   (((LENGTH((SUBSTRING({{ var('TEXT_BOX_42') }}, 1, 7)))) - 2) + 1), 
                                   2)
                               ), 
                               '^[0-9]+', 
                               0)
                           ) AS INT64), 
                           0)
                       ) < 8
                     )
               )
               AND (
                     (
                       coalesce(
                         CAST((SUBSTRING({{ var('TEXT_BOX_42') }}, (((LENGTH({{ var('TEXT_BOX_42') }})) - 2) + 1), 2)) AS FLOAT64), 
                         CAST((REGEXP_EXTRACT((SUBSTRING({{ var('TEXT_BOX_42') }}, (((LENGTH({{ var('TEXT_BOX_42') }})) - 2) + 1), 2)), '^[0-9]+', 0)) AS INT64), 
                         0)
                     ) > 0
                   )
             ) AS BOOLEAN)
           )
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 60')
         END AS check_config60

)

SELECT *

FROM Error_60
