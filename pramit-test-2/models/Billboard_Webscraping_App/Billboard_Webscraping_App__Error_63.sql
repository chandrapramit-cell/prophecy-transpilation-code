{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_63 AS (

  SELECT CASE
           WHEN (
             NOT CAST((
               (
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
                 ) > 12
               )
               OR (
                    (
                      coalesce(
                        CAST((SUBSTRING({{ var('TEXT_BOX_42') }}, (((LENGTH({{ var('TEXT_BOX_42') }})) - 2) + 1), 2)) AS FLOAT64), 
                        CAST((REGEXP_EXTRACT((SUBSTRING({{ var('TEXT_BOX_42') }}, (((LENGTH({{ var('TEXT_BOX_42') }})) - 2) + 1), 2)), '^[0-9]+', 0)) AS INT64), 
                        0)
                    ) > 31
                  )
             ) AS BOOLEAN)
           )
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 63')
         END AS check_config63

)

SELECT *

FROM Error_63
