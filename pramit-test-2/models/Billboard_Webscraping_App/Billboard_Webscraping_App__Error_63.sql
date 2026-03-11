{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH Error_63 AS (

  SELECT CASE
           WHEN not(
             CAST((
               (
                 coalesce(
                   CAST(substring(
                     substring({{ var('Text_Box_42') }}, 1, 7), 
                     ((length(substring({{ var('Text_Box_42') }}, 1, 7)) - 2) + 1), 
                     2) AS DOUBLE), 
                   CAST(regexp_extract(
                     substring(
                       substring({{ var('Text_Box_42') }}, 1, 7), 
                       ((length(substring({{ var('Text_Box_42') }}, 1, 7)) - 2) + 1), 
                       2), 
                     '^[0-9]+', 
                     0) AS INT), 
                   0) > 12
               )
               OR (
                    coalesce(
                      CAST(substring({{ var('Text_Box_42') }}, ((length({{ var('Text_Box_42') }}) - 2) + 1), 2) AS DOUBLE), 
                      CAST(regexp_extract(substring({{ var('Text_Box_42') }}, ((length({{ var('Text_Box_42') }}) - 2) + 1), 2), '^[0-9]+', 0) AS INT), 
                      0) > 31
                  )
             ) AS BOOLEAN))
             THEN TRUE
           ELSE ERROR('Error validating config for tool: 63')
         END AS check_config63

)

SELECT *

FROM Error_63
