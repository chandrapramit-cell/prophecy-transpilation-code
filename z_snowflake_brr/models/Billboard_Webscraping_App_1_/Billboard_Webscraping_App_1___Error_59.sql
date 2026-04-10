{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Error_59 AS (

  SELECT CASE
           WHEN (
             NOT(
               (
                 coalesce(
                   CAST((SUBSTRING({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, 1, 4)) AS DOUBLE), 
                   CAST((REGEXP_SUBSTR((SUBSTRING({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, 1, 4)), '^[0-9]+')) AS INTEGER), 
                   0)
               ) < 1958)
           )
             THEN TRUE
           ELSE 1 / 0
         END AS check_config59

)

SELECT *

FROM Error_59
