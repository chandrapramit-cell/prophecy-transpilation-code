{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Error_58 AS (

  SELECT CASE
           WHEN (
             NOT(
               NOT(
                 (
                   LENGTH(
                     (REGEXP_SUBSTR({{ var('INPUT_A_DATE_TO_SEE_CORRESPONDING_BILLBOARD_HOT_100_DATA__') }}, '\\d{4}-\\d{2}-\\d{2}')))
                 ) > 0))
           )
             THEN TRUE
           ELSE RAISE_ERROR('Error validating config for tool: 58')
         END AS check_config58

)

SELECT *

FROM Error_58
