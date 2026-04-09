{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Error_64 AS (

  SELECT CASE
           WHEN (
             NOT(
               NOT(
                 CONTAINS(
                   (coalesce(LOWER({{ var('SELECT_WHICH_FIELDS_YOU_WOULD_LIKE_TO_SEE_DISPLAYED') }}), '')), 
                   LOWER('True'))))
           )
             THEN TRUE
           ELSE RAISE_ERROR('Error validating config for tool: 64')
         END AS check_config64

)

SELECT *

FROM Error_64
