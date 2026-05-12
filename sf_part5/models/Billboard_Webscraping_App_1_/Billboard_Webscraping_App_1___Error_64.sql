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
                 coalesce(
                   (POSITION(LOWER('True') IN LOWER({{ var('SELECT_WHICH_FIELDS_YOU_WOULD_LIKE_TO_SEE_DISPLAYED') }})) > 0), 
                   FALSE)))
           )
             THEN TRUE
           ELSE RAISE_ERROR('Error validating config for tool: 64')
         END AS check_config64

)

SELECT *

FROM Error_64
