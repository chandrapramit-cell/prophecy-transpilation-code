{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "QA_SCHEMA"
  })
}}

WITH seed_reserved_keywords AS (

  SELECT * 
  
  FROM {{ ref('seed_reserved_keywords')}}

),

reserved_keywords_output AS (

  SELECT 
    "SELECT" AS "SELECT",
    "FROM" AS "FROM",
    "WHERE" AS "WHERE"
  
  FROM seed_reserved_keywords AS in0

)

SELECT *

FROM reserved_keywords_output
