{{
  config({    
    "materialized": "table",
    "alias": "r1",
    "database": "agent_testing",
    "schema": "information_schema"
  })
}}

WITH Configuration_t_83 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Configuration_t_83_ref') }}

)

{#Overwrites the r1 table with the latest agent testing information to keep the agent dataset current and consistent.#}
SELECT *

FROM Configuration_t_83
