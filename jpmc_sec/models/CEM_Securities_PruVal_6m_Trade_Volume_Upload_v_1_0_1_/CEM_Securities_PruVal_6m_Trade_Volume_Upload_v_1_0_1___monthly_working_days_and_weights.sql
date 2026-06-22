{{
  config({    
    "materialized": "table",
    "alias": "catalog_privileges",
    "database": "agent_testing",
    "schema": "information_schema"
  })
}}

WITH Configuration_t_83 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Configuration_t_83_ref') }}

)

{#Refreshes catalog privileges from the agent testing information schema, overwriting the existing table.#}
SELECT *

FROM Configuration_t_83
