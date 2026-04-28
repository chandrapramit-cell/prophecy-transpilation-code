{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH DBreference AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'DBreference') }}

),

lookup_dbref_value4 AS (

  {{ prophecy_basics.ToDo('Component not supported in sql: TLookup') }}

)

SELECT *

FROM lookup_dbref_value4
