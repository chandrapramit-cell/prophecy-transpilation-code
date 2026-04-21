{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "pramit_test"
  })
}}

WITH DBATTRIBUTE AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'DBATTRIBUTE') }}

),

find_business_date AS (

  {{ prophecy_basics.ToDo('Component not supported in sql: TLookup') }}

)

SELECT *

FROM find_business_date
