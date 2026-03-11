{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH GenerateRows_43_createRow AS (

  {{ tes_tbq_t.create_data(n = 1, alias = 'seq') }}

)

SELECT *

FROM GenerateRows_43_createRow
