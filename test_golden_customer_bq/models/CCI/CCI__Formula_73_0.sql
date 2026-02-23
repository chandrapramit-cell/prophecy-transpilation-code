{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH dcm_phpdatabase_72 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'dcm_phpdatabase_72_ref') }}

),

Formula_73_0 AS (

  SELECT 
    0 AS POINTS,
    *
  
  FROM dcm_phpdatabase_72 AS in0

)

SELECT *

FROM Formula_73_0
