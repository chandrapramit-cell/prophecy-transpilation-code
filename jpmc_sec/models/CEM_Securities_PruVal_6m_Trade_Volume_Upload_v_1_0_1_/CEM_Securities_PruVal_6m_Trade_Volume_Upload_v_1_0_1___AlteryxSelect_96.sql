{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Configuration_t_88 AS (

  SELECT * 
  
  FROM {{ source('transpiled_sources', 'Configuration_t_88_ref') }}

),

AlteryxSelect_96 AS (

  SELECT 
    `Bond description` AS `Instrument Description`,
    * EXCEPT (`Bond description`)
  
  FROM Configuration_t_88 AS in0

)

SELECT *

FROM AlteryxSelect_96
