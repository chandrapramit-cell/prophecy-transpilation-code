{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Table_1 AS (

  SELECT * 
  
  FROM {{ ref('se2')}}

),

TextInput_42_cast AS (

  SELECT 
    CAST(DAY_OF_WEEK AS STRING) AS DAY_OF_WEEK,
    DAY_OF_MONTH AS DAY_OF_MONTH
  
  FROM Table_1 AS in0

)

SELECT *

FROM TextInput_42_cast
