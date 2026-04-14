{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_213 AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT * 
  
  FROM {{ ref('seed_213')}}

),

TextInput_213_cast AS (

  {#VisualGroup: SKUOptionmapping#}
  SELECT 
    ATTRIBUTE_NO__ AS ATTRIBUTE_NO__,
    CAST(ATTRIBUTE AS STRING) AS ATTRIBUTE,
    CAST("THUOC TINH" AS STRING) AS "THUOC TINH",
    CAST("ATTRIBUTE CODE" AS STRING) AS "ATTRIBUTE CODE"
  
  FROM TextInput_213 AS in0

)

SELECT *

FROM TextInput_213_cast
