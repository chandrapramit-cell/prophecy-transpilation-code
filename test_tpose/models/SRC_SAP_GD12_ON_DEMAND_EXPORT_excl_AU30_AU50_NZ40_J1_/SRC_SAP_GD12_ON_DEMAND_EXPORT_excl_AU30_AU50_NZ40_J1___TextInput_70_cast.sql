{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_70 AS (

  {#VisualGroup: J1directory#}
  SELECT * 
  
  FROM {{ ref('seed_70')}}

),

TextInput_70_cast AS (

  {#VisualGroup: J1directory#}
  SELECT CAST(COMPANY_CODE AS string) AS COMPANY_CODE
  
  FROM TextInput_70 AS in0

)

SELECT *

FROM TextInput_70_cast
