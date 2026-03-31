{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_95_0 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory__Formula_95_0')}}

),

AlteryxSelect_96 AS (

  SELECT STARTDATE AS STARTDATE
  
  FROM Formula_95_0 AS in0

)

SELECT *

FROM AlteryxSelect_96
