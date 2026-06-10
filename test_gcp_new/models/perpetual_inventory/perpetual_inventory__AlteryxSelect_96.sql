{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Formula_95_0 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory__Formula_95_0')}}

),

AlteryxSelect_96 AS (

  SELECT StartDate AS StartDate
  
  FROM Formula_95_0 AS in0

)

SELECT *

FROM AlteryxSelect_96
