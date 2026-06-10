{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH Formula_27_0 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory_details__Formula_27_0')}}

),

AlteryxSelect_39 AS (

  SELECT StartDate AS StartDate
  
  FROM Formula_27_0 AS in0

)

SELECT *

FROM AlteryxSelect_39
