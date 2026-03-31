{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Formula_27_0 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory_details__Formula_27_0')}}

),

AlteryxSelect_48 AS (

  SELECT ENDDATE AS ENDDATE
  
  FROM Formula_27_0 AS in0

)

SELECT *

FROM AlteryxSelect_48
