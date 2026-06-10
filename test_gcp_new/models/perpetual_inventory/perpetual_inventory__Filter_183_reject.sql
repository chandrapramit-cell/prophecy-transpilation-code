{{
  config({    
    "materialized": "ephemeral",
    "database": "anubhav",
    "schema": "default"
  })
}}

WITH AlteryxSelect_63 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory__AlteryxSelect_63')}}

),

Filter_183_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_63 AS in0
  
  WHERE (NOT ((FileDate <= to_date('2023-01-08'))) OR isnull((FileDate <= to_date('2023-01-08'))))

)

SELECT *

FROM Filter_183_reject
