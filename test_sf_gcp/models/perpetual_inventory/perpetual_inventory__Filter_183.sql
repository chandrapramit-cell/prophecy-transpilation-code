{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_63 AS (

  SELECT *
  
  FROM {{ ref('perpetual_inventory__AlteryxSelect_63')}}

),

Filter_183 AS (

  SELECT * 
  
  FROM AlteryxSelect_63 AS in0
  
  WHERE (FILEDATE <= (TO_DATE(CAST('2023-01-08' AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')))

)

SELECT *

FROM Filter_183
