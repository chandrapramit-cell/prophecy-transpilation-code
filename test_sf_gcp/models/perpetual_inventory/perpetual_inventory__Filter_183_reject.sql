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

Filter_183_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_63 AS in0
  
  WHERE (
          (
            NOT(
              FILEDATE <= (TO_DATE(CAST('2023-01-08' AS string), 'YYYY-MM-DD HH24:MI:SS.FF4')))
          )
          OR ((FILEDATE <= (TO_DATE(CAST('2023-01-08' AS string), 'YYYY-MM-DD HH24:MI:SS.FF4'))) IS NULL)
        )

)

SELECT *

FROM Filter_183_reject
