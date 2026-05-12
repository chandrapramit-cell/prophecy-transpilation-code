{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_20 AS (

  SELECT * 
  
  FROM {{ ref('seed_20')}}

),

TextInput_20_cast AS (

  SELECT CAST(URL AS STRING) AS URL
  
  FROM TextInput_20 AS in0

),

Download_21_21_requestComponents_0 AS (

  SELECT 
    '' AS _PARAMSORDATA,
    (
      CASE
        WHEN ((URL LIKE 'https://%') OR (URL LIKE 'http://%'))
          THEN URL
        ELSE CAST((CONCAT('https://', URL)) AS string)
      END
    ) AS _PROCESSEDURL,
    *
  
  FROM TextInput_20_cast AS in0

)

SELECT *

FROM Download_21_21_requestComponents_0
