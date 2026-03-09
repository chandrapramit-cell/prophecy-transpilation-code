{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "qa_test_dataset"
  })
}}

WITH TextInput_2 AS (

  SELECT * 
  
  FROM {{ ref('seed_2')}}

),

TextInput_2_cast AS (

  SELECT CAST(URL AS STRING) AS URL
  
  FROM TextInput_2 AS in0

),

Download_1_1_requestComponents_0 AS (

  SELECT 
    to_json(
      map(
        'Content-Type', 
        'Application/json', 
        'charset', 
        'utf-8', 
        'cookie', 
        'session=53616c7465645f5fc3913806ecf94697b2e0b82d5a0f81e0ce61d2ba17a3dda6e11cb48d600350402232e44646e4b8d85d4eba34489874efe6fffd130deeb09c')) AS _headers,
    '' AS _paramsOrData,
    (
      CASE
        WHEN ((URL LIKE 'https://%') OR (URL LIKE 'http://%'))
          THEN URL
        ELSE CAST((CONCAT('https://', URL)) AS STRING)
      END
    ) AS _processedUrl,
    *
  
  FROM TextInput_2_cast AS in0

)

SELECT *

FROM Download_1_1_requestComponents_0
