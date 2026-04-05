{{
  config({    
    "materialized": "ephemeral",
    "database": "prophecy-databricks-qa",
    "schema": "anurag_test"
  })
}}

WITH TextInput_123 AS (

  SELECT * 
  
  FROM {{ ref('seed_123')}}

),

TextInput_123_cast AS (

  SELECT 
    CustomerID AS CustomerID,
    CAST(FirstName AS STRING) AS FirstName,
    CAST(LastName AS STRING) AS LastName,
    CAST(Gender AS STRING) AS Gender,
    (
      CASE
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(JoinDate AS string)) IS NOT NULL)
          THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%E3%f%f', CAST(JoinDate AS string))
        WHEN (SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(JoinDate AS string)) IS NOT NULL)
          THEN SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%%f', CAST(JoinDate AS string))
        ELSE SAFE.PARSE_TIMESTAMP('%Y-%m-%d', CAST(JoinDate AS string))
      END
    ) AS JoinDate,
    CAST(Region AS STRING) AS Region
  
  FROM TextInput_123 AS in0

)

SELECT *

FROM TextInput_123_cast
