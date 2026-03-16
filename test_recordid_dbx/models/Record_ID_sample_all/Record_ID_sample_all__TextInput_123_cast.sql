{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH TextInput_123 AS (

  SELECT * 
  
  FROM {{ ref('seed_123')}}

),

TextInput_123_cast AS (

  SELECT 
    CAST(CustomerID AS INTEGER) AS CustomerID,
    CAST(FirstName AS STRING) AS FirstName,
    CAST(LastName AS STRING) AS LastName,
    CAST(Gender AS STRING) AS Gender,
    (
      CASE
        WHEN ((TRY_TO_TIMESTAMP(CAST(JoinDate AS STRING), 'yyyy-MM-dd HH:mm:ss.SSSS')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(JoinDate AS STRING), 'yyyy-MM-dd HH:mm:ss.SSSS'))
        WHEN ((TRY_TO_TIMESTAMP(CAST(JoinDate AS STRING), 'yyyy-MM-dd HH:mm:ss')) IS NOT NULL)
          THEN (TRY_TO_TIMESTAMP(CAST(JoinDate AS STRING), 'yyyy-MM-dd HH:mm:ss'))
        ELSE (TRY_TO_TIMESTAMP(CAST(JoinDate AS STRING), 'yyyy-MM-dd'))
      END
    ) AS JoinDate,
    CAST(Region AS STRING) AS Region
  
  FROM TextInput_123 AS in0

)

SELECT *

FROM TextInput_123_cast
