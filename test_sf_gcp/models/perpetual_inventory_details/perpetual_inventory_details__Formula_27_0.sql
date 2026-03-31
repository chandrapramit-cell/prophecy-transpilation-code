{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH TextInput_26 AS (

  SELECT * 
  
  FROM {{ ref('seed_26')}}

),

TextInput_26_cast AS (

  SELECT 
    CAST((TRY_TO_TIMESTAMP(CAST(STARTDATE AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE) AS STARTDATE,
    CAST((TRY_TO_TIMESTAMP(CAST(ENDDATE AS string), 'yyyy-MM-dd HH:mm:ss.SSSS')) AS DATE) AS ENDDATE
  
  FROM TextInput_26 AS in0

),

Formula_27_0 AS (

  SELECT 
    (DATEADD('day', 1, ENDDATE)) AS ENDDATE,
    * EXCLUDE ("ENDDATE")
  
  FROM TextInput_26_cast AS in0

)

SELECT *

FROM Formula_27_0
