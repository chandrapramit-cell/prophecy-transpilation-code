{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH AlteryxSelect_41 AS (

  SELECT *
  
  FROM {{ ref('Log_Extraction__AlteryxSelect_41')}}

),

Summarize_37 AS (

  SELECT DISTINCT LOGOUTPUTPATH AS LOGOUTPUTPATH
  
  FROM AlteryxSelect_41 AS in0

),

CountRecords_39 AS (

  SELECT COUNT(*) AS "COUNT"
  
  FROM Summarize_37 AS in0

),

Summarize_38 AS (

  SELECT DISTINCT DIRECTORY_NEW AS DIRECTORY_NEW
  
  FROM AlteryxSelect_41 AS in0

),

AppendFields_40 AS (

  SELECT 
    in1."COUNT" AS NBRLOGS,
    in0.*,
    in1.* EXCLUDE ("COUNT")
  
  FROM Summarize_38 AS in0
  INNER JOIN CountRecords_39 AS in1
     ON TRUE

)

SELECT *

FROM AppendFields_40
