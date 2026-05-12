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

AlteryxSelect_22 AS (

  SELECT 
    FIELD_1 AS FIELD_1,
    LOGOUTPUTPATH AS LOGOUTPUTPATH
  
  FROM AlteryxSelect_41 AS in0

)

SELECT *

FROM AlteryxSelect_22
