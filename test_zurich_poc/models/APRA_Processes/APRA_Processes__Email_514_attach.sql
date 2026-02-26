{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH AppendFields_641 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__AppendFields_641')}}

),

Email_514_attach AS (

  SELECT 
    Filename AS Filename,
    Unmapped_Filename AS Unmapped_Filename,
    MappingFile AS MappingFile
  
  FROM AppendFields_641 AS in0

)

SELECT *

FROM Email_514_attach
