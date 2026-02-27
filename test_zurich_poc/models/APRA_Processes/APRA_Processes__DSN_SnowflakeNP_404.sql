{{
  config({    
    "materialized": "table",
    "alias": "DSN_SnowflakeNP_404",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Formula_446_0 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Formula_446_0')}}

)

SELECT *

FROM Formula_446_0
