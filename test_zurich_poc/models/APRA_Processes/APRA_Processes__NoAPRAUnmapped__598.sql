{{
  config({    
    "materialized": "table",
    "alias": "NoAPRAUnmapped__598",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_482 AS (

  SELECT *
  
  FROM {{ ref('APRA_Processes__Union_482')}}

),

Sample_600 AS (

  {{ prophecy_basics.Sample('Union_482', [], 1002, 'firstN', 1) }}

),

Formula_601_0 AS (

  SELECT 
    CAST('No mapping issues' AS STRING) AS Title,
    *
  
  FROM Sample_600 AS in0

)

SELECT *

FROM Formula_601_0
