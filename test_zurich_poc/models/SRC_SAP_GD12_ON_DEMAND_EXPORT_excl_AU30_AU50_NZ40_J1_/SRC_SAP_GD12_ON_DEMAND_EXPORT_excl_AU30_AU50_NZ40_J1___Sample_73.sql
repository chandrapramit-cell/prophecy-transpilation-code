{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Union_44 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Union_44')}}

),

Sample_73 AS (

  {{ prophecy_basics.Sample('Union_44', [], 1002, 'firstN', 8) }}

)

SELECT *

FROM Sample_73
