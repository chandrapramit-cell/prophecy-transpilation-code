{{
  config({    
    "materialized": "table",
    "alias": "DSN_SnowflakePR_20",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH AlteryxSelect_25 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___AlteryxSelect_25')}}

)

SELECT *

FROM AlteryxSelect_25
