{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH Union_44 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Union_44')}}

),

CountRecords_6 AS (

  {#VisualGroup: Checkanyfilestoread#}
  SELECT COUNT('1') AS `Count`
  
  FROM Union_44 AS in0

),

Filter_7 AS (

  {#VisualGroup: Checkanyfilestoread#}
  SELECT * 
  
  FROM CountRecords_6 AS in0
  
  WHERE (Count = CAST('0' AS INTEGER))

)

SELECT *

FROM Filter_7
