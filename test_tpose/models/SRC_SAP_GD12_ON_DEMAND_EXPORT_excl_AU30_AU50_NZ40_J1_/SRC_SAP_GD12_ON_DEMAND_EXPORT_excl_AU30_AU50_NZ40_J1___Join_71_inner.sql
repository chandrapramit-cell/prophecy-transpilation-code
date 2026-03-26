{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH TextInput_70_cast AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___TextInput_70_cast')}}

),

RegEx_68 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___RegEx_68')}}

),

Join_71_inner AS (

  {#VisualGroup: J1directory#}
  SELECT 
    in0.*,
    in1.* EXCEPT (`COMPANY_CODE`)
  
  FROM RegEx_68 AS in0
  INNER JOIN TextInput_70_cast AS in1
     ON (in0.CC_Check = in1.COMPANY_CODE)

)

SELECT *

FROM Join_71_inner
