{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
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

  SELECT 
    in0.*,
    in1.* EXCLUDE ("COMPANY_CODE")
  
  FROM RegEx_68 AS in0
  INNER JOIN TextInput_70_cast AS in1
     ON (in0.CC_CHECK = in1.COMPANY_CODE)

)

SELECT *

FROM Join_71_inner
