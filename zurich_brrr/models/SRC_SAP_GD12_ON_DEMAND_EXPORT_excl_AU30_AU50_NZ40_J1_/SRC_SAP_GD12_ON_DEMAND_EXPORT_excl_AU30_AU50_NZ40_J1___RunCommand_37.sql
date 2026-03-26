{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Sample_73 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Sample_73')}}

),

Formula_33_to_Formula_34_0 AS (

  SELECT 
    CAST((CONCAT(DIRECTORY, 'Archive\\')) AS VARstring) AS LANDING_DIRECTORY,
    CAST(CURRENT_TIMESTAMP AS VARstring) AS MOVE_TIMESTAMP,
    *
  
  FROM Sample_73 AS in0

),

Formula_33_to_Formula_34_1 AS (

  SELECT 
    CAST((CONCAT('IF EXIST "', FULLPATH, '" (move "', FULLPATH, '"  "', LANDING_DIRECTORY, FILENAME, '")')) AS VARstring) AS RUNCMD,
    *
  
  FROM Formula_33_to_Formula_34_0 AS in0

),

RunCommand_37 AS (

  {{ prophecy_basics.ToDo('Transpiler does not support running custom files as scripts') }}

)

SELECT *

FROM RunCommand_37
