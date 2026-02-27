{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default"
  })
}}

WITH Sample_73 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___Sample_73')}}

),

Formula_33_to_Formula_34_0 AS (

  SELECT 
    CAST((CONCAT(Directory, 'Archive\\')) AS string) AS Landing_Directory,
    CAST(CURRENT_TIMESTAMP AS string) AS MOVE_TIMESTAMP,
    *
  
  FROM Sample_73 AS in0

),

Formula_33_to_Formula_34_1 AS (

  SELECT 
    CAST((CONCAT('IF EXIST "', FullPath, '" (move "', FullPath, '"  "', Landing_Directory, FileName, '")')) AS string) AS RunCmd,
    *
  
  FROM Formula_33_to_Formula_34_0 AS in0

),

RunCommand_37 AS (

  {{ prophecy_basics.ToDo('Transpiler does not support running custom files as scripts') }}

)

SELECT *

FROM RunCommand_37
