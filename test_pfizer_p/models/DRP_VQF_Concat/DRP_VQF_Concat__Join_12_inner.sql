{{
  config({    
    "materialized": "ephemeral",
    "database": "sony",
    "schema": "orch_test"
  })
}}

WITH AlteryxSelect_34 AS (

  SELECT *
  
  FROM {{ ref('DRP_VQF_Concat__AlteryxSelect_34')}}

),

FieldInfo_11 AS (

  {{ SchemaInfo('AlteryxSelect_34') }}

),

DynamicRename_9 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('DRP_VQF_Concat', 'DynamicRename_9') }}

),

FieldInfo_10 AS (

  {{ SchemaInfo('DynamicRename_9') }}

),

Join_12_inner AS (

  SELECT 
    in1.Name AS Right_Name,
    in1.variableType AS Right_Type,
    in1.Size AS Right_Size,
    in1.Scale AS Right_Scale,
    in1.Source AS Right_Source,
    in1.Description AS Right_Description,
    in0.*,
    in1.* EXCEPT (`Name`, `variableType`, `Size`, `Scale`, `Source`, `Description`)
  
  FROM FieldInfo_10 AS in0
  INNER JOIN FieldInfo_11 AS in1
     ON (in0.Name = in1.Name)

)

SELECT *

FROM Join_12_inner
