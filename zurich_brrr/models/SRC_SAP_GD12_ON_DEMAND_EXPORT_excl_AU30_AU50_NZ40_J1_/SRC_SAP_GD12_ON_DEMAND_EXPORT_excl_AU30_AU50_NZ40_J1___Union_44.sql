{{
  config({    
    "materialized": "ephemeral",
    "database": "QA_DATABASE",
    "schema": "PUBLIC"
  })
}}

WITH Directory_43 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1_', 'Directory_43') }}

),

TextInput_70_cast AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___TextInput_70_cast')}}

),

RegEx_68 AS (

  SELECT *
  
  FROM {{ ref('SRC_SAP_GD12_ON_DEMAND_EXPORT_excl_AU30_AU50_NZ40_J1___RegEx_68')}}

),

Join_71_left AS (

  SELECT in0.*
  
  FROM RegEx_68 AS in0
  LEFT JOIN TextInput_70_cast AS in1
     ON (in0.CC_CHECK = in1.COMPANY_CODE)

),

Directory_43_reformat AS (

  SELECT 
    PATH AS FULLPATH,
    PARENT_DIRECTORY AS DIRECTORY,
    NAME AS FILENAME,
    NAME AS SHORTFILENAME,
    CREATION_TIME AS CREATIONTIME,
    MODIFICATION_TIME AS LASTACCESSTIME,
    MODIFICATION_TIME AS LASTWRITETIME,
    SIZE_IN_BYTES AS SIZE,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTEARCHIVE,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTECOMPRESSED,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTEENCRYPTED,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTEHIDDEN,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTEREADONLY,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTESYSTEM,
    CAST(NULL AS BOOLEAN) AS ATTRIBUTETEMPORARY
  
  FROM Directory_43 AS in0

),

Union_44 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_71_left', 'Directory_43_reformat'], 
      [
        '[{"name": "ATTRIBUTETEMPORARY", "dataType": "Boolean"}, {"name": "ATTRIBUTEENCRYPTED", "dataType": "Boolean"}, {"name": "ATTRIBUTESYSTEM", "dataType": "Boolean"}, {"name": "FILENAME", "dataType": "String"}, {"name": "ATTRIBUTECOMPRESSED", "dataType": "Boolean"}, {"name": "CC_CHECK", "dataType": "String"}, {"name": "SIZE", "dataType": "Integer"}, {"name": "ATTRIBUTEARCHIVE", "dataType": "Boolean"}, {"name": "LASTACCESSTIME", "dataType": "Timestamp"}, {"name": "DIRECTORY", "dataType": "String"}, {"name": "ATTRIBUTEREADONLY", "dataType": "Boolean"}, {"name": "CREATIONTIME", "dataType": "Timestamp"}, {"name": "FULLPATH", "dataType": "String"}, {"name": "LASTWRITETIME", "dataType": "Timestamp"}, {"name": "SHORTFILENAME", "dataType": "String"}, {"name": "ATTRIBUTEHIDDEN", "dataType": "Boolean"}]', 
        '[{"name": "ATTRIBUTETEMPORARY", "dataType": "Boolean"}, {"name": "ATTRIBUTEENCRYPTED", "dataType": "Boolean"}, {"name": "ATTRIBUTESYSTEM", "dataType": "Boolean"}, {"name": "FILENAME", "dataType": "String"}, {"name": "ATTRIBUTECOMPRESSED", "dataType": "Boolean"}, {"name": "SIZE", "dataType": "Integer"}, {"name": "ATTRIBUTEARCHIVE", "dataType": "Boolean"}, {"name": "LASTACCESSTIME", "dataType": "Timestamp"}, {"name": "DIRECTORY", "dataType": "String"}, {"name": "ATTRIBUTEREADONLY", "dataType": "Boolean"}, {"name": "CREATIONTIME", "dataType": "Timestamp"}, {"name": "FULLPATH", "dataType": "String"}, {"name": "LASTWRITETIME", "dataType": "Timestamp"}, {"name": "SHORTFILENAME", "dataType": "String"}, {"name": "ATTRIBUTEHIDDEN", "dataType": "Boolean"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_44
