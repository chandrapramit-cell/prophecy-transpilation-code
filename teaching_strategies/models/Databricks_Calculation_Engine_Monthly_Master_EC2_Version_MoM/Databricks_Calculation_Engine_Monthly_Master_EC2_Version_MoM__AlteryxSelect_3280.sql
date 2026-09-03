{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH CrossTab_3267 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__CrossTab_3267')}}

),

FieldInfo_3278 AS (

  {{ SchemaInfo('CrossTab_3267') }}

),

Formula_3279_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (UPPER(variableType) = UPPER('Int64'))
          THEN CAST((CONCAT(Name, ' Count')) AS string)
        ELSE Name
      END
    ) AS string) AS `Updated Name`,
    *
  
  FROM FieldInfo_3278 AS in0

),

AlteryxSelect_3280 AS (

  SELECT 
    Name AS Name,
    `Updated Name` AS `Updated Name`
  
  FROM Formula_3279_0 AS in0

)

SELECT *

FROM AlteryxSelect_3280
