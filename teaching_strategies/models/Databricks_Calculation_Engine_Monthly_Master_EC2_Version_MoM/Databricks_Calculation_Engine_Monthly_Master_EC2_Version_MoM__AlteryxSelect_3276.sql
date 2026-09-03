{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH CrossTab_3266 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__CrossTab_3266')}}

),

FieldInfo_3274 AS (

  {{ SchemaInfo('CrossTab_3266') }}

),

Formula_3275_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (UPPER(variableType) = UPPER('Double'))
          THEN CAST((CONCAT(Name, ' Amount')) AS string)
        ELSE Name
      END
    ) AS string) AS `Updated Name`,
    *
  
  FROM FieldInfo_3274 AS in0

),

AlteryxSelect_3276 AS (

  SELECT 
    Name AS Name,
    `Updated Name` AS `Updated Name`
  
  FROM Formula_3275_0 AS in0

)

SELECT *

FROM AlteryxSelect_3276
