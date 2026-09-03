{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH AlteryxSelect_3342 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__AlteryxSelect_3342')}}

),

Filter_1054 AS (

  SELECT * 
  
  FROM AlteryxSelect_3342 AS in0
  
  WHERE (
          NOT(
            (LENGTH(`Mas90 Customer Number`)) = 0)
        )

),

Summarize_1055 AS (

  SELECT 
    DISTINCT Sector AS Sector,
    `Territory Name` AS `Territory Name`,
    `Mas90 Customer Number` AS `Mas90 Customer Number`,
    `Account Owner` AS `Account Owner`,
    State AS State,
    `Partner Success Owner` AS `Partner Success Owner`,
    variableType AS variableType
  
  FROM Filter_1054 AS in0

),

Unique_1094 AS (

  SELECT * 
  
  FROM Summarize_1055 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY `Mas90 Customer Number` ORDER BY `Mas90 Customer Number`) = 1

)

SELECT *

FROM Unique_1094
