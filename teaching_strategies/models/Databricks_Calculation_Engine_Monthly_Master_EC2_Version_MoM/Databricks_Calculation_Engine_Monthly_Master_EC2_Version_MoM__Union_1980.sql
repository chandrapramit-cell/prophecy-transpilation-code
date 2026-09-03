{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH FullStack_Stati_2085 AS (

  SELECT *
  
  FROM {{
    prophecy_tmp_source(
      'Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM', 
      'FullStack_Stati_2085'
    )
  }}

),

Formula_2006_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2006_0')}}

),

Filter_2009 AS (

  SELECT * 
  
  FROM FullStack_Stati_2085 AS in0
  
  WHERE (RevMonth < to_date(date_trunc('month', {{ var('User__Current_Period') }})))

),

Unique_123 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Unique_123')}}

),

Join_3113_left_UnionLeftOuter AS (

  SELECT 
    in0.Product AS Product,
    in0.`Account Size` AS `Account Size`,
    in0.Sector AS Sector,
    in0.variableType AS variableType,
    in0.`Territory Name` AS `Territory Name`,
    in0.State AS State,
    in0.`Account Owner` AS `Account Owner`,
    in0.`Partner Success Owner` AS `Partner Success Owner`,
    in0.RevMonth AS RevMonth,
    in0.Quantity AS Quantity,
    in0.ARR AS ARR,
    in0.StaticHistoryMonth AS StaticHistoryMonth,
    in0.StaticHistoryYearEnd AS StaticHistoryYearEnd,
    in0.YetToRenewARR AS YetToRenewARR,
    in0.MRR AS MRR,
    in0.MaxIteration AS MaxIteration,
    in0.ChangeLog AS ChangeLog,
    (
      CASE
        WHEN (in0.CustomerName = in1.`Loser Mas90 Customer Number`)
          THEN in1.`Winner Mas90 Customer Number`
        ELSE NULL
      END
    ) AS CustomerName,
    in0.* EXCEPT (`Product`, 
    `Account Size`, 
    `Sector`, 
    `variableType`, 
    `Territory Name`, 
    `State`, 
    `Account Owner`, 
    `Partner Success Owner`, 
    `RevMonth`, 
    `Quantity`, 
    `ARR`, 
    `StaticHistoryMonth`, 
    `StaticHistoryYearEnd`, 
    `YetToRenewARR`, 
    `MRR`, 
    `MaxIteration`, 
    `ChangeLog`, 
    `CustomerName`),
    in1.* EXCEPT (`Loser Mas90 Customer Number`, `Winner Mas90 Customer Number`)
  
  FROM Filter_2009 AS in0
  LEFT JOIN Unique_123 AS in1
     ON (in0.CustomerName = in1.`Loser Mas90 Customer Number`)

),

Filter_2313 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Filter_2313')}}

),

Union_2011 AS (

  {{
    prophecy_basics.UnionByName(
      ['Filter_2313', 'Join_3113_left_UnionLeftOuter'], 
      [
        '[{"name": "ARR", "dataType": "Double"}, {"name": "Quantity", "dataType": "Double"}, {"name": "MRR", "dataType": "Double"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Sector", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "variableType", "dataType": "String"}]', 
        '[{"name": "Product", "dataType": "String"}, {"name": "Account Size", "dataType": "String"}, {"name": "Sector", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "MRR", "dataType": "Double"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "Output Name", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Filter_2008 AS (

  SELECT * 
  
  FROM Union_2011 AS in0
  
  WHERE TRUE

),

Union_1980 AS (

  {{
    prophecy_basics.UnionByName(
      ['Formula_2006_0', 'Filter_2008'], 
      [
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]', 
        '[{"name": "StaticHistoryYearEnd", "dataType": "Date"}, {"name": "Quantity", "dataType": "Double"}, {"name": "YetToRenewARR", "dataType": "Double"}, {"name": "Sector", "dataType": "String"}, {"name": "ChangeLog", "dataType": "String"}, {"name": "Territory Name", "dataType": "String"}, {"name": "CustomerName", "dataType": "String"}, {"name": "variableType", "dataType": "String"}, {"name": "RevMonth", "dataType": "Date"}, {"name": "Output Name", "dataType": "String"}, {"name": "MaxIteration", "dataType": "Double"}, {"name": "ARR", "dataType": "Double"}, {"name": "Account Size", "dataType": "String"}, {"name": "StaticHistoryMonth", "dataType": "Date"}, {"name": "Account Owner", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "State", "dataType": "String"}, {"name": "Partner Success Owner", "dataType": "String"}, {"name": "MRR", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

)

SELECT *

FROM Union_1980
