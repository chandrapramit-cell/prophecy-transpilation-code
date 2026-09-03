{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Summarize_156_3124 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_3124__Summarize_156_3124')}}

),

Summarize_159_3124 AS (

  SELECT 
    DISTINCT `Mas90 Customer Number` AS `Mas90 Customer Number`,
    Product AS Product,
    StartDate_Annualization AS StartDate_Annualization,
    EndDate_Annualization AS EndDate_Annualization
  
  FROM Summarize_156_3124 AS in0

),

GenerateRows_160_3124 AS (

  {{
    prophecy_basics.GenerateRows(
      ['Summarize_159_3124'], 
      '[{"name": "Mas90 Customer Number", "dataType": "String"}, {"name": "Product", "dataType": "String"}, {"name": "StartDate_Annualization", "dataType": "Date"}, {"name": "EndDate_Annualization", "dataType": "Date"}]', 
      'last_day(payload.StartDate_Annualization)', 
      '(ARRMonth <= payload.EndDate_Annualization)', 
      'last_day(add_months(ARRMonth, 1))', 
      'ARRMonth', 
      '100', 
      'recursive'
    )
  }}

),

Summarize_161_3124 AS (

  SELECT 
    DISTINCT `Mas90 Customer Number` AS `Mas90 Customer Number`,
    Product AS Product,
    ARRMonth AS ARRMonth
  
  FROM GenerateRows_160_3124 AS in0

)

SELECT *

FROM Summarize_161_3124
