{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Unique_1094 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Unique_1094')}}

),

Union_1837 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Union_1837')}}

),

AlteryxSelect_2961 AS (

  SELECT 
    `Mas90 Customer Number` AS `Account Name: Mas90 Customer Number`,
    Product AS Product,
    `Product Code` AS `Product Code`,
    `Order: Sales Order Number` AS `Order: Sales Order Number`,
    StartDate_Annualization AS StartDate_Annualization,
    EndDate_Annualization AS EndDate_Annualization,
    `Expected Renewal Date` AS `Expected Renewal Date`,
    ACV AS ACV,
    TCV AS TCV,
    Quantity AS Quantity,
    Origin AS Origin,
    Stage AS Stage
  
  FROM Union_1837 AS in0

),

Filter_2965 AS (

  SELECT * 
  
  FROM AlteryxSelect_2961 AS in0
  
  WHERE not(contains(Stage, 'Closed'))

),

Join_2963_left_UnionLeftOuter AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Mas90 Customer Number`)
  
  FROM Filter_2965 AS in0
  LEFT JOIN Unique_1094 AS in1
     ON (in0.`Account Name: Mas90 Customer Number` = in1.`Mas90 Customer Number`)

),

MultiFieldFormula_2966 AS (

  {{
    prophecy_basics.MultiColumnEdit(
      ['Join_2963_left_UnionLeftOuter'], 
      "CASE WHEN (isnull(column_value) OR (length(column_value) = 0)) THEN 'N/A' ELSE column_value END", 
      [
        'Quantity', 
        'Order: Sales Order Number', 
        'Sector', 
        'Territory Name', 
        'variableType', 
        'StartDate_Annualization', 
        'Origin', 
        'Account Name: Mas90 Customer Number', 
        'ACV', 
        'TCV', 
        'Expected Renewal Date', 
        'Product Code', 
        'Account Owner', 
        'Product', 
        'State', 
        'Stage', 
        'Partner Success Owner', 
        'EndDate_Annualization'
      ], 
      ['Sector', 'variableType', 'Territory Name', 'State', 'Account Owner', 'Partner Success Owner'], 
      false, 
      'Suffix', 
      ''
    )
  }}

)

SELECT *

FROM MultiFieldFormula_2966
