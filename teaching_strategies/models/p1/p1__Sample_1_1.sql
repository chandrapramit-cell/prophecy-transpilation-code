{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_590 AS (

  SELECT * 
  
  FROM {{ ref('seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_590')}}

),

Sample_1 AS (

  {{
    prophecy_basics.Sample(
      ['seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_590'], 
      '[{"name": "Item/Product", "dataType": "String"}, {"name": "Product", "dataType": "String"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      80, 
      []
    )
  }}

),

Sample_1_1 AS (

  {{
    prophecy_basics.Sample(
      ['Sample_1'], 
      '[{"name": "Item/Product", "dataType": "String"}, {"name": "Product", "dataType": "String"}]', 
      'sampleDataset', 
      [], 
      1002, 
      'firstN', 
      80, 
      []
    )
  }}

)

SELECT *

FROM Sample_1_1
