{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH TextInput_590 AS (

  SELECT * 
  
  FROM {{ ref('seed_Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM_590')}}

),

TextInput_590_cast AS (

  SELECT 
    CAST(`Item/Product` AS string) AS `Item/Product`,
    CAST(Product AS string) AS Product
  
  FROM TextInput_590 AS in0

),

Formula_592_0 AS (

  SELECT 
    CAST(UPPER(`Item/Product`) AS string) AS `Item/Product`,
    * EXCEPT (`item/product`)
  
  FROM TextInput_590_cast AS in0

),

Summarize_608 AS (

  SELECT 
    DISTINCT `Item/Product` AS `Item/Product`,
    Product AS Product
  
  FROM Formula_592_0 AS in0

)

SELECT *

FROM Summarize_608
