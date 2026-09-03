{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Join_2621_inner AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_2621_inner')}}

),

AlteryxSelect_2960 AS (

  SELECT * EXCEPT (`CustProdKeyStr`, `YetToRenew`)
  
  FROM Join_2621_inner AS in0

)

SELECT *

FROM AlteryxSelect_2960
