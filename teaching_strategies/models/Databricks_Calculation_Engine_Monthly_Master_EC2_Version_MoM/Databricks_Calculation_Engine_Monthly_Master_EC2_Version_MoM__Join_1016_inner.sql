{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH TextInput_596_cast AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__TextInput_596_cast')}}

),

Join_387_left_UnionLeftOuter AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Join_387_left_UnionLeftOuter')}}

),

Join_1016_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Item/Product`)
  
  FROM Join_387_left_UnionLeftOuter AS in0
  INNER JOIN TextInput_596_cast AS in1
     ON (in0.`Product Code` = in1.`Item/Product`)

)

SELECT *

FROM Join_1016_inner
