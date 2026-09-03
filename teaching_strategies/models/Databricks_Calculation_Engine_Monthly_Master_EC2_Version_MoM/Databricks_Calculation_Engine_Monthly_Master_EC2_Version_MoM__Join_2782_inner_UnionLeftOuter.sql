{{
  config({    
    "materialized": "ephemeral",
    "database": "hive_metastore",
    "schema": "default"
  })
}}

WITH Formula_2790_to_Formula_2826_0 AS (

  SELECT *
  
  FROM {{ ref('Databricks_Calculation_Engine_Monthly_Master_EC2_Version_MoM__Formula_2790_to_Formula_2826_0')}}

),

Join_2782_inner_UnionLeftOuter AS (

  SELECT 
    in1.YetToRenew AS `Previous YetToRenew`,
    in0.* EXCEPT (`SubCustSeg5`, 
    `Volume`, 
    `SubCustSeg1`, 
    `Customer Active Flag`, 
    `CustomerName`, 
    `SubCustSeg6`, 
    `Customer Segment`, 
    `RevMonth`, 
    `Revenue`, 
    `SubCustSeg3`, 
    `SubCustSeg4`, 
    `SubCustSeg2`),
    in1.* EXCEPT (`YetToRenew`)
  
  FROM Formula_2790_to_Formula_2826_0 AS in0
  LEFT JOIN Formula_2790_to_Formula_2826_0 AS in1
     ON (in0.CustomerName = in1.CustomerName)

)

SELECT *

FROM Join_2782_inner_UnionLeftOuter
